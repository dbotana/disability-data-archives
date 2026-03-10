"""
scheduler.py — APScheduler AsyncIOScheduler job definitions and lifecycle management.

Job groups:
  high_freq   — every 6 hours: SSA, Regulations.gov, SODA APIs
  medium_freq — every 24 hours: CDC, Census, NIH, ACL, DOL, ED HTML pages
  low_freq    — weekly: ADA.gov, deep diffs, Wayback comparisons
  daily_digest— cron at 08:00: email/disk digest report
  wayback_verify — daily: verify pending Wayback submissions via CDX API

All jobs use max_instances=1 + coalesce=True to prevent pile-up.
All job bodies are wrapped in try/except to prevent silent scheduler death.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, JobExecutionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from crawler import Crawler, CrawlerConfig
from diff_engine import DiffEngine
from reporter import Reporter
from semantic_analyzer import SemanticAnalyzer
from storage import ChangeModel, StorageManager, TargetModel
from wayback import WaybackClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Scheduling configuration (populated from config.yaml)
# ---------------------------------------------------------------------------


class ScheduleConfig:
    def __init__(
        self,
        high_frequency_interval_hours: int = 6,
        medium_frequency_interval_hours: int = 24,
        low_frequency_interval_days: int = 7,
        daily_digest_hour: int = 8,
        daily_digest_minute: int = 0,
        timezone: str = "America/New_York",
    ) -> None:
        self.high_hours = high_frequency_interval_hours
        self.medium_hours = medium_frequency_interval_hours
        self.low_days = low_frequency_interval_days
        self.digest_hour = daily_digest_hour
        self.digest_minute = daily_digest_minute
        self.timezone = timezone


# ---------------------------------------------------------------------------
# MonitorScheduler
# ---------------------------------------------------------------------------


class MonitorScheduler:
    """Orchestrates all scheduled monitoring jobs using APScheduler."""

    def __init__(
        self,
        schedule_config: ScheduleConfig,
        storage: StorageManager,
        crawler: Crawler,
        diff_engine: DiffEngine,
        semantic_analyzer: SemanticAnalyzer,
        wayback: WaybackClient,
        reporter: Reporter,
        nlp_keywords: Optional[list[str]] = None,
    ) -> None:
        self._schedule = schedule_config
        self._storage = storage
        self._crawler = crawler
        self._diff_engine = diff_engine
        self._semantic = semantic_analyzer
        self._wayback = wayback
        self._reporter = reporter
        self._keywords = nlp_keywords or []

        self.scheduler = AsyncIOScheduler(timezone=schedule_config.timezone)
        self._setup_event_listeners()

    def setup_jobs(self) -> None:
        """Register all periodic jobs with the scheduler."""

        # High frequency: every N hours
        self.scheduler.add_job(
            self._run_high_freq,
            trigger="interval",
            hours=self._schedule.high_hours,
            id="high_freq_check",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )

        # Medium frequency: every N hours
        self.scheduler.add_job(
            self._run_medium_freq,
            trigger="interval",
            hours=self._schedule.medium_hours,
            id="medium_freq_check",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=7200,
        )

        # Low frequency: weekly
        self.scheduler.add_job(
            self._run_low_freq,
            trigger="interval",
            days=self._schedule.low_days,
            id="low_freq_check",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=86400,
        )

        # Daily digest at configured time
        self.scheduler.add_job(
            self._run_daily_digest,
            trigger="cron",
            hour=self._schedule.digest_hour,
            minute=self._schedule.digest_minute,
            id="daily_digest",
            max_instances=1,
            coalesce=True,
        )

        # Wayback CDX verification — daily
        self.scheduler.add_job(
            self._run_wayback_verification,
            trigger="interval",
            hours=24,
            id="wayback_verify",
            max_instances=1,
            coalesce=True,
        )

        # Also run each group immediately on first start (jitter=60 so they don't all fire at once)
        self.scheduler.add_job(
            self._run_high_freq,
            trigger="date",
            run_date=None,  # "now" — APScheduler will run at next tick
            id="high_freq_initial",
        )

        logger.info(
            f"Scheduled jobs: high={self._schedule.high_hours}h, "
            f"medium={self._schedule.medium_hours}h, "
            f"low={self._schedule.low_days}d"
        )

    def start(self) -> None:
        """Start the scheduler. Must be called inside a running event loop."""
        self.scheduler.start()
        logger.info("MonitorScheduler started")

    def shutdown(self, wait: bool = False) -> None:
        """Gracefully shut down the scheduler."""
        self.scheduler.shutdown(wait=wait)
        logger.info("MonitorScheduler shut down")

    # ------------------------------------------------------------------
    # Job implementations
    # ------------------------------------------------------------------

    async def _run_high_freq(self) -> None:
        """Run all high-frequency monitoring targets."""
        try:
            targets = await self._storage.get_targets_by_frequency("high")
            logger.info(f"High-freq check: {len(targets)} targets")
            await self._check_targets(targets)
        except Exception as e:
            logger.error(f"High-freq job failed: {e}", exc_info=True)

    async def _run_medium_freq(self) -> None:
        """Run all medium-frequency monitoring targets."""
        try:
            targets = await self._storage.get_targets_by_frequency("medium")
            logger.info(f"Medium-freq check: {len(targets)} targets")
            await self._check_targets(targets)
        except Exception as e:
            logger.error(f"Medium-freq job failed: {e}", exc_info=True)

    async def _run_low_freq(self) -> None:
        """Run all low-frequency (weekly) monitoring targets + Wayback comparisons."""
        try:
            targets = await self._storage.get_targets_by_frequency("low")
            logger.info(f"Low-freq (weekly) check: {len(targets)} targets")
            await self._check_targets(targets)
        except Exception as e:
            logger.error(f"Low-freq job failed: {e}", exc_info=True)

    async def _run_daily_digest(self) -> None:
        """Generate and dispatch the daily digest report."""
        try:
            logger.info("Generating daily digest")
            await self._reporter.send_daily_digest()
        except Exception as e:
            logger.error(f"Daily digest job failed: {e}", exc_info=True)

    async def _run_wayback_verification(self) -> None:
        """Verify pending Wayback submissions via CDX API."""
        try:
            pending = await self._storage.get_pending_wayback_verifications()
            logger.info(f"Verifying {len(pending)} Wayback submissions")
            for sub in pending:
                url = sub.get("url", "")
                if not url:
                    continue
                verified = await self._wayback.verify_capture(url)
                if verified:
                    await self._storage.mark_wayback_verified(sub["submission_id"])
                    logger.info(f"CDX verified: {url}")
        except Exception as e:
            logger.error(f"Wayback verification job failed: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Core check pipeline
    # ------------------------------------------------------------------

    async def _check_targets(self, targets: list[TargetModel]) -> None:
        """
        For each target:
          1. Fetch current state
          2. Compare to prior snapshot
          3. If changed: run NLP, save change, trigger Wayback if CRITICAL, send alert
          4. Save new snapshot
        """
        if not targets:
            return

        results = await self._crawler.fetch_all(targets)
        loop = asyncio.get_event_loop()

        # Build a lookup of agency by target id for archive path
        agency_map = {t.id: t.agency for t in targets}

        for result, target in zip(results, targets):
            try:
                agency = agency_map.get(result.target_id, "unknown")
                is_error = result.http_status == 0

                # Mark checked (with error flag if fetch failed)
                await self._storage.mark_target_checked(result.target_id, error=is_error)

                # Retrieve prior snapshot
                prior = await self._storage.get_latest_snapshot(result.target_id)

                change: Optional[ChangeModel] = None

                if prior is not None and not is_error:
                    # Diff against prior snapshot
                    change = self._diff_engine.compare(prior, result)

                elif prior is None and not is_error:
                    # First snapshot — establish baseline, no change to report
                    logger.info(f"First snapshot for {result.target_id} (baseline)")

                # Save new snapshot
                snapshot_id = await self._storage.save_snapshot(result, agency=agency)

                if change is not None:
                    # Update snapshot_after now that we have the id
                    change.snapshot_after = snapshot_id

                    # Run semantic analysis in thread pool
                    if prior and prior.text_content and result.text_content:
                        analysis = await self._semantic.analyze_change(
                            text_before=prior.text_content,
                            text_after=result.text_content,
                            diff_text=change.diff_text or "",
                        )
                        change.semantic_similarity = analysis.get("semantic_similarity")
                        change.semantic_labels = analysis.get("semantic_labels", [])
                        if analysis.get("keywords_removed"):
                            kw_note = f"Disability keywords removed: {', '.join(analysis['keywords_removed'])}"
                            change.notes = (
                                f"{change.notes}\n{kw_note}" if change.notes else kw_note
                            )

                    # Save change to DB
                    change_id = await self._storage.save_change(change)
                    change.id = change_id

                    # Enrich change data for alert dispatch
                    change_data = {
                        "change_id": change_id,
                        "target_id": change.target_id,
                        "target_name": target.name,
                        "target_agency": target.agency,
                        "target_url": target.url,
                        "detected_at": change.detected_at,
                        "severity": change.severity,
                        "change_type": change.change_type,
                        "pct_text_changed": change.pct_text_changed,
                        "semantic_similarity": change.semantic_similarity,
                        "semantic_labels": change.semantic_labels,
                        "diff_text": change.diff_text,
                    }

                    # Trigger Wayback Save for CRITICAL changes
                    if change.severity == "CRITICAL":
                        wb_result = await self._wayback.submit_url(target.url)
                        if wb_result.get("status") == "submitted":
                            await self._storage.save_wayback_submission(
                                snapshot_id=snapshot_id,
                                wayback_url=wb_result.get("wayback_url"),
                                status="submitted",
                            )

                    # Dispatch alerts
                    await self._reporter.send_alert(change_data)

            except Exception as e:
                logger.error(
                    f"Error processing target {getattr(result, 'target_id', 'unknown')}: {e}",
                    exc_info=True,
                )

    # ------------------------------------------------------------------
    # APScheduler event listeners
    # ------------------------------------------------------------------

    def _setup_event_listeners(self) -> None:
        self.scheduler.add_listener(self._on_job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)

    def _on_job_executed(self, event: JobExecutionEvent) -> None:
        logger.debug(f"Job executed: {event.job_id} at {datetime.now(timezone.utc).isoformat()}")

    def _on_job_error(self, event: JobExecutionEvent) -> None:
        logger.error(
            f"Job {event.job_id} raised an exception: {event.exception}",
            exc_info=event.traceback,
        )
