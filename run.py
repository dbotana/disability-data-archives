"""
run.py — Single-shot monitoring run with GitHub-backed storage.

This is the main entry point for both:
  - GitHub Actions (automatic daily + manual dispatch):
      python run.py --github
  - Local on-demand run (pushes results to GitHub):
      python run.py --github
  - Local run without GitHub (results in local data/ only):
      python run.py

The script:
  1. Loads config.yaml and environment variables
  2. Pulls current snapshot state from GitHub (or local data/)
  3. Fetches all configured target URLs
  4. Diffs each against its prior snapshot
  5. Records changes with NLP analysis
  6. Commits updated CSV files back to GitHub (if --github)
  7. Writes a run summary to data/run_summary.md (shown in Actions UI)
  8. Exits cleanly (non-zero exit code if any CRITICAL changes detected)

Usage:
  python run.py                      # Local run, save to data/ only
  python run.py --github             # Pull from / push to GitHub
  python run.py --frequency high     # Only check high-frequency targets
  python run.py --frequency all      # Check all targets (default)
  python run.py --dry-run            # Fetch and diff but do not save/commit
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging setup (minimal — full structured logging is for the daemon)
# ---------------------------------------------------------------------------

_LOG_DIR = Path("logs")
_LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(_LOG_DIR / "monitor.log", mode="a", encoding="utf-8"),
    ],
)
# Suppress noisy third-party loggers
for noisy in ("httpx", "httpcore", "urllib3", "sentence_transformers"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loading (reuse from main.py pattern)
# ---------------------------------------------------------------------------


def _expand_env(obj: Any) -> Any:
    import re
    if isinstance(obj, str):
        return re.sub(r"\$\{([^}]+)\}", lambda m: os.environ.get(m.group(1), m.group(0)), obj)
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    return obj


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return _expand_env(yaml.safe_load(f))


# ---------------------------------------------------------------------------
# Main async run logic
# ---------------------------------------------------------------------------


async def run(
    config_path: str = "config.yaml",
    use_github: bool = False,
    frequency_filter: str = "all",
    dry_run: bool = False,
) -> int:
    """
    Execute one full monitoring cycle.

    Returns the count of CRITICAL changes detected (used as exit code signal).
    """
    cfg = load_config(config_path)

    # Ensure runtime dirs exist
    Path("logs").mkdir(exist_ok=True)
    Path("data/digests").mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Determine GitHub repo info
    # ------------------------------------------------------------------
    github_repo = os.environ.get("GITHUB_REPOSITORY", "")
    github_branch = os.environ.get("GITHUB_REF_NAME", "main")
    github_token = os.environ.get("GITHUB_TOKEN", "")

    if use_github and not github_repo:
        # Try to detect from git remote
        try:
            import subprocess
            result = subprocess.run(
                ["git", "remote", "get-url", "origin"],
                capture_output=True, text=True, timeout=5
            )
            remote = result.stdout.strip()
            # Parse github.com/owner/repo or git@github.com:owner/repo
            import re
            m = re.search(r"github\.com[/:](.+/.+?)(?:\.git)?$", remote)
            if m:
                github_repo = m.group(1)
                logger.info(f"Detected GitHub repo: {github_repo}")
        except Exception:
            pass

    if use_github and not github_repo:
        logger.error(
            "Cannot determine GitHub repo. Set GITHUB_REPOSITORY env var "
            "or run from within a GitHub Actions workflow."
        )
        sys.exit(1)

    # ------------------------------------------------------------------
    # Initialize storage backend
    # ------------------------------------------------------------------
    if use_github:
        from github_storage import GitHubStorageBackend
        storage = GitHubStorageBackend(
            repo=github_repo,
            branch=github_branch,
            token=github_token,
            data_dir=cfg.get("system", {}).get("data_dir", "data"),
        )
        logger.info(f"Loading current state from GitHub ({github_repo}@{github_branch})")
        await storage.load()
    else:
        storage = LocalFileStorage(data_dir=cfg.get("system", {}).get("data_dir", "data"))
        storage.load()

    # ------------------------------------------------------------------
    # Build targets list from config
    # ------------------------------------------------------------------
    from storage import TargetModel

    all_targets: list[TargetModel] = []
    for t in (cfg.get("targets") or []):
        try:
            all_targets.append(TargetModel(
                id=t["id"], url=t["url"], agency=t["agency"],
                name=t["name"], frequency=t["frequency"],
                type=t.get("type", "html"),
                tags=t.get("tags", []),
                row_count_check=t.get("row_count_check", False),
            ))
        except Exception as e:
            logger.warning(f"Skipping invalid target {t.get('id', '?')}: {e}")

    # Apply frequency filter
    if frequency_filter != "all":
        all_targets = [t for t in all_targets if t.frequency == frequency_filter]

    logger.info(f"Checking {len(all_targets)} targets (filter: {frequency_filter})")

    # ------------------------------------------------------------------
    # Initialize crawler
    # ------------------------------------------------------------------
    from crawler import Crawler, CrawlerConfig

    crawl_cfg_raw = cfg.get("crawling", {})
    crawler_config = CrawlerConfig(
        min_delay_seconds=float(crawl_cfg_raw.get("min_delay_seconds", 2.0)),
        max_concurrent_requests=int(crawl_cfg_raw.get("max_concurrent_requests", 5)),
        request_timeout_seconds=int(crawl_cfg_raw.get("request_timeout_seconds", 30)),
        max_retries=int(crawl_cfg_raw.get("max_retries", 3)),
        user_agents=list(crawl_cfg_raw.get("user_agents", ["FederalDataMonitor/1.0"])),
    )

    # ------------------------------------------------------------------
    # Initialize NLP
    # ------------------------------------------------------------------
    from semantic_analyzer import NLPConfig, SemanticAnalyzer

    nlp_raw = cfg.get("nlp", {})
    nlp_config = NLPConfig(
        embedding_model=nlp_raw.get("embedding_model", "all-MiniLM-L6-v2"),
        zero_shot_enabled=bool(nlp_raw.get("zero_shot_enabled", False)),
        device=nlp_raw.get("device", "cpu"),
        cache_dir=nlp_raw.get("cache_dir", "models/"),
        disability_keywords=list(nlp_raw.get("disability_keywords", [])),
    )
    semantic = SemanticAnalyzer(nlp_config)
    await semantic.load_models()

    # ------------------------------------------------------------------
    # Initialize Wayback client
    # ------------------------------------------------------------------
    from wayback import WaybackClient, WaybackConfig

    wb_raw = cfg.get("wayback", {})
    wb_config = WaybackConfig(
        enabled=bool(wb_raw.get("enabled", True)),
        submit_on_critical=bool(wb_raw.get("submit_on_critical", True)),
        access_key=wb_raw.get("access_key") or None,
        secret_key=wb_raw.get("secret_key") or None,
    )

    # ------------------------------------------------------------------
    # Initialize diff engine
    # ------------------------------------------------------------------
    from diff_engine import DiffEngine
    diff_engine = DiffEngine()

    # ------------------------------------------------------------------
    # Run checks
    # ------------------------------------------------------------------
    run_start = datetime.now(timezone.utc)
    change_count = 0
    critical_count = 0
    high_count = 0
    error_count = 0
    change_id_counter = len(storage.get_all_changes()) + 1

    # Build lookup maps
    target_map = {t.id: t for t in all_targets}

    async with Crawler(crawler_config) as crawler:
        async with WaybackClient(wb_config) as wayback:
            # Fetch all targets concurrently
            logger.info("Fetching all targets...")
            results = await crawler.fetch_all(all_targets)
            logger.info(f"Fetched {len(results)} targets")

            for result, target in zip(results, all_targets):
                try:
                    # CDX proxy fallback: if a target is blocked (403/0) and has
                    # cdx_proxy: true in config, use the Wayback CDX API instead
                    # of recording an error.  CDX returns a sha1 digest of each
                    # archived snapshot — a change in digest means content changed.
                    target_cfg = next(
                        (t for t in (cfg.get("targets") or []) if t.get("id") == target.id),
                        {}
                    )
                    if result.http_status in (403, 0) and target_cfg.get("cdx_proxy"):
                        try:
                            history = await wayback.get_snapshot_history(target.url, limit=3)
                            if history:
                                latest = history[0]
                                cdx_digest = latest.get("digest", "")
                                cdx_ts = latest.get("timestamp", "")
                                import hashlib as _hashlib
                                # Convert CDX sha1 to a consistent pseudo sha256 so the
                                # diff engine can compare across runs using body_hash_sha256
                                pseudo_hash = _hashlib.sha256(cdx_digest.encode()).hexdigest()
                                result = result.model_copy(update=dict(
                                    http_status=200,
                                    body_hash_sha256=pseudo_hash,
                                    text_hash_sha256=pseudo_hash,
                                    text_content=f"[CDX proxy] digest={cdx_digest} ts={cdx_ts}",
                                    error_message=None,
                                ))
                                logger.info(
                                    f"CDX proxy used for {target.id}: "
                                    f"digest={cdx_digest} ts={cdx_ts}"
                                )
                        except Exception as cdx_err:
                            logger.warning(f"CDX proxy failed for {target.id}: {cdx_err}")

                    is_error = result.http_status == 0
                    if is_error:
                        error_count += 1

                    # Get prior snapshot from storage backend
                    prior_row = storage.get_latest_snapshot(target.id)
                    prior = None
                    if prior_row is not None:
                        from github_storage import snapshot_row_to_snapshot_model
                        prior = snapshot_row_to_snapshot_model(prior_row)

                    # Detect change
                    change = None
                    if prior is not None and not is_error:
                        change = diff_engine.compare(prior, result)
                    elif prior is None and not is_error:
                        logger.info(f"First snapshot for {target.id} (baseline)")

                    # Update snapshot record
                    from github_storage import fetch_result_to_snapshot_row
                    consecutive_errors = int(
                        (prior_row or {}).get("consecutive_errors", 0)
                    )
                    consecutive_errors = consecutive_errors + 1 if is_error else 0

                    snapshot_row = fetch_result_to_snapshot_row(
                        result=result,
                        target_name=target.name,
                        target_agency=target.agency,
                        target_url=target.url,
                        consecutive_errors=consecutive_errors,
                    )

                    if not dry_run:
                        storage.record_snapshot(snapshot_row)

                    if change is not None:
                        # Semantic analysis
                        prior_text = ""
                        if prior_row:
                            # Text content not stored in snapshots.csv to save space —
                            # similarity unavailable between runs, but keyword delta works
                            prior_text = ""
                        after_text = result.text_content or ""

                        analysis = await semantic.analyze_change(
                            text_before=prior_text,
                            text_after=after_text,
                            diff_text=change.diff_text or "",
                        )
                        change.semantic_similarity = analysis.get("semantic_similarity")
                        change.semantic_labels = analysis.get("semantic_labels", [])
                        keywords_removed = analysis.get("keywords_removed", [])

                        # Build change row
                        from github_storage import change_to_row
                        change_row = change_to_row(
                            change=change,
                            target_name=target.name,
                            target_agency=target.agency,
                            target_url=target.url,
                            change_id=change_id_counter,
                        )
                        change_row["keywords_removed"] = "|".join(keywords_removed)
                        change_id_counter += 1
                        change_count += 1

                        if change.severity == "CRITICAL":
                            critical_count += 1
                        elif change.severity == "HIGH":
                            high_count += 1

                        if not dry_run:
                            storage.record_change(change_row)

                        # Wayback submission for CRITICAL
                        if change.severity == "CRITICAL" and not dry_run:
                            logger.info(f"Submitting CRITICAL change to Wayback: {target.url}")
                            await wayback.submit_url(target.url)

                        _log_change(change_row)

                except Exception as e:
                    logger.error(f"Error processing {target.id}: {e}", exc_info=True)
                    error_count += 1

    # ------------------------------------------------------------------
    # Commit to GitHub
    # ------------------------------------------------------------------
    run_duration = (datetime.now(timezone.utc) - run_start).total_seconds()
    run_summary = _build_run_summary(
        total=len(all_targets),
        changes=change_count,
        critical=critical_count,
        high=high_count,
        errors=error_count,
        duration=run_duration,
        new_changes=storage.new_changes if hasattr(storage, "new_changes") else [],
    )

    # Save summary for GitHub Actions step summary
    Path("data/run_summary.md").write_text(run_summary, encoding="utf-8")

    if not dry_run:
        if use_github:
            logger.info("Committing results to GitHub...")
            success = await storage.commit_results(run_summary)
            if not success:
                logger.error("GitHub commit failed")
                sys.exit(2)

            # Also commit digest
            date_str = run_start.strftime("%Y-%m-%d")
            digest_md = _build_digest_markdown(
                storage.new_changes if hasattr(storage, "new_changes") else [],
                date_str=date_str,
                total_targets=len(all_targets),
            )
            await storage.commit_digest(date_str, digest_md, html="")
        else:
            # Local: write CSV files to data/
            storage.save_local()

    logger.info(
        f"Run complete: {change_count} changes ({critical_count} CRITICAL, {high_count} HIGH), "
        f"{error_count} errors, {run_duration:.0f}s"
    )

    semantic.shutdown()
    return critical_count


# ---------------------------------------------------------------------------
# Local file storage fallback (no GitHub)
# ---------------------------------------------------------------------------


class LocalFileStorage:
    """Minimal local-only storage backend mirroring GitHubStorageBackend's API."""

    def __init__(self, data_dir: str = "data") -> None:
        self._data_dir = Path(data_dir)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._snapshots: dict[str, dict] = {}
        self._changes: list[dict] = []
        self._new_changes: list[dict] = []

    def load(self) -> None:
        from github_storage import CHANGES_COLUMNS, SNAPSHOTS_COLUMNS

        changes_path = self._data_dir / "changes.csv"
        if changes_path.exists():
            with open(changes_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                self._changes = list(reader)

        snapshots_path = self._data_dir / "snapshots.csv"
        if snapshots_path.exists():
            with open(snapshots_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                self._snapshots = {r["target_id"]: r for r in reader}

        logger.info(
            f"Loaded {len(self._changes)} changes, "
            f"{len(self._snapshots)} snapshots from local data/"
        )

    def get_latest_snapshot(self, target_id: str) -> Optional[dict]:
        return self._snapshots.get(target_id)

    def get_all_changes(self) -> list[dict]:
        return list(self._changes)

    def record_snapshot(self, row: dict) -> None:
        self._snapshots[row["target_id"]] = row

    def record_change(self, row: dict) -> None:
        self._new_changes.append(row)
        self._changes.append(row)

    def save_local(self) -> None:
        from github_storage import CHANGES_COLUMNS, SNAPSHOTS_COLUMNS, _render_csv

        (self._data_dir / "changes.csv").write_text(
            _render_csv(self._changes, CHANGES_COLUMNS), encoding="utf-8"
        )
        (self._data_dir / "snapshots.csv").write_text(
            _render_csv(list(self._snapshots.values()), SNAPSHOTS_COLUMNS), encoding="utf-8"
        )
        logger.info(f"Saved results to {self._data_dir}/")

    @property
    def new_changes(self) -> list[dict]:
        return list(self._new_changes)


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------


def _log_change(row: dict) -> None:
    logger.info(
        f"  [{row['severity']}] {row['target_name']} ({row['target_agency']}) — "
        f"{row['change_type']}"
        + (f", {row['pct_text_changed']}% text" if row.get("pct_text_changed") else "")
    )


def _build_run_summary(
    total: int,
    changes: int,
    critical: int,
    high: int,
    errors: int,
    duration: float,
    new_changes: list[dict],
) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"## Monitoring Run — {now}",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Targets checked | {total} |",
        f"| Changes detected | {changes} |",
        f"| CRITICAL | {critical} |",
        f"| HIGH | {high} |",
        f"| Fetch errors | {errors} |",
        f"| Duration | {duration:.0f}s |",
        "",
    ]
    if new_changes:
        lines.append("### Changes Detected")
        lines.append("")
        for c in new_changes:
            lines.append(
                f"- **[{c.get('severity')}]** {c.get('target_name')} "
                f"({c.get('target_agency')}) — `{c.get('change_type')}`"
            )
        lines.append("")
    else:
        lines.append("*No changes detected in this run.*")
        lines.append("")
    return "\n".join(lines)


def _build_digest_markdown(
    new_changes: list[dict],
    date_str: str,
    total_targets: int,
) -> str:
    critical = [c for c in new_changes if c.get("severity") == "CRITICAL"]
    high = [c for c in new_changes if c.get("severity") == "HIGH"]
    medium = [c for c in new_changes if c.get("severity") == "MEDIUM"]

    lines = [
        f"# Federal Disability Data Monitor — {date_str}",
        "",
        "> Non-partisan public interest research tool. Records factual changes to",
        "> publicly available federal data without editorial commentary.",
        "",
        f"**Targets monitored:** {total_targets}  ",
        f"**Changes detected:** {len(new_changes)} "
        f"({len(critical)} CRITICAL, {len(high)} HIGH, {len(medium)} MEDIUM)",
        "",
    ]

    for sev, group in [("CRITICAL", critical), ("HIGH", high), ("MEDIUM", medium)]:
        if not group:
            continue
        lines.append(f"## {sev} Changes")
        lines.append("")
        for c in group:
            lines.append(f"### {c.get('target_name')} ({c.get('target_agency')})")
            lines.append(f"- **URL:** {c.get('target_url')}")
            lines.append(f"- **Type:** `{c.get('change_type')}`")
            if c.get("pct_text_changed"):
                lines.append(f"- **Content changed:** {c.get('pct_text_changed')}%")
            if c.get("keywords_removed"):
                kws = c.get("keywords_removed", "").replace("|", ", ")
                lines.append(f"- **Keywords removed:** {kws}")
            if c.get("diff_text_preview"):
                lines.append("")
                lines.append("<details><summary>Diff preview</summary>")
                lines.append("")
                lines.append("```diff")
                lines.append(c.get("diff_text_preview", ""))
                lines.append("```")
                lines.append("</details>")
            lines.append("")

    if not new_changes:
        lines.append("*No changes detected.*")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Federal Disability Data Monitor — single monitoring run"
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument(
        "--github", action="store_true",
        help="Use GitHub as storage backend (pull prior state + commit results)"
    )
    parser.add_argument(
        "--frequency", default="all",
        choices=["all", "high", "medium", "low"],
        help="Only check targets with this frequency (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and diff but do not save or commit any results"
    )
    args = parser.parse_args()

    # Allow MONITOR_FREQUENCY env var override (set by GitHub Actions workflow)
    frequency = os.environ.get("MONITOR_FREQUENCY", args.frequency)

    critical_count = asyncio.run(
        run(
            config_path=args.config,
            use_github=args.github,
            frequency_filter=frequency,
            dry_run=args.dry_run,
        )
    )

    # Exit code 0 = success (even if changes found); non-zero only on fatal error
    sys.exit(0)


if __name__ == "__main__":
    main()
