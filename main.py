"""
main.py — Federal Disability Data Monitor daemon entry point.

Starts the APScheduler (background jobs) and FastAPI dashboard (uvicorn)
in the same asyncio event loop. Handles SIGTERM/SIGINT gracefully.

For the recommended GitHub-backed on-demand mode, use run.py instead:
  python run.py --github          # single run, commits results to GitHub

Usage:
  python main.py                  # Persistent daemon with local SQLite DB
  python main.py --config /path/to/config.yaml
  python main.py --once           # Run one check cycle then exit (testing)
  python main.py --dashboard-only # Serve dashboard reading from GitHub data/
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import logging.config
import os
import signal
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import uvicorn
import yaml
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env first so environment variable overrides work during config parse
# ---------------------------------------------------------------------------
load_dotenv()


# ---------------------------------------------------------------------------
# Structured JSON logging setup
# ---------------------------------------------------------------------------


def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> None:
    """Configure structured JSON logging via pythonjsonlogger."""
    handlers: dict[str, Any] = {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "json",
        }
    }

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": log_file,
            "maxBytes": 10_485_760,  # 10 MB
            "backupCount": 5,
            "formatter": "json",
        }

    try:
        from pythonjsonlogger import jsonlogger  # type: ignore

        formatter = "json"
        formatters = {
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            }
        }
    except ImportError:
        # Fallback to plain text if pythonjsonlogger not installed
        formatter = "plain"
        formatters = {
            "plain": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "json": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            },
        }

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": formatters,
            "handlers": handlers,
            "root": {
                "level": log_level.upper(),
                "handlers": list(handlers.keys()),
            },
            "loggers": {
                "apscheduler": {"level": "WARNING"},
                "httpx": {"level": "WARNING"},
                "httpcore": {"level": "WARNING"},
                "uvicorn.access": {"level": "WARNING"},
            },
        }
    )


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loading (expand ${ENV_VAR} placeholders)
# ---------------------------------------------------------------------------


def _expand_env(obj: Any) -> Any:
    """Recursively expand ${VAR} placeholders in config strings."""
    if isinstance(obj, str):
        import re
        return re.sub(
            r"\$\{([^}]+)\}",
            lambda m: os.environ.get(m.group(1), m.group(0)),
            obj,
        )
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    return obj


def load_config(config_path: str) -> dict:
    """Load and validate config.yaml, expanding environment variable references."""
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return _expand_env(raw)


# ---------------------------------------------------------------------------
# Simple config accessor dataclasses (wraps dict for attribute access)
# ---------------------------------------------------------------------------


class ConfigNamespace:
    """Thin wrapper turning a dict into attribute access."""

    def __init__(self, d: dict) -> None:
        for k, v in d.items():
            if isinstance(v, dict):
                setattr(self, k, ConfigNamespace(v))
            elif isinstance(v, list) and v and isinstance(v[0], dict) and "id" in v[0]:
                # list of targets — keep as list of dicts
                setattr(self, k, v)
            else:
                setattr(self, k, v)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)


# ---------------------------------------------------------------------------
# Build all components from config
# ---------------------------------------------------------------------------


async def build_components(cfg: ConfigNamespace):
    """Instantiate and wire all system components."""
    from crawler import Crawler, CrawlerConfig
    from diff_engine import DiffEngine
    from reporter import AlertsConfig, EmailConfig, Reporter, RSSConfig, SlackConfig
    from scheduler import MonitorScheduler, ScheduleConfig
    from semantic_analyzer import NLPConfig, SemanticAnalyzer
    from storage import StorageManager, TargetModel, create_db_engine
    from wayback import WaybackClient, WaybackConfig

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------
    db_path = cfg.system.get("db_path", "data/monitoring.db")
    archive_dir = cfg.system.get("archive_dir", "archive")
    engine = create_db_engine(db_path)
    storage = StorageManager(engine, archive_dir=archive_dir)
    await storage.init_db()

    # ------------------------------------------------------------------
    # Sync targets from config
    # ------------------------------------------------------------------
    raw_targets = cfg.get("targets") or []
    targets = []
    for t in raw_targets:
        try:
            targets.append(TargetModel(
                id=t["id"],
                url=t["url"],
                agency=t["agency"],
                name=t["name"],
                frequency=t["frequency"],
                type=t.get("type", "html"),
                tags=t.get("tags", []),
                row_count_check=t.get("row_count_check", False),
            ))
        except Exception as e:
            logger.warning(f"Skipping invalid target {t.get('id', '?')}: {e}")

    synced = await storage.sync_targets(targets)
    logger.info(f"Synced {synced} targets from config")

    # ------------------------------------------------------------------
    # Crawler
    # ------------------------------------------------------------------
    crawl_cfg = cfg.get("crawling") or {}
    crawler_config = CrawlerConfig(
        min_delay_seconds=float(getattr(crawl_cfg, "min_delay_seconds", 2.0)),
        max_concurrent_requests=int(getattr(crawl_cfg, "max_concurrent_requests", 5)),
        request_timeout_seconds=int(getattr(crawl_cfg, "request_timeout_seconds", 30)),
        max_retries=int(getattr(crawl_cfg, "max_retries", 3)),
        retry_backoff_multiplier=float(getattr(crawl_cfg, "retry_backoff_multiplier", 2.0)),
        user_agents=list(getattr(crawl_cfg, "user_agents", ["FederalDataMonitor/1.0"])),
    )
    crawler = await Crawler(crawler_config).__aenter__()

    # ------------------------------------------------------------------
    # NLP / Semantic Analyzer
    # ------------------------------------------------------------------
    nlp_raw = cfg.get("nlp") or {}
    nlp_config = NLPConfig(
        embedding_model=getattr(nlp_raw, "embedding_model", "all-MiniLM-L6-v2"),
        zero_shot_model=getattr(nlp_raw, "zero_shot_model", "facebook/bart-large-mnli"),
        zero_shot_enabled=bool(getattr(nlp_raw, "zero_shot_enabled", False)),
        device=getattr(nlp_raw, "device", "cpu"),
        cache_dir=getattr(nlp_raw, "cache_dir", "models/"),
        disability_keywords=list(getattr(nlp_raw, "disability_keywords", [])),
        zero_shot_candidate_labels=list(getattr(nlp_raw, "zero_shot_candidate_labels", [])),
    )
    semantic = SemanticAnalyzer(nlp_config)
    # Load models in background thread (non-blocking)
    await semantic.load_models()

    # ------------------------------------------------------------------
    # Wayback
    # ------------------------------------------------------------------
    wb_raw = cfg.get("wayback") or {}
    wb_config = WaybackConfig(
        enabled=bool(getattr(wb_raw, "enabled", True)),
        submit_on_critical=bool(getattr(wb_raw, "submit_on_critical", True)),
        access_key=getattr(wb_raw, "access_key", None) or None,
        secret_key=getattr(wb_raw, "secret_key", None) or None,
        submit_rate_limit_per_minute=int(getattr(wb_raw, "submit_rate_limit_per_minute", 10)),
    )
    wayback = await WaybackClient(wb_config).__aenter__()

    # ------------------------------------------------------------------
    # Reporter
    # ------------------------------------------------------------------
    alerts_raw = cfg.get("alerts") or {}

    email_raw = getattr(alerts_raw, "email", {}) or {}
    email_cfg = EmailConfig(
        enabled=bool(getattr(email_raw, "enabled", False)),
        smtp_host=getattr(email_raw, "smtp_host", "smtp.gmail.com"),
        smtp_port=int(getattr(email_raw, "smtp_port", 587)),
        use_tls=bool(getattr(email_raw, "use_tls", True)),
        smtp_user=getattr(email_raw, "smtp_user", ""),
        smtp_password=getattr(email_raw, "smtp_password", ""),
        from_address=getattr(email_raw, "from_address", ""),
        to_addresses=list(getattr(email_raw, "to_addresses", [])),
        min_severity=getattr(email_raw, "min_severity", "HIGH"),
    )

    slack_raw = getattr(alerts_raw, "slack", {}) or {}
    slack_cfg = SlackConfig(
        enabled=bool(getattr(slack_raw, "enabled", False)),
        webhook_url=getattr(slack_raw, "webhook_url", ""),
        channel=getattr(slack_raw, "channel", "#alerts"),
        min_severity=getattr(slack_raw, "min_severity", "HIGH"),
    )

    rss_raw = getattr(alerts_raw, "rss", {}) or {}
    rss_cfg = RSSConfig(
        enabled=bool(getattr(rss_raw, "enabled", True)),
        output_path=getattr(rss_raw, "output_path", "data/feed.xml"),
        feed_title=getattr(rss_raw, "feed_title", "Federal Disability Data Monitor"),
        feed_description=getattr(rss_raw, "feed_description", ""),
        feed_link=getattr(rss_raw, "feed_link", "http://localhost:8000"),
        max_items=int(getattr(rss_raw, "max_items", 200)),
    )

    from reporter import AlertsConfig
    alerts_config = AlertsConfig(email=email_cfg, slack=slack_cfg, rss=rss_cfg)
    reporter = await Reporter(alerts_config, storage).__aenter__()

    # ------------------------------------------------------------------
    # Scheduler
    # ------------------------------------------------------------------
    sched_raw = cfg.get("scheduling") or {}
    tz = getattr(cfg.system, "timezone", "America/New_York")
    schedule_config = ScheduleConfig(
        high_frequency_interval_hours=int(getattr(sched_raw, "high_frequency_interval_hours", 6)),
        medium_frequency_interval_hours=int(getattr(sched_raw, "medium_frequency_interval_hours", 24)),
        low_frequency_interval_days=int(getattr(sched_raw, "low_frequency_interval_days", 7)),
        daily_digest_hour=int(getattr(sched_raw, "daily_digest_hour", 8)),
        daily_digest_minute=int(getattr(sched_raw, "daily_digest_minute", 0)),
        timezone=tz,
    )

    scheduler = MonitorScheduler(
        schedule_config=schedule_config,
        storage=storage,
        crawler=crawler,
        diff_engine=DiffEngine(),
        semantic_analyzer=semantic,
        wayback=wayback,
        reporter=reporter,
        nlp_keywords=nlp_config.disability_keywords,
    )

    return {
        "storage": storage,
        "crawler": crawler,
        "semantic": semantic,
        "wayback": wayback,
        "reporter": reporter,
        "scheduler": scheduler,
        "engine": engine,
        "cfg": cfg,
    }


# ---------------------------------------------------------------------------
# Main async entrypoint
# ---------------------------------------------------------------------------


async def run(config_path: str, once: bool = False) -> None:
    """Full daemon run loop."""
    cfg_dict = load_config(config_path)
    cfg = ConfigNamespace(cfg_dict)

    # Setup logging
    sys_cfg = cfg.get("system") or {}
    setup_logging(
        log_level=getattr(sys_cfg, "log_level", "INFO"),
        log_file=getattr(sys_cfg, "log_file", None),
    )

    logger.info("Federal Disability Data Monitor starting up")

    components = await build_components(cfg)
    scheduler: MonitorScheduler = components["scheduler"]
    storage: StorageManager = components["storage"]
    crawler = components["crawler"]
    semantic = components["semantic"]
    wayback = components["wayback"]
    reporter = components["reporter"]
    engine = components["engine"]

    if once:
        # Single-run mode for testing/CI
        logger.info("Running single check cycle (--once mode)")
        targets = await storage.get_all_targets()
        await scheduler._check_targets(targets)
        logger.info("Single cycle complete — exiting")
        return

    # ------------------------------------------------------------------
    # Start scheduler
    # ------------------------------------------------------------------
    scheduler.setup_jobs()
    scheduler.start()

    # ------------------------------------------------------------------
    # Start FastAPI via uvicorn
    # ------------------------------------------------------------------
    from dashboard import create_app

    dash_raw = cfg.get("dashboard") or {}
    dash_dict = {}
    if hasattr(dash_raw, "__dict__"):
        dash_dict = {k: v for k, v in dash_raw.__dict__.items() if not k.startswith("_")}
    elif isinstance(dash_raw, dict):
        dash_dict = dash_raw

    app = create_app(storage=storage, config=cfg)

    host = dash_dict.get("host", "0.0.0.0")
    port = int(dash_dict.get("port", 8000))
    debug = bool(dash_dict.get("debug", False))

    # Graceful shutdown handler
    shutdown_event = asyncio.Event()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.info(f"Received signal {sig.name} — shutting down")
        shutdown_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal, sig)
        except NotImplementedError:
            # Windows does not support add_signal_handler for all signals
            signal.signal(sig, lambda s, f: shutdown_event.set())

    server_config = uvicorn.Config(
        app=app,
        host=host,
        port=port,
        log_level="warning",
        access_log=False,
    )
    server = uvicorn.Server(server_config)

    logger.info(f"Dashboard starting at http://{host}:{port}")

    # Run scheduler and uvicorn concurrently
    try:
        server_task = asyncio.create_task(server.serve())
        shutdown_task = asyncio.create_task(shutdown_event.wait())

        done, pending = await asyncio.wait(
            [server_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
    finally:
        logger.info("Shutting down components")
        scheduler.shutdown(wait=False)
        await crawler.__aexit__(None, None, None)
        await wayback.__aexit__(None, None, None)
        await reporter.__aexit__(None, None, None)
        semantic.shutdown()
        await engine.dispose()
        logger.info("Shutdown complete")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Federal Disability Data Monitor — perpetual monitoring daemon.\n"
                    "For on-demand GitHub-backed runs, use: python run.py --github"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config.yaml (default: config.yaml)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one monitoring cycle then exit (useful for testing)",
    )
    parser.add_argument(
        "--dashboard-only",
        action="store_true",
        dest="dashboard_only",
        help=(
            "Serve the dashboard only (no monitoring). Reads from local data/ "
            "which should be populated by 'python run.py --github' or git pull."
        ),
    )
    args = parser.parse_args()

    if not Path(args.config).exists():
        print(f"ERROR: Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    if args.dashboard_only:
        # Lightweight mode: just serve the dashboard from existing data/
        asyncio.run(_serve_dashboard_only(args.config))
    else:
        asyncio.run(run(args.config, once=args.once))


async def _serve_dashboard_only(config_path: str) -> None:
    """Serve the FastAPI dashboard reading from local data/ CSV files (no scheduler)."""
    from run import LocalFileStorage, load_config

    cfg_dict = load_config(config_path)

    setup_logging(
        log_level=cfg_dict.get("system", {}).get("log_level", "INFO"),
        log_file=cfg_dict.get("system", {}).get("log_file"),
    )

    # Use a lightweight adapter that serves CSV-backed data to the dashboard
    storage = _CSVDashboardStorage(data_dir=cfg_dict.get("system", {}).get("data_dir", "data"))
    storage.load()

    cfg = ConfigNamespace(cfg_dict)
    from dashboard import create_app
    app = create_app(storage=storage, config=cfg)

    dash_cfg = cfg_dict.get("dashboard", {})
    host = dash_cfg.get("host", "0.0.0.0")
    port = int(dash_cfg.get("port", 8000))

    logger.info(f"Dashboard-only mode: http://{host}:{port}  (Ctrl+C to stop)")
    server = uvicorn.Server(uvicorn.Config(app=app, host=host, port=port, log_level="warning"))
    await server.serve()


class _CSVDashboardStorage:
    """
    Minimal read-only adapter that serves the dashboard from data/changes.csv
    and data/snapshots.csv — no SQLite required.
    """

    def __init__(self, data_dir: str = "data") -> None:
        self._data_dir = Path(data_dir)
        self._changes: list[dict] = []
        self._snapshots: dict[str, dict] = {}
        self._targets: list = []

    def load(self) -> None:
        import csv as _csv
        changes_path = self._data_dir / "changes.csv"
        if changes_path.exists():
            with open(changes_path, newline="", encoding="utf-8") as f:
                self._changes = list(_csv.DictReader(f))

        snapshots_path = self._data_dir / "snapshots.csv"
        if snapshots_path.exists():
            with open(snapshots_path, newline="", encoding="utf-8") as f:
                rows = list(_csv.DictReader(f))
                self._snapshots = {r["target_id"]: r for r in rows}
                # Reconstruct target-like objects for the dashboard
                from storage import TargetModel
                for r in rows:
                    try:
                        self._targets.append(TargetModel(
                            id=r["target_id"], url=r["target_url"],
                            agency=r["target_agency"], name=r["target_name"],
                            frequency="medium", type="html",
                        ))
                    except Exception:
                        pass

        logger.info(
            f"Dashboard loaded {len(self._changes)} changes, "
            f"{len(self._snapshots)} snapshots from {self._data_dir}/"
        )

    async def get_dashboard_stats(self) -> dict:
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)

        counts: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
        recent: list[dict] = []
        for c in self._changes:
            try:
                dt = datetime.fromisoformat(c.get("detected_at", ""))
                if dt >= last_24h:
                    sev = c.get("severity", "LOW")
                    counts[sev] = counts.get(sev, 0) + 1
                if dt >= last_7d:
                    recent.append({
                        "target_name": c.get("target_name", ""),
                        "agency": c.get("target_agency", ""),
                        "severity": c.get("severity", ""),
                        "detected_at": c.get("detected_at", "")[:16],
                        "change_type": c.get("change_type", ""),
                        "change_id": c.get("change_id", ""),
                    })
            except Exception:
                pass
        recent.sort(key=lambda x: x.get("detected_at", ""), reverse=True)
        return {"counts_24h": counts, "total_targets": len(self._targets), "recent_changes": recent[:10]}

    async def get_changes_since(self, hours: int = 24, severity=None, agency=None,
                                 page: int = 1, page_size: int = 50) -> list[dict]:
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        results = []
        for c in reversed(self._changes):
            try:
                dt = datetime.fromisoformat(c.get("detected_at", ""))
                if dt < cutoff:
                    continue
                if severity and c.get("severity") != severity:
                    continue
                if agency and c.get("target_agency") != agency:
                    continue
                results.append({
                    "change_id": c.get("change_id", ""),
                    "target_id": c.get("target_id", ""),
                    "target_name": c.get("target_name", ""),
                    "target_agency": c.get("target_agency", ""),
                    "target_url": c.get("target_url", ""),
                    "detected_at": c.get("detected_at", ""),
                    "severity": c.get("severity", ""),
                    "change_type": c.get("change_type", ""),
                    "pct_text_changed": float(c["pct_text_changed"]) if c.get("pct_text_changed") else None,
                    "semantic_similarity": float(c["semantic_similarity"]) if c.get("semantic_similarity") else None,
                    "semantic_labels": c.get("semantic_labels", "").split("|") if c.get("semantic_labels") else [],
                    "diff_text": c.get("diff_text_preview", ""),
                    "snapshot_before": None,
                    "snapshot_after": None,
                })
            except Exception:
                pass
        start = (page - 1) * page_size
        return results[start:start + page_size]

    async def get_all_targets(self) -> list:
        return list(self._targets)

    async def get_latest_snapshot(self, target_id: str):
        return None  # Not needed for dashboard display

    async def get_snapshot_by_id(self, snapshot_id: int):
        return None

    async def export_csv(self) -> str:
        import csv as _csv, io as _io
        buf = _io.StringIO()
        if not self._changes:
            return ""
        writer = _csv.DictWriter(buf, fieldnames=list(self._changes[0].keys()))
        writer.writeheader()
        writer.writerows(self._changes)
        return buf.getvalue()


if __name__ == "__main__":
    main()
