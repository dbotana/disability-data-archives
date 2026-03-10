"""
storage.py — SQLAlchemy 2.x async ORM, Pydantic v2 models, and StorageManager.

Tables:
  monitoring_targets   — canonical list of URLs to watch
  snapshots            — every successful fetch result
  changes              — detected diffs between consecutive snapshots
  alerts               — outbound notification records
  wayback_submissions  — Wayback Machine archival requests

Uses SQLite with WAL mode for safe concurrent reads (FastAPI) + writes (scheduler).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiofiles
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    event,
    select,
    text,
    update,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQLAlchemy ORM Base + Table Definitions
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class MonitoringTarget(Base):
    __tablename__ = "monitoring_targets"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    agency: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    frequency: Mapped[str] = mapped_column(String(16), nullable=False)  # high/medium/low
    type: Mapped[str] = mapped_column(String(16), nullable=False)       # html/json/csv/xml
    tags: Mapped[Optional[str]] = mapped_column(Text)                   # JSON array string
    row_count_check: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    last_changed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    consecutive_errors: Mapped[int] = mapped_column(Integer, default=0)


class Snapshot(Base):
    __tablename__ = "snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    target_id: Mapped[str] = mapped_column(String(128), ForeignKey("monitoring_targets.id"), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    http_status: Mapped[int] = mapped_column(Integer, nullable=False)
    content_length: Mapped[Optional[int]] = mapped_column(Integer)
    body_hash_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    text_hash_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    text_content: Mapped[Optional[str]] = mapped_column(Text)
    row_count: Mapped[Optional[int]] = mapped_column(Integer)
    headers_json: Mapped[Optional[str]] = mapped_column(Text)
    archive_path: Mapped[Optional[str]] = mapped_column(Text)
    etag: Mapped[Optional[str]] = mapped_column(String(256))
    last_modified: Mapped[Optional[str]] = mapped_column(String(128))
    redirect_url: Mapped[Optional[str]] = mapped_column(Text)
    is_baseline: Mapped[bool] = mapped_column(Boolean, default=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text)


class Change(Base):
    __tablename__ = "changes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    target_id: Mapped[str] = mapped_column(String(128), ForeignKey("monitoring_targets.id"), nullable=False)
    detected_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    snapshot_before: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("snapshots.id"))
    snapshot_after: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey("snapshots.id"))
    severity: Mapped[str] = mapped_column(String(16), nullable=False)   # CRITICAL/HIGH/MEDIUM/LOW
    change_type: Mapped[str] = mapped_column(String(64), nullable=False)
    pct_content_changed: Mapped[Optional[float]] = mapped_column(Float)
    pct_text_changed: Mapped[Optional[float]] = mapped_column(Float)
    row_count_before: Mapped[Optional[int]] = mapped_column(Integer)
    row_count_after: Mapped[Optional[int]] = mapped_column(Integer)
    semantic_similarity: Mapped[Optional[float]] = mapped_column(Float)
    semantic_labels: Mapped[Optional[str]] = mapped_column(Text)        # JSON array string
    diff_text: Mapped[Optional[str]] = mapped_column(Text)              # truncated at 10KB
    alert_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    notes: Mapped[Optional[str]] = mapped_column(Text)


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    change_id: Mapped[int] = mapped_column(Integer, ForeignKey("changes.id"), nullable=False)
    sent_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)    # email/slack/rss
    status: Mapped[str] = mapped_column(String(16), nullable=False)     # sent/failed/pending
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    payload_json: Mapped[Optional[str]] = mapped_column(Text)


class WaybackSubmission(Base):
    __tablename__ = "wayback_submissions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    snapshot_id: Mapped[int] = mapped_column(Integer, ForeignKey("snapshots.id"), nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    wayback_url: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), nullable=False)     # submitted/confirmed/failed
    cdx_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    cdx_verified_at: Mapped[Optional[datetime]] = mapped_column(DateTime)


# ---------------------------------------------------------------------------
# Pydantic v2 Data Models (used for cross-module type safety)
# ---------------------------------------------------------------------------


class TargetModel(BaseModel):
    """Represents a monitoring target loaded from config.yaml."""
    id: str
    url: str
    agency: str
    name: str
    frequency: str
    type: str
    tags: list[str] = Field(default_factory=list)
    row_count_check: bool = False

    @field_validator("frequency")
    @classmethod
    def validate_frequency(cls, v: str) -> str:
        if v not in ("high", "medium", "low"):
            raise ValueError(f"frequency must be high/medium/low, got: {v}")
        return v

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in ("html", "json", "csv", "xml"):
            raise ValueError(f"type must be html/json/csv/xml, got: {v}")
        return v


class FetchResult(BaseModel):
    """Output from crawler.py for a single fetch attempt."""
    target_id: str
    url: str
    fetched_at: datetime
    http_status: int
    content_length: Optional[int] = None
    body_hash_sha256: str
    text_hash_sha256: str
    text_content: str = ""
    row_count: Optional[int] = None
    headers: dict[str, str] = Field(default_factory=dict)
    redirect_url: Optional[str] = None
    error_message: Optional[str] = None
    raw_body: bytes = b""

    class Config:
        arbitrary_types_allowed = True


class SnapshotModel(BaseModel):
    """Snapshot record as returned from the database."""
    id: int
    target_id: str
    fetched_at: datetime
    http_status: int
    content_length: Optional[int] = None
    body_hash_sha256: str
    text_hash_sha256: str
    text_content: Optional[str] = None
    row_count: Optional[int] = None
    headers_json: Optional[str] = None
    archive_path: Optional[str] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    redirect_url: Optional[str] = None
    is_baseline: bool = False
    error_message: Optional[str] = None


class ChangeModel(BaseModel):
    """Change record for use in reporter, dashboard, and scheduler."""
    id: Optional[int] = None
    target_id: str
    target_name: str = ""
    target_agency: str = ""
    url: str = ""
    detected_at: datetime
    snapshot_before: Optional[int] = None
    snapshot_after: Optional[int] = None
    severity: str                           # CRITICAL/HIGH/MEDIUM/LOW
    change_type: str
    pct_content_changed: Optional[float] = None
    pct_text_changed: Optional[float] = None
    row_count_before: Optional[int] = None
    row_count_after: Optional[int] = None
    semantic_similarity: Optional[float] = None
    semantic_labels: list[str] = Field(default_factory=list)
    diff_text: Optional[str] = None
    alert_sent: bool = False
    notes: Optional[str] = None


# ---------------------------------------------------------------------------
# Database engine factory
# ---------------------------------------------------------------------------


def create_db_engine(db_path: str) -> AsyncEngine:
    """Create async SQLAlchemy engine for SQLite with WAL mode."""
    # Ensure parent directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{db_path}",
        connect_args={"check_same_thread": False},
        echo=False,
    )

    # Enable WAL mode on every new connection for concurrent reads
    @event.listens_for(engine.sync_engine, "connect")
    def set_wal_mode(dbapi_connection: Any, connection_record: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()

    return engine


# ---------------------------------------------------------------------------
# StorageManager
# ---------------------------------------------------------------------------


class StorageManager:
    """High-level async data access layer for all ORM tables."""

    def __init__(self, engine: AsyncEngine, archive_dir: str = "archive") -> None:
        self._engine = engine
        self._archive_dir = Path(archive_dir)
        self._session_factory = async_sessionmaker(
            engine,
            class_=AsyncSession,
            expire_on_commit=False,  # Critical: prevents MissingGreenlet in async context
        )

    async def init_db(self) -> None:
        """Create all tables if they don't exist."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database initialized")

    # ------------------------------------------------------------------
    # Target management
    # ------------------------------------------------------------------

    async def sync_targets(self, targets: list[TargetModel]) -> int:
        """Insert or update targets from config. Returns count synced."""
        now = datetime.now(timezone.utc)
        count = 0
        async with self._session_factory() as session:
            async with session.begin():
                for t in targets:
                    result = await session.execute(
                        select(MonitoringTarget).where(MonitoringTarget.id == t.id)
                    )
                    existing = result.scalar_one_or_none()
                    if existing is None:
                        obj = MonitoringTarget(
                            id=t.id,
                            url=t.url,
                            agency=t.agency,
                            name=t.name,
                            frequency=t.frequency,
                            type=t.type,
                            tags=json.dumps(t.tags),
                            row_count_check=t.row_count_check,
                            is_active=True,
                            created_at=now,
                        )
                        session.add(obj)
                    else:
                        # Update mutable fields in case config changed
                        existing.url = t.url
                        existing.agency = t.agency
                        existing.name = t.name
                        existing.frequency = t.frequency
                        existing.type = t.type
                        existing.tags = json.dumps(t.tags)
                        existing.row_count_check = t.row_count_check
                    count += 1
        logger.info(f"Synced {count} targets to database")
        return count

    async def get_targets_by_frequency(self, frequency: str) -> list[TargetModel]:
        """Fetch active targets matching the given frequency."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(MonitoringTarget).where(
                    MonitoringTarget.frequency == frequency,
                    MonitoringTarget.is_active == True,  # noqa: E712
                )
            )
            rows = result.scalars().all()
        return [
            TargetModel(
                id=r.id,
                url=r.url,
                agency=r.agency,
                name=r.name,
                frequency=r.frequency,
                type=r.type,
                tags=json.loads(r.tags) if r.tags else [],
                row_count_check=r.row_count_check,
            )
            for r in rows
        ]

    async def get_all_targets(self) -> list[TargetModel]:
        """Return all active targets."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(MonitoringTarget).where(MonitoringTarget.is_active == True)  # noqa: E712
            )
            rows = result.scalars().all()
        return [
            TargetModel(
                id=r.id,
                url=r.url,
                agency=r.agency,
                name=r.name,
                frequency=r.frequency,
                type=r.type,
                tags=json.loads(r.tags) if r.tags else [],
                row_count_check=r.row_count_check,
            )
            for r in rows
        ]

    async def mark_target_checked(self, target_id: str, error: bool = False) -> None:
        """Update last_checked_at and optionally increment consecutive_errors."""
        now = datetime.now(timezone.utc)
        async with self._session_factory() as session:
            async with session.begin():
                stmt = (
                    update(MonitoringTarget)
                    .where(MonitoringTarget.id == target_id)
                    .values(last_checked_at=now)
                )
                await session.execute(stmt)
                if error:
                    await session.execute(
                        update(MonitoringTarget)
                        .where(MonitoringTarget.id == target_id)
                        .values(consecutive_errors=MonitoringTarget.consecutive_errors + 1)
                    )
                else:
                    await session.execute(
                        update(MonitoringTarget)
                        .where(MonitoringTarget.id == target_id)
                        .values(consecutive_errors=0)
                    )

    # ------------------------------------------------------------------
    # Snapshot management
    # ------------------------------------------------------------------

    async def save_snapshot(self, result: FetchResult, agency: str = "unknown") -> int:
        """Persist a FetchResult to the snapshots table and archive raw HTML.

        Returns the new snapshot row id.
        """
        archive_path: Optional[str] = None

        if result.raw_body:
            archive_path = await self._archive_raw(result, agency)

        snapshot = Snapshot(
            target_id=result.target_id,
            fetched_at=result.fetched_at,
            http_status=result.http_status,
            content_length=result.content_length,
            body_hash_sha256=result.body_hash_sha256,
            text_hash_sha256=result.text_hash_sha256,
            text_content=result.text_content[:50_000] if result.text_content else None,
            row_count=result.row_count,
            headers_json=json.dumps(result.headers),
            archive_path=str(archive_path) if archive_path else None,
            etag=result.headers.get("etag"),
            last_modified=result.headers.get("last-modified"),
            redirect_url=result.redirect_url,
            error_message=result.error_message,
        )

        async with self._session_factory() as session:
            async with session.begin():
                session.add(snapshot)
            # After commit, snapshot.id is available because expire_on_commit=False
            snapshot_id = snapshot.id

        logger.debug(f"Saved snapshot {snapshot_id} for {result.target_id}")
        return snapshot_id

    async def _archive_raw(self, result: FetchResult, agency: str) -> Path:
        """Write raw body bytes to archive/{agency}/{YYYY-MM-DD}/{target_id}/."""
        date_str = result.fetched_at.strftime("%Y-%m-%d")
        dir_path = self._archive_dir / agency / date_str / result.target_id
        dir_path.mkdir(parents=True, exist_ok=True)

        ext = {"html": ".html", "json": ".json", "csv": ".csv", "xml": ".xml"}.get("html", ".bin")
        file_path = dir_path / f"snapshot_{result.fetched_at.strftime('%H%M%S')}{ext}"

        async with aiofiles.open(file_path, "wb") as f:
            await f.write(result.raw_body)

        return file_path

    async def get_latest_snapshot(self, target_id: str) -> Optional[SnapshotModel]:
        """Return the most recent snapshot for a target, or None."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Snapshot)
                .where(Snapshot.target_id == target_id)
                .order_by(Snapshot.fetched_at.desc())
                .limit(1)
            )
            row = result.scalar_one_or_none()
        if row is None:
            return None
        return SnapshotModel(
            id=row.id,
            target_id=row.target_id,
            fetched_at=row.fetched_at,
            http_status=row.http_status,
            content_length=row.content_length,
            body_hash_sha256=row.body_hash_sha256,
            text_hash_sha256=row.text_hash_sha256,
            text_content=row.text_content,
            row_count=row.row_count,
            headers_json=row.headers_json,
            archive_path=row.archive_path,
            etag=row.etag,
            last_modified=row.last_modified,
            redirect_url=row.redirect_url,
            is_baseline=row.is_baseline,
            error_message=row.error_message,
        )

    async def get_snapshot_by_id(self, snapshot_id: int) -> Optional[SnapshotModel]:
        """Fetch a specific snapshot by primary key."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Snapshot).where(Snapshot.id == snapshot_id)
            )
            row = result.scalar_one_or_none()
        if row is None:
            return None
        return SnapshotModel(
            id=row.id,
            target_id=row.target_id,
            fetched_at=row.fetched_at,
            http_status=row.http_status,
            content_length=row.content_length,
            body_hash_sha256=row.body_hash_sha256,
            text_hash_sha256=row.text_hash_sha256,
            text_content=row.text_content,
            row_count=row.row_count,
            headers_json=row.headers_json,
            archive_path=row.archive_path,
            etag=row.etag,
            last_modified=row.last_modified,
            redirect_url=row.redirect_url,
            is_baseline=row.is_baseline,
            error_message=row.error_message,
        )

    # ------------------------------------------------------------------
    # Change management
    # ------------------------------------------------------------------

    async def save_change(self, change: ChangeModel) -> int:
        """Persist a ChangeModel to the changes table. Returns new row id."""
        obj = Change(
            target_id=change.target_id,
            detected_at=change.detected_at,
            snapshot_before=change.snapshot_before,
            snapshot_after=change.snapshot_after,
            severity=change.severity,
            change_type=change.change_type,
            pct_content_changed=change.pct_content_changed,
            pct_text_changed=change.pct_text_changed,
            row_count_before=change.row_count_before,
            row_count_after=change.row_count_after,
            semantic_similarity=change.semantic_similarity,
            semantic_labels=json.dumps(change.semantic_labels),
            diff_text=change.diff_text,
            alert_sent=change.alert_sent,
            notes=change.notes,
        )
        async with self._session_factory() as session:
            async with session.begin():
                session.add(obj)
            change_id = obj.id

        # Update target's last_changed_at
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    update(MonitoringTarget)
                    .where(MonitoringTarget.id == change.target_id)
                    .values(last_changed_at=change.detected_at)
                )

        logger.info(
            f"Saved change {change_id}: {change.severity} for {change.target_id} "
            f"({change.change_type})"
        )
        return change_id

    async def get_changes_since(
        self,
        hours: int = 24,
        severity: Optional[str] = None,
        agency: Optional[str] = None,
        page: int = 1,
        page_size: int = 50,
    ) -> list[dict[str, Any]]:
        """Fetch changes with optional filters. Returns enriched dicts with target info."""
        from datetime import timedelta

        since = datetime.now(timezone.utc) - timedelta(hours=hours)

        async with self._session_factory() as session:
            stmt = (
                select(Change, MonitoringTarget)
                .join(MonitoringTarget, Change.target_id == MonitoringTarget.id)
                .where(Change.detected_at >= since)
            )
            if severity:
                stmt = stmt.where(Change.severity == severity)
            if agency:
                stmt = stmt.where(MonitoringTarget.agency == agency)

            stmt = stmt.order_by(Change.detected_at.desc())
            stmt = stmt.offset((page - 1) * page_size).limit(page_size)

            result = await session.execute(stmt)
            rows = result.all()

        return [
            {
                "change_id": c.id,
                "target_id": c.target_id,
                "target_name": t.name,
                "target_agency": t.agency,
                "target_url": t.url,
                "detected_at": c.detected_at.isoformat(),
                "severity": c.severity,
                "change_type": c.change_type,
                "pct_text_changed": c.pct_text_changed,
                "semantic_similarity": c.semantic_similarity,
                "semantic_labels": json.loads(c.semantic_labels) if c.semantic_labels else [],
                "diff_text": c.diff_text,
                "alert_sent": c.alert_sent,
                "snapshot_before": c.snapshot_before,
                "snapshot_after": c.snapshot_after,
            }
            for c, t in rows
        ]

    async def get_unsent_alerts(self) -> list[dict[str, Any]]:
        """Return changes where alert_sent=False."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(Change, MonitoringTarget)
                .join(MonitoringTarget, Change.target_id == MonitoringTarget.id)
                .where(Change.alert_sent == False)  # noqa: E712
                .order_by(Change.detected_at.desc())
            )
            rows = result.all()
        return [
            {
                "change_id": c.id,
                "target_id": c.target_id,
                "target_name": t.name,
                "target_agency": t.agency,
                "target_url": t.url,
                "detected_at": c.detected_at,
                "severity": c.severity,
                "change_type": c.change_type,
                "pct_text_changed": c.pct_text_changed,
                "diff_text": c.diff_text,
            }
            for c, t in rows
        ]

    async def mark_alert_sent(self, change_id: int) -> None:
        """Mark a change as having its alert dispatched."""
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    update(Change)
                    .where(Change.id == change_id)
                    .values(alert_sent=True)
                )

    # ------------------------------------------------------------------
    # Alert log
    # ------------------------------------------------------------------

    async def save_alert_record(
        self,
        change_id: int,
        channel: str,
        status: str,
        payload: Optional[dict] = None,
        error_message: Optional[str] = None,
    ) -> None:
        alert = Alert(
            change_id=change_id,
            sent_at=datetime.now(timezone.utc),
            channel=channel,
            status=status,
            error_message=error_message,
            payload_json=json.dumps(payload) if payload else None,
        )
        async with self._session_factory() as session:
            async with session.begin():
                session.add(alert)

    # ------------------------------------------------------------------
    # Wayback submissions
    # ------------------------------------------------------------------

    async def save_wayback_submission(
        self, snapshot_id: int, wayback_url: Optional[str], status: str
    ) -> int:
        obj = WaybackSubmission(
            snapshot_id=snapshot_id,
            submitted_at=datetime.now(timezone.utc),
            wayback_url=wayback_url,
            status=status,
        )
        async with self._session_factory() as session:
            async with session.begin():
                session.add(obj)
            return obj.id

    async def get_pending_wayback_verifications(self) -> list[dict[str, Any]]:
        """Return submissions that haven't been CDX-verified yet."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(WaybackSubmission, Snapshot)
                .join(Snapshot, WaybackSubmission.snapshot_id == Snapshot.id)
                .where(WaybackSubmission.cdx_verified == False)  # noqa: E712
                .where(WaybackSubmission.status == "submitted")
            )
            rows = result.all()
        return [
            {
                "submission_id": w.id,
                "snapshot_id": w.snapshot_id,
                "target_id": s.target_id,
                "url": s.redirect_url or "",
                "wayback_url": w.wayback_url,
                "submitted_at": w.submitted_at,
            }
            for w, s in rows
        ]

    async def mark_wayback_verified(self, submission_id: int) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    update(WaybackSubmission)
                    .where(WaybackSubmission.id == submission_id)
                    .values(cdx_verified=True, cdx_verified_at=datetime.now(timezone.utc))
                )

    # ------------------------------------------------------------------
    # Dashboard statistics
    # ------------------------------------------------------------------

    async def get_dashboard_stats(self) -> dict[str, Any]:
        """Aggregate statistics for the dashboard index page."""
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)

        async with self._session_factory() as session:
            # Count changes by severity in last 24h
            counts: dict[str, int] = {}
            for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
                r = await session.execute(
                    select(Change)
                    .where(Change.severity == sev)
                    .where(Change.detected_at >= last_24h)
                )
                counts[sev] = len(r.scalars().all())

            # Total targets
            r = await session.execute(
                select(MonitoringTarget).where(MonitoringTarget.is_active == True)  # noqa: E712
            )
            total_targets = len(r.scalars().all())

            # Most recently changed targets
            r = await session.execute(
                select(Change, MonitoringTarget)
                .join(MonitoringTarget, Change.target_id == MonitoringTarget.id)
                .where(Change.detected_at >= last_7d)
                .order_by(Change.detected_at.desc())
                .limit(10)
            )
            recent = [
                {
                    "target_name": t.name,
                    "agency": t.agency,
                    "severity": c.severity,
                    "detected_at": c.detected_at.strftime("%Y-%m-%d %H:%M UTC"),
                    "change_type": c.change_type,
                    "change_id": c.id,
                }
                for c, t in r.all()
            ]

        return {
            "counts_24h": counts,
            "total_targets": total_targets,
            "recent_changes": recent,
        }

    async def export_csv(self) -> str:
        """Export all changes to CSV string."""
        import csv
        import io

        changes = await self.get_changes_since(hours=87600)  # ~10 years
        buf = io.StringIO()
        if not changes:
            return ""
        writer = csv.DictWriter(buf, fieldnames=list(changes[0].keys()))
        writer.writeheader()
        writer.writerows(changes)
        return buf.getvalue()
