"""
test_storage.py — Unit tests for storage.py

Uses in-memory SQLite via the async_db fixture from conftest.py.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio

from storage import FetchResult, StorageManager, TargetModel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fetch(
    target_id: str = "test-ssa",
    text: str = "Sample disability content ADA Section 504",
    http_status: int = 200,
    row_count: int | None = None,
) -> FetchResult:
    body = text.encode("utf-8")
    return FetchResult(
        target_id=target_id,
        url=f"https://www.example.gov/{target_id}",
        fetched_at=datetime.now(timezone.utc),
        http_status=http_status,
        content_length=len(body),
        body_hash_sha256=hashlib.sha256(body).hexdigest(),
        text_hash_sha256=hashlib.sha256(text.encode()).hexdigest(),
        text_content=text,
        row_count=row_count,
        headers={"content-type": "text/html", "etag": '"abc"'},
        raw_body=body,
    )


# ---------------------------------------------------------------------------
# Target sync
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sync_targets_inserts_new(async_db: StorageManager):
    targets = [
        TargetModel(
            id="ssa-test", url="https://ssa.gov/test", agency="SSA",
            name="SSA Test", frequency="high", type="html",
        )
    ]
    count = await async_db.sync_targets(targets)
    assert count == 1


@pytest.mark.asyncio
async def test_sync_targets_updates_existing(async_db: StorageManager):
    targets = [
        TargetModel(
            id="ssa-test", url="https://ssa.gov/test", agency="SSA",
            name="SSA Test", frequency="high", type="html",
        )
    ]
    await async_db.sync_targets(targets)

    # Update name
    targets[0] = TargetModel(
        id="ssa-test", url="https://ssa.gov/test", agency="SSA",
        name="SSA Test UPDATED", frequency="medium", type="html",
    )
    await async_db.sync_targets(targets)

    all_targets = await async_db.get_all_targets()
    assert len(all_targets) == 1
    assert all_targets[0].name == "SSA Test UPDATED"
    assert all_targets[0].frequency == "medium"


@pytest.mark.asyncio
async def test_get_targets_by_frequency(async_db: StorageManager):
    targets = [
        TargetModel(id="t1", url="https://a.gov/1", agency="A", name="T1", frequency="high", type="html"),
        TargetModel(id="t2", url="https://a.gov/2", agency="A", name="T2", frequency="medium", type="html"),
        TargetModel(id="t3", url="https://a.gov/3", agency="B", name="T3", frequency="high", type="json"),
    ]
    await async_db.sync_targets(targets)

    high = await async_db.get_targets_by_frequency("high")
    assert len(high) == 2
    assert all(t.frequency == "high" for t in high)

    medium = await async_db.get_targets_by_frequency("medium")
    assert len(medium) == 1


# ---------------------------------------------------------------------------
# Snapshot save and retrieve
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_and_retrieve_snapshot(async_db: StorageManager):
    targets = [
        TargetModel(id="ssa-test", url="https://ssa.gov/test", agency="SSA",
                    name="SSA Test", frequency="high", type="html")
    ]
    await async_db.sync_targets(targets)

    fetch = _make_fetch("ssa-test")
    snap_id = await async_db.save_snapshot(fetch, agency="SSA")
    assert snap_id is not None
    assert snap_id > 0

    retrieved = await async_db.get_snapshot_by_id(snap_id)
    assert retrieved is not None
    assert retrieved.target_id == "ssa-test"
    assert retrieved.http_status == 200
    assert len(retrieved.body_hash_sha256) == 64


@pytest.mark.asyncio
async def test_get_latest_snapshot_returns_most_recent(async_db: StorageManager):
    targets = [
        TargetModel(id="ssa-test", url="https://ssa.gov/test", agency="SSA",
                    name="SSA Test", frequency="high", type="html")
    ]
    await async_db.sync_targets(targets)

    fetch1 = _make_fetch("ssa-test", text="version one")
    fetch2 = _make_fetch("ssa-test", text="version two newer")
    id1 = await async_db.save_snapshot(fetch1, agency="SSA")
    id2 = await async_db.save_snapshot(fetch2, agency="SSA")

    latest = await async_db.get_latest_snapshot("ssa-test")
    assert latest is not None
    assert latest.id == id2  # Most recent


@pytest.mark.asyncio
async def test_get_latest_snapshot_returns_none_for_unknown(async_db: StorageManager):
    result = await async_db.get_latest_snapshot("nonexistent-target")
    assert result is None


# ---------------------------------------------------------------------------
# Change management
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_change_and_query(async_db: StorageManager):
    from storage import ChangeModel

    targets = [
        TargetModel(id="ssa-test", url="https://ssa.gov/test", agency="SSA",
                    name="SSA Test", frequency="high", type="html")
    ]
    await async_db.sync_targets(targets)

    change = ChangeModel(
        target_id="ssa-test",
        detected_at=datetime.now(timezone.utc),
        severity="CRITICAL",
        change_type="http_error",
        pct_text_changed=100.0,
    )
    change_id = await async_db.save_change(change)
    assert change_id > 0


@pytest.mark.asyncio
async def test_get_changes_since_filters_by_time(async_db: StorageManager):
    from storage import ChangeModel

    targets = [
        TargetModel(id="ssa-test", url="https://ssa.gov/test", agency="SSA",
                    name="SSA Test", frequency="high", type="html")
    ]
    await async_db.sync_targets(targets)

    now = datetime.now(timezone.utc)

    change_recent = ChangeModel(
        target_id="ssa-test",
        detected_at=now,
        severity="HIGH",
        change_type="content_major",
        pct_text_changed=45.0,
    )
    await async_db.save_change(change_recent)

    # Query last 24h — should find the change
    results = await async_db.get_changes_since(hours=24)
    assert len(results) >= 1
    assert any(r["change_type"] == "content_major" for r in results)


@pytest.mark.asyncio
async def test_get_changes_since_filters_by_severity(async_db: StorageManager):
    from storage import ChangeModel

    targets = [
        TargetModel(id="ssa-test", url="https://ssa.gov/test", agency="SSA",
                    name="SSA Test", frequency="high", type="html")
    ]
    await async_db.sync_targets(targets)

    now = datetime.now(timezone.utc)
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        c = ChangeModel(
            target_id="ssa-test",
            detected_at=now,
            severity=sev,
            change_type="test",
        )
        await async_db.save_change(c)

    critical_only = await async_db.get_changes_since(hours=24, severity="CRITICAL")
    assert all(r["severity"] == "CRITICAL" for r in critical_only)


# ---------------------------------------------------------------------------
# Alert dispatch tracking
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_alert_sent(async_db: StorageManager):
    from storage import ChangeModel

    targets = [
        TargetModel(id="ssa-test", url="https://ssa.gov/test", agency="SSA",
                    name="SSA Test", frequency="high", type="html")
    ]
    await async_db.sync_targets(targets)

    change = ChangeModel(
        target_id="ssa-test",
        detected_at=datetime.now(timezone.utc),
        severity="CRITICAL",
        change_type="http_error",
        alert_sent=False,
    )
    change_id = await async_db.save_change(change)

    unsent = await async_db.get_unsent_alerts()
    assert any(u["change_id"] == change_id for u in unsent)

    await async_db.mark_alert_sent(change_id)

    unsent_after = await async_db.get_unsent_alerts()
    assert not any(u["change_id"] == change_id for u in unsent_after)


# ---------------------------------------------------------------------------
# Dashboard stats
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dashboard_stats_returns_structure(async_db: StorageManager):
    stats = await async_db.get_dashboard_stats()
    assert "counts_24h" in stats
    assert "total_targets" in stats
    assert "recent_changes" in stats
    assert isinstance(stats["counts_24h"], dict)
    for sev in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
        assert sev in stats["counts_24h"]


# ---------------------------------------------------------------------------
# Mark target checked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_target_checked(async_db: StorageManager):
    targets = [
        TargetModel(id="ssa-test", url="https://ssa.gov/test", agency="SSA",
                    name="SSA Test", frequency="high", type="html")
    ]
    await async_db.sync_targets(targets)
    # Should not raise
    await async_db.mark_target_checked("ssa-test", error=False)
    await async_db.mark_target_checked("ssa-test", error=True)
