"""
test_diff_engine.py — Unit tests for diff_engine.py

Covers all severity classification rules, diff generation, and edge cases.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pytest

from diff_engine import (
    CHANGE_CONTENT_MAJOR,
    CHANGE_CONTENT_MINOR,
    CHANGE_CONTENT_TRIVIAL,
    CHANGE_EXTERNAL_REDIRECT,
    CHANGE_GOV_REDIRECT,
    CHANGE_HTTP_ERROR,
    CHANGE_METADATA_ONLY,
    CHANGE_REMOVAL,
    CHANGE_RESURRECTION,
    CHANGE_ROW_COUNT_DROP,
    SEVERITY_CRITICAL,
    SEVERITY_HIGH,
    SEVERITY_LOW,
    SEVERITY_MEDIUM,
    DiffEngine,
    _generate_diff,
    _is_gov_url,
    _pct_changed,
    compute_keyword_delta,
)
from storage import FetchResult, SnapshotModel


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_snapshot(
    text: str = "Hello world disability ADA",
    http_status: int = 200,
    row_count: int | None = None,
    etag: str | None = '"etag1"',
    last_modified: str | None = "Mon, 01 Jan 2025 00:00:00 GMT",
    redirect_url: str | None = None,
    snapshot_id: int = 1,
) -> SnapshotModel:
    body = text.encode("utf-8")
    return SnapshotModel(
        id=snapshot_id,
        target_id="test-target",
        fetched_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        http_status=http_status,
        content_length=len(body),
        body_hash_sha256=hashlib.sha256(body).hexdigest(),
        text_hash_sha256=hashlib.sha256(text.encode()).hexdigest(),
        text_content=text,
        row_count=row_count,
        headers_json="{}",
        etag=etag,
        last_modified=last_modified,
        redirect_url=redirect_url,
    )


def _make_fetch(
    text: str = "Hello world disability ADA",
    http_status: int = 200,
    row_count: int | None = None,
    etag: str | None = '"etag1"',
    redirect_url: str | None = None,
) -> FetchResult:
    body = text.encode("utf-8")
    return FetchResult(
        target_id="test-target",
        url="https://www.ssa.gov/test",
        fetched_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        http_status=http_status,
        content_length=len(body),
        body_hash_sha256=hashlib.sha256(body).hexdigest(),
        text_hash_sha256=hashlib.sha256(text.encode()).hexdigest(),
        text_content=text,
        row_count=row_count,
        headers={"etag": etag} if etag else {},
        redirect_url=redirect_url,
        raw_body=body,
    )


engine = DiffEngine()


# ---------------------------------------------------------------------------
# No-change cases
# ---------------------------------------------------------------------------


def test_no_change_when_hashes_identical():
    before = _make_snapshot("same content")
    after = _make_fetch("same content")
    assert engine.compare(before, after) is None


def test_no_change_on_metadata_only_same_hashes():
    """If only etag changes but body hash is identical, compare returns None
    (body hash check fires first)."""
    text = "constant text"
    before = _make_snapshot(text, etag='"old"')
    after = _make_fetch(text, etag='"new"')
    # same body hash → returns None (our early-exit check)
    assert engine.compare(before, after) is None


# ---------------------------------------------------------------------------
# CRITICAL severity
# ---------------------------------------------------------------------------


def test_critical_on_404():
    before = _make_snapshot()
    after = _make_fetch(http_status=404, text="")
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_CRITICAL
    assert result.change_type == CHANGE_HTTP_ERROR


def test_critical_on_410():
    before = _make_snapshot()
    after = _make_fetch(http_status=410, text="")
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_CRITICAL


def test_critical_on_451():
    before = _make_snapshot()
    after = _make_fetch(http_status=451, text="")
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_CRITICAL


def test_critical_on_external_redirect():
    before = _make_snapshot()
    after = _make_fetch(redirect_url="https://example.com/new-location")
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_CRITICAL
    assert result.change_type == CHANGE_EXTERNAL_REDIRECT


def test_critical_on_row_count_drop_over_10pct():
    before = _make_snapshot(row_count=100)
    after = _make_fetch(text="different text now", row_count=85)
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_CRITICAL
    assert result.change_type == CHANGE_ROW_COUNT_DROP
    assert result.row_count_before == 100
    assert result.row_count_after == 85


def test_no_critical_on_row_count_drop_under_10pct():
    before = _make_snapshot(row_count=100)
    after = _make_fetch(text="slightly different", row_count=95)
    result = engine.compare(before, after)
    # 5% drop is not CRITICAL
    if result is not None:
        assert result.severity != SEVERITY_CRITICAL


def test_critical_on_page_not_found_phrase():
    before = _make_snapshot("Disability benefits page with lots of content here")
    # Short body with removal phrase
    after = _make_fetch("Page not found", http_status=200)
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_CRITICAL
    assert result.change_type == CHANGE_REMOVAL


def test_critical_on_resurrection():
    before = _make_snapshot(http_status=404, text="")
    after = _make_fetch(text="Disability page restored with content", http_status=200)
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_CRITICAL
    assert result.change_type == CHANGE_RESURRECTION


# ---------------------------------------------------------------------------
# HIGH severity
# ---------------------------------------------------------------------------


def test_high_on_major_content_change():
    before = _make_snapshot("A " * 500)
    after = _make_fetch("B " * 500)
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_HIGH
    assert result.pct_text_changed is not None
    assert result.pct_text_changed > 30.0


def test_high_on_gov_redirect():
    before = _make_snapshot()
    after = _make_fetch(redirect_url="https://other-agency.gov/newpath")
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_HIGH
    assert result.change_type == CHANGE_GOV_REDIRECT


# ---------------------------------------------------------------------------
# MEDIUM severity
# ---------------------------------------------------------------------------


def test_medium_on_moderate_content_change():
    base = "disability accommodation ADA Section 504 SSDI SSI benefits support "
    before = _make_snapshot(base * 10)
    # Replace ~20% of content
    modified = base * 8 + "completely different text here " * 2
    after = _make_fetch(modified)
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity in (SEVERITY_MEDIUM, SEVERITY_HIGH)


# ---------------------------------------------------------------------------
# LOW severity
# ---------------------------------------------------------------------------


def test_low_on_trivial_content_change():
    base = "disability accommodation ADA " * 100
    before = _make_snapshot(base)
    # Trivial change: add one word
    after = _make_fetch(base + " updated")
    result = engine.compare(before, after)
    assert result is not None
    assert result.severity == SEVERITY_LOW


# ---------------------------------------------------------------------------
# Diff text
# ---------------------------------------------------------------------------


def test_diff_text_generated():
    before = _make_snapshot("old text here")
    after = _make_fetch("new text here")
    result = engine.compare(before, after)
    assert result is not None
    assert result.diff_text is not None
    assert "---" in result.diff_text or "-old" in result.diff_text


def test_diff_text_truncated_at_10kb():
    long_text_before = "A" * 50_000
    long_text_after = "B" * 50_000
    diff = _generate_diff(long_text_before, long_text_after)
    assert len(diff.encode("utf-8")) <= 10_240 + 100  # +100 for truncation notice


def test_diff_starts_with_unified_format():
    diff = _generate_diff("old line\n", "new line\n", "before", "after")
    # Unified diff starts with --- and +++ header lines
    assert "---" in diff or "@@" in diff


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def test_pct_changed_identical_text():
    assert _pct_changed("hello", "hello") == 0.0


def test_pct_changed_empty_before():
    assert _pct_changed("", "something") == 100.0


def test_pct_changed_empty_after():
    assert _pct_changed("something", "") == 100.0


def test_pct_changed_both_empty():
    assert _pct_changed("", "") == 0.0


def test_is_gov_url_true():
    assert _is_gov_url("https://www.ssa.gov/disability") is True
    assert _is_gov_url("https://data.cdc.gov/api") is True
    assert _is_gov_url("https://www.defense.mil/news") is True


def test_is_gov_url_false():
    assert _is_gov_url("https://example.com/page") is False
    assert _is_gov_url("https://archive.org/save") is False


def test_compute_keyword_delta():
    before = "disability ADA Section 504 accommodation"
    after = "employment workforce initiative"
    delta = compute_keyword_delta(["disability", "ADA", "accommodation"], before, after)
    assert delta["disability"]["before"] == 1
    assert delta["disability"]["after"] == 0
    assert delta["ADA"]["before"] == 1
    assert delta["ADA"]["after"] == 0
