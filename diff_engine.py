"""
diff_engine.py — Change detection, severity classification, and diff generation.

Severity tiers:
  CRITICAL — page removed (404/410/451), external redirect, row count drop >10%,
             body matches maintenance/removal phrases
  HIGH     — >30% text change, intra-.gov redirect from previously-200 page
  MEDIUM   — 5-30% text change, ETag/Last-Modified changed without content change
  LOW      — <5% text change

Returns None when no meaningful change is detected (identical body hash).
"""

from __future__ import annotations

import difflib
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from storage import ChangeModel, FetchResult, SnapshotModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_HIGH = "HIGH"
SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_LOW = "LOW"

# HTTP status codes that always indicate removal
REMOVAL_STATUSES = {404, 410, 451}

# Body patterns that signal the page is gone even with a 200 status
_REMOVAL_PATTERNS = re.compile(
    r"(page\s+not\s+found|404|no\s+longer\s+available|has\s+been\s+removed"
    r"|this\s+page\s+has\s+moved|content\s+unavailable|temporarily\s+unavailable"
    r"|under\s+construction|site\s+is\s+undergoing\s+maintenance"
    r"|access\s+denied|403\s+forbidden|this\s+resource\s+is\s+not\s+available)",
    re.IGNORECASE,
)

# Maximum body size for text comparison (100KB)
_MAX_COMPARE_BYTES = 100_000

# Maximum diff text to store (10KB)
_MAX_DIFF_BYTES = 10_000


# ---------------------------------------------------------------------------
# Change type constants
# ---------------------------------------------------------------------------

CHANGE_HTTP_ERROR = "http_error"
CHANGE_REMOVAL = "page_removal"
CHANGE_EXTERNAL_REDIRECT = "external_redirect"
CHANGE_GOV_REDIRECT = "gov_redirect"
CHANGE_ROW_COUNT_DROP = "row_count_drop"
CHANGE_ROW_COUNT_INCREASE = "row_count_increase"
CHANGE_CONTENT_MAJOR = "content_major"
CHANGE_CONTENT_MINOR = "content_minor"
CHANGE_CONTENT_TRIVIAL = "content_trivial"
CHANGE_METADATA_ONLY = "metadata_only"
CHANGE_RESURRECTION = "resurrection"


# ---------------------------------------------------------------------------
# DiffEngine
# ---------------------------------------------------------------------------


class DiffEngine:
    """Stateless change detector. Call compare() for each (before, after) pair."""

    def compare(
        self,
        before: SnapshotModel,
        after: FetchResult,
    ) -> Optional[ChangeModel]:
        """
        Compare a previous snapshot against a new fetch result.

        Returns a ChangeModel if a noteworthy change is detected, else None.
        """
        # ------------------------------------------------------------------
        # 1. No change at all — identical body hash
        # ------------------------------------------------------------------
        if (
            before.body_hash_sha256 == after.body_hash_sha256
            and after.http_status == before.http_status
        ):
            logger.debug(f"No change for {after.target_id}")
            return None

        # ------------------------------------------------------------------
        # 2. CRITICAL: HTTP removal status (404, 410, 451)
        # ------------------------------------------------------------------
        if after.http_status in REMOVAL_STATUSES:
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_CRITICAL,
                change_type=CHANGE_HTTP_ERROR,
                notes=f"HTTP {after.http_status} — page removed or inaccessible",
            )

        # ------------------------------------------------------------------
        # 3. CRITICAL: Previous page was 200, now redirects outside .gov
        # ------------------------------------------------------------------
        if (
            after.redirect_url
            and not _is_gov_url(after.redirect_url)
            and before.http_status == 200
        ):
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_CRITICAL,
                change_type=CHANGE_EXTERNAL_REDIRECT,
                notes=f"Redirected to non-.gov: {after.redirect_url}",
            )

        # ------------------------------------------------------------------
        # 4. Compute text similarity
        # ------------------------------------------------------------------
        before_text = (before.text_content or "")[:_MAX_COMPARE_BYTES]
        after_text = (after.text_content or "")[:_MAX_COMPARE_BYTES]
        pct_text_changed = _pct_changed(before_text, after_text)
        pct_body_changed = _pct_changed_bytes(before, after)

        # ------------------------------------------------------------------
        # 5. CRITICAL: Resurrection (previously 404 → now 200)
        # ------------------------------------------------------------------
        if before.http_status in REMOVAL_STATUSES and after.http_status == 200:
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_CRITICAL,
                change_type=CHANGE_RESURRECTION,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
                notes="Previously removed page is now accessible (possible court-ordered restoration)",
            )

        # ------------------------------------------------------------------
        # 6. CRITICAL: Short body (< 1KB) containing removal phrases
        # ------------------------------------------------------------------
        if (
            after.http_status == 200
            and (after.content_length or len(after.raw_body)) < 1024
            and _REMOVAL_PATTERNS.search(after.text_content or "")
        ):
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_CRITICAL,
                change_type=CHANGE_REMOVAL,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
                notes="Page body is very short and contains removal/maintenance phrases",
            )

        # ------------------------------------------------------------------
        # 7. CRITICAL: Row count dropped by > 10%
        # ------------------------------------------------------------------
        if (
            before.row_count is not None
            and after.row_count is not None
            and before.row_count > 0
        ):
            drop_pct = (before.row_count - after.row_count) / before.row_count * 100
            if drop_pct > 10:
                return self._make_change(
                    after=after,
                    before=before,
                    severity=SEVERITY_CRITICAL,
                    change_type=CHANGE_ROW_COUNT_DROP,
                    pct_text=pct_text_changed,
                    pct_body=pct_body_changed,
                    row_before=before.row_count,
                    row_after=after.row_count,
                    notes=f"Row count dropped {drop_pct:.1f}%: {before.row_count} → {after.row_count}",
                )

        # ------------------------------------------------------------------
        # 8. HIGH: Intra-.gov redirect from previously-200 page
        # ------------------------------------------------------------------
        if (
            after.redirect_url
            and _is_gov_url(after.redirect_url)
            and before.http_status == 200
            and after.http_status in (200, 301, 302, 307, 308)
        ):
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_HIGH,
                change_type=CHANGE_GOV_REDIRECT,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
                notes=f"Page now redirects within .gov: {after.redirect_url}",
            )

        # ------------------------------------------------------------------
        # 9. HIGH: Previous 200 now returns a non-200, non-removal status
        # ------------------------------------------------------------------
        if before.http_status == 200 and after.http_status not in (200,):
            if after.http_status not in REMOVAL_STATUSES:  # already handled above
                return self._make_change(
                    after=after,
                    before=before,
                    severity=SEVERITY_HIGH,
                    change_type=CHANGE_HTTP_ERROR,
                    pct_text=pct_text_changed,
                    pct_body=pct_body_changed,
                    notes=f"HTTP status changed: {before.http_status} → {after.http_status}",
                )

        # ------------------------------------------------------------------
        # 10. HIGH: Major content change (>30%)
        # ------------------------------------------------------------------
        if pct_text_changed > 30.0:
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_HIGH,
                change_type=CHANGE_CONTENT_MAJOR,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
                notes=f"Major content change: {pct_text_changed:.1f}% text altered",
            )

        # ------------------------------------------------------------------
        # 11. Row count increased (informational — MEDIUM)
        # ------------------------------------------------------------------
        if (
            before.row_count is not None
            and after.row_count is not None
            and after.row_count > before.row_count
        ):
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_MEDIUM,
                change_type=CHANGE_ROW_COUNT_INCREASE,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
                row_before=before.row_count,
                row_after=after.row_count,
                notes=f"Row count increased: {before.row_count} → {after.row_count}",
            )

        # ------------------------------------------------------------------
        # 12. MEDIUM: Moderate content change (5-30%)
        # ------------------------------------------------------------------
        if pct_text_changed > 5.0:
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_MEDIUM,
                change_type=CHANGE_CONTENT_MINOR,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
            )

        # ------------------------------------------------------------------
        # 13. MEDIUM: Metadata-only change (ETag or Last-Modified changed,
        #     but body hash is the same or trivially different)
        # ------------------------------------------------------------------
        before_etag = before.etag
        after_etag = after.headers.get("etag")
        before_lm = before.last_modified
        after_lm = after.headers.get("last-modified")

        metadata_changed = (
            (before_etag and after_etag and before_etag != after_etag)
            or (before_lm and after_lm and before_lm != after_lm)
        )
        if metadata_changed and pct_text_changed <= 5.0 and pct_text_changed > 0:
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_MEDIUM,
                change_type=CHANGE_METADATA_ONLY,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
                notes="ETag or Last-Modified changed alongside minor content change",
            )

        # ------------------------------------------------------------------
        # 14. LOW: Small content change (0-5%)
        # ------------------------------------------------------------------
        if pct_text_changed > 0:
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_LOW,
                change_type=CHANGE_CONTENT_TRIVIAL,
                pct_text=pct_text_changed,
                pct_body=pct_body_changed,
            )

        # ------------------------------------------------------------------
        # 15. Only ETag/Last-Modified changed, no content change
        # ------------------------------------------------------------------
        if metadata_changed:
            return self._make_change(
                after=after,
                before=before,
                severity=SEVERITY_LOW,
                change_type=CHANGE_METADATA_ONLY,
                pct_text=0.0,
                pct_body=pct_body_changed,
                notes="Only HTTP metadata (ETag/Last-Modified) changed",
            )

        logger.debug(f"No significant change for {after.target_id}")
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _make_change(
        self,
        *,
        after: FetchResult,
        before: SnapshotModel,
        severity: str,
        change_type: str,
        pct_text: float = 0.0,
        pct_body: float = 0.0,
        row_before: Optional[int] = None,
        row_after: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> ChangeModel:
        """Build a ChangeModel and generate a human-readable unified diff."""
        diff_text = _generate_diff(
            before.text_content or "",
            after.text_content or "",
            label_before=f"before ({before.fetched_at.date() if before.fetched_at else 'prior'})",
            label_after=f"after ({after.fetched_at.date() if after.fetched_at else 'now'})",
        )

        logger.info(
            f"[{severity}] Change detected for {after.target_id}: "
            f"{change_type}, {pct_text:.1f}% text changed"
        )

        return ChangeModel(
            target_id=after.target_id,
            detected_at=datetime.now(timezone.utc),
            snapshot_before=before.id,
            snapshot_after=None,   # filled in by scheduler after saving the new snapshot
            severity=severity,
            change_type=change_type,
            pct_content_changed=pct_body,
            pct_text_changed=pct_text,
            row_count_before=row_before if row_before is not None else before.row_count,
            row_count_after=row_after if row_after is not None else after.row_count,
            diff_text=diff_text,
            notes=notes,
        )


# ---------------------------------------------------------------------------
# Standalone utility functions
# ---------------------------------------------------------------------------


def _pct_changed(text_before: str, text_after: str) -> float:
    """Return percentage of text that changed (0–100)."""
    if not text_before and not text_after:
        return 0.0
    if not text_before or not text_after:
        return 100.0
    ratio = difflib.SequenceMatcher(None, text_before, text_after, autojunk=False).ratio()
    return round((1.0 - ratio) * 100, 2)


def _pct_changed_bytes(before: SnapshotModel, after: FetchResult) -> float:
    """Rough byte-level change percentage based on SHA-256 inequality."""
    if before.body_hash_sha256 == after.body_hash_sha256:
        return 0.0
    # Without the old raw body we can only tell it changed, not by how much
    # Use content-length ratio as a proxy
    b_len = before.content_length or 1
    a_len = after.content_length or 1
    ratio = min(b_len, a_len) / max(b_len, a_len)
    return round((1.0 - ratio) * 100, 2)


def _is_gov_url(url: str) -> bool:
    """Return True if URL is on a .gov or .mil domain."""
    from urllib.parse import urlparse
    host = urlparse(url).netloc.lower()
    return host.endswith(".gov") or host.endswith(".mil")


def _generate_diff(
    text_before: str,
    text_after: str,
    label_before: str = "before",
    label_after: str = "after",
) -> str:
    """
    Generate a unified diff between two text strings.

    Result is truncated to _MAX_DIFF_BYTES to prevent bloat.
    """
    if not text_before and not text_after:
        return ""

    lines_before = text_before.splitlines(keepends=True)
    lines_after = text_after.splitlines(keepends=True)

    diff_lines = list(
        difflib.unified_diff(
            lines_before,
            lines_after,
            fromfile=label_before,
            tofile=label_after,
            lineterm="",
            n=3,
        )
    )

    diff_str = "\n".join(diff_lines)

    if len(diff_str.encode("utf-8")) > _MAX_DIFF_BYTES:
        diff_str = diff_str[:_MAX_DIFF_BYTES] + "\n... [diff truncated]"

    return diff_str


def compute_keyword_delta(
    keywords: list[str],
    text_before: str,
    text_after: str,
) -> dict[str, dict[str, int]]:
    """
    Return counts of each keyword in before/after text.

    Example return: {"disability": {"before": 12, "after": 0}}
    """
    result: dict[str, dict[str, int]] = {}
    before_lower = text_before.lower()
    after_lower = text_after.lower()
    for kw in keywords:
        kw_lower = kw.lower()
        result[kw] = {
            "before": before_lower.count(kw_lower),
            "after": after_lower.count(kw_lower),
        }
    return result
