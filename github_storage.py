"""
github_storage.py — GitHub-backed persistence layer.

Replaces the local SQLite DB for storing run results. Each monitoring run:
  1. Fetches current data/changes.csv and data/snapshots.csv from the repo via GitHub API
  2. Appends new changes detected in this run
  3. Commits the updated files back to the repo in a single commit

No local DB required. The repo IS the database.

Uses the GitHub Contents API (no extra dependencies — just httpx which is already required).
Requires a GitHub Personal Access Token with `contents: write` scope, stored as the
GITHUB_TOKEN secret (automatically provided in GitHub Actions, or set manually for local runs).

File layout in the repo:
  data/
    changes.csv          — append-only log of all detected changes
    snapshots.csv        — latest snapshot state per target URL
    feed.xml             — RSS 2.0 feed (latest 200 items)
    digests/
      YYYY-MM-DD.md      — daily digest markdown
      YYYY-MM-DD.html    — daily digest HTML
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CSV column definitions — changes.csv
# ---------------------------------------------------------------------------

CHANGES_COLUMNS = [
    "change_id",
    "target_id",
    "target_name",
    "target_agency",
    "target_url",
    "detected_at",
    "severity",
    "change_type",
    "pct_text_changed",
    "pct_content_changed",
    "row_count_before",
    "row_count_after",
    "semantic_similarity",
    "semantic_labels",
    "keywords_removed",
    "diff_text_preview",   # First 500 chars of diff only
    "notes",
]

# CSV column definitions — snapshots.csv
SNAPSHOTS_COLUMNS = [
    "target_id",
    "target_name",
    "target_agency",
    "target_url",
    "last_checked_at",
    "last_status",
    "body_hash_sha256",
    "text_hash_sha256",
    "content_length",
    "row_count",
    "etag",
    "last_modified",
    "redirect_url",
    "consecutive_errors",
]


# ---------------------------------------------------------------------------
# GitHubStorageBackend
# ---------------------------------------------------------------------------


class GitHubStorageBackend:
    """
    Read/write persistent data to GitHub via the Contents API.

    All file reads happen at the start of a run (to get current state).
    All file writes happen in a single batch commit at the end of a run.

    Thread-safe for single-process use (GitHub Actions is single-job).
    """

    def __init__(
        self,
        repo: str,              # "owner/repo-name"
        branch: str = "main",
        token: Optional[str] = None,
        data_dir: str = "data",
    ) -> None:
        self._repo = repo
        self._branch = branch
        self._token = token or os.environ.get("GITHUB_TOKEN", "")
        self._data_dir = data_dir
        self._api_base = "https://api.github.com"

        if not self._token:
            raise ValueError(
                "GITHUB_TOKEN environment variable is required for GitHub storage. "
                "In GitHub Actions it is provided automatically. "
                "For local runs, create a PAT with 'contents: write' scope."
            )

        # In-memory state loaded from GitHub at startup
        self._existing_changes: list[dict] = []
        self._existing_snapshots: dict[str, dict] = {}  # target_id → snapshot row
        self._file_shas: dict[str, Optional[str]] = {}  # path → GitHub blob SHA (for updates)
        self._new_changes: list[dict] = []   # Changes detected in this run
        self._loaded = False

    # ------------------------------------------------------------------
    # Initialization — load current state from GitHub
    # ------------------------------------------------------------------

    async def load(self) -> None:
        """Pull current CSV files from GitHub into memory."""
        async with httpx.AsyncClient(timeout=30) as client:
            self._existing_changes = await self._read_csv(
                client, f"{self._data_dir}/changes.csv", CHANGES_COLUMNS
            )
            snapshots_list = await self._read_csv(
                client, f"{self._data_dir}/snapshots.csv", SNAPSHOTS_COLUMNS
            )
            self._existing_snapshots = {r["target_id"]: r for r in snapshots_list}

        self._loaded = True
        logger.info(
            f"Loaded {len(self._existing_changes)} existing changes, "
            f"{len(self._existing_snapshots)} existing snapshots from GitHub"
        )

    async def _read_csv(
        self, client: httpx.AsyncClient, path: str, expected_columns: list[str]
    ) -> list[dict]:
        """Fetch a CSV file from the GitHub repo. Returns [] if file doesn't exist yet."""
        url = f"{self._api_base}/repos/{self._repo}/contents/{path}"
        response = await client.get(
            url,
            headers=self._headers(),
            params={"ref": self._branch},
        )

        if response.status_code == 404:
            logger.info(f"GitHub file not found (first run?): {path}")
            self._file_shas[path] = None
            return []

        if response.status_code != 200:
            logger.warning(f"Failed to read {path} from GitHub: {response.status_code}")
            self._file_shas[path] = None
            return []

        data = response.json()
        self._file_shas[path] = data.get("sha")
        content_b64 = data.get("content", "")
        content = base64.b64decode(content_b64).decode("utf-8")

        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)
        # Fill missing columns with empty string for schema evolution compatibility
        for row in rows:
            for col in expected_columns:
                row.setdefault(col, "")
        return rows

    # ------------------------------------------------------------------
    # Latest snapshot retrieval (used by diff_engine to get prior state)
    # ------------------------------------------------------------------

    def get_latest_snapshot(self, target_id: str) -> Optional[dict]:
        """Return the most recent snapshot dict for a target, or None."""
        return self._existing_snapshots.get(target_id)

    def get_all_snapshots(self) -> dict[str, dict]:
        return dict(self._existing_snapshots)

    def get_all_changes(self) -> list[dict]:
        return list(self._existing_changes)

    # ------------------------------------------------------------------
    # Recording new results (in-memory, committed at end of run)
    # ------------------------------------------------------------------

    def record_snapshot(self, snapshot: dict) -> None:
        """Update the snapshot record for a target (upsert by target_id)."""
        self._existing_snapshots[snapshot["target_id"]] = snapshot

    def record_change(self, change: dict) -> None:
        """Append a new change record."""
        self._new_changes.append(change)
        self._existing_changes.append(change)
        logger.info(
            f"Recorded change: [{change.get('severity')}] {change.get('target_id')} "
            f"({change.get('change_type')})"
        )

    # ------------------------------------------------------------------
    # Commit — write all updates to GitHub in one commit
    # ------------------------------------------------------------------

    async def commit_results(self, run_summary: str = "") -> bool:
        """
        Write all accumulated changes and updated snapshots to GitHub.

        Returns True on success, False on failure.
        """
        if not self._new_changes and not self._existing_snapshots:
            logger.info("No new data to commit to GitHub")
            return True

        files_to_commit: dict[str, str] = {}

        # changes.csv — full file (new rows appended)
        changes_csv = _render_csv(self._existing_changes, CHANGES_COLUMNS)
        files_to_commit[f"{self._data_dir}/changes.csv"] = changes_csv

        # snapshots.csv — full file (upserted by target_id)
        snapshots_list = list(self._existing_snapshots.values())
        snapshots_csv = _render_csv(snapshots_list, SNAPSHOTS_COLUMNS)
        files_to_commit[f"{self._data_dir}/snapshots.csv"] = snapshots_csv

        # Commit message
        n_new = len(self._new_changes)
        n_critical = sum(1 for c in self._new_changes if c.get("severity") == "CRITICAL")
        n_high = sum(1 for c in self._new_changes if c.get("severity") == "HIGH")

        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        if n_new == 0:
            message = f"monitor: no changes detected ({now})"
        else:
            sev_str = ""
            if n_critical:
                sev_str += f" {n_critical} CRITICAL"
            if n_high:
                sev_str += f" {n_high} HIGH"
            message = f"monitor: {n_new} change(s) detected{sev_str} ({now})"

        if run_summary:
            message += f"\n\n{run_summary}"

        # Use GitHub Tree API for multi-file atomic commit
        success = await self._commit_tree(files_to_commit, message)

        if success:
            logger.info(f"Committed {len(files_to_commit)} files to GitHub: {message}")
        else:
            logger.error("Failed to commit results to GitHub")

        return success

    async def commit_digest(self, date_str: str, markdown: str, html: str) -> None:
        """Commit daily digest files to data/digests/."""
        files = {
            f"{self._data_dir}/digests/{date_str}.md": markdown,
            f"{self._data_dir}/digests/{date_str}.html": html,
        }
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        await self._commit_tree(files, f"monitor: daily digest {date_str} ({now})")

    async def commit_rss(self, rss_xml: str) -> None:
        """Commit RSS feed update."""
        files = {f"{self._data_dir}/feed.xml": rss_xml}
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        await self._commit_tree(files, f"monitor: RSS feed update ({now})")

    # ------------------------------------------------------------------
    # GitHub Tree API — multi-file atomic commit
    # ------------------------------------------------------------------

    async def _commit_tree(self, files: dict[str, str], message: str) -> bool:
        """
        Create a multi-file commit using the Git Data API (tree + commit + ref update).

        This is the correct way to commit multiple files atomically without
        making a separate API call per file.

        If the ref moved between reading HEAD and updating it (race condition with
        the Actions checkout commit), we retry once on 422 using the updated HEAD
        as the new parent. Our data is always complete so this is safe.
        """
        async with httpx.AsyncClient(timeout=60) as client:
            for attempt in range(2):
                # 1. Get current HEAD SHA (re-read on retry to get latest)
                ref_url = f"{self._api_base}/repos/{self._repo}/git/refs/heads/{self._branch}"
                resp = await client.get(ref_url, headers=self._headers())
                if resp.status_code != 200:
                    logger.error(f"Failed to get HEAD ref: {resp.status_code} {resp.text[:200]}")
                    return False
                head_sha = resp.json()["object"]["sha"]

                # 2. Get base tree SHA from HEAD commit
                commit_url = f"{self._api_base}/repos/{self._repo}/git/commits/{head_sha}"
                resp = await client.get(commit_url, headers=self._headers())
                if resp.status_code != 200:
                    logger.error(f"Failed to get HEAD commit: {resp.status_code}")
                    return False
                base_tree_sha = resp.json()["tree"]["sha"]

                # 3. Build tree blobs
                tree_items = [
                    {"path": path, "mode": "100644", "type": "blob", "content": content}
                    for path, content in files.items()
                ]

                # 4. Create new tree
                tree_url = f"{self._api_base}/repos/{self._repo}/git/trees"
                resp = await client.post(
                    tree_url,
                    headers=self._headers(),
                    json={"base_tree": base_tree_sha, "tree": tree_items},
                )
                if resp.status_code not in (200, 201):
                    logger.error(f"Failed to create tree: {resp.status_code} {resp.text[:200]}")
                    return False
                new_tree_sha = resp.json()["sha"]

                # 5. Create commit
                commit_create_url = f"{self._api_base}/repos/{self._repo}/git/commits"
                resp = await client.post(
                    commit_create_url,
                    headers=self._headers(),
                    json={
                        "message": message,
                        "tree": new_tree_sha,
                        "parents": [head_sha],
                        "author": {
                            "name": "Disability Data Monitor",
                            "email": "monitor@noreply.github.com",
                            "date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        },
                    },
                )
                if resp.status_code not in (200, 201):
                    logger.error(f"Failed to create commit: {resp.status_code} {resp.text[:200]}")
                    return False
                new_commit_sha = resp.json()["sha"]

                # 6. Update branch ref
                resp = await client.patch(
                    ref_url,
                    headers=self._headers(),
                    json={"sha": new_commit_sha},
                )
                if resp.status_code in (200, 201):
                    return True

                if resp.status_code == 422 and attempt == 0:
                    # Ref moved under us — retry once with the fresh HEAD
                    logger.warning(
                        "Ref not a fast-forward (422); retrying commit with updated HEAD"
                    )
                    continue

                logger.error(f"Failed to update ref: {resp.status_code} {resp.text[:200]}")
                return False

        return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    @property
    def new_changes(self) -> list[dict]:
        return list(self._new_changes)

    @property
    def is_loaded(self) -> bool:
        return self._loaded


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _render_csv(rows: list[dict], columns: list[str]) -> str:
    """Render a list of dicts to a CSV string with a fixed column order."""
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=columns, extrasaction="ignore", lineterminator="\n"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in columns})
    return buf.getvalue()


def fetch_result_to_snapshot_row(
    result: Any,
    target_name: str,
    target_agency: str,
    target_url: str,
    consecutive_errors: int = 0,
) -> dict:
    """Convert a FetchResult to a snapshots.csv row dict."""
    return {
        "target_id": result.target_id,
        "target_name": target_name,
        "target_agency": target_agency,
        "target_url": target_url,
        "last_checked_at": result.fetched_at.isoformat() if result.fetched_at else "",
        "last_status": str(result.http_status),
        "body_hash_sha256": result.body_hash_sha256,
        "text_hash_sha256": result.text_hash_sha256,
        "content_length": str(result.content_length or ""),
        "row_count": str(result.row_count or ""),
        "etag": result.headers.get("etag", "") if result.headers else "",
        "last_modified": result.headers.get("last-modified", "") if result.headers else "",
        "redirect_url": result.redirect_url or "",
        "consecutive_errors": str(consecutive_errors),
    }


def change_to_row(
    change: Any,
    target_name: str,
    target_agency: str,
    target_url: str,
    change_id: int,
) -> dict:
    """Convert a ChangeModel to a changes.csv row dict."""
    return {
        "change_id": str(change_id),
        "target_id": change.target_id,
        "target_name": target_name,
        "target_agency": target_agency,
        "target_url": target_url,
        "detected_at": change.detected_at.isoformat() if change.detected_at else "",
        "severity": change.severity,
        "change_type": change.change_type,
        "pct_text_changed": f"{change.pct_text_changed:.1f}" if change.pct_text_changed is not None else "",
        "pct_content_changed": f"{change.pct_content_changed:.1f}" if change.pct_content_changed is not None else "",
        "row_count_before": str(change.row_count_before or ""),
        "row_count_after": str(change.row_count_after or ""),
        "semantic_similarity": f"{change.semantic_similarity:.3f}" if change.semantic_similarity is not None else "",
        "semantic_labels": "|".join(change.semantic_labels) if change.semantic_labels else "",
        "keywords_removed": "|".join(getattr(change, "keywords_removed", [])),
        "diff_text_preview": (change.diff_text or "")[:500],
        "notes": change.notes or "",
    }


def snapshot_row_to_snapshot_model(row: dict) -> Any:
    """
    Convert a snapshots.csv row back to a SnapshotModel-compatible object.
    Returns a simple namespace so diff_engine.compare() can read it.
    """
    import hashlib
    from datetime import datetime, timezone
    from types import SimpleNamespace

    def _safe_int(v: str) -> Optional[int]:
        try:
            return int(v) if v else None
        except (ValueError, TypeError):
            return None

    def _safe_dt(v: str) -> Optional[datetime]:
        try:
            return datetime.fromisoformat(v) if v else None
        except (ValueError, TypeError):
            return None

    return SimpleNamespace(
        id=0,
        target_id=row.get("target_id", ""),
        fetched_at=_safe_dt(row.get("last_checked_at", "")) or datetime.now(timezone.utc),
        http_status=_safe_int(row.get("last_status", "200")) or 200,
        content_length=_safe_int(row.get("content_length", "")),
        body_hash_sha256=row.get("body_hash_sha256", hashlib.sha256(b"").hexdigest()),
        text_hash_sha256=row.get("text_hash_sha256", hashlib.sha256(b"").hexdigest()),
        text_content=None,  # Not stored in snapshots.csv (saves space)
        row_count=_safe_int(row.get("row_count", "")),
        headers_json=None,
        archive_path=None,
        etag=row.get("etag") or None,
        last_modified=row.get("last_modified") or None,
        redirect_url=row.get("redirect_url") or None,
        is_baseline=False,
        error_message=None,
    )
