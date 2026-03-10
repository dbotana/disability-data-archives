"""
wayback.py — Internet Archive (Wayback Machine) integration.

Two operations:
  1. Save API  — submit a URL for archival when a CRITICAL change is detected.
  2. CDX API   — verify a URL has been captured; query snapshot history.

Rate limit: Wayback Save API allows ~10–15 requests/min for authenticated users.
Enforce via a per-domain rate limiter (same pattern as crawler.py).
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote_plus

import httpx

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class WaybackConfig:
    enabled: bool = True
    save_api_base: str = "https://web.archive.org/save"
    cdx_api_base: str = "http://web.archive.org/cdx/search/cdx"
    availability_api: str = "https://archive.org/wayback/available"
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    submit_on_critical: bool = True
    submit_rate_limit_per_minute: int = 10
    verify_after_hours: int = 24


# ---------------------------------------------------------------------------
# WaybackClient
# ---------------------------------------------------------------------------


class WaybackClient:
    """Async Wayback Machine client for Save and CDX operations."""

    def __init__(self, config: WaybackConfig) -> None:
        self._config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._last_save_time: float = 0.0
        self._save_interval: float = 60.0 / max(1, config.submit_rate_limit_per_minute)

    async def __aenter__(self) -> "WaybackClient":
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0),
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Save API
    # ------------------------------------------------------------------

    async def submit_url(self, url: str) -> dict:
        """
        Submit a URL to the Wayback Machine Save API.

        Returns a dict with keys: status, wayback_url, error.
        """
        if not self._config.enabled:
            return {"status": "disabled", "wayback_url": None, "error": None}

        assert self._client is not None

        # Enforce rate limit
        await self._enforce_save_rate_limit()

        save_url = f"{self._config.save_api_base}/{url}"

        headers: dict[str, str] = {
            "User-Agent": "FederalDataMonitor/1.0 (Public Interest Research)",
        }
        if self._config.access_key and self._config.secret_key:
            headers["Authorization"] = (
                f"LOW {self._config.access_key}:{self._config.secret_key}"
            )

        try:
            response = await self._client.post(save_url, headers=headers)
            self._last_save_time = time.monotonic()

            if response.status_code in (200, 201, 302):
                # Wayback returns the archive URL in Content-Location header
                archive_url = response.headers.get("Content-Location", "")
                if archive_url and not archive_url.startswith("http"):
                    archive_url = f"https://web.archive.org{archive_url}"

                logger.info(f"Submitted to Wayback: {url} → {archive_url}")
                return {
                    "status": "submitted",
                    "wayback_url": archive_url or None,
                    "error": None,
                }

            logger.warning(
                f"Wayback Save API returned {response.status_code} for {url}"
            )
            return {
                "status": "failed",
                "wayback_url": None,
                "error": f"HTTP {response.status_code}: {response.text[:200]}",
            }

        except httpx.TimeoutException:
            logger.error(f"Wayback Save API timed out for {url}")
            return {"status": "failed", "wayback_url": None, "error": "timeout"}
        except Exception as e:
            logger.error(f"Wayback Save API error for {url}: {e}")
            return {"status": "failed", "wayback_url": None, "error": str(e)}

    # ------------------------------------------------------------------
    # CDX API
    # ------------------------------------------------------------------

    async def verify_capture(self, url: str) -> bool:
        """
        Check if a URL has at least one capture in the Wayback Machine.

        Returns True if a capture exists, False otherwise.
        """
        assert self._client is not None

        try:
            response = await self._client.get(
                self._config.cdx_api_base,
                params={
                    "url": url,
                    "output": "json",
                    "limit": "1",
                    "fl": "timestamp,statuscode",
                },
            )
            if response.status_code != 200:
                return False
            data = response.json()
            # First row is the header ["timestamp", "statuscode"]
            return len(data) > 1
        except Exception as e:
            logger.error(f"CDX verification failed for {url}: {e}")
            return False

    async def get_snapshot_history(
        self, url: str, limit: int = 20
    ) -> list[dict]:
        """
        Retrieve the snapshot history for a URL from the CDX API.

        Returns list of dicts with keys: timestamp, statuscode, url.
        """
        assert self._client is not None

        try:
            response = await self._client.get(
                self._config.cdx_api_base,
                params={
                    "url": url,
                    "output": "json",
                    "limit": str(limit),
                    "fl": "timestamp,statuscode,original",
                    "collapse": "timestamp:8",  # One snapshot per day
                },
            )
            if response.status_code != 200:
                return []

            data = response.json()
            if not data or len(data) <= 1:
                return []

            # Skip header row
            keys = data[0]
            return [dict(zip(keys, row)) for row in data[1:]]

        except Exception as e:
            logger.error(f"CDX history fetch failed for {url}: {e}")
            return []

    async def check_availability(self, url: str) -> Optional[str]:
        """
        Use the Availability API to find the closest snapshot to now.

        Returns the Wayback URL if available, else None.
        """
        assert self._client is not None

        try:
            response = await self._client.get(
                self._config.availability_api,
                params={"url": url},
            )
            if response.status_code != 200:
                return None
            data = response.json()
            snapshot = data.get("archived_snapshots", {}).get("closest", {})
            if snapshot.get("available"):
                return snapshot.get("url")
            return None
        except Exception as e:
            logger.error(f"Availability check failed for {url}: {e}")
            return None

    async def was_in_end_of_term_archive(self, url: str) -> bool:
        """
        Check if URL was captured in the End of Term 2024 Web Archive
        (October 2024 – January 20, 2025).
        """
        assert self._client is not None

        try:
            response = await self._client.get(
                self._config.cdx_api_base,
                params={
                    "url": url,
                    "output": "json",
                    "limit": "1",
                    "fl": "timestamp,statuscode",
                    "from": "20241001",
                    "to": "20250120",
                    "filter": "statuscode:200",
                },
            )
            if response.status_code != 200:
                return False
            data = response.json()
            return len(data) > 1
        except Exception as e:
            logger.error(f"End-of-term archive check failed for {url}: {e}")
            return False

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    async def _enforce_save_rate_limit(self) -> None:
        """Sleep if necessary to respect Wayback Save API rate limit."""
        elapsed = time.monotonic() - self._last_save_time
        delay = self._save_interval - elapsed
        if delay > 0:
            logger.debug(f"Wayback rate limit: sleeping {delay:.1f}s")
            await asyncio.sleep(delay)
