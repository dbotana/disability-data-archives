"""
crawler.py — Async HTTP crawler with SHA-256 hashing, rate limiting, and retry logic.

Uses a single shared httpx.AsyncClient (NOT created per-request) to avoid
file descriptor leaks. Per-domain rate limiting prevents overloading servers.
robots.txt is cached per domain with a 24-hour TTL.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import logging
import re
import time
import urllib.robotparser
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from storage import FetchResult, TargetModel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration dataclass (populated from config.yaml via main.py)
# ---------------------------------------------------------------------------


@dataclass
class CrawlerConfig:
    min_delay_seconds: float = 2.0
    max_concurrent_requests: int = 5
    request_timeout_seconds: int = 30
    max_retries: int = 3
    retry_backoff_multiplier: float = 2.0
    user_agents: list[str] = field(default_factory=lambda: [
        "FederalDataMonitor/1.0 (Public Interest Research)",
    ])


# ---------------------------------------------------------------------------
# robots.txt cache entry
# ---------------------------------------------------------------------------


@dataclass
class _RobotsCacheEntry:
    parser: urllib.robotparser.RobotFileParser
    expires_at: float  # monotonic time


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------


class Crawler:
    """Async HTTP crawler. Must be used as an async context manager."""

    def __init__(self, config: CrawlerConfig) -> None:
        self._config = config
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._ua_cycle = itertools.cycle(config.user_agents)
        self._domain_last_fetch: dict[str, float] = {}
        self._robots_cache: dict[str, _RobotsCacheEntry] = {}
        self._robots_ttl: float = 86_400.0  # 24 hours

    async def __aenter__(self) -> "Crawler":
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent_requests)
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self._config.request_timeout_seconds),
            follow_redirects=True,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch(self, target: TargetModel) -> FetchResult:
        """Fetch a single target URL. Always returns a FetchResult (never raises)."""
        assert self._client is not None, "Crawler must be used as async context manager"
        assert self._semaphore is not None

        domain = _extract_domain(target.url)
        await self._enforce_rate_limit(domain)

        async with self._semaphore:
            try:
                result = await self._fetch_with_retry(target)
            except Exception as exc:
                logger.error(f"Unrecoverable fetch error for {target.url}: {exc}")
                result = _error_result(target, str(exc))

        self._domain_last_fetch[domain] = time.monotonic()
        return result

    async def fetch_all(self, targets: list[TargetModel]) -> list[FetchResult]:
        """Concurrently fetch all targets. Returns results in the same order."""
        tasks = [self.fetch(t) for t in targets]
        return list(await asyncio.gather(*tasks))

    # ------------------------------------------------------------------
    # Internal fetch with tenacity retry
    # ------------------------------------------------------------------

    async def _fetch_with_retry(self, target: TargetModel) -> FetchResult:
        """Inner fetch wrapped with tenacity retry for transient errors."""
        ua = next(self._ua_cycle)

        @retry(
            retry=retry_if_exception_type((httpx.TimeoutException, httpx.NetworkError)),
            wait=wait_exponential(
                multiplier=self._config.retry_backoff_multiplier, min=4, max=60
            ),
            stop=stop_after_attempt(self._config.max_retries),
            reraise=True,
        )
        async def _do_fetch() -> FetchResult:
            response = await self._client.get(  # type: ignore[union-attr]
                target.url,
                headers={
                    "User-Agent": ua,
                    "Accept": "text/html,application/xhtml+xml,application/json,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )

            # Handle explicit 429 with Retry-After
            if response.status_code == 429:
                retry_after = int(response.headers.get("retry-after", "60"))
                logger.warning(
                    f"Rate limited by {target.url}, waiting {retry_after}s"
                )
                await asyncio.sleep(retry_after)
                raise httpx.NetworkError("429 rate limit — retrying")

            return _build_result(target, response)

        return await _do_fetch()

    # ------------------------------------------------------------------
    # Domain rate limiting
    # ------------------------------------------------------------------

    async def _enforce_rate_limit(self, domain: str) -> None:
        """Sleep if necessary to respect per-domain minimum delay."""
        last = self._domain_last_fetch.get(domain, 0.0)
        elapsed = time.monotonic() - last
        delay = self._config.min_delay_seconds - elapsed
        if delay > 0:
            await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # robots.txt (informational — public interest research override)
    # ------------------------------------------------------------------

    async def _check_robots(self, url: str) -> bool:
        """Return True if robots.txt permits crawling. Cached per domain."""
        domain = _extract_domain(url)
        now = time.monotonic()
        entry = self._robots_cache.get(domain)

        if entry is None or now > entry.expires_at:
            robots_url = f"https://{domain}/robots.txt"
            parser = urllib.robotparser.RobotFileParser(robots_url)
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, parser.read)
            except Exception:
                # If robots.txt can't be fetched, assume allowed
                pass
            self._robots_cache[domain] = _RobotsCacheEntry(
                parser=parser,
                expires_at=now + self._robots_ttl,
            )

        ua = next(self._ua_cycle)
        return self._robots_cache[domain].parser.can_fetch(ua, url)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _extract_domain(url: str) -> str:
    """Extract netloc (domain) from a URL."""
    parsed = urlparse(url)
    return parsed.netloc.lower()


def _build_result(target: TargetModel, response: httpx.Response) -> FetchResult:
    """Build a FetchResult from an httpx response."""
    raw_body = response.content
    body_hash = hashlib.sha256(raw_body).hexdigest()

    # Extract visible text (strip scripts, styles, nav, footer)
    text_content = _extract_visible_text(raw_body, target.type)
    text_hash = hashlib.sha256(text_content.encode("utf-8", errors="replace")).hexdigest()

    # Row count detection
    row_count: Optional[int] = None
    if target.row_count_check:
        row_count = _estimate_row_count(raw_body, text_content, target.type)

    # Normalise headers to lowercase keys
    headers = {k.lower(): v for k, v in response.headers.items()}

    # Capture final URL after redirects
    redirect_url: Optional[str] = None
    if str(response.url) != target.url:
        redirect_url = str(response.url)

    return FetchResult(
        target_id=target.id,
        url=target.url,
        fetched_at=datetime.now(timezone.utc),
        http_status=response.status_code,
        content_length=len(raw_body),
        body_hash_sha256=body_hash,
        text_hash_sha256=text_hash,
        text_content=text_content,
        row_count=row_count,
        headers=headers,
        redirect_url=redirect_url,
        raw_body=raw_body,
    )


def _error_result(target: TargetModel, error_message: str) -> FetchResult:
    """Create a FetchResult representing a failed fetch (status 0)."""
    empty_hash = hashlib.sha256(b"").hexdigest()
    return FetchResult(
        target_id=target.id,
        url=target.url,
        fetched_at=datetime.now(timezone.utc),
        http_status=0,
        body_hash_sha256=empty_hash,
        text_hash_sha256=empty_hash,
        error_message=error_message,
    )


def _extract_visible_text(raw_body: bytes, content_type: str = "html") -> str:
    """Extract human-readable text from HTML, stripping noise elements."""
    if content_type == "json":
        try:
            text = raw_body.decode("utf-8", errors="replace")
            # For JSON, return the raw text (hash of structure)
            return text[:100_000]
        except Exception:
            return ""

    try:
        soup = BeautifulSoup(raw_body, "lxml")
    except Exception:
        try:
            soup = BeautifulSoup(raw_body, "html.parser")
        except Exception:
            return raw_body.decode("utf-8", errors="replace")[:100_000]

    # Remove non-content tags
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript",
                     "iframe", "svg", "img", "meta", "link"]):
        tag.decompose()

    text = soup.get_text(separator=" ", strip=True)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text[:100_000]  # Cap at 100KB for diffing


def _estimate_row_count(
    raw_body: bytes, text_content: str, content_type: str
) -> Optional[int]:
    """Estimate the number of data rows for tabular content."""
    if content_type == "html":
        try:
            soup = BeautifulSoup(raw_body, "lxml")
            rows = soup.find_all("tr")
            if rows:
                return max(0, len(rows) - 1)  # subtract header row
        except Exception:
            pass
        # Fallback: count list items as a proxy
        try:
            soup = BeautifulSoup(raw_body, "lxml")
            return len(soup.find_all("li"))
        except Exception:
            pass

    elif content_type == "json":
        import json as _json
        try:
            data = _json.loads(raw_body)
            if isinstance(data, list):
                return len(data)
            if isinstance(data, dict):
                # Try common wrapper keys
                for key in ("results", "data", "items", "records", "datasets"):
                    if key in data and isinstance(data[key], list):
                        return len(data[key])
                return len(data)
        except Exception:
            pass

    elif content_type == "csv":
        return max(0, text_content.count("\n") - 1)

    return None


def compute_keyword_counts(text: str, keywords: list[str]) -> dict[str, int]:
    """Count occurrences of each keyword (case-insensitive) in text."""
    text_lower = text.lower()
    return {kw: text_lower.count(kw.lower()) for kw in keywords}
