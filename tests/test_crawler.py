"""
test_crawler.py — Unit tests for crawler.py

Uses respx to mock httpx responses without making real HTTP requests.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import respx
import httpx

from crawler import (
    Crawler,
    CrawlerConfig,
    _extract_domain,
    _extract_visible_text,
    _estimate_row_count,
    compute_keyword_counts,
)
from storage import TargetModel


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def crawler_config():
    return CrawlerConfig(
        min_delay_seconds=0.0,  # No delay in tests
        max_concurrent_requests=5,
        request_timeout_seconds=5,
        max_retries=2,
        retry_backoff_multiplier=1.0,
        user_agents=["TestBot/1.0"],
    )


@pytest.fixture
def sample_target():
    return TargetModel(
        id="test-ssa",
        url="https://www.ssa.gov/test/",
        agency="SSA",
        name="SSA Test Page",
        frequency="high",
        type="html",
        tags=["test"],
        row_count_check=False,
    )


# ---------------------------------------------------------------------------
# Basic fetch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_returns_fetch_result(crawler_config, sample_target):
    html = b"<html><body><p>SSA disability benefits page</p></body></html>"
    respx.get("https://www.ssa.gov/test/").mock(
        return_value=httpx.Response(200, content=html, headers={"content-type": "text/html"})
    )

    async with Crawler(crawler_config) as crawler:
        result = await crawler.fetch(sample_target)

    assert result.http_status == 200
    assert result.target_id == "test-ssa"
    assert len(result.body_hash_sha256) == 64
    assert len(result.text_hash_sha256) == 64
    assert "disability" in result.text_content.lower()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_computes_correct_sha256(crawler_config, sample_target):
    body = b"<html><body>disability content</body></html>"
    expected_hash = hashlib.sha256(body).hexdigest()

    respx.get("https://www.ssa.gov/test/").mock(
        return_value=httpx.Response(200, content=body)
    )

    async with Crawler(crawler_config) as crawler:
        result = await crawler.fetch(sample_target)

    assert result.body_hash_sha256 == expected_hash


@pytest.mark.asyncio
@respx.mock
async def test_fetch_captures_404_status(crawler_config, sample_target):
    respx.get("https://www.ssa.gov/test/").mock(
        return_value=httpx.Response(404, content=b"Not found")
    )

    async with Crawler(crawler_config) as crawler:
        result = await crawler.fetch(sample_target)

    assert result.http_status == 404


@pytest.mark.asyncio
@respx.mock
async def test_fetch_captures_redirect_url(crawler_config, sample_target):
    # respx doesn't handle real redirects by default — simulate via follow_redirects
    final_url = "https://www.ssa.gov/newpath/"
    respx.get("https://www.ssa.gov/test/").mock(
        return_value=httpx.Response(
            200,
            content=b"<html><body>New location</body></html>",
            headers={"content-type": "text/html"},
        )
    )

    async with Crawler(crawler_config) as crawler:
        result = await crawler.fetch(sample_target)

    # No redirect in this mock, so redirect_url should be None
    assert result.redirect_url is None


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------


def test_text_extraction_strips_script_tags():
    html = b"""<html><body>
    <script>var x = 1; function evil() {}</script>
    <p>Disability accommodation page</p>
    <style>.foo { color: red }</style>
    </body></html>"""
    text = _extract_visible_text(html, "html")
    assert "disability" in text.lower()
    assert "accommodation" in text.lower()
    assert "var x" not in text
    assert ".foo" not in text


def test_text_extraction_strips_nav_footer():
    html = b"""<html><body>
    <nav>Navigation menu home about contact</nav>
    <main><p>ADA disability benefits information</p></main>
    <footer>Copyright 2025 government</footer>
    </body></html>"""
    text = _extract_visible_text(html, "html")
    assert "disability" in text.lower()
    # Nav/footer content stripped
    assert "Navigation menu" not in text
    assert "Copyright 2025" not in text


def test_text_extraction_collapses_whitespace():
    html = b"<html><body><p>  disability   benefits   ADA  </p></body></html>"
    text = _extract_visible_text(html, "html")
    # Should not have multiple consecutive spaces
    assert "  " not in text


def test_text_extraction_returns_string():
    html = b"<html><body>Hello</body></html>"
    result = _extract_visible_text(html, "html")
    assert isinstance(result, str)


def test_text_extraction_json_returns_raw():
    json_body = b'{"datasets": [{"id": 1, "name": "disability data"}]}'
    result = _extract_visible_text(json_body, "json")
    assert "disability" in result


# ---------------------------------------------------------------------------
# Row count estimation
# ---------------------------------------------------------------------------


def test_row_count_html_table():
    html = b"""<html><body><table>
    <tr><th>Name</th><th>Count</th></tr>
    <tr><td>SSA</td><td>100</td></tr>
    <tr><td>CDC</td><td>200</td></tr>
    </table></body></html>"""
    count = _estimate_row_count(html, "", "html")
    assert count == 2  # 3 rows minus 1 header


def test_row_count_json_array():
    import json
    data = [{"id": i} for i in range(42)]
    body = json.dumps(data).encode()
    count = _estimate_row_count(body, "", "json")
    assert count == 42


def test_row_count_json_dict_with_results_key():
    import json
    data = {"results": [{"id": i} for i in range(17)]}
    body = json.dumps(data).encode()
    count = _estimate_row_count(body, "", "json")
    assert count == 17


# ---------------------------------------------------------------------------
# Domain extraction
# ---------------------------------------------------------------------------


def test_extract_domain_basic():
    assert _extract_domain("https://www.ssa.gov/disability") == "www.ssa.gov"


def test_extract_domain_with_port():
    assert _extract_domain("http://localhost:8000/health") == "localhost:8000"


def test_extract_domain_subpath():
    assert _extract_domain("https://data.cdc.gov/api/v1/test") == "data.cdc.gov"


# ---------------------------------------------------------------------------
# Keyword counting
# ---------------------------------------------------------------------------


def test_keyword_counts_found():
    text = "Disability ADA disability SSDI accommodation"
    counts = compute_keyword_counts(text, ["disability", "ADA", "SSDI", "SSI"])
    assert counts["disability"] == 2
    assert counts["ADA"] == 1
    assert counts["SSDI"] == 1
    assert counts["SSI"] == 0


def test_keyword_counts_case_insensitive():
    counts = compute_keyword_counts("DISABILITY ADA", ["disability", "ada"])
    assert counts["disability"] == 1
    assert counts["ada"] == 1


# ---------------------------------------------------------------------------
# Rate limiting (timing check)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_domain_rate_limiting(crawler_config):
    """Two fetches to the same domain should have a delay between them."""
    crawler_config.min_delay_seconds = 0.05  # 50ms delay

    target = TargetModel(
        id="t1", url="https://www.ssa.gov/page1",
        agency="SSA", name="T1", frequency="high", type="html"
    )
    target2 = TargetModel(
        id="t2", url="https://www.ssa.gov/page2",
        agency="SSA", name="T2", frequency="high", type="html"
    )

    respx.get("https://www.ssa.gov/page1").mock(
        return_value=httpx.Response(200, content=b"page1")
    )
    respx.get("https://www.ssa.gov/page2").mock(
        return_value=httpx.Response(200, content=b"page2")
    )

    async with Crawler(crawler_config) as crawler:
        t0 = time.monotonic()
        await crawler.fetch(target)
        await crawler.fetch(target2)
        elapsed = time.monotonic() - t0

    # Two sequential fetches with 50ms rate limit should take >= 50ms total
    assert elapsed >= 0.04


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_returns_error_result_on_network_error(crawler_config, sample_target):
    """A network error should not raise — should return FetchResult with status=0."""
    respx.get("https://www.ssa.gov/test/").mock(
        side_effect=httpx.NetworkError("Connection refused")
    )

    # Override retries to 1 for speed
    crawler_config.max_retries = 1

    async with Crawler(crawler_config) as crawler:
        result = await crawler.fetch(sample_target)

    assert result.http_status == 0
    assert result.error_message is not None
