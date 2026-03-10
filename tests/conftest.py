"""
conftest.py — Shared pytest fixtures for all test modules.

Provides:
  - async_db: in-memory SQLite StorageManager
  - mock_semantic: SemanticAnalyzer with mocked models
  - sample_target: a TargetModel fixture
  - sample_snapshot: a SnapshotModel fixture
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from storage import Base, SnapshotModel, StorageManager, TargetModel


# ---------------------------------------------------------------------------
# pytest-asyncio mode
# ---------------------------------------------------------------------------

pytest_plugins = ("pytest_asyncio",)


# ---------------------------------------------------------------------------
# In-memory SQLite database
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def async_db() -> AsyncGenerator[StorageManager, None]:
    """Provide a fresh in-memory StorageManager for each test."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    storage = StorageManager(engine, archive_dir="/tmp/test_archive")
    yield storage
    await engine.dispose()


# ---------------------------------------------------------------------------
# Sample target
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_target() -> TargetModel:
    return TargetModel(
        id="test-ssa-press",
        url="https://www.ssa.gov/news/press/releases/",
        agency="SSA",
        name="SSA Press Releases (test)",
        frequency="high",
        type="html",
        tags=["benefits", "test"],
        row_count_check=False,
    )


@pytest.fixture
def sample_target_row_count() -> TargetModel:
    return TargetModel(
        id="test-cdc-soda",
        url="https://data.cdc.gov/api/disability",
        agency="CDC",
        name="CDC SODA Disability (test)",
        frequency="high",
        type="json",
        tags=["statistics"],
        row_count_check=True,
    )


# ---------------------------------------------------------------------------
# Sample snapshots
# ---------------------------------------------------------------------------

_SAMPLE_TEXT = (
    "Disability benefits provide essential support to millions of Americans. "
    "The ADA and Section 504 protect the rights of disabled individuals. "
    "SSI and SSDI programs are administered by the Social Security Administration."
)

_SAMPLE_HTML = f"<html><body><p>{_SAMPLE_TEXT}</p></body></html>".encode("utf-8")


@pytest.fixture
def sample_snapshot() -> SnapshotModel:
    text = _SAMPLE_TEXT
    body = _SAMPLE_HTML
    return SnapshotModel(
        id=1,
        target_id="test-ssa-press",
        fetched_at=datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc),
        http_status=200,
        content_length=len(body),
        body_hash_sha256=hashlib.sha256(body).hexdigest(),
        text_hash_sha256=hashlib.sha256(text.encode()).hexdigest(),
        text_content=text,
        row_count=None,
        headers_json='{"content-type": "text/html"}',
        etag='"abc123"',
        last_modified="Wed, 15 Jan 2025 10:00:00 GMT",
        redirect_url=None,
        is_baseline=True,
    )


@pytest.fixture
def sample_snapshot_with_rows() -> SnapshotModel:
    text = "Dataset with 100 rows of disability data."
    body = text.encode()
    return SnapshotModel(
        id=2,
        target_id="test-cdc-soda",
        fetched_at=datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc),
        http_status=200,
        content_length=len(body),
        body_hash_sha256=hashlib.sha256(body).hexdigest(),
        text_hash_sha256=hashlib.sha256(text.encode()).hexdigest(),
        text_content=text,
        row_count=100,
        headers_json="{}",
        etag=None,
        last_modified=None,
        redirect_url=None,
        is_baseline=True,
    )


# ---------------------------------------------------------------------------
# Mock SemanticAnalyzer
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_semantic():
    """Return a SemanticAnalyzer with mocked model loading."""
    with patch("semantic_analyzer.SemanticAnalyzer.load_models_sync"):
        from semantic_analyzer import NLPConfig, SemanticAnalyzer

        cfg = NLPConfig(
            zero_shot_enabled=False,
            disability_keywords=[
                "disability", "ADA", "Section 504", "SSDI", "SSI",
                "accommodation", "accessibility",
            ],
        )
        analyzer = SemanticAnalyzer(cfg)
        # Mock the embedder to return a fixed numpy array
        try:
            import numpy as np
            mock_model = MagicMock()
            mock_model.encode.return_value = np.array([0.1, 0.2, 0.3])
            analyzer._embedder = mock_model
        except ImportError:
            analyzer._embedder = MagicMock()
        analyzer._loaded = True
        yield analyzer
        analyzer.shutdown()
