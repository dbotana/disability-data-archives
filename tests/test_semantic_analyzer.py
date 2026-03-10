"""
test_semantic_analyzer.py — Unit tests for semantic_analyzer.py

Models are mocked so tests run without downloading sentence-transformers.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from semantic_analyzer import NLPConfig, SemanticAnalyzer, SIMILARITY_HIGH, SIMILARITY_MODERATE


# ---------------------------------------------------------------------------
# Fixture: analyzer with mocked models
# ---------------------------------------------------------------------------


@pytest.fixture
def analyzer():
    cfg = NLPConfig(
        zero_shot_enabled=False,
        disability_keywords=[
            "disability", "ADA", "Section 504", "SSDI", "SSI",
            "accommodation", "accessibility", "wheelchair", "blind",
            "intellectual disability", "long COVID",
        ],
    )
    a = SemanticAnalyzer(cfg)
    a._loaded = True

    # Mock embedder: encode returns a predictable numpy-like array
    try:
        import numpy as np

        mock_model = MagicMock()
        # Default: return same vector for both → similarity = 1.0
        mock_model.encode.return_value = np.array([1.0, 0.0, 0.0])
        a._embedder = mock_model
    except ImportError:
        a._embedder = MagicMock()

    return a


# ---------------------------------------------------------------------------
# Keyword scanning (no model required)
# ---------------------------------------------------------------------------


def test_keyword_scan_finds_known_terms(analyzer):
    text = "The ADA and Section 504 protect disability rights."
    found = analyzer.keyword_scan(text)
    assert "ADA" in found
    assert "Section 504" in found
    assert "disability" in found


def test_keyword_scan_case_insensitive(analyzer):
    text = "DISABILITY accommodation WHEELCHAIR"
    found = analyzer.keyword_scan(text)
    assert "disability" in found
    assert "accommodation" in found
    assert "wheelchair" in found


def test_keyword_scan_returns_empty_for_unrelated_text(analyzer):
    text = "Stock market analysis of technology companies in Q4."
    found = analyzer.keyword_scan(text)
    assert len(found) == 0


def test_keyword_counts_sums_occurrences(analyzer):
    text = "disability disability disability ADA"
    counts = analyzer.keyword_counts(text)
    assert counts["disability"] == 3
    assert counts["ADA"] == 1
    assert counts.get("SSI", 0) == 0


def test_keyword_scan_multiword_term(analyzer):
    text = "patients with intellectual disability require special support"
    found = analyzer.keyword_scan(text)
    assert "intellectual disability" in found


# ---------------------------------------------------------------------------
# Similarity (mocked model)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_similarity_returns_float(analyzer):
    try:
        import numpy as np
        from sentence_transformers import util as st_util

        # Mock cos_sim to return a mock with .item()
        mock_result = MagicMock()
        mock_result.item.return_value = 0.85
        with patch("semantic_analyzer.util") as mock_util:
            mock_util.cos_sim.return_value = mock_result
            sim = await analyzer.compute_similarity("text A", "text B")
        assert sim is not None
        assert 0.0 <= sim <= 1.0
    except ImportError:
        pytest.skip("sentence_transformers not installed")


@pytest.mark.asyncio
async def test_similarity_returns_none_when_no_model():
    cfg = NLPConfig(zero_shot_enabled=False)
    a = SemanticAnalyzer(cfg)
    a._embedder = None
    result = await a.compute_similarity("text A", "text B")
    assert result is None


def test_similarity_interpretation_high(analyzer):
    assert analyzer.interpret_similarity(0.95) == "formatting_change"


def test_similarity_interpretation_moderate(analyzer):
    assert analyzer.interpret_similarity(0.72) == "semantic_substitution"


def test_similarity_interpretation_low(analyzer):
    assert analyzer.interpret_similarity(0.30) == "semantic_removal"


def test_similarity_interpretation_boundary_high(analyzer):
    assert analyzer.interpret_similarity(SIMILARITY_HIGH) == "formatting_change"


def test_similarity_interpretation_boundary_moderate(analyzer):
    assert analyzer.interpret_similarity(SIMILARITY_MODERATE) == "semantic_substitution"


# ---------------------------------------------------------------------------
# Zero-shot classification (disabled — should return empty list)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_classify_returns_empty_when_disabled(analyzer):
    analyzer._classifier = None
    labels = await analyzer.classify_change("Some diff text here")
    assert labels == []


@pytest.mark.asyncio
async def test_classify_returns_empty_for_empty_diff(analyzer):
    labels = await analyzer.classify_change("")
    assert labels == []


# ---------------------------------------------------------------------------
# Text truncation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_similarity_truncates_to_5000_chars(analyzer):
    """Verify that encode() is called with text no longer than 5000 chars."""
    try:
        import numpy as np
        from sentence_transformers import util as st_util

        long_text = "disability " * 1000  # 11,000 chars
        mock_result = MagicMock()
        mock_result.item.return_value = 1.0

        encode_calls = []

        def capture_encode(text, **kwargs):
            encode_calls.append(text)
            return np.array([1.0, 0.0, 0.0])

        analyzer._embedder.encode.side_effect = capture_encode

        with patch("semantic_analyzer.util") as mock_util:
            mock_util.cos_sim.return_value = mock_result
            await analyzer.compute_similarity(long_text, long_text)

        for call_text in encode_calls:
            assert len(call_text) <= 5000
    except ImportError:
        pytest.skip("sentence_transformers not installed")


# ---------------------------------------------------------------------------
# Full analysis pipeline
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_analyze_change_returns_expected_keys(analyzer):
    """analyze_change should always return the expected dict structure."""
    analyzer._embedder = None  # Force similarity to None

    result = await analyzer.analyze_change(
        text_before="disability ADA accommodation",
        text_after="employment workforce",
        diff_text="-disability\n+employment",
    )

    assert "semantic_similarity" in result
    assert "semantic_labels" in result
    assert "interpretation" in result
    assert "keywords_removed" in result
    assert "keywords_added" in result
    assert isinstance(result["semantic_labels"], list)
    assert isinstance(result["keywords_removed"], list)


@pytest.mark.asyncio
async def test_analyze_change_detects_keyword_removal(analyzer):
    """Keywords present in before text but absent in after should appear in keywords_removed."""
    analyzer._embedder = None

    result = await analyzer.analyze_change(
        text_before="disability ADA accommodation accessibility",
        text_after="workforce employment initiative",
    )

    assert "disability" in result["keywords_removed"]
    assert "ADA" in result["keywords_removed"]
    assert "accommodation" in result["keywords_removed"]
    assert "accessibility" in result["keywords_removed"]
