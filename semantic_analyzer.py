"""
semantic_analyzer.py — NLP-based semantic change analysis.

Uses sentence-transformers (all-MiniLM-L6-v2) to compute cosine similarity
between before/after text to distinguish:
  - Semantic removal   (high text change, low similarity  → content deleted)
  - Semantic substitution (high change, moderate similarity → different framing)
  - Formatting change  (high text change, high similarity → restructure only)

Also performs keyword presence tracking and optional zero-shot classification
via facebook/bart-large-mnli (gated behind zero_shot_enabled flag).

IMPORTANT: model.encode() is synchronous + CPU-bound. It must NEVER be called
directly in an async function — always use run_in_executor().
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class NLPConfig:
    embedding_model: str = "all-MiniLM-L6-v2"
    zero_shot_model: str = "facebook/bart-large-mnli"
    zero_shot_enabled: bool = False
    device: str = "cpu"
    cache_dir: str = "models/"
    batch_size: int = 8
    disability_keywords: list[str] = field(default_factory=lambda: [
        "disability", "disabilities", "disabled", "ADA",
        "Americans with Disabilities Act", "Section 504", "Section 508",
        "Rehabilitation Act", "SSDI", "SSI", "Social Security Disability",
        "IDEA", "accommodation", "accessibility", "assistive technology",
        "wheelchair", "blind", "deaf", "autism", "intellectual disability",
        "developmental disability", "mental health", "long COVID",
        "vocational rehabilitation", "independent living",
    ])
    zero_shot_candidate_labels: list[str] = field(default_factory=lambda: [
        "dataset deletion", "policy change", "data update",
        "website restructure", "access restriction", "new publication",
        "funding change", "regulatory update", "program elimination",
        "content restoration",
    ])


# ---------------------------------------------------------------------------
# Semantic change interpretation thresholds
# ---------------------------------------------------------------------------

SIMILARITY_HIGH = 0.85      # >= this → formatting-only change
SIMILARITY_MODERATE = 0.60  # >= this → semantic substitution
# below SIMILARITY_MODERATE → semantic removal / major meaning change


# ---------------------------------------------------------------------------
# SemanticAnalyzer
# ---------------------------------------------------------------------------


class SemanticAnalyzer:
    """
    Wraps sentence-transformers and zero-shot classification.

    Instantiate at application startup. Load models in a thread pool to avoid
    blocking the event loop during the potentially slow model load.
    """

    def __init__(self, config: NLPConfig) -> None:
        self._config = config
        self._embedder: Optional[object] = None
        self._classifier: Optional[object] = None
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="nlp")
        self._loaded = False

    def load_models_sync(self) -> None:
        """
        Load models synchronously (call this from run_in_executor at startup).
        """
        try:
            from sentence_transformers import SentenceTransformer  # type: ignore
            self._embedder = SentenceTransformer(
                self._config.embedding_model,
                cache_folder=self._config.cache_dir,
                device=self._config.device,
            )
            logger.info(f"Loaded embedding model: {self._config.embedding_model}")
        except ImportError:
            logger.warning(
                "sentence-transformers not installed. "
                "Semantic similarity will return None."
            )
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")

        if self._config.zero_shot_enabled:
            try:
                from transformers import pipeline  # type: ignore
                self._classifier = pipeline(
                    "zero-shot-classification",
                    model=self._config.zero_shot_model,
                    device=-1,  # CPU
                )
                logger.info(f"Loaded zero-shot model: {self._config.zero_shot_model}")
            except ImportError:
                logger.warning("transformers not installed. Zero-shot classification disabled.")
            except Exception as e:
                logger.error(f"Failed to load zero-shot model: {e}")

        self._loaded = True

    async def load_models(self) -> None:
        """Load models asynchronously (non-blocking)."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(self._executor, self.load_models_sync)

    # ------------------------------------------------------------------
    # Async public API — all CPU work in thread pool
    # ------------------------------------------------------------------

    async def compute_similarity(
        self, text_before: str, text_after: str
    ) -> Optional[float]:
        """
        Compute cosine similarity between before and after text.

        Returns a float 0.0–1.0, or None if models aren't loaded.
        """
        if self._embedder is None:
            return None
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._compute_similarity_sync,
            text_before,
            text_after,
        )

    def _compute_similarity_sync(self, text_before: str, text_after: str) -> float:
        """Synchronous similarity computation — runs in thread pool."""
        try:
            from sentence_transformers import util  # type: ignore
            import numpy as np  # type: ignore

            # Truncate to 5000 chars (MiniLM handles ~256 tokens ≈ 1000-1500 chars)
            t_before = text_before[:5000] if text_before else ""
            t_after = text_after[:5000] if text_after else ""

            if not t_before and not t_after:
                return 1.0
            if not t_before or not t_after:
                return 0.0

            emb_before = self._embedder.encode(t_before, convert_to_tensor=True)  # type: ignore
            emb_after = self._embedder.encode(t_after, convert_to_tensor=True)  # type: ignore
            similarity = util.cos_sim(emb_before, emb_after).item()
            return float(max(0.0, min(1.0, similarity)))
        except Exception as e:
            logger.error(f"Similarity computation failed: {e}")
            return 0.5  # Neutral fallback

    async def classify_change(self, diff_text: str) -> list[str]:
        """
        Zero-shot classify the nature of a detected change.

        Returns top-3 labels, or empty list if classifier unavailable.
        """
        if self._classifier is None:
            return []
        if not diff_text.strip():
            return []

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self._classify_sync,
            diff_text,
        )

    def _classify_sync(self, diff_text: str) -> list[str]:
        """Synchronous zero-shot classification — runs in thread pool."""
        try:
            truncated = diff_text[:512]
            result = self._classifier(  # type: ignore
                truncated,
                self._config.zero_shot_candidate_labels,
                multi_label=False,
            )
            return result["labels"][:3]
        except Exception as e:
            logger.error(f"Zero-shot classification failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Keyword scanning (synchronous — fast, no model needed)
    # ------------------------------------------------------------------

    def keyword_scan(self, text: str) -> list[str]:
        """Return list of disability keywords present in text (case-insensitive)."""
        text_lower = text.lower()
        found = []
        for kw in self._config.disability_keywords:
            if kw.lower() in text_lower:
                found.append(kw)
        return found

    def keyword_counts(self, text: str) -> dict[str, int]:
        """Count occurrences of each keyword."""
        text_lower = text.lower()
        return {
            kw: text_lower.count(kw.lower())
            for kw in self._config.disability_keywords
        }

    # ------------------------------------------------------------------
    # Interpretation
    # ------------------------------------------------------------------

    def interpret_similarity(self, similarity: float) -> str:
        """Return a human-readable interpretation of a similarity score."""
        if similarity >= SIMILARITY_HIGH:
            return "formatting_change"
        if similarity >= SIMILARITY_MODERATE:
            return "semantic_substitution"
        return "semantic_removal"

    # ------------------------------------------------------------------
    # Full analysis (convenience wrapper used by scheduler)
    # ------------------------------------------------------------------

    async def analyze_change(
        self,
        text_before: str,
        text_after: str,
        diff_text: str = "",
    ) -> dict:
        """
        Run all analyses and return a dict suitable for updating a ChangeModel.

        Returns:
          {
            "semantic_similarity": float | None,
            "semantic_labels": list[str],
            "interpretation": str,
            "keywords_removed": list[str],
            "keywords_added": list[str],
          }
        """
        similarity = await self.compute_similarity(text_before, text_after)

        labels: list[str] = []
        if diff_text and self._config.zero_shot_enabled:
            labels = await self.classify_change(diff_text)

        interpretation = (
            self.interpret_similarity(similarity)
            if similarity is not None
            else "unknown"
        )

        # Keyword delta
        before_kws = set(self.keyword_scan(text_before))
        after_kws = set(self.keyword_scan(text_after))
        keywords_removed = sorted(before_kws - after_kws)
        keywords_added = sorted(after_kws - before_kws)

        return {
            "semantic_similarity": similarity,
            "semantic_labels": labels,
            "interpretation": interpretation,
            "keywords_removed": keywords_removed,
            "keywords_added": keywords_added,
        }

    def shutdown(self) -> None:
        """Clean up thread pool."""
        self._executor.shutdown(wait=False)
