"""Local sentence embedding model for semantic NLU + search.

Uses sentence-transformers/all-MiniLM-L6-v2 (22M params, ~80MB, CPU).
Optional dependency -- if not installed, is_available() returns False
and the system falls back to regex NLU + TF-IDF search.

All processing is local. No API calls. No PHI leaves the machine.
"""
from __future__ import annotations

import logging

import numpy as np

logger = logging.getLogger("healthbot")

# Lazy import -- sentence-transformers is optional
_ST_AVAILABLE = False
try:
    from sentence_transformers import SentenceTransformer  # type: ignore[import-untyped]

    _ST_AVAILABLE = True
except ImportError:
    pass

DEFAULT_MODEL = "all-MiniLM-L6-v2"

# Singleton instance (model weights contain no PHI, safe across vault lock/unlock)
_instance: EmbeddingModel | None = None


class EmbeddingModel:
    """Lazy-loaded sentence embedding model (singleton).

    Model persists across vault lock/unlock -- weights contain no PHI,
    only encryption keys are zeroed on lock.
    """

    def __init__(self, model_name: str = DEFAULT_MODEL) -> None:
        if not _ST_AVAILABLE:
            raise RuntimeError(
                "sentence-transformers not installed. "
                "Run: pip install sentence-transformers"
            )
        self._model_name = model_name
        self._model: SentenceTransformer | None = None

    def _ensure_loaded(self) -> None:
        """Load model on first use."""
        if self._model is None:
            logger.info("Loading embedding model '%s'...", self._model_name)
            self._model = SentenceTransformer(self._model_name)
            logger.info("Embedding model loaded.")

    def encode(self, texts: list[str]) -> np.ndarray:
        """Encode a batch of texts to dense vectors.

        Returns shape (n_texts, embedding_dim).
        """
        self._ensure_loaded()
        assert self._model is not None
        return self._model.encode(texts, convert_to_numpy=True, show_progress_bar=False)

    def encode_single(self, text: str) -> np.ndarray:
        """Encode a single text. Returns shape (embedding_dim,)."""
        return self.encode([text])[0]

    def cosine_similarity(
        self, query_vec: np.ndarray, corpus_matrix: np.ndarray
    ) -> np.ndarray:
        """Compute cosine similarity between query and corpus.

        Args:
            query_vec: shape (dim,)
            corpus_matrix: shape (n_docs, dim)

        Returns:
            shape (n_docs,) of similarity scores in [-1, 1]
        """
        query_norm = query_vec / (np.linalg.norm(query_vec) + 1e-10)
        corpus_norms = corpus_matrix / (
            np.linalg.norm(corpus_matrix, axis=1, keepdims=True) + 1e-10
        )
        return corpus_norms @ query_norm

    @staticmethod
    def is_available() -> bool:
        """Check if sentence-transformers is installed."""
        return _ST_AVAILABLE

    @staticmethod
    def get_instance(model_name: str = DEFAULT_MODEL) -> EmbeddingModel:
        """Get or create the singleton instance."""
        global _instance
        if _instance is None:
            _instance = EmbeddingModel(model_name)
        return _instance
