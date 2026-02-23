"""Tests for the embedding model wrapper.

All tests mock sentence-transformers -- no real model loaded.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pytest


class TestEmbeddingModelMocked:
    """Test EmbeddingModel with mocked sentence-transformers."""

    def test_encode_returns_ndarray(self) -> None:
        """encode() should return numpy array from model."""
        mock_st = MagicMock()
        mock_st.encode.return_value = np.array([[0.1, 0.2], [0.3, 0.4]])

        with patch.dict("sys.modules", {"sentence_transformers": MagicMock()}):
            from healthbot.nlu.embeddings import EmbeddingModel

            model = EmbeddingModel.__new__(EmbeddingModel)
            model._model_name = "test"
            model._model = mock_st

            result = model.encode(["hello", "world"])
            assert isinstance(result, np.ndarray)
            assert result.shape == (2, 2)

    def test_encode_single_returns_1d(self) -> None:
        """encode_single() should return 1D vector."""
        mock_st = MagicMock()
        mock_st.encode.return_value = np.array([[0.1, 0.2, 0.3]])

        with patch.dict("sys.modules", {"sentence_transformers": MagicMock()}):
            from healthbot.nlu.embeddings import EmbeddingModel

            model = EmbeddingModel.__new__(EmbeddingModel)
            model._model_name = "test"
            model._model = mock_st

            result = model.encode_single("hello")
            assert result.shape == (3,)

    def test_cosine_similarity(self) -> None:
        """cosine_similarity() should produce correct scores."""
        with patch.dict("sys.modules", {"sentence_transformers": MagicMock()}):
            from healthbot.nlu.embeddings import EmbeddingModel

            model = EmbeddingModel.__new__(EmbeddingModel)
            model._model_name = "test"
            model._model = MagicMock()

            query = np.array([1.0, 0.0])
            corpus = np.array([[1.0, 0.0], [0.0, 1.0], [0.707, 0.707]])

            scores = model.cosine_similarity(query, corpus)
            assert scores.shape == (3,)
            # Identical vector should have similarity ~1.0
            assert scores[0] == pytest.approx(1.0, abs=0.01)
            # Orthogonal vector should have similarity ~0.0
            assert scores[1] == pytest.approx(0.0, abs=0.01)

    def test_is_available_reflects_import(self) -> None:
        """is_available() should reflect whether sentence-transformers is installed."""
        from healthbot.nlu.embeddings import EmbeddingModel

        # Just verify it returns a bool (actual value depends on env)
        assert isinstance(EmbeddingModel.is_available(), bool)
