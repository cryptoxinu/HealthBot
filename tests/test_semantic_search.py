"""Tests for hybrid TF-IDF + dense embedding search.

All tests mock the embedding model -- no real model loaded.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.retrieval.search import SearchEngine
from healthbot.security.key_manager import KeyManager
from healthbot.security.vault import Vault

PASSPHRASE = "test-search-passphrase"


@pytest.fixture
def search_setup(tmp_path: Path):
    """Create search engine with real DB and mock embedding model."""
    vault_home = tmp_path / "vault"
    vault_home.mkdir()
    config = Config(vault_home=vault_home)
    config.ensure_dirs()

    km = KeyManager(config)
    km.setup(PASSPHRASE)

    db = HealthDB(config, km)
    db.open()
    db.run_migrations()

    vault = Vault(config.blobs_dir, km)

    # Insert test data into search index
    db.conn.execute(
        "INSERT INTO search_index (doc_id, record_type, date_effective, "
        "text_for_search) VALUES (?, ?, ?, ?)",
        ("doc1", "lab_result", "2025-06-15", "glucose blood sugar 108 mg/dL"),
    )
    db.conn.execute(
        "INSERT INTO search_index (doc_id, record_type, date_effective, "
        "text_for_search) VALUES (?, ?, ?, ?)",
        ("doc2", "lab_result", "2025-06-15", "cholesterol lipid panel 210 mg/dL"),
    )
    db.conn.execute(
        "INSERT INTO search_index (doc_id, record_type, date_effective, "
        "text_for_search) VALUES (?, ?, ?, ?)",
        (
            "doc3", "lab_result", "2025-06-15",
            "cardiac troponin heart attack marker normal",
        ),
    )
    db.conn.commit()

    engine = SearchEngine(config, db, vault)
    yield engine, config, db, km, vault


class TestTfIdfSearch:
    """TF-IDF search should work without embeddings."""

    def test_basic_search(self, search_setup) -> None:
        engine, *_ = search_setup
        engine.build_index()
        results = engine.search("glucose")
        assert len(results) >= 1
        assert results[0].record_id == "doc1"

    def test_synonym_expansion(self, search_setup) -> None:
        engine, *_ = search_setup
        engine.build_index()
        results = engine.search("blood sugar")
        assert len(results) >= 1

    def test_no_results_for_unrelated(self, search_setup) -> None:
        engine, *_ = search_setup
        engine.build_index()
        results = engine.search("quantum physics")
        assert len(results) == 0


class TestHybridScoring:
    """Test hybrid TF-IDF + dense scoring."""

    def test_hybrid_scores_combined(self, search_setup) -> None:
        """When both TF-IDF and dense scores are available, they combine."""
        engine, *_ = search_setup
        tfidf_scores = np.array([0.8, 0.2, 0.0])
        dense_scores = np.array([0.3, 0.9, 0.1])

        # Set up mock embedding model
        mock_model = MagicMock()
        mock_model.encode_single.return_value = np.zeros(384)
        mock_model.cosine_similarity.return_value = dense_scores
        engine._embed_model = mock_model
        engine._dense_matrix = np.zeros((3, 384))
        engine._dense_doc_ids = ["doc1", "doc2", "doc3"]

        combined = engine._compute_hybrid_scores(tfidf_scores, "test query")
        # doc1 has high TF-IDF, doc2 has high dense -- both should score well
        assert combined[0] > 0.3  # High TF-IDF contribution
        assert combined[1] > 0.3  # High dense contribution

    def test_tfidf_only_when_no_embeddings(self, search_setup) -> None:
        """Without embeddings, should return TF-IDF scores unchanged."""
        engine, *_ = search_setup
        tfidf_scores = np.array([0.8, 0.2, 0.0])

        # No dense model
        engine._embed_model = None
        engine._dense_matrix = None

        combined = engine._compute_hybrid_scores(tfidf_scores, "test")
        np.testing.assert_array_equal(combined, tfidf_scores)

    def test_normalize_scores(self, search_setup) -> None:
        """Score normalization should map to [0, 1]."""
        engine, *_ = search_setup
        scores = np.array([0.0, 0.5, 1.0])
        normed = engine._normalize_scores(scores)
        assert normed[0] == pytest.approx(0.0)
        assert normed[2] == pytest.approx(1.0)

    def test_normalize_constant_scores(self, search_setup) -> None:
        """Constant scores should normalize to zeros (no info)."""
        engine, *_ = search_setup
        scores = np.array([0.5, 0.5, 0.5])
        normed = engine._normalize_scores(scores)
        np.testing.assert_array_equal(normed, np.zeros(3))


class TestDenseVectorStore:
    """Test saving/loading dense vectors through VectorStore."""

    def test_round_trip(self, search_setup) -> None:
        """Dense matrix should survive encrypt-save-load-decrypt."""
        _, config, _, km, vault = search_setup
        from healthbot.retrieval.vector_store import VectorStore

        vs = VectorStore(vault, config.vectors_dir)
        matrix = np.random.randn(5, 384).astype(np.float32)
        doc_ids = ["a", "b", "c", "d", "e"]

        vs.save_dense_matrix("test", matrix, doc_ids)
        result = vs.load_dense_matrix("test")

        assert result is not None
        loaded_matrix, loaded_ids = result
        np.testing.assert_array_almost_equal(loaded_matrix, matrix)
        assert loaded_ids == doc_ids

    def test_load_nonexistent(self, search_setup) -> None:
        """Loading a non-existent matrix should return None."""
        _, config, _, km, vault = search_setup
        from healthbot.retrieval.vector_store import VectorStore

        vs = VectorStore(vault, config.vectors_dir)
        assert vs.load_dense_matrix("nonexistent") is None
