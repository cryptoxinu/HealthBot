"""Tests for SearchEngine."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from healthbot.retrieval.search import SearchEngine


def _make_engine(texts=None):
    """Create SearchEngine with mocked DB and Vault."""
    config = MagicMock()
    config.tfidf_max_features = 5000
    config.search_top_k = 10
    config.vectors_dir = Path("/tmp/test_vectors")

    db = MagicMock()
    entries = texts or []
    db.get_all_search_texts.return_value = entries

    vault = MagicMock()
    # VectorStore methods return None (no saved index)
    vault.load = MagicMock(return_value=None)

    engine = SearchEngine(config, db, vault)
    # Disable encrypted vector storage (mock environment)
    engine._vector_store = MagicMock()
    engine._vector_store.load_sparse_matrix.return_value = None
    engine._vector_store.load_vocabulary.return_value = None
    return engine, db


class TestBuildAndSearch:
    """Build index and verify search returns results."""

    def test_build_and_find(self) -> None:
        texts = [
            ("doc1", "lab_result", "glucose blood sugar 100 mg/dL normal range"),
            ("doc2", "lab_result", "cholesterol total 200 mg/dL borderline"),
            ("doc3", "vital_sign", "heart rate 72 bpm resting"),
        ]
        engine, db = _make_engine(texts)

        count = engine.build_index()
        assert count == 3

        # Mock the DB lookup for search results
        def mock_execute(sql, params=()):
            mock_row = MagicMock()
            doc_id = params[0] if params else ""
            for tid, rtype, text in texts:
                if tid == doc_id:
                    mock_row.__getitem__ = lambda self, k, t=text, r=rtype: {
                        "text_for_search": t,
                        "record_type": r,
                        "date_effective": "2025-06-15",
                        "encrypted_text": None,
                    }[k]
                    result = MagicMock()
                    result.fetchone.return_value = mock_row
                    return result
            result = MagicMock()
            result.fetchone.return_value = None
            return result

        db.conn.execute = mock_execute

        results = engine.search("glucose blood sugar")
        assert len(results) > 0
        assert results[0].record_id == "doc1"

    def test_empty_db_returns_empty(self) -> None:
        engine, _ = _make_engine([])
        engine.build_index()
        results = engine.search("glucose")
        assert results == []


class TestSynonymExpansion:
    """Medical synonyms should expand search queries."""

    def test_blood_sugar_expands_to_glucose(self) -> None:
        engine, _ = _make_engine()
        expanded = engine._expand_synonyms("blood sugar levels")
        assert "glucose" in expanded

    def test_a1c_expands_to_hba1c(self) -> None:
        engine, _ = _make_engine()
        expanded = engine._expand_synonyms("my a1c results")
        assert "hba1c" in expanded

    def test_thyroid_expands_to_tsh(self) -> None:
        engine, _ = _make_engine()
        expanded = engine._expand_synonyms("how is my thyroid")
        assert "tsh" in expanded
