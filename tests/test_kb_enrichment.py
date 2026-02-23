"""Tests for proactive KB enrichment (Phase 4)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.reasoning.kb_enrichment import KBEnrichmentEngine


@pytest.fixture()
def mock_db():
    db = MagicMock()
    db.conn = MagicMock()
    return db


class TestKBEnrichmentEngine:
    @patch("healthbot.reasoning.kb_enrichment.KnowledgeBase")
    def test_store_trend_finding(self, mock_kb_cls, mock_db):
        mock_kb_cls.return_value.find_similar.return_value = False
        mock_kb_cls.return_value.store_finding.return_value = "kb-001"

        enricher = KBEnrichmentEngine(mock_db)
        result = enricher.store_trend_finding(
            "glucose: increasing (+15.0%)", user_id=1,
        )
        assert result == "kb-001"
        mock_kb_cls.return_value.store_finding.assert_called_once()
        call_kwargs = mock_kb_cls.return_value.store_finding.call_args
        assert call_kwargs[1]["source"] == "auto_analysis"

    @patch("healthbot.reasoning.kb_enrichment.KnowledgeBase")
    def test_dedup_skips_similar(self, mock_kb_cls, mock_db):
        mock_kb_cls.return_value.find_similar.return_value = True

        enricher = KBEnrichmentEngine(mock_db)
        result = enricher.store_trend_finding(
            "glucose: increasing (+15.0%)", user_id=1,
        )
        assert result is None
        mock_kb_cls.return_value.store_finding.assert_not_called()

    @patch("healthbot.reasoning.kb_enrichment.KnowledgeBase")
    def test_store_interaction_finding(self, mock_kb_cls, mock_db):
        mock_kb_cls.return_value.find_similar.return_value = False
        mock_kb_cls.return_value.store_finding.return_value = "kb-002"

        enricher = KBEnrichmentEngine(mock_db)
        result = enricher.store_interaction_finding(
            "metformin -> glucose: may lower", user_id=1,
        )
        assert result == "kb-002"

    @patch("healthbot.reasoning.kb_enrichment.KnowledgeBase")
    def test_store_gap_finding(self, mock_kb_cls, mock_db):
        mock_kb_cls.return_value.find_similar.return_value = False
        mock_kb_cls.return_value.store_finding.return_value = "kb-003"

        enricher = KBEnrichmentEngine(mock_db)
        result = enricher.store_gap_finding(
            "Missing iron panel for anemia workup", user_id=1,
        )
        assert result == "kb-003"

    @patch("healthbot.reasoning.kb_enrichment.KnowledgeBase")
    def test_cleanup_stale(self, mock_kb_cls, mock_db):
        mock_kb_cls.return_value.delete_stale.return_value = 5

        enricher = KBEnrichmentEngine(mock_db)
        count = enricher.cleanup_stale(max_age_days=90)
        assert count == 5
        mock_kb_cls.return_value.delete_stale.assert_called_once_with(
            "auto_analysis", 90,
        )


class TestKBFindSimilar:
    def test_finds_similar_entry(self, mock_db):
        from healthbot.research.knowledge_base import KnowledgeBase

        mock_db.conn.execute.return_value.fetchall.return_value = [
            {
                "topic": "trend:glucose",
                "finding": "glucose: increasing (+14.8%)",
            },
        ]
        kb = KnowledgeBase(mock_db)
        assert kb.find_similar(
            "trend:glucose",
            "glucose: increasing (+15.0%)",
            "auto_analysis",
        )

    def test_no_similar_entry(self, mock_db):
        from healthbot.research.knowledge_base import KnowledgeBase

        mock_db.conn.execute.return_value.fetchall.return_value = [
            {
                "topic": "trend:ferritin",
                "finding": "ferritin: decreasing (-20%)",
            },
        ]
        kb = KnowledgeBase(mock_db)
        assert not kb.find_similar(
            "trend:glucose",
            "glucose: increasing (+15.0%)",
            "auto_analysis",
        )

    def test_empty_kb_returns_false(self, mock_db):
        from healthbot.research.knowledge_base import KnowledgeBase

        mock_db.conn.execute.return_value.fetchall.return_value = []
        kb = KnowledgeBase(mock_db)
        assert not kb.find_similar(
            "trend:glucose",
            "glucose: increasing (+15.0%)",
            "auto_analysis",
        )


class TestKBDeleteStale:
    def test_deletes_old_auto_entries(self, mock_db):
        from healthbot.research.knowledge_base import KnowledgeBase

        mock_db.conn.execute.return_value.rowcount = 3
        kb = KnowledgeBase(mock_db)
        count = kb.delete_stale("auto_analysis", max_age_days=90)
        assert count == 3
        mock_db.conn.commit.assert_called_once()
