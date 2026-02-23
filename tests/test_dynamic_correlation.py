"""Tests for dynamic correlation discovery (Phase 3)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.reasoning.correlate import Correlation, CorrelationEngine


@pytest.fixture()
def mock_db():
    db = MagicMock()
    db.conn = MagicMock()
    return db


@pytest.fixture()
def engine(mock_db):
    return CorrelationEngine(mock_db)


class TestCorrelationPValue:
    def test_p_value_field_exists(self):
        c = Correlation(
            metric_a="glucose",
            metric_b="hrv",
            pearson_r=0.75,
            n_observations=10,
            time_window_days=90,
            interpretation="strong positive correlation",
            p_value=0.01,
        )
        assert c.p_value == 0.01

    def test_p_value_defaults_to_none(self):
        c = Correlation(
            metric_a="glucose",
            metric_b="hrv",
            pearson_r=0.75,
            n_observations=10,
            time_window_days=90,
            interpretation="strong positive correlation",
        )
        assert c.p_value is None

    @patch.object(CorrelationEngine, "correlate_lab_wearable")
    def test_correlate_returns_p_value(self, mock_corr, engine):
        """Verify p_value is populated when scipy is available."""
        mock_corr.return_value = Correlation(
            metric_a="glucose",
            metric_b="hrv",
            pearson_r=-0.65,
            n_observations=15,
            time_window_days=90,
            interpretation="strong negative correlation",
            p_value=0.008,
        )
        result = engine.correlate_lab_wearable("glucose", "hrv", 90)
        assert result is not None
        assert result.p_value == pytest.approx(0.008)


class TestDiscoverAndStore:
    @patch.object(CorrelationEngine, "auto_discover")
    @patch("healthbot.research.knowledge_base.KnowledgeBase")
    def test_stores_significant_correlations(
        self, mock_kb_cls, mock_discover, engine,
    ):
        """Correlations with |r|>=0.5 and p<0.05 are stored."""
        mock_discover.return_value = [
            Correlation(
                metric_a="glucose",
                metric_b="hrv",
                pearson_r=-0.65,
                n_observations=15,
                time_window_days=90,
                interpretation="strong negative",
                p_value=0.008,
            ),
            Correlation(
                metric_a="crp",
                metric_b="sleep_score",
                pearson_r=-0.35,
                n_observations=10,
                time_window_days=90,
                interpretation="moderate negative",
                p_value=0.03,
            ),
        ]
        mock_kb_cls.return_value.store_finding.return_value = "kb-123"

        stored = engine.discover_and_store(user_id=1)

        # Only the first one has |r| >= 0.5
        assert len(stored) == 1
        assert stored[0].metric_a == "glucose"
        mock_kb_cls.return_value.store_finding.assert_called_once()

    @patch.object(CorrelationEngine, "auto_discover")
    def test_filters_high_p_value(self, mock_discover, engine):
        """Correlations with p >= 0.05 are excluded."""
        mock_discover.return_value = [
            Correlation(
                metric_a="glucose",
                metric_b="hrv",
                pearson_r=-0.55,
                n_observations=10,
                time_window_days=90,
                interpretation="strong negative",
                p_value=0.08,  # Not significant
            ),
        ]

        stored = engine.discover_and_store(user_id=1)
        assert stored == []

    @patch.object(CorrelationEngine, "auto_discover")
    def test_filters_low_n(self, mock_discover, engine):
        """Correlations with n < min_n are excluded."""
        mock_discover.return_value = [
            Correlation(
                metric_a="glucose",
                metric_b="hrv",
                pearson_r=-0.70,
                n_observations=5,
                time_window_days=90,
                interpretation="strong negative",
                p_value=0.01,
            ),
        ]

        stored = engine.discover_and_store(user_id=1, min_n=7)
        assert stored == []

    @patch.object(CorrelationEngine, "auto_discover")
    def test_empty_discovery_returns_empty(self, mock_discover, engine):
        mock_discover.return_value = []
        stored = engine.discover_and_store(user_id=1)
        assert stored == []


class TestAutoDiscoverLimit:
    def test_auto_discover_adds_limit_to_query(self, mock_db):
        """auto_discover should query DB for lab names."""
        mock_db.conn.execute.return_value.fetchall.return_value = []
        engine = CorrelationEngine(mock_db)
        results = engine.auto_discover(user_id=1)
        assert results == []
        mock_db.conn.execute.assert_called_once()
