"""Tests for reasoning/health_score.py — composite health scoring."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from healthbot.reasoning.health_score import (
    CompositeHealthEngine,
    CompositeHealthScore,
    _grade,
    _reweight,
)


class TestGrade:
    def test_a_plus(self):
        assert _grade(95) == "A+"

    def test_a(self):
        assert _grade(85) == "A"

    def test_b(self):
        assert _grade(75) == "B"

    def test_c(self):
        assert _grade(65) == "C"

    def test_d(self):
        assert _grade(55) == "D"

    def test_f(self):
        assert _grade(30) == "F"

    def test_boundary_90(self):
        assert _grade(90) == "A+"

    def test_boundary_80(self):
        assert _grade(80) == "A"

    def test_zero(self):
        assert _grade(0) == "F"


class TestReweight:
    def test_single_component(self):
        result = _reweight({"biomarker": 0.5})
        assert abs(result["biomarker"] - 1.0) < 0.001

    def test_two_components(self):
        result = _reweight({"biomarker": 0.5, "recovery": 0.25})
        assert abs(result["biomarker"] - 2 / 3) < 0.01
        assert abs(result["recovery"] - 1 / 3) < 0.01

    def test_empty(self):
        result = _reweight({})
        assert result == {}


class TestCompositeHealthEngine:
    def _make_db(self):
        db = MagicMock()
        db.query_observations.return_value = []
        db.query_wearable_daily.return_value = []
        return db

    def test_no_data_returns_zero(self):
        db = self._make_db()
        engine = CompositeHealthEngine(db)
        with patch.object(engine, "_biomarker_score", return_value=None), \
             patch.object(engine, "_recovery_score", return_value=None), \
             patch.object(engine, "_trend_trajectory_score", return_value=(None, "stable")), \
             patch.object(engine, "_anomaly_score", return_value=None):
            result = engine.compute(user_id=1)
        assert result.overall == 0.0
        assert result.grade == "F"
        assert "No data available" in result.limiting_factors

    def test_biomarker_only(self):
        db = self._make_db()
        engine = CompositeHealthEngine(db)
        with patch.object(engine, "_biomarker_score", return_value=85.0), \
             patch.object(engine, "_recovery_score", return_value=None), \
             patch.object(engine, "_trend_trajectory_score", return_value=(None, "stable")), \
             patch.object(engine, "_anomaly_score", return_value=None):
            result = engine.compute(user_id=1)
        assert result.overall == 85.0
        assert result.grade == "A"
        assert result.data_coverage["biomarker"] is True
        assert result.data_coverage["recovery"] is False

    def test_all_components(self):
        db = self._make_db()
        engine = CompositeHealthEngine(db)
        with patch.object(engine, "_biomarker_score", return_value=80.0), \
             patch.object(engine, "_recovery_score", return_value=70.0), \
             patch.object(engine, "_trend_trajectory_score", return_value=(60.0, "declining")), \
             patch.object(engine, "_anomaly_score", return_value=100.0):
            result = engine.compute(user_id=1)
        # Weighted: 80*0.5 + 70*0.25 + 60*0.15 + 100*0.10 = 40+17.5+9+10 = 76.5
        assert abs(result.overall - 76.5) < 0.2
        assert result.grade == "B"
        assert result.trend_direction == "declining"

    def test_low_scores_add_limiting_factors(self):
        db = self._make_db()
        engine = CompositeHealthEngine(db)
        with patch.object(engine, "_biomarker_score", return_value=45.0), \
             patch.object(engine, "_recovery_score", return_value=30.0), \
             patch.object(engine, "_trend_trajectory_score", return_value=(50.0, "stable")), \
             patch.object(engine, "_anomaly_score", return_value=40.0):
            result = engine.compute(user_id=1)
        assert len(result.limiting_factors) >= 2

    def test_returns_dataclass(self):
        db = self._make_db()
        engine = CompositeHealthEngine(db)
        with patch.object(engine, "_biomarker_score", return_value=90.0), \
             patch.object(engine, "_recovery_score", return_value=None), \
             patch.object(engine, "_trend_trajectory_score", return_value=(None, "stable")), \
             patch.object(engine, "_anomaly_score", return_value=None):
            result = engine.compute(user_id=1)
        assert isinstance(result, CompositeHealthScore)
        assert isinstance(result.breakdown, dict)
        assert isinstance(result.data_coverage, dict)
