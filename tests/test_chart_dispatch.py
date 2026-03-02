"""Tests for chart_dispatch.py — registry and individual dispatchers."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from healthbot.export.chart_dispatch import dispatch

_PNG_HEADER = b"\x89PNG"


def _mock_db():
    """Create a MagicMock DB with common query methods."""
    db = MagicMock()
    db.query_observations.return_value = []
    db.query_wearable_daily.return_value = []
    return db


# ── Backward compat ──────────────────────────────────────────────


class TestBackwardCompat:
    def test_missing_type_defaults_to_trend(self):
        """CHART block without 'type' key defaults to trend dispatcher."""
        db = _mock_db()
        # No data, so returns None — but shouldn't crash
        result = dispatch({"metric": "hrv", "source": "wearable"}, db, 1)
        assert result is None

    def test_unknown_type_returns_none(self):
        result = dispatch({"type": "nonexistent"}, _mock_db(), 1)
        assert result is None


# ── Trend dispatcher ─────────────────────────────────────────────


class TestTrendDispatch:
    def test_wearable_trend(self):
        from healthbot.reasoning.wearable_trends import WearableTrendResult

        trend = WearableTrendResult(
            metric_name="hrv", display_name="HRV", direction="increasing",
            slope=0.4, r_squared=0.92, data_points=5,
            first_date="2025-01-01", last_date="2025-01-29",
            first_value=45.0, last_value=55.0, pct_change=22.2,
            values=[("2025-01-01", 45.0), ("2025-01-15", 50.0), ("2025-01-29", 55.0)],
        )
        db = _mock_db()
        with patch(
            "healthbot.reasoning.wearable_trends.WearableTrendAnalyzer",
        ) as mock_cls:
            mock_cls.return_value.analyze_metric.return_value = trend
            result = dispatch(
                {"type": "trend", "metric": "hrv", "source": "wearable"}, db, 1,
            )
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_lab_trend(self):
        from healthbot.reasoning.trends import TrendResult

        trend = TrendResult(
            test_name="LDL", canonical_name="ldl", direction="increasing",
            slope=0.11, r_squared=0.95, data_points=4,
            first_date="2024-01-15", last_date="2024-10-15",
            first_value=100.0, last_value=130.0, pct_change=30.0,
            values=[("2024-01-15", 100.0), ("2024-07-15", 120.0), ("2024-10-15", 130.0)],
        )
        db = _mock_db()
        with patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_cls:
            mock_cls.return_value.analyze_test.return_value = trend
            result = dispatch(
                {"type": "trend", "metric": "ldl", "source": "lab"}, db, 1,
            )
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_insufficient_data_returns_none(self):
        db = _mock_db()
        with patch(
            "healthbot.reasoning.wearable_trends.WearableTrendAnalyzer",
        ) as mock_cls:
            mock_cls.return_value.analyze_metric.return_value = None
            result = dispatch(
                {"type": "trend", "metric": "hrv", "source": "wearable"}, db, 1,
            )
        assert result is None


# ── Dashboard dispatcher ─────────────────────────────────────────


class TestDashboardDispatch:
    def test_with_scores(self):
        from healthbot.reasoning.insights import DomainScore

        scores = [
            DomainScore("metabolic", "Metabolic", 90.0, 5, 5, []),
            DomainScore("cardio", "Cardiovascular", 70.0, 4, 4, []),
        ]
        db = _mock_db()
        with (
            patch("healthbot.reasoning.insights.InsightEngine") as mock_cls,
            patch("healthbot.reasoning.triage.TriageEngine"),
            patch("healthbot.reasoning.trends.TrendAnalyzer"),
        ):
            mock_cls.return_value.compute_domain_scores.return_value = scores
            result = dispatch({"type": "dashboard"}, db, 1)
        assert result is not None
        assert result[:4] == _PNG_HEADER


# ── Radar dispatcher ─────────────────────────────────────────────


class TestRadarDispatch:
    def test_with_scores(self):
        from healthbot.reasoning.insights import DomainScore

        scores = [
            DomainScore("metabolic", "Metabolic", 90.0, 5, 5, []),
            DomainScore("cardio", "Cardiovascular", 70.0, 4, 4, []),
            DomainScore("blood", "Blood", 85.0, 5, 5, []),
        ]
        db = _mock_db()
        with (
            patch("healthbot.reasoning.insights.InsightEngine") as mock_cls,
            patch("healthbot.reasoning.triage.TriageEngine"),
            patch("healthbot.reasoning.trends.TrendAnalyzer"),
        ):
            mock_cls.return_value.compute_domain_scores.return_value = scores
            result = dispatch({"type": "radar"}, db, 1)
        assert result is not None
        assert result[:4] == _PNG_HEADER


# ── Composite dispatcher ─────────────────────────────────────────


class TestCompositeDispatch:
    def test_with_score(self):
        from healthbot.reasoning.health_score import CompositeHealthScore

        score = CompositeHealthScore(
            overall=78.5, grade="B",
            breakdown={"biomarker": 85.0, "recovery": 70.0},
            trend_direction="improving", limiting_factors=[],
        )
        db = _mock_db()
        with patch(
            "healthbot.reasoning.health_score.CompositeHealthEngine",
        ) as mock_cls:
            mock_cls.return_value.compute.return_value = score
            result = dispatch({"type": "composite"}, db, 1)
        assert result is not None
        assert result[:4] == _PNG_HEADER


# ── Sleep dispatcher ──────────────────────────────────────────────


class TestSleepDispatch:
    def test_with_data(self):
        data = [
            {"_date": f"2025-01-{i:02d}", "sleep_duration_min": 420 + i,
             "deep_min": 60, "rem_min": 90}
            for i in range(1, 8)
        ]
        db = _mock_db()
        db.query_wearable_daily.return_value = data
        result = dispatch({"type": "sleep"}, db, 1)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_no_data_returns_none(self):
        result = dispatch({"type": "sleep"}, _mock_db(), 1)
        assert result is None


# ── Wearable sparklines dispatcher ───────────────────────────────


class TestWearableSparklinesDispatch:
    def test_with_data(self):
        data = [
            {"_date": f"2025-01-{i:02d}", "hrv": 45 + i, "rhr": 62 - i * 0.3,
             "sleep_score": 70 + i}
            for i in range(1, 10)
        ]
        db = _mock_db()
        db.query_wearable_daily.return_value = data
        result = dispatch({"type": "wearable_sparklines"}, db, 1)
        assert result is not None
        assert result[:4] == _PNG_HEADER


# ── Heatmap dispatcher ───────────────────────────────────────────


class TestHeatmapDispatch:
    def test_with_lab_data(self):
        obs = [
            {"test_name": "LDL", "value": 100, "ref_low": 0, "ref_high": 130,
             "_meta": {"date_effective": "2024-01-15"}},
            {"test_name": "LDL", "value": 145, "ref_low": 0, "ref_high": 130,
             "_meta": {"date_effective": "2024-06-15"}},
        ]
        db = _mock_db()
        db.query_observations.return_value = obs
        result = dispatch({"type": "heatmap"}, db, 1)
        assert result is not None
        assert result[:4] == _PNG_HEADER


# ── Correlation dispatcher ────────────────────────────────────────


class TestCorrelationDispatch:
    def test_with_paired_data(self):
        data = [
            {"_date": f"2025-01-{i:02d}", "hrv": 45 + i, "sleep_score": 70 + i}
            for i in range(1, 8)
        ]
        db = _mock_db()
        db.query_wearable_daily.return_value = data
        result = dispatch(
            {"type": "correlation", "x": "hrv", "y": "sleep_score"}, db, 1,
        )
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_missing_metrics_returns_none(self):
        result = dispatch({"type": "correlation"}, _mock_db(), 1)
        assert result is None


# ── Health card dispatcher ────────────────────────────────────────


class TestHealthCardDispatch:
    def test_with_all_data(self):
        from healthbot.reasoning.health_score import CompositeHealthScore
        from healthbot.reasoning.insights import DomainScore
        from healthbot.reasoning.trends import TrendResult

        composite = CompositeHealthScore(
            overall=78.5, grade="B",
            breakdown={"biomarker": 85.0, "recovery": 70.0},
            trend_direction="improving", limiting_factors=[],
        )
        scores = [
            DomainScore("metabolic", "Metabolic", 90.0, 5, 5, []),
            DomainScore("cardio", "Cardiovascular", 70.0, 4, 4, []),
            DomainScore("blood", "Blood", 85.0, 5, 5, []),
        ]
        trend = TrendResult(
            test_name="LDL", canonical_name="ldl", direction="increasing",
            slope=0.11, r_squared=0.95, data_points=3,
            first_date="2024-01-15", last_date="2024-10-15",
            first_value=100.0, last_value=130.0, pct_change=30.0,
            values=[
                ("2024-01-15", 100.0), ("2024-07-15", 120.0),
                ("2024-10-15", 130.0),
            ],
        )
        wearable_data = [
            {"_date": f"2025-01-{i:02d}", "hrv": 45 + i,
             "rhr": 62 - i * 0.3, "sleep_score": 70 + i}
            for i in range(1, 10)
        ]

        db = _mock_db()
        db.query_wearable_daily.return_value = wearable_data
        with (
            patch(
                "healthbot.reasoning.health_score.CompositeHealthEngine",
            ) as mock_comp,
            patch(
                "healthbot.reasoning.insights.InsightEngine",
            ) as mock_insight,
            patch(
                "healthbot.reasoning.trends.TrendAnalyzer",
            ) as mock_trend,
            patch("healthbot.reasoning.triage.TriageEngine"),
        ):
            mock_comp.return_value.compute.return_value = composite
            mock_insight.return_value.compute_domain_scores.return_value = (
                scores
            )
            mock_trend.return_value.detect_all_trends.return_value = [trend]
            result = dispatch({"type": "health_card"}, db, 1)

        assert result is not None
        assert result[:4] == _PNG_HEADER


# ── Exception handling ────────────────────────────────────────────


class TestExceptionHandling:
    def test_dispatch_catches_exceptions(self):
        """dispatch() should catch exceptions and return None."""
        db = _mock_db()
        with patch(
            "healthbot.reasoning.wearable_trends.WearableTrendAnalyzer",
        ) as mock_cls:
            mock_cls.return_value.analyze_metric.side_effect = (
                RuntimeError("boom")
            )
            result = dispatch(
                {"type": "trend", "metric": "hrv", "source": "wearable"},
                db, 1,
            )
        assert result is None
