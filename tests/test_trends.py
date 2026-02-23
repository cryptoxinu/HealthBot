"""Tests for trend analysis."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult
from healthbot.reasoning.trends import TrendAnalyzer


class TestTrends:
    """Test time-series trend detection."""

    def _insert_series(self, db, test_name, values_dates):
        """Helper: insert a series of lab results."""
        for value, d in values_dates:
            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name=test_name,
                canonical_name=test_name.lower(),
                value=value,
                unit="mg/dL",
                date_collected=d,
            )
            db.insert_observation(lab)

    def test_increasing_trend(self, db):
        """LDL increasing should be detected."""
        today = date.today()
        series = [
            (100, today - timedelta(days=180)),
            (120, today - timedelta(days=120)),
            (140, today - timedelta(days=60)),
            (160, today),
        ]
        self._insert_series(db, "LDL", series)

        analyzer = TrendAnalyzer(db)
        result = analyzer.analyze_test("ldl")
        assert result is not None
        assert result.direction == "increasing"
        assert result.pct_change > 0

    def test_decreasing_trend(self, db):
        """Decreasing values should be detected."""
        today = date.today()
        series = [
            (200, today - timedelta(days=180)),
            (180, today - timedelta(days=120)),
            (160, today - timedelta(days=60)),
            (140, today),
        ]
        self._insert_series(db, "cholesterol", series)

        analyzer = TrendAnalyzer(db)
        result = analyzer.analyze_test("cholesterol")
        assert result is not None
        assert result.direction == "decreasing"

    def test_stable_trend(self, db):
        """Stable values should not trigger concern."""
        today = date.today()
        series = [
            (100, today - timedelta(days=180)),
            (101, today - timedelta(days=120)),
            (99, today - timedelta(days=60)),
            (100, today),
        ]
        self._insert_series(db, "glucose", series)

        analyzer = TrendAnalyzer(db)
        result = analyzer.analyze_test("glucose")
        assert result is not None
        assert result.direction == "stable"

    def test_insufficient_data(self, db):
        """Less than 2 points should return None."""
        analyzer = TrendAnalyzer(db)
        result = analyzer.analyze_test("nonexistent_test")
        assert result is None
