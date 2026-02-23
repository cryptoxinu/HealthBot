"""Tests for sleep architecture analysis."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import WhoopDaily
from healthbot.reasoning.sleep_analysis import SleepArchitectureAnalyzer


class TestSleepArchitecture:
    """Test sleep stage analysis."""

    def _insert_night(self, db, days_ago=0, **metrics):
        """Insert a single night of sleep data."""
        d = date.today() - timedelta(days=days_ago)
        wd = WhoopDaily(id=uuid.uuid4().hex, date=d, **metrics)
        db.insert_wearable_daily(wd)

    def test_normal_architecture_no_concerns(self, db):
        """Normal sleep stages should produce no concerns."""
        self._insert_night(
            db,
            sleep_duration_min=480,  # 8 hours
            rem_min=110,  # ~23%
            deep_min=100,  # ~21%
            light_min=270,  # ~56%
        )
        analyzer = SleepArchitectureAnalyzer(db)
        result = analyzer.analyze_night()
        assert result is not None
        assert result.rem_status == "normal"
        assert result.deep_status == "normal"
        assert result.concerns == []

    def test_low_rem_flagged(self, db):
        """REM below 15% should be flagged."""
        self._insert_night(
            db,
            sleep_duration_min=480,
            rem_min=50,   # ~10%
            deep_min=120,  # 25%
            light_min=310,  # 65%
        )
        analyzer = SleepArchitectureAnalyzer(db)
        result = analyzer.analyze_night()
        assert result is not None
        assert result.rem_status == "low"
        assert any("REM" in c for c in result.concerns)

    def test_low_deep_flagged(self, db):
        """Deep below 15% should be flagged."""
        self._insert_night(
            db,
            sleep_duration_min=480,
            rem_min=120,  # 25%
            deep_min=50,  # ~10%
            light_min=310,  # 65%
        )
        analyzer = SleepArchitectureAnalyzer(db)
        result = analyzer.analyze_night()
        assert result is not None
        assert result.deep_status == "low"
        assert any("deep" in c.lower() for c in result.concerns)

    def test_no_data_returns_none(self, db):
        """No wearable data should return None."""
        analyzer = SleepArchitectureAnalyzer(db)
        result = analyzer.analyze_night()
        assert result is None

    def test_no_stage_data_returns_none(self, db):
        """Duration without stage breakdown should return None."""
        self._insert_night(
            db,
            sleep_duration_min=420,
            # No rem_min, deep_min, light_min
        )
        analyzer = SleepArchitectureAnalyzer(db)
        result = analyzer.analyze_night()
        assert result is None

    def test_format_includes_percentages(self, db):
        """Formatted output should include stage percentages."""
        self._insert_night(
            db,
            sleep_duration_min=480,
            rem_min=110,
            deep_min=100,
            light_min=270,
        )
        analyzer = SleepArchitectureAnalyzer(db)
        result = analyzer.analyze_night()
        assert result is not None
        text = analyzer.format_architecture(result)
        assert "REM" in text
        assert "Deep" in text
        assert "8h" in text

    def test_summary_multiple_nights(self, db):
        """Multi-night summary should average correctly."""
        for i in range(5):
            self._insert_night(
                db,
                days_ago=i,
                sleep_duration_min=420 + i * 10,
                rem_min=90 + i * 5,
                deep_min=80 + i * 3,
                light_min=250,
            )
        analyzer = SleepArchitectureAnalyzer(db)
        text = analyzer.format_summary(days=7)
        assert text is not None
        assert "5 nights" in text
        assert "Avg" in text

    def test_trends_compare_7_vs_30(self, db):
        """Trend analysis should compare recent vs baseline."""
        today = date.today()
        # 30 days of data with declining REM
        for i in range(30):
            d = today - timedelta(days=29 - i)
            # Earlier days: good REM (25%), recent days: low REM (12%)
            rem_pct = 25 if i < 20 else 12
            total = 420
            rem_min = int(total * rem_pct / 100)
            deep_min = int(total * 0.20)
            light_min = total - rem_min - deep_min
            wd = WhoopDaily(
                id=uuid.uuid4().hex, date=d,
                sleep_duration_min=total,
                rem_min=rem_min,
                deep_min=deep_min,
                light_min=light_min,
            )
            db.insert_wearable_daily(wd)

        analyzer = SleepArchitectureAnalyzer(db)
        trends = analyzer.analyze_trends(days=30)
        assert len(trends) > 0
        rem_trend = next(
            (t for t in trends if t.metric == "rem_pct"), None,
        )
        assert rem_trend is not None
        # Recent 7 days should have lower REM than 30-day average
        assert rem_trend.avg_last_7 < rem_trend.avg_last_30
