"""Tests for wearable trend analysis and anomaly detection."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import WhoopDaily
from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer


class TestWearableTrends:
    """Test wearable metric trend detection."""

    def _insert_series(self, db, metric: str, values_dates: list[tuple[float, date]]):
        """Helper: insert WhoopDaily records with a specific metric."""
        for value, d in values_dates:
            wd = WhoopDaily(id=uuid.uuid4().hex, date=d, **{metric: value})
            db.insert_wearable_daily(wd)

    def test_declining_hrv_detected(self, db):
        """HRV dropping over 7 days should be detected."""
        today = date.today()
        series = [(100 - i * 8, today - timedelta(days=6 - i)) for i in range(7)]
        self._insert_series(db, "hrv", series)

        analyzer = WearableTrendAnalyzer(db)
        result = analyzer.analyze_metric("hrv", days=14)
        assert result is not None
        assert result.direction == "decreasing"
        assert result.pct_change < 0

    def test_increasing_rhr_detected(self, db):
        """RHR rising over 7 days should be detected."""
        today = date.today()
        series = [(55 + i * 3, today - timedelta(days=6 - i)) for i in range(7)]
        self._insert_series(db, "rhr", series)

        analyzer = WearableTrendAnalyzer(db)
        result = analyzer.analyze_metric("rhr", days=14)
        assert result is not None
        assert result.direction == "increasing"
        assert result.pct_change > 0

    def test_stable_metric_no_trend(self, db):
        """Flat values should return stable direction."""
        today = date.today()
        series = [(75, today - timedelta(days=6 - i)) for i in range(7)]
        self._insert_series(db, "sleep_score", series)

        analyzer = WearableTrendAnalyzer(db)
        result = analyzer.analyze_metric("sleep_score", days=14)
        assert result is not None
        assert result.direction == "stable"

    def test_insufficient_data_returns_none(self, db):
        """Fewer than 5 data points should return None."""
        today = date.today()
        series = [(80, today - timedelta(days=2 - i)) for i in range(3)]
        self._insert_series(db, "hrv", series)

        analyzer = WearableTrendAnalyzer(db)
        result = analyzer.analyze_metric("hrv", days=14)
        assert result is None

    def test_detect_all_trends_filters_stable(self, db):
        """detect_all_trends should only return non-stable metrics."""
        today = date.today()
        # HRV declining (should be returned)
        for i in range(7):
            d = today - timedelta(days=6 - i)
            wd = WhoopDaily(
                id=uuid.uuid4().hex, date=d,
                hrv=100 - i * 8,  # declining
                sleep_score=75.0,  # stable
                rhr=60.0,  # stable
                recovery_score=70.0,  # stable
                strain=10.0,  # stable
                sleep_duration_min=420,  # stable
            )
            db.insert_wearable_daily(wd)

        analyzer = WearableTrendAnalyzer(db)
        trends = analyzer.detect_all_trends(days=14)
        # HRV should show up, stable metrics should not
        metric_names = [t.metric_name for t in trends]
        assert "hrv" in metric_names
        # Stable metrics should not appear
        assert "sleep_score" not in metric_names

    def test_no_data_returns_empty(self, db):
        """No wearable data should return empty trends."""
        analyzer = WearableTrendAnalyzer(db)
        trends = analyzer.detect_all_trends(days=14)
        assert trends == []


class TestWearableAnomalyDetection:
    """Test single-day outlier detection."""

    def _insert_baseline(self, db, days: int = 10, **metrics):
        """Insert stable baseline data for the given metrics."""
        today = date.today()
        for i in range(days):
            d = today - timedelta(days=days - i)
            wd = WhoopDaily(id=uuid.uuid4().hex, date=d, **metrics)
            db.insert_wearable_daily(wd)

    def test_low_sleep_score_flagged(self, db):
        """sleep_score < 40 should produce urgent anomaly."""
        today = date.today()
        # 7 days of good sleep baseline
        self._insert_baseline(db, days=8, sleep_score=75.0)
        # Today: terrible sleep
        wd = WhoopDaily(id=uuid.uuid4().hex, date=today, sleep_score=30.0)
        db.insert_wearable_daily(wd)

        analyzer = WearableTrendAnalyzer(db)
        anomalies = analyzer.detect_anomalies(days=1)
        sleep_anomalies = [a for a in anomalies if a.metric_name == "sleep_score"]
        assert len(sleep_anomalies) == 1
        assert sleep_anomalies[0].severity == "urgent"

    def test_hrv_drop_30pct_flagged(self, db):
        """HRV >30% below 7-day avg should produce urgent anomaly."""
        today = date.today()
        # 7 days of normal HRV
        self._insert_baseline(db, days=8, hrv=100.0)
        # Today: HRV crashed to 50 (50% drop)
        wd = WhoopDaily(id=uuid.uuid4().hex, date=today, hrv=50.0)
        db.insert_wearable_daily(wd)

        analyzer = WearableTrendAnalyzer(db)
        anomalies = analyzer.detect_anomalies(days=1)
        hrv_anomalies = [a for a in anomalies if a.metric_name == "hrv"]
        assert len(hrv_anomalies) == 1
        assert hrv_anomalies[0].severity == "urgent"

    def test_spo2_below_92_flagged(self, db):
        """SpO2 < 92% should produce urgent anomaly."""
        today = date.today()
        self._insert_baseline(db, days=8, spo2=97.0)
        wd = WhoopDaily(id=uuid.uuid4().hex, date=today, spo2=90.0)
        db.insert_wearable_daily(wd)

        analyzer = WearableTrendAnalyzer(db)
        anomalies = analyzer.detect_anomalies(days=1)
        spo2_anomalies = [a for a in anomalies if a.metric_name == "spo2"]
        assert len(spo2_anomalies) == 1
        assert spo2_anomalies[0].severity == "urgent"

    def test_normal_values_no_anomaly(self, db):
        """Normal values within baseline should produce no anomalies."""
        today = date.today()
        self._insert_baseline(
            db, days=8, hrv=80.0, rhr=58.0, sleep_score=78.0,
            spo2=97.0, recovery_score=65.0,
        )
        # Today: all normal
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=82.0, rhr=57.0, sleep_score=80.0,
            spo2=97.0, recovery_score=68.0,
        )
        db.insert_wearable_daily(wd)

        analyzer = WearableTrendAnalyzer(db)
        anomalies = analyzer.detect_anomalies(days=1)
        assert anomalies == []

    def test_insufficient_baseline_no_anomaly(self, db):
        """Without enough baseline data, no baseline anomalies should fire."""
        today = date.today()
        # Only 1 day of data — not enough for 7-day baseline
        wd = WhoopDaily(id=uuid.uuid4().hex, date=today, hrv=30.0)
        db.insert_wearable_daily(wd)

        analyzer = WearableTrendAnalyzer(db)
        anomalies = analyzer.detect_anomalies(days=1)
        # HRV has pct_below_baseline rule — needs baseline, so no anomaly
        hrv_anomalies = [a for a in anomalies if a.metric_name == "hrv"]
        assert hrv_anomalies == []
