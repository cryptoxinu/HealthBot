"""Tests for the background health watcher (deterministic alert generation)."""
from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

from healthbot.reasoning.watcher import HealthWatcher
from healthbot.reasoning.wearable_trends import WearableAnomaly, WearableTrendResult


@dataclass
class _FakeOverdueItem:
    test_name: str
    canonical_name: str
    last_date: str
    interval_months: int
    days_overdue: int


@dataclass
class _FakeTrendResult:
    test_name: str
    canonical_name: str
    direction: str
    slope: float
    r_squared: float
    data_points: int
    first_date: str
    last_date: str
    first_value: float
    last_value: float
    pct_change: float
    values: list[tuple[str, float]]


class TestHealthWatcher:
    """Unit tests for HealthWatcher alert generation."""

    def _make_watcher(self) -> HealthWatcher:
        db = MagicMock()
        return HealthWatcher(db)

    def test_no_alerts_when_no_data(self) -> None:
        """No overdue items and no trends -> empty alert list."""
        watcher = self._make_watcher()
        with (
            patch("healthbot.reasoning.overdue.OverdueDetector") as mock_od,
            patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_ta,
        ):
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = []
            alerts = watcher.check_all()
        assert alerts == []

    def test_overdue_alert_generated(self) -> None:
        """Overdue items >60 days should produce watch-severity alerts."""
        watcher = self._make_watcher()
        item = _FakeOverdueItem(
            test_name="LDL",
            canonical_name="ldl",
            last_date="2024-06-01",
            interval_months=12,
            days_overdue=180,
        )
        with (
            patch("healthbot.reasoning.overdue.OverdueDetector") as mock_od,
            patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_ta,
        ):
            mock_od.return_value.check_overdue.return_value = [item]
            mock_ta.return_value.detect_all_trends.return_value = []
            alerts = watcher.check_all()

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.alert_type == "overdue"
        assert alert.severity == "watch"
        assert "LDL" in alert.title
        assert "6 months overdue" in alert.body

    def test_trend_alert_generated(self) -> None:
        """Trends with >10% change and >=3 data points should produce alerts."""
        watcher = self._make_watcher()
        trend = _FakeTrendResult(
            test_name="Glucose",
            canonical_name="glucose",
            direction="increasing",
            slope=0.5,
            r_squared=0.9,
            data_points=4,
            first_date="2024-01-01",
            last_date="2024-07-01",
            first_value=90.0,
            last_value=130.0,
            pct_change=44.4,
            values=[],
        )
        with (
            patch("healthbot.reasoning.overdue.OverdueDetector") as mock_od,
            patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_ta,
        ):
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = [trend]
            alerts = watcher.check_all()

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.alert_type == "trend"
        assert alert.severity == "urgent"  # >25% -> urgent
        assert "rose" in alert.title
        assert "44%" in alert.body

    def test_dedup_key_weekly_uniqueness(self) -> None:
        """Dedup keys for the same alert type+key should be identical within a run."""
        watcher = self._make_watcher()
        key1 = watcher._dedup_hash("overdue", "ldl")
        key2 = watcher._dedup_hash("overdue", "ldl")
        assert key1 == key2
        assert len(key1) == 16

    def test_dedup_key_differs_by_type(self) -> None:
        """Different alert types should produce different dedup keys."""
        watcher = self._make_watcher()
        key_overdue = watcher._dedup_hash("overdue", "ldl")
        key_trend = watcher._dedup_hash("trend", "ldl")
        assert key_overdue != key_trend

    def test_minor_overdue_not_alerted(self) -> None:
        """Overdue items <=60 days should NOT produce alerts."""
        watcher = self._make_watcher()
        item = _FakeOverdueItem(
            test_name="TSH",
            canonical_name="tsh",
            last_date="2024-11-01",
            interval_months=12,
            days_overdue=45,
        )
        with (
            patch("healthbot.reasoning.overdue.OverdueDetector") as mock_od,
            patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_ta,
        ):
            mock_od.return_value.check_overdue.return_value = [item]
            mock_ta.return_value.detect_all_trends.return_value = []
            alerts = watcher.check_all()
        assert alerts == []

    def test_small_trend_not_alerted(self) -> None:
        """Trends with <=10% change should NOT produce alerts."""
        watcher = self._make_watcher()
        trend = _FakeTrendResult(
            test_name="Hemoglobin",
            canonical_name="hemoglobin",
            direction="increasing",
            slope=0.01,
            r_squared=0.5,
            data_points=5,
            first_date="2024-01-01",
            last_date="2024-07-01",
            first_value=14.0,
            last_value=15.0,
            pct_change=7.1,
            values=[],
        )
        with (
            patch("healthbot.reasoning.overdue.OverdueDetector") as mock_od,
            patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_ta,
        ):
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = [trend]
            alerts = watcher.check_all()
        assert alerts == []

    def test_trend_watch_severity_for_moderate_change(self) -> None:
        """Trends between 10% and 25% should get 'watch' severity, not 'urgent'."""
        watcher = self._make_watcher()
        trend = _FakeTrendResult(
            test_name="ALT",
            canonical_name="alt",
            direction="increasing",
            slope=0.1,
            r_squared=0.8,
            data_points=3,
            first_date="2024-01-01",
            last_date="2024-06-01",
            first_value=30.0,
            last_value=36.0,
            pct_change=20.0,
            values=[],
        )
        with (
            patch("healthbot.reasoning.overdue.OverdueDetector") as mock_od,
            patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_ta,
        ):
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = [trend]
            alerts = watcher.check_all()

        assert len(alerts) == 1
        assert alerts[0].severity == "watch"

    def test_few_data_points_not_alerted(self) -> None:
        """Trends with <3 data points should NOT produce alerts even if >10%."""
        watcher = self._make_watcher()
        trend = _FakeTrendResult(
            test_name="Ferritin",
            canonical_name="ferritin",
            direction="increasing",
            slope=1.0,
            r_squared=1.0,
            data_points=2,
            first_date="2024-01-01",
            last_date="2024-07-01",
            first_value=50.0,
            last_value=80.0,
            pct_change=60.0,
            values=[],
        )
        with (
            patch("healthbot.reasoning.overdue.OverdueDetector") as mock_od,
            patch("healthbot.reasoning.trends.TrendAnalyzer") as mock_ta,
        ):
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = [trend]
            alerts = watcher.check_all()
        assert alerts == []


class TestWearableAlerts:
    """Tests for wearable trend and anomaly alerts."""

    def _make_watcher(self) -> HealthWatcher:
        db = MagicMock()
        return HealthWatcher(db)

    def _patch_all(self):
        """Context manager to patch all watcher dependencies."""
        return (
            patch("healthbot.reasoning.overdue.OverdueDetector"),
            patch("healthbot.reasoning.trends.TrendAnalyzer"),
            patch("healthbot.reasoning.wearable_trends.WearableTrendAnalyzer"),
        )

    def test_wearable_trend_alert_generated(self) -> None:
        """Declining HRV over 7+ days with >20% change should produce alert."""
        watcher = self._make_watcher()
        trend = WearableTrendResult(
            metric_name="hrv",
            display_name="HRV",
            direction="decreasing",
            slope=-3.0,
            r_squared=0.85,
            data_points=7,
            first_date="2026-02-07",
            last_date="2026-02-14",
            first_value=95.0,
            last_value=70.0,
            pct_change=-26.3,
            values=[],
        )
        p_od, p_ta, p_wt = self._patch_all()
        with p_od as mock_od, p_ta as mock_ta, p_wt as mock_wt:
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = []
            mock_wt.return_value.detect_all_trends.return_value = [trend]
            mock_wt.return_value.detect_anomalies.return_value = []
            alerts = watcher.check_all()

        wearable_alerts = [a for a in alerts if a.alert_type == "wearable_trend"]
        assert len(wearable_alerts) == 1
        assert "HRV" in wearable_alerts[0].title
        assert "declining" in wearable_alerts[0].title
        assert wearable_alerts[0].severity == "watch"

    def test_wearable_anomaly_alert_generated(self) -> None:
        """Low sleep score anomaly should produce wearable_anomaly alert."""
        watcher = self._make_watcher()
        anomaly = WearableAnomaly(
            metric_name="sleep_score",
            display_name="Sleep Score",
            date="2026-02-14",
            value=30.0,
            baseline=0.0,
            deviation_pct=0.0,
            severity="urgent",
            message="Sleep score critically low (30/100)",
        )
        p_od, p_ta, p_wt = self._patch_all()
        with p_od as mock_od, p_ta as mock_ta, p_wt as mock_wt:
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = []
            mock_wt.return_value.detect_all_trends.return_value = []
            mock_wt.return_value.detect_anomalies.return_value = [anomaly]
            alerts = watcher.check_all()

        anomaly_alerts = [a for a in alerts if a.alert_type == "wearable_anomaly"]
        assert len(anomaly_alerts) == 1
        assert anomaly_alerts[0].severity == "urgent"
        assert "Sleep Score" in anomaly_alerts[0].title

    def test_no_wearable_alerts_when_no_data(self) -> None:
        """Empty wearable data should produce no wearable alerts."""
        watcher = self._make_watcher()
        p_od, p_ta, p_wt = self._patch_all()
        with p_od as mock_od, p_ta as mock_ta, p_wt as mock_wt:
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = []
            mock_wt.return_value.detect_all_trends.return_value = []
            mock_wt.return_value.detect_anomalies.return_value = []
            alerts = watcher.check_all()

        wearable_alerts = [
            a for a in alerts
            if a.alert_type in ("wearable_trend", "wearable_anomaly")
        ]
        assert wearable_alerts == []

    def test_wearable_alerts_use_dedup(self) -> None:
        """Wearable trend alerts should use weekly dedup keys."""
        watcher = self._make_watcher()
        trend = WearableTrendResult(
            metric_name="hrv",
            display_name="HRV",
            direction="decreasing",
            slope=-3.0,
            r_squared=0.85,
            data_points=7,
            first_date="2026-02-07",
            last_date="2026-02-14",
            first_value=95.0,
            last_value=70.0,
            pct_change=-26.3,
            values=[],
        )
        p_od, p_ta, p_wt = self._patch_all()
        with p_od as mock_od, p_ta as mock_ta, p_wt as mock_wt:
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = []
            mock_wt.return_value.detect_all_trends.return_value = [trend]
            mock_wt.return_value.detect_anomalies.return_value = []
            alerts = watcher.check_all()

        wearable_alerts = [a for a in alerts if a.alert_type == "wearable_trend"]
        assert len(wearable_alerts) == 1
        # Dedup key should be a 16-char hex hash
        assert len(wearable_alerts[0].dedup_key) == 16
        # Same watcher, same metric, same week -> same dedup key
        expected_key = watcher._dedup_hash("wearable_trend", "hrv")
        assert wearable_alerts[0].dedup_key == expected_key

    def test_wearable_trend_below_threshold_not_alerted(self) -> None:
        """HRV declining only 10% (below 20% threshold) should not alert."""
        watcher = self._make_watcher()
        trend = WearableTrendResult(
            metric_name="hrv",
            display_name="HRV",
            direction="decreasing",
            slope=-1.0,
            r_squared=0.6,
            data_points=7,
            first_date="2026-02-07",
            last_date="2026-02-14",
            first_value=80.0,
            last_value=72.0,
            pct_change=-10.0,
            values=[],
        )
        p_od, p_ta, p_wt = self._patch_all()
        with p_od as mock_od, p_ta as mock_ta, p_wt as mock_wt:
            mock_od.return_value.check_overdue.return_value = []
            mock_ta.return_value.detect_all_trends.return_value = []
            mock_wt.return_value.detect_all_trends.return_value = [trend]
            mock_wt.return_value.detect_anomalies.return_value = []
            alerts = watcher.check_all()

        wearable_alerts = [a for a in alerts if a.alert_type == "wearable_trend"]
        assert wearable_alerts == []
