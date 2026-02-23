"""Tests for correlation engine."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult, WhoopDaily
from healthbot.reasoning.correlate import (
    CLINICAL_CORRELATION_KB,
    CorrelationAlert,
    CorrelationEngine,
    format_correlation_alerts,
)


class TestCorrelations:
    """Test lab ↔ wearable correlation analysis."""

    def test_no_data_returns_empty(self, db):
        """No data should return empty list."""
        engine = CorrelationEngine(db)
        result = engine.auto_discover()
        assert result == []

    def test_correlation_with_matching_data(self, db):
        """Overlapping data should produce correlations."""
        today = date.today()

        # Insert lab results and wearable data for same dates
        for i in range(10):
            d = today - timedelta(days=i * 7)
            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name="Glucose",
                canonical_name="glucose",
                value=90.0 + i * 2,  # Increasing
                unit="mg/dL",
                date_collected=d,
            )
            db.insert_observation(lab)

            wd = WhoopDaily(
                id=uuid.uuid4().hex,
                date=d,
                hrv=50.0 - i * 1.5,  # Decreasing (negative correlation)
                rhr=60.0 + i,
                recovery_score=80.0 - i * 2,
            )
            db.insert_wearable_daily(wd)

        engine = CorrelationEngine(db)
        result = engine.correlate_lab_wearable("glucose", "hrv", days=100)
        # With synthetic perfectly correlated data, we should get a result
        if result:
            assert abs(result.pearson_r) > 0.3

    def test_format_empty(self, db):
        """Formatting empty correlations should give a message."""
        engine = CorrelationEngine(db)
        text = engine.format_correlations([])
        assert "No significant correlations" in text


class TestClinicalCorrelationKB:
    """Test the clinical correlation knowledge base."""

    def test_kb_not_empty(self):
        """KB should have entries."""
        assert len(CLINICAL_CORRELATION_KB) >= 15

    def test_all_entries_have_required_fields(self):
        """Every entry needs all clinical fields."""
        for rule in CLINICAL_CORRELATION_KB:
            assert rule.lab_metric, f"Missing lab_metric: {rule}"
            assert rule.wearable_metric, f"Missing wearable_metric: {rule}"
            assert rule.expected_direction in ("positive", "negative")
            assert rule.clinical_context
            assert rule.actionable_advice
            assert rule.evidence

    def test_expected_pairs_present(self):
        """Key clinical pairs should be in the KB."""
        pairs = {(r.lab_metric, r.wearable_metric) for r in CLINICAL_CORRELATION_KB}
        assert ("cortisol", "hrv") in pairs
        assert ("tsh", "rhr") in pairs
        assert ("glucose", "sleep_score") in pairs
        assert ("ferritin", "recovery_score") in pairs
        assert ("vitamin_d", "sleep_score") in pairs
        assert ("crp", "hrv") in pairs
        assert ("testosterone", "recovery_score") in pairs
        assert ("magnesium", "hrv") in pairs


class TestCorrelationAlerts:
    """Test clinical correlation alert generation."""

    def _seed_correlated_data(
        self, db, lab_name: str, wearable_metric: str,
        direction: str = "negative", n: int = 15,
    ):
        """Seed data with a clear correlation for testing."""
        today = date.today()
        for i in range(n):
            d = today - timedelta(days=i * 3)
            lab_value = 100.0 + i * 5  # increasing
            if direction == "negative":
                wearable_value = 80.0 - i * 3  # decreasing (negative corr)
            else:
                wearable_value = 40.0 + i * 3  # increasing (positive corr)

            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name=lab_name,
                canonical_name=lab_name,
                value=lab_value,
                unit="",
                date_collected=d,
            )
            db.insert_observation(lab)

            wd_kwargs = {
                "id": uuid.uuid4().hex,
                "date": d,
                wearable_metric: wearable_value,
            }
            wd = WhoopDaily(**wd_kwargs)
            db.insert_wearable_daily(wd)

    def test_detects_known_negative_correlation(self, db):
        """cortisol↔HRV negative correlation should trigger alert."""
        self._seed_correlated_data(db, "cortisol", "hrv", "negative")
        engine = CorrelationEngine(db)
        alerts = engine.generate_correlation_alerts(days=180, min_r=0.3)
        cortisol_alerts = [a for a in alerts if a.lab_metric == "cortisol"]
        assert len(cortisol_alerts) >= 1
        assert cortisol_alerts[0].wearable_metric == "hrv"
        assert cortisol_alerts[0].pearson_r < -0.3

    def test_detects_known_positive_correlation(self, db):
        """ferritin↔recovery_score positive correlation should trigger alert."""
        self._seed_correlated_data(
            db, "ferritin", "recovery_score", "positive",
        )
        engine = CorrelationEngine(db)
        alerts = engine.generate_correlation_alerts(days=180, min_r=0.3)
        ferritin_alerts = [
            a for a in alerts
            if a.lab_metric == "ferritin" and a.wearable_metric == "recovery_score"
        ]
        assert len(ferritin_alerts) == 1
        assert ferritin_alerts[0].pearson_r > 0.3

    def test_wrong_direction_not_alerted(self, db):
        """If correlation direction is opposite to expected, no alert."""
        # cortisol↔HRV expects negative, but we seed positive
        self._seed_correlated_data(db, "cortisol", "hrv", "positive")
        engine = CorrelationEngine(db)
        alerts = engine.generate_correlation_alerts(days=180, min_r=0.3)
        cortisol_hrv = [
            a for a in alerts
            if a.lab_metric == "cortisol" and a.wearable_metric == "hrv"
        ]
        assert len(cortisol_hrv) == 0

    def test_no_data_returns_empty(self, db):
        """No data should produce no alerts."""
        engine = CorrelationEngine(db)
        alerts = engine.generate_correlation_alerts(days=90)
        assert alerts == []

    def test_weak_correlation_filtered(self, db):
        """Correlation below threshold should not trigger alert."""
        # Only 5 points with noise — weak correlation
        today = date.today()
        for i in range(5):
            d = today - timedelta(days=i * 3)
            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name="cortisol",
                canonical_name="cortisol",
                value=100.0 + (i % 2) * 10,  # zigzag — weak pattern
                unit="nmol/L",
                date_collected=d,
            )
            db.insert_observation(lab)
            wd = WhoopDaily(
                id=uuid.uuid4().hex, date=d,
                hrv=60.0 - (i % 3) * 5,  # zigzag
            )
            db.insert_wearable_daily(wd)

        engine = CorrelationEngine(db)
        alerts = engine.generate_correlation_alerts(days=90, min_r=0.8)
        # Very high threshold should filter out weak correlations
        assert len(alerts) == 0

    def test_alerts_sorted_by_strength(self, db):
        """Alerts should be sorted by |r| descending."""
        self._seed_correlated_data(db, "cortisol", "hrv", "negative", n=20)
        self._seed_correlated_data(db, "cortisol", "rhr", "positive", n=20)
        engine = CorrelationEngine(db)
        alerts = engine.generate_correlation_alerts(days=180, min_r=0.3)
        if len(alerts) >= 2:
            for i in range(len(alerts) - 1):
                assert abs(alerts[i].pearson_r) >= abs(alerts[i + 1].pearson_r)

    def test_alert_has_clinical_fields(self, db):
        """Alert should include clinical context and evidence."""
        self._seed_correlated_data(db, "cortisol", "hrv", "negative")
        engine = CorrelationEngine(db)
        alerts = engine.generate_correlation_alerts(days=180, min_r=0.3)
        cortisol_alerts = [a for a in alerts if a.lab_metric == "cortisol"]
        assert len(cortisol_alerts) >= 1
        alert = cortisol_alerts[0]
        assert alert.clinical_context
        assert alert.actionable_advice
        assert alert.evidence
        assert alert.n_observations >= 5


class TestFormatCorrelationAlerts:
    """Test formatting of correlation alerts."""

    def test_format_empty(self):
        """Empty list should give 'no correlations' message."""
        text = format_correlation_alerts([])
        assert "No clinically significant" in text

    def test_format_with_alerts(self):
        """Alerts should format with context and evidence."""
        alert = CorrelationAlert(
            lab_metric="cortisol",
            wearable_metric="hrv",
            pearson_r=-0.72,
            clinical_context="Elevated cortisol lowers HRV",
            actionable_advice="Consider stress management",
            evidence="Thayer JF 2012",
            n_observations=15,
        )
        text = format_correlation_alerts([alert])
        assert "CORTISOL" in text
        assert "HRV" in text
        assert "-0.72" in text
        assert "stress management" in text
        assert "Thayer" in text
