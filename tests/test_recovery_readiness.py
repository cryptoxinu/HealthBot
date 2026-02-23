"""Tests for recovery readiness scoring."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import WhoopDaily
from healthbot.reasoning.recovery_readiness import RecoveryReadinessEngine


class TestRecoveryReadiness:
    """Test recovery readiness computation."""

    def _insert_baseline(self, db, days: int = 30, **metrics):
        """Insert baseline wearable data."""
        today = date.today()
        for i in range(days):
            d = today - timedelta(days=days - i)
            wd = WhoopDaily(id=uuid.uuid4().hex, date=d, **metrics)
            db.insert_wearable_daily(wd)

    def test_high_readiness_peak_grade(self, db):
        """Good HRV, low RHR, good sleep, low strain → peak."""
        # 30 days baseline
        self._insert_baseline(
            db, days=30,
            hrv=80.0, rhr=55.0, sleep_score=85.0,
            recovery_score=90.0, strain=8.0,
        )
        # Today: above baseline HRV, below baseline RHR
        today = date.today()
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=100.0,  # 125% of baseline (80)
            rhr=48.0,   # 87% of baseline (55)
            sleep_score=90.0,
            recovery_score=95.0,
            strain=6.0,
        )
        db.insert_wearable_daily(wd)

        engine = RecoveryReadinessEngine(db)
        result = engine.compute()
        assert result is not None
        assert result.grade == "peak"
        assert result.score >= 80

    def test_low_readiness_rest_grade(self, db):
        """Bad HRV, elevated RHR, poor sleep, high strain → rest."""
        # 30 days baseline
        self._insert_baseline(
            db, days=30,
            hrv=80.0, rhr=55.0, sleep_score=75.0,
            recovery_score=70.0, strain=12.0,
        )
        # Today: everything bad
        today = date.today()
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=40.0,   # 50% of baseline — below 60% threshold
            rhr=66.0,   # 120% of baseline — above 115% threshold
            sleep_score=25.0,
            recovery_score=15.0,
            strain=19.0,
        )
        db.insert_wearable_daily(wd)

        engine = RecoveryReadinessEngine(db)
        result = engine.compute()
        assert result is not None
        assert result.grade == "rest"
        assert result.score < 40

    def test_no_data_returns_none(self, db):
        """No wearable data should return None."""
        engine = RecoveryReadinessEngine(db)
        result = engine.compute()
        assert result is None

    def test_partial_data_uses_defaults(self, db):
        """Missing recovery_score should default to 50."""
        today = date.today()
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=80.0, rhr=55.0, sleep_score=75.0,
            # No recovery_score, no strain
        )
        db.insert_wearable_daily(wd)

        engine = RecoveryReadinessEngine(db)
        result = engine.compute()
        assert result is not None
        assert result.components["recovery_score"] == 50.0

    def test_limiting_factors_populated(self, db):
        """Components below 40 should appear in limiting_factors."""
        self._insert_baseline(
            db, days=30,
            hrv=80.0, rhr=55.0, sleep_score=75.0,
            recovery_score=70.0, strain=12.0,
        )
        today = date.today()
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=40.0,  # Very low vs baseline → score ~0
            rhr=55.0,
            sleep_score=75.0,
            recovery_score=70.0,
            strain=12.0,
        )
        db.insert_wearable_daily(wd)

        engine = RecoveryReadinessEngine(db)
        result = engine.compute()
        assert result is not None
        assert any("HRV" in f for f in result.limiting_factors)

    def test_format_includes_recommendation(self, db):
        """Formatted output should include the recommendation text."""
        today = date.today()
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=80.0, rhr=55.0, sleep_score=75.0,
            recovery_score=70.0, strain=10.0,
        )
        db.insert_wearable_daily(wd)

        engine = RecoveryReadinessEngine(db)
        result = engine.compute()
        assert result is not None
        text = engine.format_readiness(result)
        assert result.recommendation in text
        assert result.grade in text

    def test_training_guidance_returns_zone(self, db):
        """Training guidance should return a zone based on score."""
        today = date.today()
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=80.0, rhr=55.0, sleep_score=85.0,
            recovery_score=80.0, strain=10.0,
        )
        db.insert_wearable_daily(wd)

        engine = RecoveryReadinessEngine(db)
        guidance = engine.get_training_guidance()
        assert guidance is not None
        assert guidance.zone.name in (
            "Peak Performance", "Ready", "Moderate Recovery", "Rest & Recover",
        )
        assert guidance.zone.strain_target

    def test_training_guidance_no_data(self, db):
        """No wearable data → None."""
        engine = RecoveryReadinessEngine(db)
        guidance = engine.get_training_guidance()
        assert guidance is None

    def test_format_training_guidance(self, db):
        """Format should include zone info."""
        from healthbot.reasoning.recovery_readiness import format_training_guidance
        today = date.today()
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=80.0, rhr=55.0, sleep_score=85.0,
            recovery_score=80.0, strain=10.0,
        )
        db.insert_wearable_daily(wd)

        engine = RecoveryReadinessEngine(db)
        guidance = engine.get_training_guidance()
        assert guidance is not None
        text = format_training_guidance(guidance)
        assert "TRAINING GUIDANCE" in text
        assert "Target Strain" in text
