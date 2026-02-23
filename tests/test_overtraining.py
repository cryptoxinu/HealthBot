"""Tests for overtraining syndrome detection."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import WhoopDaily
from healthbot.reasoning.overtraining_detector import OvertrainingDetector


class TestOvertrainingDetector:
    """Test overtraining signal detection."""

    def _insert_days(self, db, days=10, **metrics):
        """Insert multiple days of wearable data."""
        today = date.today()
        for i in range(days):
            d = today - timedelta(days=days - 1 - i)
            wd = WhoopDaily(id=uuid.uuid4().hex, date=d, **metrics)
            db.insert_wearable_daily(wd)

    def test_all_signals_likely(self, db):
        """All overtraining signals present → 'likely' severity."""
        today = date.today()
        # 30 days: first 20 normal, last 10 bad
        for i in range(30):
            d = today - timedelta(days=29 - i)
            if i < 20:
                # Normal baseline
                wd = WhoopDaily(
                    id=uuid.uuid4().hex, date=d,
                    hrv=80.0, rhr=55.0, sleep_score=75.0,
                    recovery_score=70.0, strain=12.0,
                )
            else:
                # Overtrained: elevated RHR, dropping HRV, high strain,
                # low recovery, poor sleep
                wd = WhoopDaily(
                    id=uuid.uuid4().hex, date=d,
                    hrv=45.0,  # way down from 80
                    rhr=72.0,  # way up from 55
                    sleep_score=45.0,  # poor
                    recovery_score=25.0,  # low
                    strain=18.0,  # high
                )
            db.insert_wearable_daily(wd)

        detector = OvertrainingDetector(db)
        result = detector.assess()
        assert result.severity == "likely"
        assert result.confidence > 0.6
        assert result.positive_count >= 3

    def test_three_signals_watch_severity(self, db):
        """3 signals (strain + recovery + sleep) → 'watch' severity."""
        today = date.today()
        # 30 days: first 20 normal, last 10 with high strain + low recovery
        # + poor sleep (but normal HRV and RHR)
        for i in range(30):
            d = today - timedelta(days=29 - i)
            if i < 20:
                wd = WhoopDaily(
                    id=uuid.uuid4().hex, date=d,
                    hrv=80.0, rhr=55.0, sleep_score=75.0,
                    recovery_score=70.0, strain=12.0,
                )
            else:
                wd = WhoopDaily(
                    id=uuid.uuid4().hex, date=d,
                    hrv=78.0,  # normal (no decline)
                    rhr=56.0,  # normal (not elevated)
                    sleep_score=45.0,  # poor (<60)
                    recovery_score=25.0,  # low (<40)
                    strain=18.0,  # high (>16)
                )
            db.insert_wearable_daily(wd)

        detector = OvertrainingDetector(db)
        result = detector.assess()
        # 3 signals: high_strain (0.20) + low_recovery (0.15) + poor_sleep (0.15) = 0.50
        # Confidence = 0.50 → "watch" severity (>= 0.3 but < 0.6)
        assert result.severity == "watch"
        assert result.positive_count >= 3

    def test_two_signals_none_or_watch(self, db):
        """Only 2 signals present → 'none' or 'watch'."""
        today = date.today()
        for i in range(15):
            d = today - timedelta(days=14 - i)
            wd = WhoopDaily(
                id=uuid.uuid4().hex, date=d,
                hrv=80.0,  # normal
                rhr=55.0,  # normal
                sleep_score=50.0,  # borderline bad (but < 60)
                recovery_score=35.0,  # low (<40)
                strain=12.0,  # normal
            )
            db.insert_wearable_daily(wd)

        detector = OvertrainingDetector(db)
        result = detector.assess()
        # Sleep and recovery might trigger, but HRV, RHR, strain are ok
        assert result.severity in ("none", "watch")

    def test_no_data_returns_none_severity(self, db):
        """No wearable data → 'none' severity."""
        detector = OvertrainingDetector(db)
        result = detector.assess()
        assert result.severity == "none"
        assert result.confidence == 0

    def test_healthy_data_no_signals(self, db):
        """All normal values → 'none' severity."""
        self._insert_days(
            db, days=15,
            hrv=80.0, rhr=55.0, sleep_score=80.0,
            recovery_score=75.0, strain=10.0,
        )
        detector = OvertrainingDetector(db)
        result = detector.assess()
        assert result.severity == "none"
        assert result.positive_count == 0

    def test_format_includes_recommendation(self, db):
        """Formatted output should include recommendation."""
        self._insert_days(
            db, days=15,
            hrv=80.0, rhr=55.0, sleep_score=80.0,
            recovery_score=75.0, strain=10.0,
        )
        detector = OvertrainingDetector(db)
        result = detector.assess()
        text = detector.format_assessment(result)
        assert result.recommendation in text
        assert "NONE" in text
