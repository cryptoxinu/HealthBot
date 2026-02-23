"""Tests for wearable-based stress detection."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.stress_detector import (
    STRESS_SIGNALS,
    StressDetector,
    format_stress,
)


def _make_db(wearables: list[dict] | None = None) -> MagicMock:
    db = MagicMock()
    wearables = wearables or []
    db.query_wearable_daily.return_value = wearables
    return db


def _day(
    dt: str, hrv: float = 50, rhr: float = 60,
    sleep: float = 80, recovery: float = 70, skin_temp: float = 0,
) -> dict:
    d = {
        "_date": dt, "hrv": hrv, "rhr": rhr,
        "sleep_score": sleep, "recovery_score": recovery,
    }
    if skin_temp:
        d["skin_temp"] = skin_temp
    return d


class TestStressSignals:
    def test_weights_sum_to_one(self):
        total = sum(s.weight for s in STRESS_SIGNALS)
        assert abs(total - 1.0) < 0.01

    def test_all_have_valid_direction(self):
        for s in STRESS_SIGNALS:
            assert s.bad_direction in ("increasing", "decreasing")


class TestInsufficientData:
    def test_no_data(self):
        db = _make_db([])
        detector = StressDetector(db)
        result = detector.assess(user_id=1)
        assert result.stress_level == "low"
        assert result.data_days == 0

    def test_two_days(self):
        db = _make_db([_day("2024-01-01"), _day("2024-01-02")])
        detector = StressDetector(db)
        result = detector.assess(user_id=1)
        assert result.stress_level == "low"
        assert result.data_days == 2


class TestLowStress:
    def test_stable_metrics(self):
        """Stable metrics → low stress."""
        days = [
            _day(f"2024-01-0{i}", hrv=50, rhr=60, sleep=80, recovery=70)
            for i in range(1, 8)
        ]
        db = _make_db(days)
        detector = StressDetector(db)
        result = detector.assess(user_id=1)

        assert result.stress_level == "low"
        assert result.stress_score < 0.2
        assert len(result.active_signals) == 0


class TestModerateStress:
    def test_hrv_declining(self):
        """HRV declining 20% → moderate stress."""
        days = [
            _day("2024-01-01", hrv=60, rhr=60, sleep=80, recovery=70),
            _day("2024-01-02", hrv=58, rhr=60, sleep=80, recovery=70),
            _day("2024-01-03", hrv=55, rhr=60, sleep=80, recovery=70),
            _day("2024-01-04", hrv=50, rhr=60, sleep=80, recovery=70),
            _day("2024-01-05", hrv=48, rhr=60, sleep=80, recovery=70),
            _day("2024-01-06", hrv=45, rhr=60, sleep=80, recovery=70),
        ]
        db = _make_db(days)
        detector = StressDetector(db)
        result = detector.assess(user_id=1)

        assert result.stress_score >= 0.2
        assert "HRV declining" in result.active_signals


class TestHighStress:
    def test_multiple_signals(self):
        """HRV down + RHR up + sleep down → high stress."""
        days = [
            _day("2024-01-01", hrv=60, rhr=58, sleep=85, recovery=75),
            _day("2024-01-02", hrv=55, rhr=60, sleep=80, recovery=70),
            _day("2024-01-03", hrv=50, rhr=63, sleep=75, recovery=60),
            _day("2024-01-04", hrv=45, rhr=65, sleep=70, recovery=55),
            _day("2024-01-05", hrv=42, rhr=67, sleep=68, recovery=50),
            _day("2024-01-06", hrv=40, rhr=70, sleep=65, recovery=45),
        ]
        db = _make_db(days)
        detector = StressDetector(db)
        result = detector.assess(user_id=1)

        assert result.stress_score >= 0.45
        assert len(result.active_signals) >= 2
        assert len(result.recommendations) > 0


class TestCriticalStress:
    def test_all_signals_bad(self):
        """All metrics deteriorating → critical."""
        days = [
            _day("2024-01-01", hrv=70, rhr=55, sleep=90, recovery=85),
            _day("2024-01-02", hrv=60, rhr=60, sleep=80, recovery=70),
            _day("2024-01-03", hrv=50, rhr=65, sleep=70, recovery=55),
            _day("2024-01-04", hrv=40, rhr=72, sleep=60, recovery=40),
            _day("2024-01-05", hrv=35, rhr=75, sleep=55, recovery=35),
            _day("2024-01-06", hrv=30, rhr=78, sleep=50, recovery=30),
        ]
        db = _make_db(days)
        detector = StressDetector(db)
        result = detector.assess(user_id=1)

        assert result.stress_score >= 0.7
        assert result.stress_level == "critical"
        assert len(result.active_signals) >= 4
        assert len(result.recommendations) > 0


class TestFormatting:
    def test_format_insufficient_data(self):
        db = _make_db([_day("2024-01-01")])
        detector = StressDetector(db)
        result = detector.assess(user_id=1)
        text = format_stress(result)
        assert "Insufficient" in text

    def test_format_with_signals(self):
        days = [
            _day("2024-01-01", hrv=60, rhr=58, sleep=85, recovery=75),
            _day("2024-01-02", hrv=50, rhr=63, sleep=75, recovery=60),
            _day("2024-01-03", hrv=45, rhr=65, sleep=70, recovery=55),
            _day("2024-01-04", hrv=40, rhr=70, sleep=65, recovery=45),
        ]
        db = _make_db(days)
        detector = StressDetector(db)
        result = detector.assess(user_id=1)
        text = format_stress(result)

        assert "STRESS ASSESSMENT" in text
        assert "Active Signals" in text
