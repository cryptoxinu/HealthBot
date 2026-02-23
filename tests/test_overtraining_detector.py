"""Tests for the overtraining detector."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.overtraining_detector import OvertrainingDetector


def _make_rows(
    *,
    rhr: list[float] | None = None,
    hrv: list[float] | None = None,
    strain: list[float] | None = None,
    recovery: list[float] | None = None,
    sleep: list[float] | None = None,
    count: int = 14,
) -> list[dict]:
    """Build synthetic wearable rows (DESC order, newest first)."""
    rows = []
    for i in range(count):
        row: dict = {}
        if rhr is not None and i < len(rhr):
            row["rhr"] = rhr[i]
        elif rhr is not None:
            row["rhr"] = rhr[-1]
        if hrv is not None and i < len(hrv):
            row["hrv"] = hrv[i]
        elif hrv is not None:
            row["hrv"] = hrv[-1]
        if strain is not None and i < len(strain):
            row["strain"] = strain[i]
        elif strain is not None:
            row["strain"] = strain[-1]
        if recovery is not None and i < len(recovery):
            row["recovery_score"] = recovery[i]
        elif recovery is not None:
            row["recovery_score"] = recovery[-1]
        if sleep is not None and i < len(sleep):
            row["sleep_score"] = sleep[i]
        elif sleep is not None:
            row["sleep_score"] = sleep[-1]
        rows.append(row)
    return rows


class TestOvertrainingDetector:
    """Overtraining signal detection from wearable data."""

    def _make_detector(self, rows: list[dict]) -> OvertrainingDetector:
        db = MagicMock()
        db.query_wearable_daily = MagicMock(return_value=rows)
        return OvertrainingDetector(db)

    def test_no_signals_severity_none(self):
        # Normal values — no overtraining
        rows = _make_rows(
            rhr=[60] * 14, hrv=[50] * 14,
            strain=[10] * 14, recovery=[70] * 14, sleep=[75] * 14,
        )
        detector = self._make_detector(rows)
        result = detector.assess(user_id=1)
        assert result.severity == "none"
        assert result.confidence < 0.3

    def test_hrv_decline_detected(self):
        # HRV drops >15% in recent 3 days vs older 3 days
        hrv = [30, 32, 31, 50, 52, 48, 50] + [50] * 7
        rows = _make_rows(hrv=hrv)
        detector = self._make_detector(rows)
        signal = detector._check_hrv_declining(rows)
        assert signal.present

    def test_rhr_elevated_detected(self):
        # Recent 7 days have RHR 10%+ above baseline
        # Baseline (older 7 days) avg = 60, elevated = 67+
        rhr = [70, 68, 69, 70, 71, 68, 67] + [60, 59, 61, 60, 58, 62, 60]
        rows = _make_rows(rhr=rhr)
        detector = self._make_detector(rows)
        signal = detector._check_rhr_elevated(rows)
        assert signal.present

    def test_low_recovery_detected(self):
        # Recovery <40% for 3+ of last 5 days
        recovery = [30, 35, 38, 70, 25] + [70] * 9
        rows = _make_rows(recovery=recovery)
        detector = self._make_detector(rows)
        signal = detector._check_low_recovery(rows)
        assert signal.present

    def test_high_strain_detected(self):
        # Average strain >16 over past 5 days
        strain = [18, 17, 19, 18, 17] + [10] * 9
        rows = _make_rows(strain=strain)
        detector = self._make_detector(rows)
        signal = detector._check_high_strain(rows)
        assert signal.present

    def test_poor_sleep_detected(self):
        # Sleep score <60 for 5+ of last 7 days
        sleep = [50, 45, 55, 40, 58, 52, 48] + [75] * 7
        rows = _make_rows(sleep=sleep)
        detector = self._make_detector(rows)
        signal = detector._check_poor_sleep(rows)
        assert signal.present

    def test_combined_signals_severity_likely(self):
        # Multiple signals present -> severity "likely"
        rows = _make_rows(
            rhr=[70, 68, 69, 70, 71, 68, 67] + [60] * 7,
            hrv=[30, 32, 31, 50, 52, 48, 50] + [50] * 7,
            strain=[18] * 14,
            recovery=[30] * 14,
            sleep=[50] * 14,
        )
        detector = self._make_detector(rows)
        result = detector.assess(user_id=1)
        assert result.severity == "likely"
        assert result.positive_count >= 3

    def test_insufficient_data_no_signals(self):
        # Too few rows -> all signals return present=False
        rows = _make_rows(rhr=[60], hrv=[50], count=3)
        detector = self._make_detector(rows)
        result = detector.assess(user_id=1)
        assert result.severity == "none"

    def test_format_assessment(self):
        rows = _make_rows(
            rhr=[60] * 14, hrv=[50] * 14,
            strain=[10] * 14, recovery=[70] * 14, sleep=[75] * 14,
        )
        detector = self._make_detector(rows)
        result = detector.assess(user_id=1)
        text = detector.format_assessment(result)
        assert "Overtraining Assessment" in text
        assert "NONE" in text
