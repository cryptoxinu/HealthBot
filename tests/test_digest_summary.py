"""Tests for build_quick_summary() in reasoning/digest.py."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.digest import build_quick_summary


def _mock_db(
    *,
    wearables: list | None = None,
    meds: list | None = None,
):
    """Create a mock DB with configurable returns."""
    db = MagicMock()
    db.query_wearable_daily.return_value = wearables or []
    db.get_active_medications.return_value = meds or []
    # Stub out methods used by sub-modules that may be imported
    db.get_user_demographics.return_value = {}
    db.query_observations.return_value = []
    db.get_ltm_by_user.return_value = []
    db.get_active_hypotheses.return_value = []
    return db


class TestBuildQuickSummary:
    def test_empty_db_returns_empty(self):
        db = _mock_db()
        result = build_quick_summary(db, user_id=0)
        assert result == ""

    def test_with_medications(self):
        db = _mock_db(meds=[
            {"name": "Levothyroxine", "dose": "50mcg", "frequency": "daily"},
            {"name": "Vitamin D", "dose": "5000IU", "frequency": "daily"},
        ])
        result = build_quick_summary(db, user_id=0)
        assert "HEALTH STATUS" in result
        assert "Meds (2)" in result
        assert "Levothyroxine" in result
        assert "Vitamin D" in result

    def test_with_wearable_data(self):
        db = _mock_db(wearables=[{
            "date": "2025-01-15",
            "hrv": 45,
            "rhr": 62,
            "recovery_score": 78,
        }])
        result = build_quick_summary(db, user_id=0)
        assert "Wearable" in result
        assert "HRV 45ms" in result
        assert "RHR 62bpm" in result

    def test_with_meds_and_wearable(self):
        db = _mock_db(
            meds=[{"name": "Metformin", "dose": "500mg"}],
            wearables=[{"date": "2025-01-15", "hrv": 50, "rhr": 60}],
        )
        result = build_quick_summary(db, user_id=0)
        assert "Meds" in result
        assert "Wearable" in result

    def test_header_present(self):
        db = _mock_db(meds=[{"name": "Aspirin"}])
        result = build_quick_summary(db, user_id=0)
        assert result.startswith("HEALTH STATUS")

    def test_empty_med_name_skipped(self):
        db = _mock_db(meds=[{"name": ""}, {"name": "Aspirin"}])
        result = build_quick_summary(db, user_id=0)
        assert "Meds (1)" in result
