"""Tests for cross-source wearable data deduplication."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.wearable_dedup import (
    DedupReport,
    WearableDedup,
)


def _make_db(wearable_rows=None, observations=None):
    """Create a mock DB with wearable and observation data."""
    db = MagicMock()
    db.query_wearable_daily = MagicMock(return_value=wearable_rows or [])
    db.query_observations = MagicMock(return_value=observations or [])
    return db


class TestDedupNoData:

    def test_no_wearable_data(self):
        db = _make_db(wearable_rows=[])
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        assert report.duplicates_found == 0
        assert report.checked_dates == 0

    def test_no_apple_health_data(self):
        db = _make_db(
            wearable_rows=[{"_date": "2025-12-01", "hrv": 45.0, "rhr": 55.0}],
            observations=[],
        )
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        assert report.duplicates_found == 0


class TestDedupOverlaps:

    def test_detects_matching_hrv(self):
        db = _make_db(
            wearable_rows=[
                {"_date": "2025-12-01", "hrv": 45.0, "rhr": 55.0},
            ],
            observations=[
                {
                    "value": 44.0,
                    "_meta": {"date_effective": "2025-12-01"},
                },
            ],
        )
        # Only return observations for 'hrv' canonical_name
        def mock_query_obs(canonical_name=None, **kwargs):
            if canonical_name in ("hrv", "heart_rate_variability"):
                return [{"value": 44.0, "_meta": {"date_effective": "2025-12-01"}}]
            return []

        db.query_observations = mock_query_obs
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        assert report.duplicates_found >= 1
        # Values are close (44 vs 45) — should not be a conflict
        hrv_entries = [e for e in report.entries if e.metric in ("hrv", "heart_rate_variability")]
        assert len(hrv_entries) >= 1
        assert not hrv_entries[0].conflict

    def test_detects_rhr_conflict(self):
        """RHR values differ significantly (>15%)."""
        def mock_query_obs(canonical_name=None, **kwargs):
            if canonical_name == "resting_heart_rate":
                return [{"value": 70.0, "_meta": {"date_effective": "2025-12-01"}}]
            return []

        db = _make_db(
            wearable_rows=[{"_date": "2025-12-01", "rhr": 55.0}],
        )
        db.query_observations = mock_query_obs
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        rhr_entries = [e for e in report.entries if e.metric == "resting_heart_rate"]
        assert len(rhr_entries) == 1
        assert rhr_entries[0].conflict  # 55 vs 70 = significant difference

    def test_multiple_dates(self):
        def mock_query_obs(canonical_name=None, **kwargs):
            if canonical_name == "resting_heart_rate":
                return [
                    {"value": 56.0, "_meta": {"date_effective": "2025-12-01"}},
                    {"value": 58.0, "_meta": {"date_effective": "2025-12-02"}},
                ]
            return []

        db = _make_db(
            wearable_rows=[
                {"_date": "2025-12-01", "rhr": 55.0},
                {"_date": "2025-12-02", "rhr": 57.0},
            ],
        )
        db.query_observations = mock_query_obs
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        assert report.checked_dates == 2
        rhr_entries = [e for e in report.entries if e.metric == "resting_heart_rate"]
        assert len(rhr_entries) == 2
        # Both within tolerance
        assert all(not e.conflict for e in rhr_entries)


class TestDedupReport:

    def test_summary_no_overlaps(self):
        report = DedupReport(checked_dates=7)
        assert "no overlaps" in report.summary()

    def test_summary_with_entries(self):
        from healthbot.reasoning.wearable_dedup import DupEntry

        report = DedupReport(
            checked_dates=7,
            duplicates_found=1,
            conflicts=0,
            entries=[
                DupEntry(
                    date="2025-12-01",
                    metric="hrv",
                    apple_value=45.0,
                    wearable_value=46.0,
                    wearable_provider="whoop",
                    pct_diff=2.2,
                    conflict=False,
                ),
            ],
        )
        s = report.summary()
        assert "1 overlapping" in s
        assert "match" in s

    def test_summary_with_conflict(self):
        from healthbot.reasoning.wearable_dedup import DupEntry

        report = DedupReport(
            checked_dates=7,
            duplicates_found=1,
            conflicts=1,
            entries=[
                DupEntry(
                    date="2025-12-01",
                    metric="rhr",
                    apple_value=70.0,
                    wearable_value=55.0,
                    wearable_provider="whoop",
                    pct_diff=-24.0,
                    conflict=True,
                ),
            ],
        )
        s = report.summary()
        assert "CONFLICT" in s
        assert "1 with significant" in s


class TestDedupEdgeCases:

    def test_no_query_observations_method(self):
        """CleanDB doesn't have query_observations — should not crash."""
        db = MagicMock()
        db.query_wearable_daily = MagicMock(return_value=[
            {"_date": "2025-12-01", "hrv": 45.0},
        ])
        db.query_observations = MagicMock(side_effect=AttributeError)
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        assert report.duplicates_found == 0

    def test_none_values_skipped(self):
        def mock_query_obs(canonical_name=None, **kwargs):
            if canonical_name == "resting_heart_rate":
                return [{"value": None, "_meta": {"date_effective": "2025-12-01"}}]
            return []

        db = _make_db(wearable_rows=[{"_date": "2025-12-01", "rhr": None}])
        db.query_observations = mock_query_obs
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        assert report.duplicates_found == 0

    def test_oura_provider(self):
        def mock_query_obs(canonical_name=None, **kwargs):
            if canonical_name == "resting_heart_rate":
                return [{"value": 58.0, "_meta": {"date_effective": "2025-12-01"}}]
            return []

        db = _make_db(wearable_rows=[{"_date": "2025-12-01", "rhr": 57.0}])
        db.query_observations = mock_query_obs
        dedup = WearableDedup(db)
        report = dedup.check(days=7, provider="oura")
        rhr_entries = [e for e in report.entries if e.metric == "resting_heart_rate"]
        assert len(rhr_entries) == 1
        assert rhr_entries[0].wearable_provider == "oura"

    def test_date_object_handled(self):
        """Wearable row with date object instead of string."""
        from datetime import date

        def mock_query_obs(canonical_name=None, **kwargs):
            if canonical_name == "resting_heart_rate":
                return [{"value": 56.0, "_meta": {"date_effective": "2025-12-01"}}]
            return []

        db = _make_db(
            wearable_rows=[{"date": date(2025, 12, 1), "rhr": 55.0}],
        )
        db.query_observations = mock_query_obs
        dedup = WearableDedup(db)
        report = dedup.check(days=7)
        rhr_entries = [e for e in report.entries if e.metric == "resting_heart_rate"]
        assert len(rhr_entries) == 1
