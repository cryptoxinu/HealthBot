"""Tests for retest reminder scheduler."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

from healthbot.reasoning.retest_scheduler import (
    RETEST_RULES,
    PendingRetest,
    RetestScheduler,
    format_retests,
)


def _make_db(observations: list[dict] | None = None) -> MagicMock:
    db = MagicMock()
    observations = observations or []

    def query_obs(
        record_type=None, canonical_name=None,
        start_date=None, end_date=None,
        triage_level=None, limit=200, user_id=None,
    ):
        results = []
        for obs in observations:
            if canonical_name and obs.get("canonical_name") != canonical_name:
                continue
            results.append(obs)
        results.sort(
            key=lambda x: x.get("date_collected", ""), reverse=True,
        )
        return results[:limit]

    db.query_observations.side_effect = query_obs
    return db


def _obs(
    name: str, value: float, dt: str, flag: str = "", unit: str = "",
) -> dict:
    return {
        "canonical_name": name,
        "value": value,
        "date_collected": dt,
        "flag": flag,
        "unit": unit,
    }


class TestRetestRules:
    def test_all_rules_have_citations(self):
        for r in RETEST_RULES:
            assert r.citation, f"{r.canonical_name} ({r.condition}) missing citation"

    def test_all_conditions_valid(self):
        for r in RETEST_RULES:
            assert r.condition in ("high", "low", "any")

    def test_all_priorities_valid(self):
        for r in RETEST_RULES:
            assert r.priority in ("urgent", "standard")

    def test_retest_window_valid(self):
        for r in RETEST_RULES:
            assert r.retest_weeks_min > 0
            assert r.retest_weeks_max >= r.retest_weeks_min

    def test_urgent_rules_are_electrolytes(self):
        urgent = [r for r in RETEST_RULES if r.priority == "urgent"]
        urgent_names = {r.canonical_name for r in urgent}
        assert "potassium" in urgent_names
        assert "sodium" in urgent_names


class TestPendingRetests:
    def test_abnormal_high_no_followup(self):
        """High TSH 7 weeks ago with no follow-up → due soon."""
        abnormal_date = (date.today() - timedelta(weeks=7)).isoformat()
        obs = [_obs("tsh", 8.5, abnormal_date, "H", "mIU/L")]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        tsh = [r for r in retests if r.canonical_name == "tsh"]
        assert len(tsh) == 1
        assert tsh[0].priority == "standard"
        assert "hypothyroidism" in tsh[0].reason.lower()

    def test_abnormal_low_no_followup(self):
        """Low ferritin 10 weeks ago → due soon."""
        abnormal_date = (date.today() - timedelta(weeks=10)).isoformat()
        obs = [_obs("ferritin", 8.0, abnormal_date, "L", "ng/mL")]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        ferritin = [r for r in retests if r.canonical_name == "ferritin"]
        assert len(ferritin) == 1
        assert "repletion" in ferritin[0].reason.lower()

    def test_followup_exists_no_reminder(self):
        """High TSH followed by a retest → no reminder."""
        abnormal_date = (date.today() - timedelta(weeks=8)).isoformat()
        followup_date = (date.today() - timedelta(weeks=1)).isoformat()
        obs = [
            _obs("tsh", 8.5, abnormal_date, "H", "mIU/L"),
            _obs("tsh", 4.0, followup_date, "", "mIU/L"),
        ]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        tsh = [r for r in retests if r.canonical_name == "tsh"]
        assert len(tsh) == 0

    def test_too_early_no_reminder(self):
        """High TSH only 1 week ago — too early for retest reminder."""
        abnormal_date = (date.today() - timedelta(weeks=1)).isoformat()
        obs = [_obs("tsh", 8.5, abnormal_date, "H", "mIU/L")]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        tsh = [r for r in retests if r.canonical_name == "tsh"]
        assert len(tsh) == 0

    def test_urgent_potassium_high(self):
        """High potassium 2 weeks ago → urgent."""
        abnormal_date = (date.today() - timedelta(weeks=2)).isoformat()
        obs = [_obs("potassium", 5.8, abnormal_date, "H", "mEq/L")]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        k = [r for r in retests if r.canonical_name == "potassium"]
        assert len(k) == 1
        assert k[0].priority == "urgent"

    def test_urgent_potassium_low(self):
        """Low potassium → urgent."""
        abnormal_date = (date.today() - timedelta(weeks=2)).isoformat()
        obs = [_obs("potassium", 3.0, abnormal_date, "L", "mEq/L")]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        k = [r for r in retests if r.canonical_name == "potassium"]
        assert len(k) == 1
        assert k[0].priority == "urgent"
        assert "hypokalemia" in k[0].reason.lower()

    def test_normal_result_no_reminder(self):
        """Normal result → no retest needed."""
        recent = (date.today() - timedelta(weeks=8)).isoformat()
        obs = [_obs("tsh", 2.5, recent, "", "mIU/L")]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)
        assert retests == []

    def test_no_data_no_reminder(self):
        """No lab data at all → no retests."""
        db = _make_db([])
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)
        assert retests == []

    def test_overdue_status(self):
        """Result overdue beyond max window."""
        # TSH high 12 weeks ago, max window is 8 weeks → overdue
        abnormal_date = (date.today() - timedelta(weeks=12)).isoformat()
        obs = [_obs("tsh", 9.0, abnormal_date, "H", "mIU/L")]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        tsh = [r for r in retests if r.canonical_name == "tsh"]
        assert len(tsh) == 1
        assert tsh[0].days_until_due < 0

    def test_multiple_abnormal_markers(self):
        """Multiple markers abnormal → multiple retests."""
        dt = (date.today() - timedelta(weeks=6)).isoformat()
        obs = [
            _obs("tsh", 8.5, dt, "H"),
            _obs("alt", 120, dt, "H"),
            _obs("vitamin_d", 15, dt, "L"),
        ]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        names = {r.canonical_name for r in retests}
        # TSH (6wk min) and ALT (4wk min) should be due; vit D (8wk min) may not be
        assert "tsh" in names
        assert "alt" in names

    def test_sort_urgent_first(self):
        """Urgent retests appear before standard."""
        dt = (date.today() - timedelta(weeks=2)).isoformat()
        obs = [
            _obs("potassium", 5.9, dt, "H"),
            _obs("tsh", 8.5, (date.today() - timedelta(weeks=7)).isoformat(), "H"),
        ]
        db = _make_db(obs)
        scheduler = RetestScheduler(db)
        retests = scheduler.get_pending_retests(user_id=1)

        assert len(retests) >= 2
        assert retests[0].priority == "urgent"


class TestFlagDirection:
    def test_high_flags(self):
        assert RetestScheduler._flag_direction("H") == "high"
        assert RetestScheduler._flag_direction("HH") == "high"
        assert RetestScheduler._flag_direction("HIGH") == "high"

    def test_low_flags(self):
        assert RetestScheduler._flag_direction("L") == "low"
        assert RetestScheduler._flag_direction("LL") == "low"
        assert RetestScheduler._flag_direction("LOW") == "low"

    def test_empty_flag(self):
        assert RetestScheduler._flag_direction("") == ""

    def test_unknown_flag(self):
        assert RetestScheduler._flag_direction("A") == ""


class TestFormatting:
    def test_format_empty(self):
        result = format_retests([])
        assert "No pending retests" in result

    def test_format_with_urgent(self):
        retests = [
            PendingRetest(
                canonical_name="potassium",
                display_name="Potassium",
                abnormal_value="5.9 mEq/L",
                abnormal_flag="H",
                abnormal_date="2024-01-15",
                retest_window="1-2 weeks",
                retest_due_date="2024-01-22",
                retest_overdue_date="2024-01-29",
                days_until_due=-5,
                reason="Hyperkalemia — risk of arrhythmia",
                priority="urgent",
                citation="Palmer BF. N Engl J Med. 2004.",
                status="urgent_overdue",
            ),
        ]
        result = format_retests(retests)
        assert "URGENT" in result
        assert "Potassium" in result
        assert "HIGH" in result
        assert "5 days overdue" in result

    def test_format_with_standard(self):
        retests = [
            PendingRetest(
                canonical_name="tsh",
                display_name="Tsh",
                abnormal_value="8.5 mIU/L",
                abnormal_flag="H",
                abnormal_date="2024-01-15",
                retest_window="6-8 weeks",
                retest_due_date="2024-02-26",
                retest_overdue_date="2024-03-11",
                days_until_due=3,
                reason="Confirm hypothyroidism",
                priority="standard",
                citation="Garber JR et al. Thyroid. 2012.",
                status="due_soon",
            ),
        ]
        result = format_retests(retests)
        assert "Standard" in result
        assert "due in 3 days" in result
