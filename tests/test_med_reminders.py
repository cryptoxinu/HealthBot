"""Tests for medication reminders."""
from __future__ import annotations

from datetime import date, datetime

from healthbot.data.models import LabResult, TriageLevel
from healthbot.reasoning.med_reminders import (
    MedReminder,
    _is_normal_result,
    check_reminder_resumes,
    format_reminder,
    format_reminder_list,
    get_due_reminders,
    review_reminders_after_ingestion,
)


class TestMedReminderDB:
    """Test DB CRUD for medication reminders."""

    def test_upsert_and_get(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        reminders = db.get_med_reminders(user_id=0)
        assert len(reminders) == 1
        assert reminders[0]["med_name"] == "Metformin"
        assert reminders[0]["_time"] == "08:00"

    def test_upsert_updates_existing(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="09:00")
        reminders = db.get_med_reminders(user_id=0)
        assert len(reminders) == 1
        assert reminders[0]["_time"] == "09:00"

    def test_multiple_meds(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        db.upsert_med_reminder(user_id=0, med_name="Lisinopril", time="20:00")
        reminders = db.get_med_reminders(user_id=0)
        assert len(reminders) == 2

    def test_disable_reminder(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        found = db.disable_med_reminder(user_id=0, med_name="Metformin")
        assert found
        reminders = db.get_med_reminders(user_id=0)
        assert len(reminders) == 0

    def test_disable_nonexistent(self, db) -> None:
        found = db.disable_med_reminder(user_id=0, med_name="Nothing")
        assert not found


class TestGetDueReminders:
    def test_due_at_correct_time(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        due = get_due_reminders(
            db, user_id=0,
            current_time=datetime(2025, 12, 1, 8, 0),
        )
        assert len(due) == 1
        assert due[0].med_name == "Metformin"

    def test_not_due_at_wrong_time(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        due = get_due_reminders(
            db, user_id=0,
            current_time=datetime(2025, 12, 1, 14, 30),
        )
        assert len(due) == 0

    def test_no_reminders(self, db) -> None:
        due = get_due_reminders(
            db, user_id=0,
            current_time=datetime(2025, 12, 1, 8, 0),
        )
        assert due == []


class TestFormatReminder:
    def test_basic_format(self) -> None:
        r = MedReminder(med_name="Metformin", time="08:00", notes="")
        msg = format_reminder(r)
        assert "Metformin" in msg

    def test_with_notes(self) -> None:
        r = MedReminder(
            med_name="Levothyroxine", time="07:00",
            notes="Take on empty stomach",
        )
        msg = format_reminder(r)
        assert "empty stomach" in msg


class TestFormatReminderList:
    def test_empty_list(self) -> None:
        msg = format_reminder_list([])
        assert "No medication reminders" in msg

    def test_with_reminders(self) -> None:
        reminders = [
            {"med_name": "Metformin", "_time": "08:00", "notes": ""},
            {"med_name": "Lisinopril", "_time": "20:00", "notes": ""},
        ]
        msg = format_reminder_list(reminders)
        assert "08:00" in msg
        assert "Metformin" in msg
        assert "Lisinopril" in msg


class TestParseReminderTime:
    def test_24h_format(self) -> None:
        from healthbot.bot.handlers_medical import MedicalHandlers
        assert MedicalHandlers._parse_reminder_time("08:00") == "08:00"
        assert MedicalHandlers._parse_reminder_time("14:30") == "14:30"

    def test_12h_am(self) -> None:
        from healthbot.bot.handlers_medical import MedicalHandlers
        assert MedicalHandlers._parse_reminder_time("7:00am") == "07:00"
        assert MedicalHandlers._parse_reminder_time("12:00am") == "00:00"

    def test_12h_pm(self) -> None:
        from healthbot.bot.handlers_medical import MedicalHandlers
        assert MedicalHandlers._parse_reminder_time("1:00pm") == "13:00"
        assert MedicalHandlers._parse_reminder_time("12:00pm") == "12:00"

    def test_invalid(self) -> None:
        from healthbot.bot.handlers_medical import MedicalHandlers
        assert MedicalHandlers._parse_reminder_time("25:00") is None
        assert MedicalHandlers._parse_reminder_time("abc") is None


class TestPauseResumeDB:
    """Test pause/resume DB operations."""

    def test_pause_reminder(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Levothyroxine", time="07:00")
        ok = db.pause_med_reminder(
            user_id=0, med_name="Levothyroxine",
            paused_reason="TSH normal (2/22)",
            resume_after="2026-08-22",
        )
        assert ok
        reminders = db.get_med_reminders(user_id=0)
        assert len(reminders) == 1
        assert reminders[0]["paused_reason"] == "TSH normal (2/22)"
        assert reminders[0]["resume_after"] == "2026-08-22"

    def test_pause_nonexistent(self, db) -> None:
        ok = db.pause_med_reminder(
            user_id=0, med_name="Nothing",
            paused_reason="test", resume_after="2026-01-01",
        )
        assert not ok

    def test_resume_reminder(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Levothyroxine", time="07:00")
        db.pause_med_reminder(
            user_id=0, med_name="Levothyroxine",
            paused_reason="TSH normal", resume_after="2026-08-22",
        )
        ok = db.resume_med_reminder(user_id=0, med_name="Levothyroxine")
        assert ok
        reminders = db.get_med_reminders(user_id=0)
        assert len(reminders) == 1
        assert reminders[0].get("paused_reason") is None
        assert reminders[0].get("resume_after") is None

    def test_resume_non_paused(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        ok = db.resume_med_reminder(user_id=0, med_name="Metformin")
        assert not ok  # Not paused, nothing to resume

    def test_get_paused_reminders(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Levothyroxine", time="07:00")
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        db.pause_med_reminder(
            user_id=0, med_name="Levothyroxine",
            paused_reason="TSH normal", resume_after="2026-08-22",
        )
        paused = db.get_paused_reminders(user_id=0)
        assert len(paused) == 1
        assert paused[0]["med_name"] == "Levothyroxine"


class TestDueSkipsPaused:
    """Test that paused reminders don't fire."""

    def test_paused_reminder_not_due(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Levothyroxine", time="07:00")
        db.pause_med_reminder(
            user_id=0, med_name="Levothyroxine",
            paused_reason="TSH normal", resume_after="2026-08-22",
        )
        due = get_due_reminders(
            db, user_id=0,
            current_time=datetime(2026, 3, 1, 7, 0),
        )
        assert len(due) == 0

    def test_unpaused_reminder_still_fires(self, db) -> None:
        db.upsert_med_reminder(user_id=0, med_name="Metformin", time="08:00")
        due = get_due_reminders(
            db, user_id=0,
            current_time=datetime(2026, 3, 1, 8, 0),
        )
        assert len(due) == 1


class TestIsNormalResult:
    """Test the _is_normal_result helper."""

    def test_normal_no_flag(self) -> None:
        assert _is_normal_result({"flag": "", "value": 2.5}) is True

    def test_high_flag(self) -> None:
        assert _is_normal_result({"flag": "H"}) is False

    def test_low_flag(self) -> None:
        assert _is_normal_result({"flag": "L"}) is False

    def test_within_reference_range(self) -> None:
        obs = {
            "flag": "", "value": 2.5,
            "reference_low": 0.5, "reference_high": 4.5,
        }
        assert _is_normal_result(obs) is True

    def test_outside_reference_range(self) -> None:
        obs = {
            "flag": "", "value": 10.0,
            "reference_low": 0.5, "reference_high": 4.5,
        }
        assert _is_normal_result(obs) is False

    def test_urgent_triage(self) -> None:
        obs = {"flag": "", "_meta": {"triage_level": "urgent"}}
        assert _is_normal_result(obs) is False


class TestReviewRemindersAfterIngestion:
    """Test auto-pause on lab ingestion."""

    def test_matching_test_pauses_reminder(self, db) -> None:
        # Set up: levothyroxine reminder + normal TSH result
        db.upsert_med_reminder(
            user_id=0, med_name="Levothyroxine", time="07:00",
        )
        lab = LabResult(
            id="tsh_1", test_name="TSH",
            canonical_name="tsh", value=2.5, unit="mIU/L",
            reference_low=0.5, reference_high=4.5,
            date_collected=date(2026, 2, 22),
            triage_level=TriageLevel.NORMAL,
        )
        db.insert_observation(lab, user_id=0)

        messages = review_reminders_after_ingestion(
            db, user_id=0, ingested_tests={"tsh"},
        )

        assert len(messages) == 1
        assert "Paused Levothyroxine" in messages[0]
        assert "TSH" in messages[0]

        # Verify reminder is actually paused
        reminders = db.get_med_reminders(user_id=0)
        assert reminders[0].get("paused_reason") is not None

    def test_no_matching_test_no_change(self, db) -> None:
        db.upsert_med_reminder(
            user_id=0, med_name="Levothyroxine", time="07:00",
        )
        messages = review_reminders_after_ingestion(
            db, user_id=0, ingested_tests={"glucose"},
        )
        assert messages == []

    def test_no_active_reminders_empty_result(self, db) -> None:
        messages = review_reminders_after_ingestion(
            db, user_id=0, ingested_tests={"tsh"},
        )
        assert messages == []

    def test_abnormal_result_no_pause(self, db) -> None:
        db.upsert_med_reminder(
            user_id=0, med_name="Levothyroxine", time="07:00",
        )
        lab = LabResult(
            id="tsh_high", test_name="TSH",
            canonical_name="tsh", value=8.5, unit="mIU/L",
            reference_low=0.5, reference_high=4.5,
            date_collected=date(2026, 2, 22),
            triage_level=TriageLevel.URGENT,
            flag="H",
        )
        db.insert_observation(lab, user_id=0)

        messages = review_reminders_after_ingestion(
            db, user_id=0, ingested_tests={"tsh"},
        )
        assert messages == []

    def test_already_paused_not_re_paused(self, db) -> None:
        db.upsert_med_reminder(
            user_id=0, med_name="Levothyroxine", time="07:00",
        )
        db.pause_med_reminder(
            user_id=0, med_name="Levothyroxine",
            paused_reason="Already paused", resume_after="2026-12-01",
        )
        lab = LabResult(
            id="tsh_2", test_name="TSH",
            canonical_name="tsh", value=2.5, unit="mIU/L",
            reference_low=0.5, reference_high=4.5,
            date_collected=date(2026, 2, 22),
            triage_level=TriageLevel.NORMAL,
        )
        db.insert_observation(lab, user_id=0)

        messages = review_reminders_after_ingestion(
            db, user_id=0, ingested_tests={"tsh"},
        )
        assert messages == []


class TestCheckReminderResumes:
    """Test auto-resume of paused reminders."""

    def test_resume_when_date_passed(self, db) -> None:
        db.upsert_med_reminder(
            user_id=0, med_name="Levothyroxine", time="07:00",
        )
        db.pause_med_reminder(
            user_id=0, med_name="Levothyroxine",
            paused_reason="TSH normal",
            resume_after="2025-01-01",  # In the past
        )
        messages = check_reminder_resumes(db, user_id=0)
        assert len(messages) == 1
        assert "Resumed Levothyroxine" in messages[0]

        # Verify no longer paused
        paused = db.get_paused_reminders(user_id=0)
        assert len(paused) == 0

    def test_no_resume_before_date(self, db) -> None:
        db.upsert_med_reminder(
            user_id=0, med_name="Levothyroxine", time="07:00",
        )
        db.pause_med_reminder(
            user_id=0, med_name="Levothyroxine",
            paused_reason="TSH normal",
            resume_after="2099-12-31",  # Far future
        )
        messages = check_reminder_resumes(db, user_id=0)
        assert messages == []


class TestFormatReminderListPaused:
    """Test format_reminder_list shows paused state."""

    def test_paused_display(self) -> None:
        reminders = [
            {
                "med_name": "Levothyroxine", "_time": "07:00",
                "notes": "take on empty stomach",
                "paused_reason": "TSH normal (2/22)",
                "resume_after": "2026-08-22",
            },
            {"med_name": "Metformin", "_time": "08:00", "notes": "with food"},
        ]
        msg = format_reminder_list(reminders)
        assert "PAUSED" in msg
        assert "TSH normal" in msg
        assert "Aug 22" in msg
        assert "Metformin" in msg
        assert "(with food)" in msg
