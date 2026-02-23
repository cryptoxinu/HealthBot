"""Tests for appointment prep auto-send (Phase T2)."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult, Medication


class TestAppointmentPrep:
    """Test the appointment prep packet builder."""

    def _add_provider_and_appointment(
        self, db, specialty: str, days_ahead: int = 1,
    ) -> tuple[str, str]:
        """Helper to add a provider and upcoming appointment."""
        prov_id = db.insert_provider(1, {
            "name": "Dr. Test",
            "specialty": specialty,
        })
        appt_date = (date.today() + timedelta(days=days_ahead)).isoformat()
        appt_id = db.insert_appointment(1, prov_id, {
            "date": appt_date,
            "reason": "Follow-up",
            "provider_name": "Dr. Test",
            "specialty": specialty,
        })
        return prov_id, appt_id

    def test_upcoming_appointment_found(self, db) -> None:
        """Upcoming appointment should be findable."""
        self._add_provider_and_appointment(db, "Endocrinology")
        upcoming = db.get_upcoming_appointments(1, within_days=2)
        assert len(upcoming) == 1

    def test_future_appointment_not_upcoming(self, db) -> None:
        """Appointment 30 days out should not be 'upcoming'."""
        self._add_provider_and_appointment(db, "Cardiology", days_ahead=30)
        upcoming = db.get_upcoming_appointments(1, within_days=2)
        assert len(upcoming) == 0

    def test_prep_includes_provider_name(self, db) -> None:
        """Prep packet should include provider name."""
        from healthbot.bot.scheduler import AlertScheduler

        self._add_provider_and_appointment(db, "Endocrinology")
        scheduler = AlertScheduler.__new__(AlertScheduler)

        text = scheduler._build_appointment_prep(
            db, 1, "Endocrinology", "Dr. Test", "2025-06-01", "Follow-up",
        )
        assert "Dr. Test" in text
        assert "2025-06-01" in text
        assert "Follow-up" in text

    def test_prep_includes_relevant_labs(self, db) -> None:
        """Prep for endocrinology should pull TSH, glucose, etc."""
        from healthbot.bot.scheduler import AlertScheduler

        # Add some lab data
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=4.5,
            unit="mIU/L",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        scheduler = AlertScheduler.__new__(AlertScheduler)
        text = scheduler._build_appointment_prep(
            db, 1, "endocrinology", "Dr. Test", "2025-06-01", "",
        )
        assert "TSH" in text
        assert "4.5" in text

    def test_prep_includes_medications(self, db) -> None:
        """Prep should list active medications."""
        from healthbot.bot.scheduler import AlertScheduler

        med = Medication(
            id=uuid.uuid4().hex,
            name="Levothyroxine",
            dose="50",
            unit="mcg",
            frequency="daily",
            start_date=date.today(),
        )
        db.insert_medication(med, user_id=1)

        scheduler = AlertScheduler.__new__(AlertScheduler)
        text = scheduler._build_appointment_prep(
            db, 1, "general", "Dr. General", "2025-06-01", "",
        )
        assert "Levothyroxine" in text
        assert "Active Medications" in text

    def test_prep_mark_sent(self, db) -> None:
        """Marking prep sent should update the flag."""
        _, appt_id = self._add_provider_and_appointment(db, "General")
        db.mark_appointment_prep_sent(appt_id)
        appts = db.get_appointments(1)
        assert appts[0]["_prep_sent"] is True

    def test_prep_not_resent_after_mark(self, db) -> None:
        """Already-sent prep should not trigger again."""
        _, appt_id = self._add_provider_and_appointment(db, "General")
        db.mark_appointment_prep_sent(appt_id)
        upcoming = db.get_upcoming_appointments(1, within_days=2)
        # Filter out already prepped
        unsent = [a for a in upcoming if not a.get("_prep_sent")]
        assert len(unsent) == 0

    def test_prep_has_discussion_points(self, db) -> None:
        """Prep should include suggested discussion points."""
        from healthbot.bot.scheduler import AlertScheduler

        scheduler = AlertScheduler.__new__(AlertScheduler)
        text = scheduler._build_appointment_prep(
            db, 1, "general", "Dr. Test", "2025-06-01", "Annual checkup",
        )
        assert "Discussion Points" in text
        assert "Annual checkup" in text
