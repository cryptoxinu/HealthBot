"""Tests for provider/doctor tracking and appointments (Phase T1)."""
from __future__ import annotations

from datetime import date, timedelta


class TestProviderCRUD:
    """Test provider create, read, delete."""

    def test_add_provider(self, db) -> None:
        """Adding a provider should return an ID."""
        prov_id = db.insert_provider(1, {
            "name": "Dr. Smith",
            "specialty": "Endocrinology",
        })
        assert prov_id

    def test_get_providers(self, db) -> None:
        """Providers should be retrievable."""
        db.insert_provider(1, {"name": "Dr. Smith", "specialty": "Endocrinology"})
        db.insert_provider(1, {"name": "Dr. Jones", "specialty": "Cardiology"})

        providers = db.get_providers(1)
        assert len(providers) == 2
        names = {p["name"] for p in providers}
        assert "Dr. Smith" in names
        assert "Dr. Jones" in names

    def test_provider_has_metadata(self, db) -> None:
        """Provider records should include _id and _created_at."""
        db.insert_provider(1, {"name": "Dr. Kim", "specialty": "GI"})
        providers = db.get_providers(1)
        assert providers[0]["_id"]
        assert providers[0]["_created_at"]

    def test_delete_provider(self, db) -> None:
        """Deleting a provider should work."""
        prov_id = db.insert_provider(1, {"name": "Dr. Temp", "specialty": "Test"})
        assert len(db.get_providers(1)) == 1
        result = db.delete_provider(prov_id)
        assert result is True
        assert len(db.get_providers(1)) == 0

    def test_delete_nonexistent(self, db) -> None:
        """Deleting a nonexistent provider returns False."""
        assert db.delete_provider("nonexistent") is False

    def test_providers_per_user(self, db) -> None:
        """Providers should be scoped to user_id."""
        db.insert_provider(1, {"name": "Dr. A", "specialty": "X"})
        db.insert_provider(2, {"name": "Dr. B", "specialty": "Y"})
        assert len(db.get_providers(1)) == 1
        assert len(db.get_providers(2)) == 1

    def test_provider_with_extra_fields(self, db) -> None:
        """Provider can store phone, address, notes."""
        db.insert_provider(1, {
            "name": "Dr. Full",
            "specialty": "Internal Medicine",
            "phone": "555-1234",
            "address": "123 Medical Plaza",
            "notes": "Preferred provider",
        })
        providers = db.get_providers(1)
        assert providers[0]["phone"] == "555-1234"
        assert providers[0]["address"] == "123 Medical Plaza"


class TestAppointmentCRUD:
    """Test appointment create, read, cancel."""

    def test_add_appointment(self, db) -> None:
        """Adding an appointment should return an ID."""
        prov_id = db.insert_provider(1, {"name": "Dr. Smith", "specialty": "Endo"})
        appt_id = db.insert_appointment(1, prov_id, {
            "date": date.today().isoformat(),
            "reason": "Follow-up",
        })
        assert appt_id

    def test_get_appointments(self, db) -> None:
        """Appointments should be retrievable."""
        prov_id = db.insert_provider(1, {"name": "Dr. Smith", "specialty": "Endo"})
        db.insert_appointment(1, prov_id, {
            "date": date.today().isoformat(),
            "reason": "Blood work",
        })
        appts = db.get_appointments(1)
        assert len(appts) == 1
        assert appts[0]["reason"] == "Blood work"
        assert appts[0]["_status"] == "scheduled"

    def test_appointment_metadata(self, db) -> None:
        """Appointment records should include metadata fields."""
        prov_id = db.insert_provider(1, {"name": "Dr. X", "specialty": "Y"})
        db.insert_appointment(1, prov_id, {
            "date": date.today().isoformat(),
        })
        appts = db.get_appointments(1)
        assert appts[0]["_id"]
        assert appts[0]["_provider_id"] == prov_id
        assert appts[0]["_appt_date"]
        assert appts[0]["_prep_sent"] is False

    def test_cancel_appointment(self, db) -> None:
        """Cancelling an appointment should change status."""
        prov_id = db.insert_provider(1, {"name": "Dr. X", "specialty": "Y"})
        appt_id = db.insert_appointment(1, prov_id, {
            "date": date.today().isoformat(),
        })
        db.update_appointment_status(appt_id, "cancelled")
        appts = db.get_appointments(1, status="scheduled")
        assert len(appts) == 0
        all_appts = db.get_appointments(1)
        assert len(all_appts) == 1
        assert all_appts[0]["_status"] == "cancelled"

    def test_delete_appointment(self, db) -> None:
        """Deleting an appointment should remove it."""
        prov_id = db.insert_provider(1, {"name": "Dr. X", "specialty": "Y"})
        appt_id = db.insert_appointment(1, prov_id, {"date": "2025-06-01"})
        assert db.delete_appointment(appt_id) is True
        assert len(db.get_appointments(1)) == 0

    def test_upcoming_appointments(self, db) -> None:
        """get_upcoming_appointments should return only near-future ones."""
        prov_id = db.insert_provider(1, {"name": "Dr. X", "specialty": "Y"})
        tomorrow = (date.today() + timedelta(days=1)).isoformat()
        next_month = (date.today() + timedelta(days=30)).isoformat()

        db.insert_appointment(1, prov_id, {"date": tomorrow, "reason": "Soon"})
        db.insert_appointment(1, prov_id, {"date": next_month, "reason": "Later"})

        upcoming = db.get_upcoming_appointments(1, within_days=2)
        assert len(upcoming) == 1
        assert upcoming[0]["reason"] == "Soon"

    def test_mark_prep_sent(self, db) -> None:
        """Marking prep sent should update the flag."""
        prov_id = db.insert_provider(1, {"name": "Dr. X", "specialty": "Y"})
        appt_id = db.insert_appointment(1, prov_id, {
            "date": date.today().isoformat(),
        })
        db.mark_appointment_prep_sent(appt_id)
        appts = db.get_appointments(1)
        assert appts[0]["_prep_sent"] is True

    def test_appointments_per_user(self, db) -> None:
        """Appointments should be scoped to user_id."""
        prov1 = db.insert_provider(1, {"name": "Dr. A", "specialty": "X"})
        prov2 = db.insert_provider(2, {"name": "Dr. B", "specialty": "Y"})
        db.insert_appointment(1, prov1, {"date": "2025-06-01"})
        db.insert_appointment(2, prov2, {"date": "2025-06-01"})
        assert len(db.get_appointments(1)) == 1
        assert len(db.get_appointments(2)) == 1
