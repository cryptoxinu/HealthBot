"""Tests for the extended clean sync workers (6 new data types)."""
from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from healthbot.data.clean_db import CleanDB
from healthbot.data.clean_sync import CleanSyncEngine, SyncReport
from healthbot.llm.anonymizer import Anonymizer
from healthbot.security.phi_firewall import PhiFirewall

# 32-byte test key for clean DB encryption (M24: _encrypt now raises
# EncryptionError instead of falling back to plaintext when no key).
_TEST_CLEAN_KEY = os.urandom(32)

# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture()
def phi_firewall():
    return PhiFirewall()


@pytest.fixture()
def clean_db(tmp_path, phi_firewall):
    db = CleanDB(tmp_path / "clean.db", phi_firewall=phi_firewall)
    db.open(clean_key=_TEST_CLEAN_KEY)
    yield db
    db.close()


@pytest.fixture()
def anonymizer(phi_firewall):
    anon = Anonymizer(phi_firewall=phi_firewall, use_ner=False)
    # The canary SSN (999-88-7777) is no longer matched by the tightened
    # PhiFirewall SSN regex (which excludes 9xx area numbers).  Mark canary
    # as pre-verified so the pipeline doesn't raise AnonymizationError.
    anon._canary_verified = True
    return anon


@pytest.fixture()
def raw_db():
    """Mock raw vault DB with all methods."""
    mock = MagicMock()
    mock.query_observations.return_value = []
    mock.get_active_medications.return_value = []
    mock.query_wearable_daily.return_value = []
    mock.get_user_demographics.return_value = {}
    mock.get_active_hypotheses.return_value = []
    mock.get_ltm_by_user.return_value = []
    mock.query_workouts.return_value = []
    mock.get_genetic_variants.return_value = []
    mock.get_health_goals.return_value = []
    mock.get_med_reminders.return_value = []
    mock.get_providers.return_value = []
    mock.get_appointments.return_value = []
    return mock


@pytest.fixture()
def engine(raw_db, clean_db, anonymizer, phi_firewall):
    return CleanSyncEngine(raw_db, clean_db, anonymizer, phi_firewall)


# ── Workouts ─────────────────────────────────────────────


class TestSyncWorkouts:
    def test_workouts_synced(self, engine, raw_db, clean_db):
        """Workouts from raw vault should appear in clean DB."""
        raw_db.query_workouts.return_value = [
            {
                "_id": "wo1",
                "_sport_type": "running",
                "_start_date": "2024-06-01T08:00:00",
                "duration_minutes": 45.0,
                "calories_burned": 350.0,
                "avg_heart_rate": 155.0,
                "max_heart_rate": 175.0,
                "min_heart_rate": 120.0,
                "distance_km": 6.5,
            },
        ]
        report = engine.sync_all(user_id=1)
        assert report.workouts_synced == 1

        workouts = clean_db.get_workouts()
        assert len(workouts) == 1
        assert workouts[0]["sport_type"] == "running"
        assert workouts[0]["duration_minutes"] == 45.0
        assert workouts[0]["distance_km"] == 6.5

    def test_stale_workouts_deleted(self, engine, raw_db, clean_db):
        """Workouts removed from raw vault should be deleted from clean DB."""
        raw_db.query_workouts.return_value = [
            {"_id": "wo1", "_sport_type": "cycling", "_start_date": "2024-06-01"},
            {"_id": "wo2", "_sport_type": "running", "_start_date": "2024-06-02"},
        ]
        engine.sync_all(user_id=1)
        assert len(clean_db.get_workouts()) == 2

        raw_db.query_workouts.return_value = [
            {"_id": "wo1", "_sport_type": "cycling", "_start_date": "2024-06-01"},
        ]
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted >= 1
        assert len(clean_db.get_workouts()) == 1

    def test_workout_query_failure_no_deletion(self, engine, raw_db, clean_db):
        """If workout query fails, existing clean data should not be deleted."""
        raw_db.query_workouts.return_value = [
            {"_id": "wo1", "_sport_type": "running", "_start_date": "2024-06-01"},
        ]
        engine.sync_all(user_id=1)

        raw_db.query_workouts.side_effect = RuntimeError("DB locked")
        engine.sync_all(user_id=1)
        assert len(clean_db.get_workouts()) == 1


# ── Genetic Variants ────────────────────────────────────


class TestSyncGeneticVariants:
    def test_variants_synced(self, engine, raw_db, clean_db):
        """Genetic variants should be synced without anonymization."""
        raw_db.get_genetic_variants.return_value = [
            {
                "_id": "gv1",
                "_rsid": "rs1801133",
                "_chromosome": "1",
                "_position": 11856378,
                "_source": "23andme",
                "genotype": "CT",
                "risk_allele": "T",
                "phenotype": "MTHFR C677T — reduced folate metabolism",
            },
        ]
        report = engine.sync_all(user_id=1)
        assert report.genetic_variants_synced == 1

        variants = clean_db.get_genetic_variants()
        assert len(variants) == 1
        assert variants[0]["rsid"] == "rs1801133"
        assert variants[0]["genotype"] == "CT"
        assert variants[0]["phenotype"] == "MTHFR C677T — reduced folate metabolism"

    def test_stale_variants_deleted(self, engine, raw_db, clean_db):
        """Removed variants should be cleaned up."""
        raw_db.get_genetic_variants.return_value = [
            {"_id": "gv1", "_rsid": "rs1801133", "_chromosome": "1", "_position": 100},
            {"_id": "gv2", "_rsid": "rs4680", "_chromosome": "22", "_position": 200},
        ]
        engine.sync_all(user_id=1)
        assert len(clean_db.get_genetic_variants()) == 2

        raw_db.get_genetic_variants.return_value = [
            {"_id": "gv1", "_rsid": "rs1801133", "_chromosome": "1", "_position": 100},
        ]
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted >= 1
        assert len(clean_db.get_genetic_variants()) == 1


# ── Health Goals ────────────────────────────────────────


class TestSyncHealthGoals:
    def test_goals_synced(self, engine, raw_db, clean_db):
        """Health goals should be synced with anonymization."""
        raw_db.get_health_goals.return_value = [
            {
                "_id": "g1",
                "_created_at": "2024-01-15",
                "goal_text": "Improve sleep quality to 80+",
            },
        ]
        report = engine.sync_all(user_id=1)
        assert report.health_goals_synced == 1

        goals = clean_db.get_health_goals()
        assert len(goals) == 1
        assert goals[0]["goal_text"] == "Improve sleep quality to 80+"

    def test_goal_with_pii_blocked(self, raw_db, clean_db, phi_firewall):
        """Goal containing PII should be blocked."""
        from healthbot.llm.anonymizer import AnonymizationError

        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("SSN: 123-45-6789 goal", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("PII detected")

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.get_health_goals.return_value = [
            {"_id": "g1", "_created_at": "2024-01-15",
             "goal_text": "SSN: 123-45-6789 goal"},
        ]
        report = eng.sync_all(user_id=1)
        assert report.pii_blocked >= 1
        assert report.health_goals_synced == 0

    def test_stale_goals_deleted(self, engine, raw_db, clean_db):
        """Goals removed from raw vault should be deleted."""
        raw_db.get_health_goals.return_value = [
            {"_id": "g1", "_created_at": "2024-01-15", "goal_text": "Goal one"},
            {"_id": "g2", "_created_at": "2024-02-20", "goal_text": "Goal two"},
        ]
        engine.sync_all(user_id=1)
        assert len(clean_db.get_health_goals()) == 2

        raw_db.get_health_goals.return_value = [
            {"_id": "g1", "_created_at": "2024-01-15", "goal_text": "Goal one"},
        ]
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted >= 1
        assert len(clean_db.get_health_goals()) == 1


# ── Med Reminders ───────────────────────────────────────


class TestSyncMedReminders:
    def test_reminders_synced(self, engine, raw_db, clean_db):
        """Med reminders should be synced with anonymization."""
        raw_db.get_med_reminders.return_value = [
            {
                "_id": "mr1",
                "_time": "08:00",
                "_enabled": True,
                "med_name": "Metformin",
                "notes": "Take with breakfast",
            },
        ]
        report = engine.sync_all(user_id=1)
        assert report.med_reminders_synced == 1

        reminders = clean_db.get_med_reminders()
        assert len(reminders) == 1
        assert reminders[0]["med_name"] == "Metformin"
        assert reminders[0]["time"] == "08:00"

    def test_reminder_with_pii_blocked(self, raw_db, clean_db, phi_firewall):
        """Reminder with PII in notes should be blocked."""
        from healthbot.llm.anonymizer import AnonymizationError

        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("Dr. John at 555-123-4567", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("PII detected")

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.get_med_reminders.return_value = [
            {"_id": "mr1", "_time": "08:00", "_enabled": True,
             "med_name": "Aspirin", "notes": "Dr. John at 555-123-4567"},
        ]
        report = eng.sync_all(user_id=1)
        assert report.pii_blocked >= 1
        assert report.med_reminders_synced == 0


# ── Providers ───────────────────────────────────────────


class TestSyncProviders:
    def test_providers_synced(self, engine, raw_db, clean_db):
        """Providers should sync specialty + notes only."""
        raw_db.get_providers.return_value = [
            {
                "_id": "prov1",
                "name": "Dr. Smith",  # intentionally present but NOT synced
                "specialty": "Endocrinology",
                "notes": "Specialist for thyroid issues",
                "address": "123 Main St",  # NOT synced
                "phone": "555-0100",  # NOT synced
            },
        ]
        report = engine.sync_all(user_id=1)
        assert report.providers_synced == 1

        providers = clean_db.get_providers()
        assert len(providers) == 1
        assert providers[0]["specialty"] == "Endocrinology"
        # Verify PII fields are NOT in clean DB
        assert "name" not in providers[0] or providers[0].get("name") is None
        assert "address" not in providers[0]
        assert "phone" not in providers[0]

    def test_stale_providers_deleted(self, engine, raw_db, clean_db):
        """Providers removed from raw vault should be deleted."""
        raw_db.get_providers.return_value = [
            {"_id": "p1", "specialty": "Cardiology", "notes": ""},
            {"_id": "p2", "specialty": "Neurology", "notes": ""},
        ]
        engine.sync_all(user_id=1)
        assert len(clean_db.get_providers()) == 2

        raw_db.get_providers.return_value = [
            {"_id": "p1", "specialty": "Cardiology", "notes": ""},
        ]
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted >= 1
        assert len(clean_db.get_providers()) == 1


# ── Appointments ────────────────────────────────────────


class TestSyncAppointments:
    def test_appointments_synced(self, engine, raw_db, clean_db):
        """Appointments should sync date/status/reason, omitting location."""
        raw_db.get_appointments.return_value = [
            {
                "_id": "appt1",
                "_provider_id": "prov1",
                "_appt_date": "2024-07-15",
                "_status": "scheduled",
                "reason": "Thyroid follow-up",
                "location": "123 Main St",  # NOT synced
            },
        ]
        report = engine.sync_all(user_id=1)
        assert report.appointments_synced == 1

        appts = clean_db.get_appointments()
        assert len(appts) == 1
        assert appts[0]["appt_date"] == "2024-07-15"
        assert appts[0]["status"] == "scheduled"
        assert appts[0]["reason"] == "Thyroid follow-up"
        assert "location" not in appts[0]

    def test_appointment_with_pii_blocked(self, raw_db, clean_db, phi_firewall):
        """Appointment with PII in reason should be blocked."""
        from healthbot.llm.anonymizer import AnonymizationError

        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("Visit Dr. SSN 123-45-6789", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("PII detected")

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.get_appointments.return_value = [
            {"_id": "appt1", "_provider_id": "p1", "_appt_date": "2024-07-15",
             "_status": "scheduled", "reason": "Visit Dr. SSN 123-45-6789"},
        ]
        report = eng.sync_all(user_id=1)
        assert report.pii_blocked >= 1
        assert report.appointments_synced == 0

    def test_stale_appointments_deleted(self, engine, raw_db, clean_db):
        """Cancelled/removed appointments should be deleted from clean DB."""
        raw_db.get_appointments.return_value = [
            {"_id": "a1", "_provider_id": "p1", "_appt_date": "2024-07-15",
             "_status": "scheduled", "reason": "Checkup"},
        ]
        engine.sync_all(user_id=1)
        assert len(clean_db.get_appointments()) == 1

        raw_db.get_appointments.return_value = []
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted >= 1
        assert len(clean_db.get_appointments()) == 0


# ── SyncReport extended fields ──────────────────────────


class TestSyncReportExtended:
    def test_summary_includes_new_types(self):
        report = SyncReport(
            workouts_synced=5,
            genetic_variants_synced=10,
            health_goals_synced=3,
            med_reminders_synced=2,
            providers_synced=4,
            appointments_synced=1,
        )
        s = report.summary()
        assert "Workouts: 5" in s
        assert "Genetics: 10" in s
        assert "Goals: 3" in s
        assert "Reminders: 2" in s
        assert "Providers: 4" in s
        assert "Appointments: 1" in s


# ── Health summary includes new sections ─────────────────


class TestHealthSummaryExtended:
    def test_summary_includes_workouts(self, clean_db):
        """Health summary markdown should include workouts section."""
        clean_db.upsert_workout(
            workout_id="wo1", sport_type="running",
            start_date="2024-06-01", duration_minutes=30.0,
        )
        md = clean_db.get_health_summary_markdown()
        assert "Recent Workouts" in md
        assert "running" in md

    def test_summary_includes_genetics(self, clean_db):
        """Health summary markdown should include genetics section."""
        clean_db.upsert_genetic_variant(
            variant_id="gv1", rsid="rs1801133", genotype="CT",
            phenotype="MTHFR C677T",
        )
        md = clean_db.get_health_summary_markdown()
        assert "Genetic Variants" in md
        assert "rs1801133" in md

    def test_summary_includes_goals(self, clean_db):
        """Health summary markdown should include goals section."""
        clean_db.upsert_health_goal(
            goal_id="g1", goal_text="Improve sleep quality",
        )
        md = clean_db.get_health_summary_markdown()
        assert "Health Goals" in md
        assert "Improve sleep quality" in md

    def test_summary_includes_reminders(self, clean_db):
        """Health summary markdown should include med reminders section."""
        clean_db.upsert_med_reminder(
            reminder_id="mr1", time="08:00",
            med_name="Metformin", notes="With food",
        )
        md = clean_db.get_health_summary_markdown()
        assert "Medication Reminders" in md
        assert "Metformin" in md

    def test_summary_includes_providers(self, clean_db):
        """Health summary markdown should include providers section."""
        clean_db.upsert_provider(
            provider_id="p1", specialty="Endocrinology",
        )
        md = clean_db.get_health_summary_markdown()
        assert "Healthcare Providers" in md
        assert "Endocrinology" in md

    def test_summary_includes_appointments(self, clean_db):
        """Health summary markdown should include appointments section."""
        clean_db.upsert_appointment(
            appt_id="a1", appt_date="2024-07-15",
            status="scheduled", reason="Follow-up",
        )
        md = clean_db.get_health_summary_markdown()
        assert "Appointments" in md
        assert "Follow-up" in md


# ── Search includes new types ────────────────────────────


class TestSearchExtended:
    def test_search_finds_workouts(self, clean_db):
        clean_db.upsert_workout(
            workout_id="wo1", sport_type="running",
            start_date="2024-06-01",
        )
        results = clean_db.search("running")
        assert any(r["source"] == "workout" for r in results)

    def test_search_finds_genetics(self, clean_db):
        clean_db.upsert_genetic_variant(
            variant_id="gv1", rsid="rs1801133", phenotype="MTHFR",
        )
        results = clean_db.search("MTHFR")
        assert any(r["source"] == "genetic" for r in results)

    def test_search_finds_goals(self, clean_db):
        clean_db.upsert_health_goal(
            goal_id="g1", goal_text="Reduce HbA1c below 5.7",
        )
        results = clean_db.search("HbA1c")
        assert any(r["source"] == "goal" for r in results)


# ── Rebuild includes new tables ──────────────────────────


class TestRebuildExtended:
    def test_rebuild_clears_new_tables(self, engine, raw_db, clean_db):
        """Rebuild should clear all new tables before re-syncing."""
        clean_db.upsert_workout(workout_id="wo1", sport_type="old")
        clean_db.upsert_genetic_variant(variant_id="gv1", rsid="old")
        clean_db.upsert_health_goal(goal_id="g1", goal_text="old goal")
        clean_db.upsert_med_reminder(reminder_id="mr1", med_name="old med")
        clean_db.upsert_provider(provider_id="p1", specialty="old spec")
        clean_db.upsert_appointment(appt_id="a1", reason="old reason")

        engine.rebuild(user_id=1)

        assert len(clean_db.get_workouts()) == 0
        assert len(clean_db.get_genetic_variants()) == 0
        assert len(clean_db.get_health_goals()) == 0
        assert len(clean_db.get_med_reminders()) == 0
        assert len(clean_db.get_providers()) == 0
        assert len(clean_db.get_appointments()) == 0
