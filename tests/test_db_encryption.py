"""Tests for database field-level encryption."""
from __future__ import annotations

import uuid
from datetime import date, datetime

from healthbot.data.models import LabResult, Medication, Workout


class TestEncryptionRoundtrip:
    """Verify encrypt/decrypt works correctly via insert/query."""

    def test_observation_roundtrip(self, db):
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date(2024, 3, 15),
            reference_low=70.0,
            reference_high=100.0,
        )
        db.insert_observation(lab)
        rows = db.query_observations(
            record_type="lab_result", canonical_name="glucose", limit=1
        )
        assert len(rows) == 1
        assert rows[0]["test_name"] == "Glucose"
        assert float(rows[0]["value"]) == 95.0
        assert rows[0]["unit"] == "mg/dL"

    def test_medication_roundtrip(self, db):
        med = Medication(
            id=uuid.uuid4().hex,
            name="Metformin",
            dose="500mg",
            frequency="twice daily",
            status="active",
        )
        db.insert_medication(med)
        meds = db.get_active_medications()
        assert len(meds) >= 1
        found = [m for m in meds if m.get("name") == "Metformin"]
        assert len(found) == 1
        assert found[0]["dose"] == "500mg"

    def test_stm_roundtrip(self, db):
        db.run_migrations()
        user_id = 12345
        db.insert_stm(user_id, "user", "How is my glucose?")
        db.insert_stm(user_id, "assistant", "Your glucose is 95 mg/dL.")
        rows = db.get_recent_stm(user_id, limit=2)
        assert len(rows) == 2
        contents = {r["content"] for r in rows}
        assert "How is my glucose?" in contents
        assert "Your glucose is 95 mg/dL." in contents


class TestRawDbHasNoPlaintext:
    """Verify encrypted data is not readable in raw SQLite."""

    def test_observation_encrypted_on_disk(self, db):
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Hemoglobin",
            canonical_name="hemoglobin",
            value=14.2,
            unit="g/dL",
            date_collected=date(2024, 5, 1),
        )
        db.insert_observation(lab)

        # Direct SQL query bypasses decryption
        cursor = db.conn.execute(
            "SELECT encrypted_data FROM observations WHERE canonical_name = 'hemoglobin'"
        )
        row = cursor.fetchone()
        assert row is not None
        raw = row["encrypted_data"]
        # encrypted_data should be bytes (ciphertext), not readable JSON
        assert isinstance(raw, (bytes, memoryview))
        # Should NOT contain plaintext test values
        if isinstance(raw, memoryview):
            raw = bytes(raw)
        assert b"Hemoglobin" not in raw
        assert b"14.2" not in raw

    def test_stm_encrypted_on_disk(self, db):
        db.run_migrations()
        db.insert_stm(99999, "user", "My SSN is 123-45-6789")
        cursor = db.conn.execute(
            "SELECT encrypted_data FROM memory_stm ORDER BY rowid DESC LIMIT 1"
        )
        row = cursor.fetchone()
        assert row is not None
        raw = row["encrypted_data"]
        if isinstance(raw, memoryview):
            raw = bytes(raw)
        assert isinstance(raw, bytes)
        assert b"123-45-6789" not in raw


class TestMultipleObservations:
    """Verify multiple records with different AAD don't interfere."""

    def test_two_different_tests(self, db):
        lab1 = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
        )
        lab2 = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=120.0,
            unit="mg/dL",
        )
        db.insert_observation(lab1)
        db.insert_observation(lab2)

        glucose = db.query_observations(
            record_type="lab_result", canonical_name="glucose", limit=1
        )
        ldl = db.query_observations(
            record_type="lab_result", canonical_name="ldl", limit=1
        )
        assert len(glucose) == 1
        assert len(ldl) == 1
        assert float(glucose[0]["value"]) == 95.0
        assert float(ldl[0]["value"]) == 120.0


class TestWorkoutDbOperations:
    """Workout insert, query, dedup, summary, and encryption."""

    def _make_workout(
        self,
        sport: str = "running",
        start: datetime | None = None,
        dur: float = 30.0,
        cal: float = 250.0,
    ) -> Workout:
        return Workout(
            id=uuid.uuid4().hex,
            sport_type=sport,
            start_time=start or datetime(2025, 6, 1, 7, 0),
            duration_minutes=dur,
            calories_burned=cal,
            source="apple_health",
        )

    def test_insert_and_query(self, db):
        wo = self._make_workout()
        db.insert_workout(wo, user_id=1)
        rows = db.query_workouts(user_id=1)
        assert len(rows) == 1
        assert rows[0]["sport_type"] == "running"
        assert float(rows[0]["duration_minutes"]) == 30.0

    def test_filter_by_sport(self, db):
        db.insert_workout(self._make_workout("running"), user_id=1)
        db.insert_workout(
            self._make_workout(
                "cycling", start=datetime(2025, 6, 2, 8, 0),
            ),
            user_id=1,
        )
        rows = db.query_workouts(sport_type="running", user_id=1)
        assert len(rows) == 1
        assert rows[0]["sport_type"] == "running"

    def test_dedup_keys(self, db):
        wo = self._make_workout()
        db.insert_workout(wo, user_id=1)
        keys = db.get_existing_workout_keys(user_id=1)
        assert len(keys) == 1
        sport, start_date = next(iter(keys))
        assert sport == "running"

    def test_encrypted_on_disk(self, db):
        wo = self._make_workout()
        db.insert_workout(wo, user_id=1)
        cursor = db.conn.execute(
            "SELECT encrypted_data FROM workouts LIMIT 1"
        )
        row = cursor.fetchone()
        raw = row["encrypted_data"]
        if isinstance(raw, memoryview):
            raw = bytes(raw)
        assert isinstance(raw, bytes)
        assert b"running" not in raw

    def test_get_workout_summary(self, db):
        db.insert_workout(
            self._make_workout("running", datetime(2025, 6, 1, 7, 0), 30, 300),
            user_id=1,
        )
        db.insert_workout(
            self._make_workout("cycling", datetime(2025, 6, 2, 8, 0), 45, 400),
            user_id=1,
        )
        summary = db.get_workout_summary(days=365, user_id=1)
        assert summary["total_workouts"] == 2
        assert summary["total_minutes"] == 75.0
        assert summary["total_calories"] == 700.0
        assert "running" in summary["by_sport"]
        assert "cycling" in summary["by_sport"]
        assert summary["by_sport"]["running"]["count"] == 1

    def test_summary_empty(self, db):
        summary = db.get_workout_summary(days=30, user_id=1)
        assert summary["total_workouts"] == 0
        assert summary["streak_days"] == 0
