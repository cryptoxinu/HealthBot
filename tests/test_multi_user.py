"""Tests for multi-user support."""
from __future__ import annotations

from pathlib import Path

import pytest

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult
from healthbot.security.key_manager import KeyManager

PASSPHRASE = "test-multiuser-pass"


@pytest.fixture
def multi_db(tmp_path: Path):
    """Create DB with migration 5 applied (user_id columns)."""
    vault_home = tmp_path / "vault"
    vault_home.mkdir()
    config = Config(vault_home=vault_home)
    config.ensure_dirs()

    km = KeyManager(config)
    km.setup(PASSPHRASE)

    db = HealthDB(config, km)
    db.open()
    db.run_migrations()

    yield db, config, km


class TestMigration:
    """Migration 5 should add user_id columns."""

    def test_observations_has_user_id(self, multi_db) -> None:
        db, _, _ = multi_db
        # Should not raise
        db.conn.execute("SELECT user_id FROM observations LIMIT 1")

    def test_medications_has_user_id(self, multi_db) -> None:
        db, _, _ = multi_db
        db.conn.execute("SELECT user_id FROM medications LIMIT 1")

    def test_wearable_has_user_id(self, multi_db) -> None:
        db, _, _ = multi_db
        db.conn.execute("SELECT user_id FROM wearable_daily LIMIT 1")

    def test_documents_has_user_id(self, multi_db) -> None:
        db, _, _ = multi_db
        db.conn.execute("SELECT user_id FROM documents LIMIT 1")


class TestUserFiltering:
    """Queries should filter by user_id when specified."""

    def test_query_observations_no_filter(self, multi_db) -> None:
        db, _, _ = multi_db
        # Default: no filter, returns all
        results = db.query_observations(record_type="lab_result")
        assert isinstance(results, list)

    def test_query_observations_with_user_id(self, multi_db) -> None:
        db, _, _ = multi_db
        # Insert observations for different users
        lab1 = LabResult(id="l1", test_name="Glucose", value=100.0, canonical_name="glucose")
        lab2 = LabResult(id="l2", test_name="TSH", value=2.5, canonical_name="tsh")

        db.insert_observation(lab1)
        db.insert_observation(lab2)

        # Set user_id directly (since insert_observation doesn't have user_id param yet)
        db.conn.execute("UPDATE observations SET user_id = 1 WHERE obs_id = 'l1'")
        db.conn.execute("UPDATE observations SET user_id = 2 WHERE obs_id = 'l2'")
        db.conn.commit()

        user1_results = db.query_observations(user_id=1)
        user2_results = db.query_observations(user_id=2)

        assert len(user1_results) == 1
        assert len(user2_results) == 1

    def test_get_active_medications_no_filter(self, multi_db) -> None:
        db, _, _ = multi_db
        results = db.get_active_medications()
        assert isinstance(results, list)

    def test_get_active_medications_with_user_id(self, multi_db) -> None:
        db, _, _ = multi_db
        from healthbot.data.models import Medication

        med = Medication(id="m1", name="Metformin", status="active")
        db.insert_medication(med)

        db.conn.execute("UPDATE medications SET user_id = 99 WHERE med_id = 'm1'")
        db.conn.commit()

        # User 99 should see the med
        results_99 = db.get_active_medications(user_id=99)
        assert len(results_99) == 1

        # User 1 should not
        results_1 = db.get_active_medications(user_id=1)
        assert len(results_1) == 0


class TestBackwardsCompat:
    """Existing callers with no user_id should still work."""

    def test_default_user_id_zero(self, multi_db) -> None:
        db, _, _ = multi_db
        lab = LabResult(id="bc1", test_name="Test", value=1.0, canonical_name="test")
        db.insert_observation(lab)

        row = db.conn.execute(
            "SELECT user_id FROM observations WHERE obs_id = 'bc1'"
        ).fetchone()
        assert row["user_id"] == 0

    def test_query_returns_all_without_user_id(self, multi_db) -> None:
        db, _, _ = multi_db
        lab = LabResult(id="bc2", test_name="Test2", value=2.0, canonical_name="test2")
        db.insert_observation(lab)

        results = db.query_observations()
        assert any(r.get("test_name") == "Test2" for r in results)
