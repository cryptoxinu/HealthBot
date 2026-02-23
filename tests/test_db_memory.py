"""Tests for db_memory mixin (STM, LTM, hypotheses, demographics)."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import date

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.data.db_memory import MemoryMixin


class MockDB(MemoryMixin):
    """Minimal stand-in for HealthDB to test the mixin."""

    def __init__(self, db_path):
        self._key = AESGCM.generate_key(bit_length=256)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS memory_stm (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                created_at TEXT NOT NULL,
                consolidated INTEGER DEFAULT 0,
                encrypted_data BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS memory_ltm (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source TEXT DEFAULT '',
                encrypted_data BLOB NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hypotheses (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                status TEXT DEFAULT 'active',
                confidence REAL DEFAULT 0.0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                encrypted_data BLOB NOT NULL
            );
        """)

    @property
    def conn(self):
        return self._conn

    def _encrypt(self, data, aad_context):
        nonce = os.urandom(12)
        aesgcm = AESGCM(self._key)
        plaintext = json.dumps(data).encode("utf-8")
        ct = aesgcm.encrypt(nonce, plaintext, aad_context.encode("utf-8"))
        return nonce + ct

    def _decrypt(self, blob, aad_context):
        nonce = blob[:12]
        ct = blob[12:]
        aesgcm = AESGCM(self._key)
        plaintext = aesgcm.decrypt(nonce, ct, aad_context.encode("utf-8"))
        return json.loads(plaintext.decode("utf-8"))

    def _now(self):
        from datetime import UTC, datetime
        return datetime.now(UTC).isoformat()

    def close(self):
        self._conn.close()


@pytest.fixture
def db(tmp_path):
    d = MockDB(tmp_path / "test.db")
    yield d
    d.close()


class TestSTM:
    def test_insert_and_retrieve(self, db):
        db.insert_stm(1, "user", "hello")
        msgs = db.get_recent_stm(1)
        assert len(msgs) == 1
        assert msgs[0]["content"] == "hello"
        assert msgs[0]["role"] == "user"

    def test_multiple_messages_ordered(self, db):
        db.insert_stm(1, "user", "first")
        db.insert_stm(1, "assistant", "second")
        msgs = db.get_recent_stm(1)
        assert len(msgs) == 2
        assert msgs[0]["content"] == "first"
        assert msgs[1]["content"] == "second"

    def test_user_isolation(self, db):
        db.insert_stm(1, "user", "user1 msg")
        db.insert_stm(2, "user", "user2 msg")
        assert len(db.get_recent_stm(1)) == 1
        assert len(db.get_recent_stm(2)) == 1

    def test_mark_consolidated(self, db):
        msg_id = db.insert_stm(1, "user", "to consolidate")
        db.mark_stm_consolidated([msg_id])
        assert len(db.get_recent_stm(1)) == 0

    def test_mark_consolidated_empty_list(self, db):
        db.mark_stm_consolidated([])  # Should not raise

    def test_clear_old_stm(self, db):
        db.insert_stm(1, "user", "old msg")
        msg_id = db.conn.execute("SELECT id FROM memory_stm").fetchone()["id"]
        db.mark_stm_consolidated([msg_id])
        deleted = db.clear_old_stm(days=0)
        assert deleted >= 1

    def test_limit_parameter(self, db):
        for i in range(10):
            db.insert_stm(1, "user", f"msg {i}")
        msgs = db.get_recent_stm(1, limit=3)
        assert len(msgs) == 3


class TestLTM:
    def test_insert_and_retrieve(self, db):
        db.insert_ltm(1, "conditions", "Type 2 diabetes")
        facts = db.get_ltm_by_user(1)
        assert len(facts) == 1
        assert facts[0]["fact"] == "Type 2 diabetes"
        assert facts[0]["_category"] == "conditions"

    def test_update_ltm(self, db):
        fact_id = db.insert_ltm(1, "conditions", "old fact")
        db.update_ltm(fact_id, "updated fact")
        facts = db.get_ltm_by_user(1)
        assert facts[0]["fact"] == "updated fact"

    def test_update_ltm_change_category(self, db):
        fact_id = db.insert_ltm(1, "conditions", "moved fact")
        db.update_ltm(fact_id, "moved fact", category="medications")
        facts = db.get_ltm_by_user(1)
        assert facts[0]["_category"] == "medications"

    def test_delete_ltm(self, db):
        fact_id = db.insert_ltm(1, "conditions", "to delete")
        db.delete_ltm(fact_id)
        assert len(db.get_ltm_by_user(1)) == 0

    def test_update_nonexistent(self, db):
        db.update_ltm("nonexistent", "new fact")  # Should not raise


class TestHypotheses:
    def test_insert_and_retrieve(self, db):
        db.insert_hypothesis(1, {
            "title": "Iron deficiency",
            "confidence": 0.7,
            "evidence_for": ["Low ferritin"],
        })
        hyps = db.get_active_hypotheses(1)
        assert len(hyps) == 1
        assert hyps[0]["title"] == "Iron deficiency"
        assert hyps[0]["_confidence"] == pytest.approx(0.7)

    def test_get_by_id(self, db):
        hyp_id = db.insert_hypothesis(1, {"title": "Test hyp", "confidence": 0.5})
        hyp = db.get_hypothesis(hyp_id)
        assert hyp is not None
        assert hyp["title"] == "Test hyp"

    def test_get_nonexistent(self, db):
        assert db.get_hypothesis("nonexistent") is None

    def test_update_hypothesis(self, db):
        hyp_id = db.insert_hypothesis(1, {"title": "Original", "confidence": 0.3})
        db.update_hypothesis(hyp_id, {
            "title": "Original",
            "confidence": 0.8,
            "status": "confirmed",
        })
        hyp = db.get_hypothesis(hyp_id)
        assert hyp["confidence"] == 0.8
        assert hyp["_status"] == "confirmed"

    def test_get_all_hypotheses(self, db):
        db.insert_hypothesis(1, {"title": "Active", "confidence": 0.5})
        hyp_id = db.insert_hypothesis(1, {"title": "Ruled out", "confidence": 0.1})
        db.update_hypothesis(hyp_id, {
            "title": "Ruled out", "confidence": 0.1, "status": "ruled_out",
        })
        all_hyps = db.get_all_hypotheses(1)
        assert len(all_hyps) == 2
        active = db.get_active_hypotheses(1)
        assert len(active) == 1


class TestDemographics:
    def test_get_demographics_with_dob(self, db):
        db.insert_ltm(1, "demographic", "Date of birth: 1990-05-15 (age 35)")
        db.insert_ltm(1, "demographic", "Biological sex: male")
        demo = db.get_user_demographics(1)
        assert demo["dob"] == date(1990, 5, 15)
        assert demo["sex"] == "male"
        assert demo["age"] is not None

    def test_get_demographics_dob_format(self, db):
        db.insert_ltm(1, "demographic", "DOB: 2000-01-01")
        demo = db.get_user_demographics(1)
        assert demo["dob"] == date(2000, 1, 1)

    def test_get_demographics_age_only_fallback(self, db):
        db.insert_ltm(1, "demographic", "Age: 28")
        demo = db.get_user_demographics(1)
        assert demo["dob"] is None
        assert demo["age"] == 28

    def test_get_demographics_female(self, db):
        db.insert_ltm(1, "demographic", "Biological sex: female")
        demo = db.get_user_demographics(1)
        assert demo["sex"] == "female"

    def test_get_demographics_no_facts(self, db):
        demo = db.get_user_demographics(1)
        assert demo["dob"] is None
        assert demo["age"] is None
        assert demo["sex"] is None

    def test_get_demographics_ignores_non_demographic(self, db):
        db.insert_ltm(1, "condition", "Date of birth: 1990-05-15 (age 35)")
        demo = db.get_user_demographics(1)
        assert demo["dob"] is None

    def test_get_demographics_user_isolation(self, db):
        db.insert_ltm(1, "demographic", "Biological sex: male")
        db.insert_ltm(2, "demographic", "Biological sex: female")
        assert db.get_user_demographics(1)["sex"] == "male"
        assert db.get_user_demographics(2)["sex"] == "female"

    def test_age_at_date_basic(self):
        dob = date(1990, 5, 15)
        assert MemoryMixin.age_at_date(dob, date(2025, 6, 1)) == 35
        assert MemoryMixin.age_at_date(dob, date(2025, 5, 14)) == 34  # day before birthday
        assert MemoryMixin.age_at_date(dob, date(2025, 5, 15)) == 35  # birthday

    def test_age_at_date_young(self):
        dob = date(2000, 12, 25)
        assert MemoryMixin.age_at_date(dob, date(2014, 6, 1)) == 13
        assert MemoryMixin.age_at_date(dob, date(2014, 12, 24)) == 13
        assert MemoryMixin.age_at_date(dob, date(2014, 12, 25)) == 14

    def test_age_at_date_same_year(self):
        dob = date(2020, 3, 1)
        assert MemoryMixin.age_at_date(dob, date(2020, 3, 1)) == 0
        assert MemoryMixin.age_at_date(dob, date(2020, 2, 28)) == 0
