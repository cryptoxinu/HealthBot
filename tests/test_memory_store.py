"""Tests for memory store (STM + LTM + hypotheses)."""
from __future__ import annotations

from pathlib import Path

import pytest

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.security.key_manager import KeyManager

PASSPHRASE = "test-memory-passphrase"


@pytest.fixture
def memory_db(tmp_path: Path):
    """Create a DB with memory tables available."""
    vault_home = tmp_path / "vault"
    vault_home.mkdir()
    config = Config(vault_home=vault_home)
    config.ensure_dirs()

    km = KeyManager(config)
    km.setup(PASSPHRASE)

    db = HealthDB(config, km)
    db.open()
    db.run_migrations()

    yield db

    db.close()
    km.lock()


class TestSTM:
    """Test short-term memory operations."""

    def test_insert_and_retrieve(self, memory_db: HealthDB):
        db = memory_db
        db.insert_stm(user_id=123, role="user", content="Hello")
        db.insert_stm(user_id=123, role="assistant", content="Hi there!")

        stm = db.get_recent_stm(user_id=123)
        assert len(stm) == 2
        assert stm[0]["role"] == "user"
        assert stm[0]["content"] == "Hello"
        assert stm[1]["role"] == "assistant"
        assert stm[1]["content"] == "Hi there!"

    def test_stm_per_user(self, memory_db: HealthDB):
        db = memory_db
        db.insert_stm(user_id=123, role="user", content="User 123 msg")
        db.insert_stm(user_id=456, role="user", content="User 456 msg")

        stm_123 = db.get_recent_stm(user_id=123)
        stm_456 = db.get_recent_stm(user_id=456)
        assert len(stm_123) == 1
        assert len(stm_456) == 1
        assert stm_123[0]["content"] == "User 123 msg"
        assert stm_456[0]["content"] == "User 456 msg"

    def test_mark_consolidated(self, memory_db: HealthDB):
        db = memory_db
        db.insert_stm(user_id=123, role="user", content="Msg 1")
        db.insert_stm(user_id=123, role="user", content="Msg 2")

        stm = db.get_recent_stm(user_id=123)
        assert len(stm) == 2

        ids = [s["_id"] for s in stm]
        db.mark_stm_consolidated(ids)

        # After consolidation, get_recent_stm returns only unconsolidated
        stm_after = db.get_recent_stm(user_id=123)
        assert len(stm_after) == 0

    def test_stm_limit(self, memory_db: HealthDB):
        db = memory_db
        for i in range(30):
            db.insert_stm(user_id=123, role="user", content=f"Msg {i}")

        stm = db.get_recent_stm(user_id=123, limit=10)
        assert len(stm) == 10

    def test_stm_encrypted(self, memory_db: HealthDB):
        """Verify STM content is encrypted in the database."""
        db = memory_db
        db.insert_stm(user_id=123, role="user", content="secret medical data")

        # Read raw from DB
        row = db.conn.execute("SELECT encrypted_data FROM memory_stm").fetchone()
        assert row is not None
        raw = row["encrypted_data"]
        assert b"secret medical data" not in raw


class TestLTM:
    """Test long-term memory operations."""

    def test_insert_and_retrieve(self, memory_db: HealthDB):
        db = memory_db
        db.insert_ltm(user_id=123, category="condition", fact="Has type 2 diabetes")
        db.insert_ltm(user_id=123, category="medication", fact="Takes Metformin 500mg")

        ltm = db.get_ltm_by_user(user_id=123)
        assert len(ltm) == 2
        facts = [f["fact"] for f in ltm]
        assert "Has type 2 diabetes" in facts
        assert "Takes Metformin 500mg" in facts

    def test_update_ltm(self, memory_db: HealthDB):
        db = memory_db
        fact_id = db.insert_ltm(user_id=123, category="demographic", fact="Age 28")

        db.update_ltm(fact_id, "Age 29")
        ltm = db.get_ltm_by_user(user_id=123)
        assert len(ltm) == 1
        assert ltm[0]["fact"] == "Age 29"

    def test_delete_ltm(self, memory_db: HealthDB):
        db = memory_db
        fact_id = db.insert_ltm(user_id=123, category="condition", fact="Test fact")
        db.delete_ltm(fact_id)

        ltm = db.get_ltm_by_user(user_id=123)
        assert len(ltm) == 0

    def test_ltm_categories(self, memory_db: HealthDB):
        db = memory_db
        db.insert_ltm(user_id=123, category="condition", fact="POTS")
        db.insert_ltm(user_id=123, category="demographic", fact="Male, 28")
        db.insert_ltm(user_id=123, category="medication", fact="Metformin")

        ltm = db.get_ltm_by_user(user_id=123)
        categories = {f["_category"] for f in ltm}
        assert categories == {"condition", "demographic", "medication"}


class TestHypotheses:
    """Test hypothesis tracking."""

    def test_create_hypothesis(self, memory_db: HealthDB):
        db = memory_db
        db.insert_hypothesis(user_id=123, data={
            "title": "POTS",
            "confidence": 0.4,
            "evidence_for": ["Elevated HR on standing"],
            "evidence_against": [],
            "missing_tests": ["Tilt table test"],
        })

        hyps = db.get_active_hypotheses(user_id=123)
        assert len(hyps) == 1
        assert hyps[0]["title"] == "POTS"
        assert hyps[0]["_confidence"] == 0.4
        assert "Elevated HR on standing" in hyps[0]["evidence_for"]

    def test_update_hypothesis(self, memory_db: HealthDB):
        db = memory_db
        hyp_id = db.insert_hypothesis(user_id=123, data={
            "title": "POTS",
            "confidence": 0.4,
            "status": "active",
        })

        db.update_hypothesis(hyp_id, {
            "title": "POTS",
            "confidence": 0.7,
            "status": "active",
            "evidence_for": ["Elevated HR", "Tilt table positive"],
        })

        hyps = db.get_active_hypotheses(user_id=123)
        assert len(hyps) == 1
        assert hyps[0]["_confidence"] == 0.7

    def test_hypothesis_status_change(self, memory_db: HealthDB):
        db = memory_db
        hyp_id = db.insert_hypothesis(user_id=123, data={
            "title": "POTS",
            "confidence": 0.9,
            "status": "active",
        })

        db.update_hypothesis(hyp_id, {
            "title": "POTS",
            "confidence": 0.9,
            "status": "confirmed",
        })

        # Active hypotheses should be empty now
        active = db.get_active_hypotheses(user_id=123)
        assert len(active) == 0

    def test_multiple_hypotheses(self, memory_db: HealthDB):
        db = memory_db
        db.insert_hypothesis(user_id=123, data={
            "title": "POTS", "confidence": 0.4,
        })
        db.insert_hypothesis(user_id=123, data={
            "title": "Iron deficiency", "confidence": 0.7,
        })

        hyps = db.get_active_hypotheses(user_id=123)
        assert len(hyps) == 2
        # Ordered by confidence DESC
        assert hyps[0]["_confidence"] >= hyps[1]["_confidence"]
