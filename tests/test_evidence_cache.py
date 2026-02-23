"""Tests for research/external_evidence_store.py — TTL, list, detail, cleanup."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from healthbot.research.external_evidence_store import ExternalEvidenceStore


@pytest.fixture
def store(config, key_manager, db) -> ExternalEvidenceStore:
    db.run_migrations()
    return ExternalEvidenceStore(db)


class TestStore:
    def test_store_returns_id(self, store):
        ev_id = store.store("claude_cli", "What is glucose?", "Glucose is a sugar.")
        assert ev_id
        assert len(ev_id) > 10

    def test_store_with_dict_result(self, store):
        ev_id = store.store("pubmed", "POTS treatment", {"text": "Treatment options..."})
        assert ev_id

    def test_store_sets_ttl(self, store, db):
        ev_id = store.store("test", "query", "result", ttl_days=7)
        row = db.conn.execute(
            "SELECT expires_at FROM external_evidence WHERE evidence_id = ?",
            (ev_id,),
        ).fetchone()
        assert row["expires_at"]
        expires = datetime.fromisoformat(row["expires_at"])
        expected = datetime.now(UTC) + timedelta(days=7)
        assert abs((expires - expected).total_seconds()) < 60


class TestLookupCached:
    def test_finds_cached(self, store):
        store.store("test", "my query", "cached result")
        result = store.lookup_cached("my query")
        assert result is not None

    def test_returns_none_for_unknown(self, store):
        assert store.lookup_cached("unknown query") is None

    def test_returns_none_for_expired(self, store, db):
        ev_id = store.store("test", "expiring query", "result", ttl_days=1)
        # Manually set expires_at to the past
        past = (datetime.now(UTC) - timedelta(days=2)).isoformat()
        db.conn.execute(
            "UPDATE external_evidence SET expires_at = ? WHERE evidence_id = ?",
            (past, ev_id),
        )
        db.conn.commit()

        assert store.lookup_cached("expiring query") is None


class TestListEvidence:
    def test_empty_list(self, store):
        assert store.list_evidence() == []

    def test_lists_entries(self, store):
        store.store("claude_cli", "glucose research", "Result 1")
        store.store("pubmed", "thyroid review", "Result 2")
        entries = store.list_evidence()
        assert len(entries) == 2
        assert all("evidence_id" in e for e in entries)
        assert all("source" in e for e in entries)

    def test_respects_limit(self, store):
        for i in range(5):
            store.store("test", f"query {i}", f"result {i}")
        entries = store.list_evidence(limit=3)
        assert len(entries) == 3

    def test_shows_expired_flag(self, store, db):
        ev_id = store.store("test", "old query", "old result", ttl_days=1)
        past = (datetime.now(UTC) - timedelta(days=2)).isoformat()
        db.conn.execute(
            "UPDATE external_evidence SET expires_at = ? WHERE evidence_id = ?",
            (past, ev_id),
        )
        db.conn.commit()

        entries = store.list_evidence()
        assert len(entries) == 1
        assert entries[0]["expired"] is True


class TestGetEvidenceDetail:
    def test_returns_detail(self, store):
        ev_id = store.store("claude_cli", "detailed query", "Full research text here.")
        detail = store.get_evidence_detail(ev_id)
        assert detail is not None
        assert detail["_source"] == "claude_cli"

    def test_returns_none_for_nonexistent(self, store):
        assert store.get_evidence_detail("nonexistent") is None


class TestCleanupExpired:
    def test_deletes_expired(self, store, db):
        ev_id = store.store("test", "old", "result", ttl_days=1)
        past = (datetime.now(UTC) - timedelta(days=2)).isoformat()
        db.conn.execute(
            "UPDATE external_evidence SET expires_at = ? WHERE evidence_id = ?",
            (past, ev_id),
        )
        db.conn.commit()

        count = store.cleanup_expired()
        assert count == 1
        assert store.list_evidence() == []

    def test_keeps_non_expired(self, store):
        store.store("test", "fresh", "result", ttl_days=30)
        count = store.cleanup_expired()
        assert count == 0
        assert len(store.list_evidence()) == 1
