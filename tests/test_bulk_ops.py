"""Tests for healthbot.data.bulk_ops — bulk delete/reset operations."""
from __future__ import annotations

from datetime import date

import pytest

from healthbot.data.bulk_ops import BulkOps
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, Medication, TriageLevel, VitalSign
from healthbot.security.vault import Vault

# --- Helpers ---

def _insert_lab(db: HealthDB, name: str = "glucose", value: float = 100.0) -> str:
    lab = LabResult(
        id="",
        test_name=name,
        canonical_name=name,
        value=value,
        unit="mg/dL",
        date_collected=date(2025, 1, 15),
        triage_level=TriageLevel.NORMAL,
    )
    return db.insert_observation(lab)


def _insert_vital(db: HealthDB) -> str:
    vital = VitalSign(id="", type="heart_rate", value=72.0, unit="bpm")
    return db.insert_observation(vital)


def _insert_medication(db: HealthDB, name: str = "metformin") -> str:
    med = Medication(id="", name=name, dose="500mg", frequency="twice daily")
    return db.insert_medication(med)


def _insert_search_index(db: HealthDB, doc_id: str, record_type: str = "lab_result") -> None:
    db.conn.execute(
        "INSERT INTO search_index (doc_id, record_type, date_effective, text_for_search) "
        "VALUES (?, ?, '2025-01-15', 'test text')",
        (doc_id, record_type),
    )
    db.conn.commit()


def _insert_stm(db: HealthDB, user_id: int = 1) -> str:
    return db.insert_stm(user_id, "user", "test message")


def _insert_ltm(db: HealthDB, user_id: int = 1) -> str:
    return db.insert_ltm(user_id, "demographic", "Age: 30")


def _insert_hypothesis(db: HealthDB, user_id: int = 1) -> str:
    return db.insert_hypothesis(user_id, {
        "title": "Test hypothesis",
        "confidence": 0.5,
        "evidence_for": [],
        "evidence_against": [],
        "missing_tests": [],
    })


@pytest.fixture
def ops(db: HealthDB, vault: Vault) -> BulkOps:
    db.run_migrations()
    return BulkOps(db, vault)


# --- Tests ---

class TestCountAll:
    def test_empty_db(self, ops: BulkOps) -> None:
        counts = ops.count_all()
        assert all(v == 0 for v in counts.values())

    def test_with_data(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_lab(db)
        _insert_lab(db, "ldl", 120.0)
        _insert_stm(db)
        _insert_ltm(db)
        counts = ops.count_all()
        assert counts["labs"] == 2
        assert counts["memory"] == 2  # 1 STM + 1 LTM
        assert counts["medications"] == 0


class TestDeleteCategory:
    def test_delete_labs(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_lab(db)
        _insert_lab(db, "ldl")
        assert ops.delete_category("labs") == 2
        assert db.conn.execute("SELECT COUNT(*) AS n FROM observations").fetchone()["n"] == 0

    def test_delete_memory(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_stm(db)
        _insert_stm(db)
        _insert_ltm(db)
        deleted = ops.delete_category("memory")
        assert deleted == 3
        assert db.conn.execute("SELECT COUNT(*) AS n FROM memory_stm").fetchone()["n"] == 0
        assert db.conn.execute("SELECT COUNT(*) AS n FROM memory_ltm").fetchone()["n"] == 0

    def test_delete_hypotheses(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_hypothesis(db)
        assert ops.delete_category("hypotheses") == 1

    def test_delete_labs_cleans_search_index(self, ops: BulkOps, db: HealthDB) -> None:
        obs_id = _insert_lab(db)
        _insert_search_index(db, obs_id, "lab_result")
        ops.delete_category("labs")
        count = db.conn.execute("SELECT COUNT(*) AS n FROM search_index").fetchone()["n"]
        assert count == 0

    def test_delete_documents_cleans_blobs(
        self, ops: BulkOps, db: HealthDB, vault: Vault
    ) -> None:
        blob_id = vault.store_blob(b"test pdf content")
        from healthbot.data.models import Document
        doc = Document(id="", source="test", sha256="abc123", enc_blob_path=blob_id)
        db.insert_document(doc)
        assert vault.blob_exists(blob_id)

        ops.delete_category("documents")
        assert not vault.blob_exists(blob_id)
        assert db.conn.execute("SELECT COUNT(*) AS n FROM documents").fetchone()["n"] == 0

    def test_delete_invalid_category(self, ops: BulkOps) -> None:
        with pytest.raises(ValueError, match="Unknown category"):
            ops.delete_category("nonexistent")

    def test_delete_all_routes_to_reset(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_lab(db)
        _insert_stm(db)
        total = ops.delete_category("all")
        assert total >= 2


class TestResetAll:
    def test_empties_all_tables(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_lab(db)
        _insert_stm(db)
        _insert_ltm(db)
        _insert_hypothesis(db)
        _insert_search_index(db, "doc1", "lab_result")

        results = ops.reset_all()
        assert results["labs"] >= 1
        assert results["memory"] >= 2
        assert results["hypotheses"] >= 1

        # Verify tables are empty
        for table in ["observations", "memory_stm", "memory_ltm", "hypotheses", "search_index"]:
            count = db.conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()["n"]
            assert count == 0, f"{table} should be empty"

    def test_preserves_vault_meta(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_lab(db)
        ops.reset_all()
        row = db.conn.execute(
            "SELECT COUNT(*) AS n FROM vault_meta"
        ).fetchone()
        assert row["n"] > 0


class TestVacuum:
    def test_vacuum_succeeds(self, ops: BulkOps, db: HealthDB) -> None:
        _insert_lab(db)
        ops.delete_category("labs")
        ops.vacuum()  # Should not raise


class TestCleanupOrphanBlobs:
    def test_deletes_orphans(self, ops: BulkOps, vault: Vault, db: HealthDB) -> None:
        orphan_id = vault.store_blob(b"orphan data")
        assert vault.blob_exists(orphan_id)
        deleted = ops.cleanup_orphan_blobs()
        assert deleted == 1
        assert not vault.blob_exists(orphan_id)

    def test_preserves_referenced_blobs(
        self, ops: BulkOps, vault: Vault, db: HealthDB
    ) -> None:
        blob_id = vault.store_blob(b"referenced data")
        from healthbot.data.models import Document
        doc = Document(id="", source="test", sha256="abc", enc_blob_path=blob_id)
        db.insert_document(doc)

        ops.cleanup_orphan_blobs()
        assert vault.blob_exists(blob_id)

    def test_no_vault_returns_zero(self, db: HealthDB) -> None:
        db.run_migrations()
        ops = BulkOps(db, vault=None)
        assert ops.cleanup_orphan_blobs() == 0
