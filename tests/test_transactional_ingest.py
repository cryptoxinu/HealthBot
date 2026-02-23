"""Tests for transactional ingestion and lab conflict resolution (Phase 1)."""
from __future__ import annotations

from contextlib import ExitStack
from datetime import date
from unittest.mock import patch

import pytest

from healthbot.data.models import LabResult
from healthbot.ingest.lab_pdf_parser import LabPdfParser
from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
from healthbot.reasoning.triage import TriageEngine
from healthbot.security.pdf_safety import PdfSafety


@pytest.fixture
def ingest_pipeline(config, key_manager, vault, db):
    """Full ingestion pipeline with real DB for transactional tests."""
    safety = PdfSafety(config)
    parser = LabPdfParser(safety)
    triage = TriageEngine()
    return TelegramPdfIngest(vault, db, parser, safety, triage)


# Minimal valid PDF bytes
MINIMAL_PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/MediaBox[0 0 612 792]/Parent 2 0 R>>endobj\n"
    b"xref\n0 4\n"
    b"0000000000 65535 f \n"
    b"0000000009 00000 n \n"
    b"0000000058 00000 n \n"
    b"0000000115 00000 n \n"
    b"trailer<</Size 4/Root 1 0 R>>\n"
    b"startxref\n183\n%%EOF"
)


def _make_lab(
    name: str = "Glucose",
    canonical: str = "glucose",
    value: float = 95,
    unit: str = "mg/dL",
    ref_low: float = 70.0,
    ref_high: float = 100.0,
    date_collected: date | None = date(2025, 6, 15),
) -> LabResult:
    """Create a LabResult for testing."""
    return LabResult(
        id="",
        test_name=name,
        canonical_name=canonical,
        value=value,
        unit=unit,
        reference_low=ref_low,
        reference_high=ref_high,
        date_collected=date_collected,
        confidence=0.92,
    )


def _run_ingest(pipeline, db, labs, extra_patches=None):
    """Run ingest() with Claude and parser mocked out."""
    with ExitStack() as stack:
        stack.enter_context(
            patch.object(pipeline, "_try_claude_extraction", return_value=labs),
        )
        stack.enter_context(
            patch.object(db, "get_user_demographics", return_value={}),
        )
        stack.enter_context(
            patch.object(pipeline._parser, "extract_text_and_tables",
                         return_value=("text", "md")),
        )
        stack.enter_context(
            patch.object(pipeline._parser, "_extract_date", return_value=None),
        )
        stack.enter_context(
            patch.object(pipeline._parser, "_extract_lab_name", return_value=""),
        )
        if extra_patches:
            for p in extra_patches:
                stack.enter_context(p)
        return pipeline.ingest(MINIMAL_PDF, user_id=1)


class TestTransactionalIngestion:
    """Test that ingest() uses DB transactions."""

    def test_all_writes_committed_together(self, ingest_pipeline, db):
        """All DB writes (doc + observations + search index) commit atomically."""
        labs = [
            _make_lab("Glucose", "glucose", 95),
            _make_lab("WBC", "wbc", 5.0, "K/uL", 3.4, 10.8),
        ]
        result = _run_ingest(ingest_pipeline, db, labs)

        assert result.success
        assert len(result.lab_results) == 2

        obs = db.query_observations(record_type="lab_result", user_id=1)
        assert len(obs) == 2

    def test_rollback_on_insert_failure(self, ingest_pipeline, db):
        """If an observation insert fails mid-batch, all writes roll back."""
        labs = [
            _make_lab("Glucose", "glucose", 95),
            _make_lab("WBC", "wbc", 5.0, "K/uL", 3.4, 10.8),
        ]
        original_insert = db.insert_observation
        call_count = [0]

        def failing_insert(obs, user_id=0, age_at_collection=None, commit=True):
            call_count[0] += 1
            if call_count[0] == 2:
                raise RuntimeError("Simulated DB error")
            return original_insert(
                obs, user_id=user_id,
                age_at_collection=age_at_collection, commit=commit,
            )

        result = _run_ingest(ingest_pipeline, db, labs, extra_patches=[
            patch.object(db, "insert_observation", side_effect=failing_insert),
        ])

        assert not result.success
        assert result.warnings

        # Rolled back — nothing in DB
        obs = db.query_observations(record_type="lab_result", user_id=1)
        assert len(obs) == 0
        docs = db.list_documents(user_id=1)
        assert len(docs) == 0

    def test_blob_cleaned_on_rollback(self, ingest_pipeline, db):
        """On transaction rollback for new upload, blob cleanup is attempted."""
        labs = [_make_lab()]
        result = _run_ingest(ingest_pipeline, db, labs, extra_patches=[
            patch.object(db, "insert_document", side_effect=RuntimeError("DB error")),
        ])
        assert not result.success


class TestLabConflictResolution:
    """Test corrected lab report handling during rescan."""

    def test_corrected_value_updates_existing(self, ingest_pipeline, db):
        """Rescan with different value -> UPDATE, not DROP."""
        original_labs = [_make_lab("Glucose", "glucose", 95)]
        result1 = _run_ingest(ingest_pipeline, db, original_labs)

        assert result1.success
        obs = db.query_observations(canonical_name="glucose", user_id=1)
        assert len(obs) == 1
        assert obs[0]["value"] == 95

        # Rescan with corrected value
        corrected_labs = [_make_lab("Glucose", "glucose", 98)]
        result2 = _run_ingest(ingest_pipeline, db, corrected_labs)

        assert result2.success
        assert result2.is_rescan
        assert result2.rescan_new == 1

        obs = db.query_observations(canonical_name="glucose", user_id=1)
        assert len(obs) == 1
        assert obs[0]["value"] == 98

    def test_identical_rescan_no_updates(self, ingest_pipeline, db):
        """Rescan with same values -> no updates, standard dedup."""
        labs = [_make_lab("Glucose", "glucose", 95)]
        result1 = _run_ingest(ingest_pipeline, db, labs)
        assert result1.success

        result2 = _run_ingest(ingest_pipeline, db, labs)
        assert result2.success
        assert result2.is_rescan
        assert result2.rescan_existing == 1
        assert result2.rescan_new == 0

        obs = db.query_observations(canonical_name="glucose", user_id=1)
        assert len(obs) == 1

    def test_rescan_new_plus_corrected(self, ingest_pipeline, db):
        """Rescan with one corrected + one new result."""
        original = [_make_lab("Glucose", "glucose", 95)]
        result1 = _run_ingest(ingest_pipeline, db, original)
        assert result1.success

        updated = [
            _make_lab("Glucose", "glucose", 98),
            _make_lab("WBC", "wbc", 5.0, "K/uL", 3.4, 10.8),
        ]
        result2 = _run_ingest(ingest_pipeline, db, updated)

        assert result2.success
        assert result2.is_rescan
        assert result2.rescan_new == 2

        glucose = db.query_observations(canonical_name="glucose", user_id=1)
        assert len(glucose) == 1
        assert glucose[0]["value"] == 98

        wbc = db.query_observations(canonical_name="wbc", user_id=1)
        assert len(wbc) == 1
        assert wbc[0]["value"] == 5.0


class TestDbCommitParam:
    """Test that commit=False parameter works correctly."""

    def test_insert_observation_no_commit(self, db):
        """insert_observation(commit=False) does not auto-commit."""
        lab = _make_lab()
        lab.id = "test-obs-1"

        db.conn.execute("BEGIN")
        db.insert_observation(lab, user_id=1, commit=False)
        db.conn.rollback()

        obs = db.query_observations(canonical_name="glucose", user_id=1)
        assert len(obs) == 0

    def test_insert_observation_with_commit(self, db):
        """insert_observation(commit=True) persists."""
        lab = _make_lab()
        lab.id = "test-obs-2"

        db.insert_observation(lab, user_id=1, commit=True)

        obs = db.query_observations(canonical_name="glucose", user_id=1)
        assert len(obs) == 1

    def test_upsert_search_text_no_commit(self, db):
        """upsert_search_text(commit=False) does not auto-commit."""
        db.conn.execute("BEGIN")
        db.upsert_search_text(
            doc_id="test-1", record_type="lab_result",
            date_effective="2025-06-15", text="Glucose 95 mg/dL",
            commit=False,
        )
        db.conn.rollback()

        texts = db.get_all_search_texts()
        assert len(texts) == 0

    def test_update_observation_value(self, db):
        """update_observation_value() updates encrypted data."""
        lab = _make_lab("Glucose", "glucose", 95)
        lab.id = "test-upd-1"
        db.insert_observation(lab, user_id=1)

        updated_lab = _make_lab("Glucose", "glucose", 98)
        updated_lab.id = "test-upd-1"
        db.update_observation_value("test-upd-1", updated_lab, user_id=1)

        obs = db.query_observations(canonical_name="glucose", user_id=1)
        assert len(obs) == 1
        assert obs[0]["value"] == 98

    def test_get_observation_details_for_doc(self, db):
        """get_observation_details_for_doc() returns values for comparison."""
        lab = _make_lab("Glucose", "glucose", 95)
        lab.id = "test-det-1"
        lab.source_blob_id = "blob-123"
        db.insert_observation(lab, user_id=1)

        details = db.get_observation_details_for_doc("blob-123")
        assert ("glucose", "2025-06-15") in details
        assert details[("glucose", "2025-06-15")]["value"] == 95
        assert details[("glucose", "2025-06-15")]["obs_id"] == "test-det-1"
