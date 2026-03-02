"""Tests for Telegram PDF ingestion pipeline."""
from __future__ import annotations

import pytest

from healthbot.data.db import HealthDB
from healthbot.ingest.lab_pdf_parser import LabPdfParser
from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
from healthbot.reasoning.triage import TriageEngine
from healthbot.security.key_manager import KeyManager, LockedError
from healthbot.security.pdf_safety import PdfSafety, PdfSafetyError
from healthbot.security.vault import Vault

# Minimal valid PDF bytes (no lab data, but valid structure)
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


@pytest.fixture
def ingest_pipeline(config, key_manager, vault, db):
    """Full ingestion pipeline."""
    safety = PdfSafety(config)
    parser = LabPdfParser(safety)
    triage = TriageEngine()
    return TelegramPdfIngest(vault, db, parser, safety, triage)


class TestIngestSecurity:
    """Test security aspects of PDF ingestion."""

    def test_rejects_while_locked(self, config, tmp_vault):
        """Ingestion must fail when vault is locked."""
        km = KeyManager(config)
        km.setup("testpass")
        km.lock()  # Lock it

        with pytest.raises(LockedError):
            vault = Vault(config.blobs_dir, km)
            safety = PdfSafety(config)
            parser = LabPdfParser(safety)
            triage = TriageEngine()
            db = HealthDB(config, km)
            pipeline = TelegramPdfIngest(vault, db, parser, safety, triage)
            pipeline.ingest(MINIMAL_PDF)

    def test_rejects_oversized(self, config, key_manager):
        """Oversized PDFs must be rejected."""
        config.max_pdf_size_bytes = 100  # Very small limit
        safety = PdfSafety(config)
        large = b"%PDF-" + b"A" * 200
        with pytest.raises(PdfSafetyError, match="too large"):
            safety.validate_bytes(large)

    def test_rejects_encrypted_pdf(self, config, key_manager):
        """Encrypted PDFs must be rejected."""
        safety = PdfSafety(config)
        data = b"%PDF-1.4\n/Encrypt some_dict"
        with pytest.raises(PdfSafetyError, match="Encrypted"):
            safety.validate_bytes(data)

    def test_rejects_javascript_pdf(self, config, key_manager):
        """PDFs with JavaScript must be rejected."""
        safety = PdfSafety(config)
        data = b"%PDF-1.4\n/JavaScript (alert('xss'))"
        with pytest.raises(PdfSafetyError, match="dangerous"):
            safety.validate_bytes(data)

    def test_rejects_launch_action(self, config, key_manager):
        """PDFs with Launch actions must be rejected."""
        safety = PdfSafety(config)
        data = b"%PDF-1.4\n/Launch cmd"
        with pytest.raises(PdfSafetyError, match="dangerous"):
            safety.validate_bytes(data)

    def test_encrypted_blob_not_pdf(self, vault):
        """Encrypted blob must NOT begin with %PDF-."""
        blob_id = vault.store_blob(MINIMAL_PDF)
        enc_path = vault._blobs_dir / f"{blob_id}.enc"
        enc_data = enc_path.read_bytes()
        assert not enc_data.startswith(b"%PDF-"), (
            "Encrypted blob must not start with %PDF-"
        )


@pytest.mark.slow
class TestIngestPipeline:
    """Test the full ingestion pipeline."""

    def test_ingest_minimal_pdf(self, ingest_pipeline):
        """Valid PDF should be ingested without errors."""
        result = ingest_pipeline.ingest(MINIMAL_PDF, filename="test.pdf")
        assert result.success
        assert result.blob_id
        assert result.doc_id

    def test_ingest_non_pdf_fails(self, ingest_pipeline):
        """Non-PDF data should fail."""
        result = ingest_pipeline.ingest(b"This is not a PDF", filename="test.txt")
        assert not result.success
        assert result.warnings


@pytest.mark.slow
class TestRescan:
    """Re-uploading same PDF triggers rescan instead of rejection."""

    def test_rescan_no_new_results(self, ingest_pipeline):
        """Re-upload same PDF: is_rescan=True, no new results."""
        result1 = ingest_pipeline.ingest(MINIMAL_PDF, filename="test.pdf")
        assert result1.success
        assert not result1.is_rescan

        result2 = ingest_pipeline.ingest(MINIMAL_PDF, filename="test.pdf")
        assert result2.success
        assert result2.is_rescan
        assert result2.rescan_new == 0
        assert not result2.is_duplicate

    def test_rescan_preserves_doc_id(self, ingest_pipeline):
        """Rescan should reference the original document, not create a new one."""
        result1 = ingest_pipeline.ingest(MINIMAL_PDF, filename="test.pdf")
        result2 = ingest_pipeline.ingest(MINIMAL_PDF, filename="test.pdf")
        assert result2.doc_id == result1.doc_id

    def test_rescan_fields_default(self):
        """IngestResult rescan fields default correctly."""
        from healthbot.ingest.telegram_pdf_ingest import IngestResult

        r = IngestResult()
        assert r.is_rescan is False
        assert r.rescan_new == 0
        assert r.rescan_existing == 0


@pytest.mark.slow
class TestDbRescanMethods:
    """Test DB methods for rescan support."""

    def test_get_observation_keys_empty(self, db):
        """No observations returns empty set."""
        keys = db.get_observation_keys_for_doc("nonexistent")
        assert keys == set()

    def test_document_exists_returns_blob_path(self, ingest_pipeline, db):
        """document_exists_by_sha256 should return enc_blob_path."""
        import hashlib

        result = ingest_pipeline.ingest(MINIMAL_PDF, filename="test.pdf")
        sha = hashlib.sha256(MINIMAL_PDF).hexdigest()
        existing = db.document_exists_by_sha256(sha)
        assert existing is not None
        assert "enc_blob_path" in existing
        assert existing["enc_blob_path"] == result.blob_id


class TestFilterValidResults:
    """Tests for _filter_valid_results qualitative handling."""

    @staticmethod
    def _make_lab_result(
        name, value, canonical="", confidence=0.85,
        ref_low=None, ref_high=None, reference_text="",
    ):
        import uuid

        from healthbot.data.models import LabResult

        return LabResult(
            id=uuid.uuid4().hex,
            test_name=name,
            canonical_name=canonical,
            value=value,
            confidence=confidence,
            reference_low=ref_low,
            reference_high=ref_high,
            reference_text=reference_text,
        )

    def test_claude_high_confidence_string_passes(self):
        """Path 3a: Claude-extracted string value with confidence >= 0.90 passes."""
        lab = self._make_lab_result(
            "SOME NOVEL TEST", "Detected",
            canonical="some_novel_test", confidence=0.92,
        )
        result = TelegramPdfIngest._filter_valid_results([lab])
        assert len(result) == 1

    def test_ollama_low_confidence_unknown_string_dropped(self):
        """Ollama-extracted unknown string (confidence 0.85) without hardcoded match is dropped."""
        lab = self._make_lab_result(
            "SOME NOVEL TEST", "Some Unknown Result",
            canonical="some_novel_test", confidence=0.85,
        )
        result = TelegramPdfIngest._filter_valid_results([lab])
        assert len(result) == 0

    def test_claude_high_confidence_empty_string_dropped(self):
        """Claude-extracted empty string value is dropped even with high confidence."""
        lab = self._make_lab_result(
            "BAD PARSE", "   ",
            canonical="bad_parse", confidence=0.92,
        )
        result = TelegramPdfIngest._filter_valid_results([lab])
        assert len(result) == 0

    def test_known_qualitative_test_passes_at_low_confidence(self):
        """Path 3b: Known qualitative test passes even with low confidence."""
        lab = self._make_lab_result(
            "JAK2 V617F", "Detected",
            canonical="jak2_v617f_mutation", confidence=0.85,
        )
        result = TelegramPdfIngest._filter_valid_results([lab])
        assert len(result) == 1

    def test_reference_text_passes_at_low_confidence(self):
        """Path 3b: Result with reference_text passes even with low confidence."""
        lab = self._make_lab_result(
            "NOVEL SCREEN", "Positive",
            canonical="novel_screen", confidence=0.85,
            reference_text="Negative",
        )
        result = TelegramPdfIngest._filter_valid_results([lab])
        assert len(result) == 1

    def test_numeric_still_works(self):
        """Numeric values with ref ranges still pass."""
        lab = self._make_lab_result(
            "Glucose", 95.0, canonical="glucose",
            ref_low=70, ref_high=100,
        )
        result = TelegramPdfIngest._filter_valid_results([lab])
        assert len(result) == 1
