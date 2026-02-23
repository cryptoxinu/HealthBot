"""Tests for Claude CLI-based lab extraction in the ingestion pipeline."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from healthbot.data.models import LabResult
from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest


def _make_pipeline() -> TelegramPdfIngest:
    """Create a TelegramPdfIngest with mocked dependencies."""
    vault = MagicMock()
    db = MagicMock()
    parser = MagicMock()
    safety = MagicMock()
    triage = MagicMock()
    config = MagicMock()
    config.ollama_url = None
    return TelegramPdfIngest(vault, db, parser, safety, triage, config=config)


def _make_anon_mock() -> MagicMock:
    """Create an anonymizer mock with unsafe=True (Python 3.14 compat)."""
    mock = MagicMock(unsafe=True)
    mock.anonymize.return_value = ("redacted text", True)
    mock.assert_safe.return_value = None
    return mock


SAMPLE_CLAUDE_JSON = json.dumps([
    {
        "test_name": "Glucose",
        "value": 95,
        "unit": "mg/dL",
        "reference_low": 70,
        "reference_high": 100,
        "flag": "",
    },
    {
        "test_name": "Hemoglobin A1c",
        "value": 5.7,
        "unit": "%",
        "reference_low": 4.0,
        "reference_high": 5.6,
        "flag": "H",
    },
    {
        "_type": "metadata",
        "collection_date": "2025-06-15",
        "lab_name": "LabCorp",
    },
])


class TestClaudeExtraction:
    """Test _try_claude_extraction method."""

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    @patch("healthbot.llm.claude_client.ClaudeClient")
    def test_happy_path(self, mock_claude_cls, mock_anon_builder):
        """Claude returns valid JSON → LabResult objects with 0.92 confidence."""
        pipeline = _make_pipeline()

        mock_client = MagicMock()
        mock_client.is_available.return_value = True
        mock_client.send.return_value = SAMPLE_CLAUDE_JSON
        mock_claude_cls.return_value = mock_client

        mock_anon_builder.return_value = _make_anon_mock()

        results = [
            LabResult(id="1", test_name="Glucose", canonical_name="glucose",
                      value=95, unit="mg/dL", confidence=0.85),
            LabResult(id="2", test_name="Hemoglobin A1c", canonical_name="hba1c",
                      value=5.7, unit="%", confidence=0.85, flag="H"),
        ]
        metadata = {"collection_date": date(2025, 6, 15), "lab_name": "LabCorp"}
        pipeline._parser._parse_ollama_response.return_value = (results, metadata)
        pipeline._parser._validate_date.return_value = True

        labs = pipeline._try_claude_extraction(
            "Glucose 95 mg/dL", "| Test | Value |", "blob123",
        )

        assert labs is not None
        assert len(labs) == 2
        for lab in labs:
            assert lab.confidence == 0.92
        assert labs[0].date_collected == date(2025, 6, 15)
        assert labs[0].lab_name == "LabCorp"
        mock_client.send.assert_called_once()

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    @patch("healthbot.llm.claude_client.ClaudeClient")
    def test_claude_unavailable_returns_none(self, mock_claude_cls, mock_anon_builder):
        """Claude CLI not installed → returns None (triggers fallback)."""
        pipeline = _make_pipeline()
        mock_client = MagicMock()
        mock_client.is_available.return_value = False
        mock_claude_cls.return_value = mock_client

        result = pipeline._try_claude_extraction("some text", "", "blob1")
        assert result is None

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    @patch("healthbot.llm.claude_client.ClaudeClient")
    def test_invalid_json_returns_none(self, mock_claude_cls, mock_anon_builder):
        """Claude returns garbage → returns None (triggers fallback)."""
        pipeline = _make_pipeline()
        mock_client = MagicMock()
        mock_client.is_available.return_value = True
        mock_client.send.return_value = "Sorry, I can't parse this."
        mock_claude_cls.return_value = mock_client

        mock_anon_builder.return_value = _make_anon_mock()
        pipeline._parser._parse_ollama_response.return_value = ([], {})

        result = pipeline._try_claude_extraction("some text", "", "blob1")
        assert result is None

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    @patch("healthbot.llm.claude_client.ClaudeClient")
    def test_claude_timeout_returns_none(self, mock_claude_cls, mock_anon_builder):
        """Claude CLI times out → returns None."""
        import subprocess

        pipeline = _make_pipeline()
        mock_client = MagicMock()
        mock_client.is_available.return_value = True
        mock_client.send.side_effect = subprocess.TimeoutExpired("claude", 120)
        mock_claude_cls.return_value = mock_client

        mock_anon_builder.return_value = _make_anon_mock()

        result = pipeline._try_claude_extraction("some text", "", "blob1")
        assert result is None

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    def test_pii_gate_blocks_send(self, mock_anon_builder):
        """assert_safe raises on all 3 attempts → nothing sent to Claude."""
        from healthbot.llm.anonymizer import AnonymizationError

        pipeline = _make_pipeline()
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("still has PII", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("PII detected")
        mock_anon_builder.return_value = mock_anon

        long_text = "Patient: John Smith labs with some padding text to pass length check"
        result = pipeline._try_claude_extraction(long_text, "", "blob1")
        assert result is None
        assert mock_anon.anonymize.call_count == 3
        assert mock_anon.assert_safe.call_count == 3

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    @patch("healthbot.llm.claude_client.ClaudeClient")
    def test_pii_retry_succeeds_on_second_pass(self, mock_claude_cls, mock_anon_builder):
        """First assert_safe fails, second pass succeeds → Claude called."""
        from healthbot.llm.anonymizer import AnonymizationError

        pipeline = _make_pipeline()
        mock_client = MagicMock()
        mock_client.is_available.return_value = True
        mock_client.send.return_value = SAMPLE_CLAUDE_JSON
        mock_claude_cls.return_value = mock_client

        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.side_effect = [
            ("still has PII", True),
            ("fully clean text", True),
        ]
        mock_anon.assert_safe.side_effect = [
            AnonymizationError("PII detected"),
            None,
        ]
        mock_anon_builder.return_value = mock_anon

        results = [LabResult(id="1", test_name="WBC", value=5.0, unit="K/uL")]
        pipeline._parser._parse_ollama_response.return_value = (results, {})

        labs = pipeline._try_claude_extraction(
            "WBC 5.0 K/uL ref 3.4-10.8 normal range test data padding",
            "", "blob1",
        )
        assert labs is not None
        assert mock_anon.anonymize.call_count == 2
        assert mock_anon.assert_safe.call_count == 2
        mock_client.send.assert_called_once()

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    @patch("healthbot.llm.claude_client.ClaudeClient")
    def test_empty_text_skipped(self, mock_claude_cls, mock_anon_builder):
        """Empty/short text → returns None without calling Claude."""
        pipeline = _make_pipeline()
        mock_client = MagicMock()
        mock_client.is_available.return_value = True
        mock_claude_cls.return_value = mock_client

        result = pipeline._try_claude_extraction("", "", "blob1")
        assert result is None
        # send() should never have been called (text too short)
        mock_client.send.assert_not_called()

    @patch("healthbot.ingest.telegram_pdf_ingest.TelegramPdfIngest._build_anonymizer")
    @patch("healthbot.llm.claude_client.ClaudeClient")
    def test_date_validation_with_demographics(
        self, mock_claude_cls, mock_anon_builder,
    ):
        """Claude's collection date is validated against demographics."""
        pipeline = _make_pipeline()
        mock_client = MagicMock()
        mock_client.is_available.return_value = True
        mock_client.send.return_value = SAMPLE_CLAUDE_JSON
        mock_claude_cls.return_value = mock_client

        mock_anon_builder.return_value = _make_anon_mock()

        results = [LabResult(id="1", test_name="WBC", value=5.0, unit="K/uL")]
        metadata = {"collection_date": date(2025, 1, 1)}
        pipeline._parser._parse_ollama_response.return_value = (results, metadata)
        pipeline._parser._validate_date.return_value = False
        # Regex fallback also returns None (no date in text either)
        pipeline._parser._extract_date.return_value = None

        labs = pipeline._try_claude_extraction(
            "WBC 5.0 K/uL ref 3.4-10.8 normal range", "", "blob1",
            demographics={"dob": "1990-01-01"},
        )
        assert labs is not None
        assert labs[0].date_collected is None


@pytest.mark.slow
class TestPdfRedaction:
    """Test _redact_pdf method."""

    def test_redacts_pii_from_pdf(self):
        """PII text in PDF is black-boxed; extracted text is clean."""
        import fitz  # PyMuPDF

        # Create a simple PDF with PII and lab data
        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "Patient: John Smith", fontsize=12)
        page.insert_text((72, 100), "SSN: 123-45-6789", fontsize=12)
        page.insert_text((72, 128), "WBC 5.0 K/uL ref 3.4-10.8", fontsize=12)
        page.insert_text((72, 156), "Glucose 95 mg/dL ref 70-100", fontsize=12)
        pdf_bytes = doc.tobytes()
        doc.close()

        pipeline = _make_pipeline()
        redacted_bytes, count = pipeline._redact_pdf(pdf_bytes)

        assert count > 0  # Should have redacted PII

        # Extract text from redacted PDF to verify
        doc2 = fitz.open(stream=redacted_bytes, filetype="pdf")
        text = doc2[0].get_text()
        doc2.close()

        # Lab values should be preserved
        assert "WBC" in text or "5.0" in text
        assert "Glucose" in text or "95" in text
        # PII should be gone (redacted from the PDF)
        assert "John Smith" not in text
        assert "123-45-6789" not in text

    def test_no_pii_pdf_unchanged(self):
        """PDF with only lab data gets 0 redactions."""
        import fitz

        doc = fitz.open()
        page = doc.new_page()
        page.insert_text((72, 72), "WBC 5.0 K/uL ref 3.4-10.8", fontsize=12)
        pdf_bytes = doc.tobytes()
        doc.close()

        pipeline = _make_pipeline()
        _, count = pipeline._redact_pdf(pdf_bytes)
        assert count == 0


class TestParseClaudeResponse:
    """Test _parse_claude_response wrapper."""

    def test_confidence_override(self):
        """Confidence is set to 0.92 for all Claude results."""
        pipeline = _make_pipeline()
        results = [
            LabResult(id="1", test_name="Glucose", value=95, confidence=0.85),
            LabResult(id="2", test_name="WBC", value=5.0, confidence=0.85),
        ]
        pipeline._parser._parse_ollama_response.return_value = (results, {})

        labs, _ = pipeline._parse_claude_response("some json", "blob1")
        assert all(r.confidence == 0.92 for r in labs)

    def test_empty_response(self):
        """Empty parse result passes through."""
        pipeline = _make_pipeline()
        pipeline._parser._parse_ollama_response.return_value = ([], {})

        labs, _ = pipeline._parse_claude_response("[]", "blob1")
        assert labs == []

    def test_strips_markdown_fences(self):
        """JSON wrapped in ```json fences is extracted correctly."""
        pipeline = _make_pipeline()
        results = [
            LabResult(id="1", test_name="Glucose", value=95, confidence=0.85),
        ]
        pipeline._parser._parse_ollama_response.return_value = (results, {})

        fenced = '```json\n[{"test_name": "Glucose", "value": 95}]\n```'
        labs, _ = pipeline._parse_claude_response(fenced, "blob1")
        assert len(labs) == 1
        assert labs[0].confidence == 0.92
        # Verify the cleaned text was passed (no fences)
        call_args = pipeline._parser._parse_ollama_response.call_args[0][0]
        assert "```" not in call_args

    def test_strips_preamble_text(self):
        """Preamble text before JSON array is stripped."""
        pipeline = _make_pipeline()
        results = [
            LabResult(id="1", test_name="WBC", value=5.0, confidence=0.85),
        ]
        pipeline._parser._parse_ollama_response.return_value = (results, {})

        response = 'Here are the results:\n\n[{"test_name": "WBC", "value": 5.0}]'
        labs, _ = pipeline._parse_claude_response(response, "blob1")
        assert len(labs) == 1
        # The text passed to parser should start with [
        call_args = pipeline._parser._parse_ollama_response.call_args[0][0]
        assert call_args.startswith("[")


class TestIngestWithClaudeFallback:
    """Test that ingest() tries Claude first then falls back."""

    def test_claude_success_skips_parse_bytes(self):
        """When Claude extraction works, parse_bytes is NOT called."""
        pipeline = _make_pipeline()
        pipeline._parser.extract_text_and_tables.return_value = ("text", "md")

        # Claude returns a valid result (with canonical_name + refs to pass filter)
        lab = LabResult(
            id="1", test_name="Glucose", canonical_name="glucose",
            value=95, unit="mg/dL", confidence=0.92,
            reference_low=70.0, reference_high=100.0,
        )
        with patch.object(pipeline, "_try_claude_extraction", return_value=[lab]):
            pipeline._parser._extract_date.return_value = None
            pipeline._parser._extract_lab_name.return_value = ""
            pipeline._db.get_user_demographics.return_value = {}
            pipeline._db.document_exists_by_sha256.return_value = None
            pipeline._triage.classify_batch.return_value = None
            pipeline._triage.get_triage_summary.return_value = ""

            result = pipeline.ingest(b"fake pdf", user_id=1)

        pipeline._parser.parse_bytes.assert_not_called()
        assert result.success
        assert len(result.lab_results) == 1
        assert result.lab_results[0].canonical_name == "glucose"

    def test_claude_failure_falls_back(self):
        """When Claude returns None, parse_bytes is called as fallback."""
        pipeline = _make_pipeline()
        pipeline._parser.extract_text_and_tables.return_value = ("text", "md")

        with patch.object(pipeline, "_try_claude_extraction", return_value=None):
            lab = LabResult(
                id="1", test_name="WBC", canonical_name="wbc",
                value=5.0, unit="K/uL", confidence=0.60,
                reference_low=3.4, reference_high=10.8,
            )
            pipeline._parser.parse_bytes.return_value = ([lab], "text")
            pipeline._parser._extract_date.return_value = None
            pipeline._parser._extract_lab_name.return_value = ""
            pipeline._db.get_user_demographics.return_value = {}
            pipeline._db.document_exists_by_sha256.return_value = None
            pipeline._triage.classify_batch.return_value = None
            pipeline._triage.get_triage_summary.return_value = ""

            result = pipeline.ingest(b"fake pdf", user_id=1)

        pipeline._parser.parse_bytes.assert_called_once()
        assert result.success


class TestCrossDocumentDedup:
    """Test cross-document dedup (different PDFs, same labs)."""

    def test_duplicate_labs_skipped(self):
        """Labs already in DB from another PDF are skipped."""
        pipeline = _make_pipeline()
        pipeline._parser.extract_text_and_tables.return_value = ("text", "md")

        lab1 = LabResult(
            id="1", test_name="Glucose", canonical_name="glucose",
            value=95, unit="mg/dL", confidence=0.92,
            reference_low=70.0, reference_high=100.0,
            date_collected=date(2025, 6, 15),
        )
        lab2 = LabResult(
            id="2", test_name="WBC", canonical_name="wbc",
            value=5.0, unit="K/uL", confidence=0.92,
            reference_low=3.4, reference_high=10.8,
            date_collected=date(2025, 6, 15),
        )
        with patch.object(
            pipeline, "_try_claude_extraction", return_value=[lab1, lab2],
        ):
            pipeline._parser._extract_date.return_value = None
            pipeline._parser._extract_lab_name.return_value = ""
            pipeline._db.get_user_demographics.return_value = {}
            pipeline._db.document_exists_by_sha256.return_value = None
            pipeline._triage.classify_batch.return_value = None
            pipeline._triage.get_triage_summary.return_value = ""

            # Simulate glucose already existing from a previous PDF
            pipeline._db.get_existing_observation_keys.return_value = {
                ("glucose", "2025-06-15"),
            }

            result = pipeline.ingest(b"fake pdf", user_id=1)

        assert result.success
        assert len(result.lab_results) == 1
        assert result.lab_results[0].canonical_name == "wbc"
        assert result.cross_doc_dupes == 1

    def test_all_duplicates_zero_new(self):
        """When ALL labs are duplicates, lab_results is empty."""
        pipeline = _make_pipeline()
        pipeline._parser.extract_text_and_tables.return_value = ("text", "md")

        lab = LabResult(
            id="1", test_name="Glucose", canonical_name="glucose",
            value=95, unit="mg/dL", confidence=0.92,
            reference_low=70.0, reference_high=100.0,
            date_collected=date(2025, 6, 15),
        )
        with patch.object(
            pipeline, "_try_claude_extraction", return_value=[lab],
        ):
            pipeline._parser._extract_date.return_value = None
            pipeline._parser._extract_lab_name.return_value = ""
            pipeline._db.get_user_demographics.return_value = {}
            pipeline._db.document_exists_by_sha256.return_value = None
            pipeline._triage.classify_batch.return_value = None
            pipeline._triage.get_triage_summary.return_value = ""

            pipeline._db.get_existing_observation_keys.return_value = {
                ("glucose", "2025-06-15"),
            }

            result = pipeline.ingest(b"fake pdf", user_id=1)

        assert result.success
        assert len(result.lab_results) == 0
        assert result.cross_doc_dupes == 1

    def test_undated_labs_not_deduped_cross_doc(self):
        """Labs without dates are NOT deduped across documents."""
        pipeline = _make_pipeline()
        pipeline._parser.extract_text_and_tables.return_value = ("text", "md")

        lab = LabResult(
            id="1", test_name="Glucose", canonical_name="glucose",
            value=95, unit="mg/dL", confidence=0.92,
            reference_low=70.0, reference_high=100.0,
            date_collected=None,  # no date
        )
        with patch.object(
            pipeline, "_try_claude_extraction", return_value=[lab],
        ):
            pipeline._parser._extract_date.return_value = None
            pipeline._parser._extract_lab_name.return_value = ""
            pipeline._db.get_user_demographics.return_value = {}
            pipeline._db.document_exists_by_sha256.return_value = None
            pipeline._triage.classify_batch.return_value = None
            pipeline._triage.get_triage_summary.return_value = ""

            # Even if glucose/None exists, undated labs are kept
            pipeline._db.get_existing_observation_keys.return_value = {
                ("glucose", None),
            }

            result = pipeline.ingest(b"fake pdf", user_id=1)

        assert result.success
        assert len(result.lab_results) == 1  # kept — can't dedup without date
        assert result.cross_doc_dupes == 0

    def test_no_dedup_on_rescan(self):
        """Rescan path uses its own dedup — cross-doc dedup is skipped."""
        pipeline = _make_pipeline()
        pipeline._parser.extract_text_and_tables.return_value = ("text", "md")

        lab = LabResult(
            id="1", test_name="Glucose", canonical_name="glucose",
            value=95, unit="mg/dL", confidence=0.92,
            reference_low=70.0, reference_high=100.0,
            date_collected=date(2025, 6, 15),
        )
        with patch.object(
            pipeline, "_try_claude_extraction", return_value=[lab],
        ):
            pipeline._parser._extract_date.return_value = None
            pipeline._parser._extract_lab_name.return_value = ""
            pipeline._db.get_user_demographics.return_value = {}
            # Rescan: same SHA256 exists
            pipeline._db.document_exists_by_sha256.return_value = {
                "doc_id": "doc1", "enc_blob_path": "blob1",
            }
            pipeline._db.get_observation_details_for_doc.return_value = {}
            pipeline._triage.classify_batch.return_value = None
            pipeline._triage.get_triage_summary.return_value = ""

            result = pipeline.ingest(b"fake pdf", user_id=1)

        assert result.is_rescan
        # get_existing_observation_keys should NOT be called for rescans
        pipeline._db.get_existing_observation_keys.assert_not_called()
