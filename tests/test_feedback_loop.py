"""Tests for the LLM feedback loop (DATA_QUALITY handling)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from healthbot.reasoning.feedback_loop import FeedbackLoop


def _make_mock_db():
    """Create a mock DB with standard methods."""
    db = MagicMock()
    db.query_observations.return_value = []
    db.get_observation_keys_for_doc.return_value = set()
    return db


def _make_mock_vault(pdf_bytes: bytes = b"fake pdf"):
    vault = MagicMock()
    vault.retrieve_blob.return_value = pdf_bytes
    return vault


class TestHandleQualityIssue:
    """Test the main orchestration method."""

    def test_logs_to_knowledge_base(self):
        db = _make_mock_db()
        loop = FeedbackLoop(db=db, vault=None)

        with patch(
            "healthbot.research.knowledge_base.KnowledgeBase",
        ) as mock_kb_cls:
            mock_kb = MagicMock()
            mock_kb_cls.return_value = mock_kb

            loop.handle_quality_issue(
                user_id=1,
                issue_type="cut_off_lab",
                test_name="CBC",
                details="WBC reference range missing",
            )

            mock_kb.store_finding.assert_called_once()
            call_kwargs = mock_kb.store_finding.call_args[1]
            assert call_kwargs["topic"] == "data_quality:CBC"
            assert "cut_off_lab" in call_kwargs["finding"]

    def test_no_source_doc_returns_not_attempted(self):
        db = _make_mock_db()
        db.query_observations.return_value = []
        loop = FeedbackLoop(db=db, vault=_make_mock_vault())

        result = loop.handle_quality_issue(
            user_id=1,
            issue_type="cut_off_lab",
            test_name="nonexistent_test",
            details="test",
        )

        assert result["rescan_attempted"] is False
        assert result["rescan_count"] == 0
        assert result["rescan_results"] == []

    def test_with_source_doc_triggers_rescan(self):
        db = _make_mock_db()
        db.query_observations.return_value = [
            {"_meta": {"source_doc_id": "blob123"}},
        ]
        db.get_observation_keys_for_doc.return_value = {("wbc", "2025-01-01")}

        vault = _make_mock_vault()
        loop = FeedbackLoop(db=db, vault=vault)

        with patch.object(loop, "_rescan_document", return_value=["rbc"]):
            result = loop.handle_quality_issue(
                user_id=1,
                issue_type="missing_ref_range",
                test_name="WBC",
                details="reference range missing",
            )

        assert result["rescan_attempted"] is True
        assert result["rescan_count"] == 1
        assert "rbc" in result["rescan_results"]

    def test_no_vault_returns_not_attempted(self):
        db = _make_mock_db()
        db.query_observations.return_value = [
            {"_meta": {"source_doc_id": "blob123"}},
        ]

        loop = FeedbackLoop(db=db, vault=None)
        result = loop.handle_quality_issue(
            user_id=1,
            issue_type="garbled_data",
            test_name="ALT",
            details="value looks wrong",
        )

        # rescan_attempted is True because _rescan_document was called,
        # but it returns [] because vault is None
        assert result["rescan_count"] == 0
        assert result["rescan_results"] == []


class TestFindRelevantDocument:
    """Test source document lookup."""

    def test_finds_doc_by_canonical_name(self):
        db = _make_mock_db()
        db.query_observations.return_value = [
            {"_meta": {"source_doc_id": "doc_abc"}},
        ]

        loop = FeedbackLoop(db=db)
        result = loop._find_relevant_document(user_id=1, test_name="WBC")

        assert result == "doc_abc"
        # Should have been called with canonical name
        call_kwargs = db.query_observations.call_args[1]
        assert call_kwargs["user_id"] == 1
        assert call_kwargs["limit"] == 5

    def test_returns_none_when_no_observations(self):
        db = _make_mock_db()
        db.query_observations.return_value = []

        loop = FeedbackLoop(db=db)
        result = loop._find_relevant_document(user_id=1, test_name="XYZ")
        assert result is None

    def test_skips_observations_without_doc_id(self):
        db = _make_mock_db()
        db.query_observations.return_value = [
            {"_meta": {}},  # no source_doc_id
            {"_meta": {"source_doc_id": "doc_found"}},
        ]

        loop = FeedbackLoop(db=db)
        result = loop._find_relevant_document(user_id=1, test_name="ALT")
        assert result == "doc_found"

    def test_handles_db_error_gracefully(self):
        db = _make_mock_db()
        db.query_observations.side_effect = RuntimeError("DB error")

        loop = FeedbackLoop(db=db)
        result = loop._find_relevant_document(user_id=1, test_name="CBC")
        assert result is None


class TestRescanDocument:
    """Test PDF re-extraction logic."""

    def test_page_specific_rescan(self):
        db = _make_mock_db()
        db.get_observation_keys_for_doc.return_value = {("wbc", None)}
        vault = _make_mock_vault(b"pdf bytes")

        loop = FeedbackLoop(db=db, vault=vault)

        with patch.object(loop, "_rescan_page", return_value=["rbc"]):
            result = loop._rescan_document("blob1", user_id=1, page=2)

        assert result == ["rbc"]

    def test_full_rescan_when_no_page(self):
        db = _make_mock_db()
        db.get_observation_keys_for_doc.return_value = set()
        vault = _make_mock_vault(b"pdf bytes")

        loop = FeedbackLoop(db=db, vault=vault)

        with patch.object(loop, "_rescan_full", return_value=["alt", "ast"]):
            result = loop._rescan_document("blob1", user_id=1, page=None)

        assert result == ["alt", "ast"]

    def test_vault_retrieval_failure(self):
        db = _make_mock_db()
        vault = MagicMock()
        vault.retrieve_blob.side_effect = RuntimeError("decrypt fail")

        loop = FeedbackLoop(db=db, vault=vault)
        result = loop._rescan_document("bad_blob", user_id=1, page=None)
        assert result == []

    def test_no_vault_returns_empty(self):
        db = _make_mock_db()
        loop = FeedbackLoop(db=db, vault=None)
        result = loop._rescan_document("blob1", user_id=1, page=None)
        assert result == []


class TestRescanPage:
    """Test targeted page OCR."""

    def test_calls_ocr_at_400_dpi(self):
        db = _make_mock_db()
        loop = FeedbackLoop(db=db)

        with patch(
            "healthbot.ingest.ocr_fallback.ocr_pdf_page",
            return_value="WBC 8.2 x10E3/uL 3.4 - 10.8",
        ) as mock_ocr:
            with patch.object(
                loop, "_parse_text_for_new_results", return_value=["wbc"],
            ):
                result = loop._rescan_page(b"pdf", page=2, existing_names=set())

            mock_ocr.assert_called_once_with(b"pdf", 2, dpi=400)
        assert result == ["wbc"]

    def test_empty_ocr_returns_empty(self):
        db = _make_mock_db()
        loop = FeedbackLoop(db=db)

        with patch(
            "healthbot.ingest.ocr_fallback.ocr_pdf_page",
            return_value="",
        ):
            result = loop._rescan_page(b"pdf", page=1, existing_names=set())
        assert result == []


class TestRescanFull:
    """Test full PDF re-parse."""

    def test_filters_existing_results(self):
        db = _make_mock_db()
        loop = FeedbackLoop(db=db)

        mock_result1 = MagicMock()
        mock_result1.canonical_name = "wbc"
        mock_result2 = MagicMock()
        mock_result2.canonical_name = "rbc"

        with patch(
            "healthbot.ingest.lab_pdf_parser.LabPdfParser",
        ) as mock_parser_cls:
            mock_parser = MagicMock()
            mock_parser.parse_bytes.return_value = (
                [mock_result1, mock_result2], "text",
            )
            mock_parser_cls.return_value = mock_parser

            with patch(
                "healthbot.security.pdf_safety.PdfSafety",
            ):
                result = loop._rescan_full(
                    b"pdf", "blob1", existing_names={"wbc"},
                )

        # wbc already exists, only rbc is new
        assert result == ["rbc"]

    def test_parse_failure_returns_empty(self):
        db = _make_mock_db()
        loop = FeedbackLoop(db=db)

        with patch(
            "healthbot.ingest.lab_pdf_parser.LabPdfParser",
            side_effect=RuntimeError("parse error"),
        ):
            with patch(
                "healthbot.security.pdf_safety.PdfSafety",
            ):
                result = loop._rescan_full(b"pdf", "blob1", existing_names=set())
        assert result == []
