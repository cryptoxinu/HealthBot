"""Tests for Ollama PDF parsing fallback."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from healthbot.ingest.lab_pdf_parser import LabPdfParser


class TestOllamaResponse:
    """Test _parse_ollama_response with various inputs."""

    def _make_parser(self):
        safety = MagicMock()
        return LabPdfParser(safety)

    def test_valid_json_response(self) -> None:
        parser = self._make_parser()
        response = (
            "["
            '{"test_name": "Glucose", "value": 108, "unit": "mg/dL",'
            ' "reference_low": 70, "reference_high": 100, "flag": "H"},'
            '{"test_name": "Cholesterol", "value": 210, "unit": "mg/dL",'
            ' "reference_low": null, "reference_high": 200, "flag": "H"}'
            "]"
        )
        results, metadata = parser._parse_ollama_response(response, "blob1")
        assert len(results) == 2
        assert results[0].test_name == "Glucose"
        assert results[0].value == 108.0
        assert results[0].flag == "H"
        assert results[0].confidence == 0.85
        assert results[1].reference_low is None

    def test_empty_json(self) -> None:
        parser = self._make_parser()
        results, metadata = parser._parse_ollama_response("[]", "blob1")
        assert results == []

    def test_malformed_json(self) -> None:
        parser = self._make_parser()
        results, metadata = parser._parse_ollama_response("this is not json", "blob1")
        assert results == []

    def test_json_with_wrapper_text(self) -> None:
        parser = self._make_parser()
        response = (
            "Here are the results:\n"
            '[{"test_name": "TSH", "value": 2.5, "unit": "mIU/L", "flag": ""}]'
            "\nDone."
        )
        results, metadata = parser._parse_ollama_response(response, "blob1")
        assert len(results) == 1
        assert results[0].test_name == "TSH"

    def test_missing_value_skipped(self) -> None:
        parser = self._make_parser()
        response = '[{"test_name": "Test", "value": null}]'
        results, metadata = parser._parse_ollama_response(response, "blob1")
        assert results == []

    def test_missing_name_skipped(self) -> None:
        parser = self._make_parser()
        response = '[{"test_name": "", "value": 5.0}]'
        results, metadata = parser._parse_ollama_response(response, "blob1")
        assert results == []


class TestOllamaPrimaryTrigger:
    """Ollama should be tried as primary parser; regex as fallback."""

    def test_ollama_tried_first(self) -> None:
        parser = LabPdfParser(MagicMock())
        long_text = (
            "Glucose              95        mg/dL      70-100\n"
            "TSH                  2.5       uIU/mL    0.4-4.0\n"
        )
        with patch.object(parser, "_ollama_parse_pages", return_value=[]) as mock_ollama:
            with patch("healthbot.ingest.lab_pdf_parser.extract_text", return_value=long_text):
                with patch("healthbot.ingest.ocr_fallback.needs_ocr", return_value=False):
                    parser.parse_bytes(b"%PDF-test-data", blob_id="test")
        # Ollama should always be tried for maximum accuracy
        mock_ollama.assert_called_once()

    def test_regex_fallback_when_ollama_returns_empty(self) -> None:
        parser = LabPdfParser(MagicMock())
        lab_text = "Glucose                  108  mg/dL  70-100"
        with patch.object(parser, "_ollama_parse_pages", return_value=[]):
            with patch("healthbot.ingest.lab_pdf_parser.extract_text", return_value=lab_text):
                with patch("healthbot.ingest.ocr_fallback.needs_ocr", return_value=False):
                    results, text = parser.parse_bytes(b"%PDF-test-data", blob_id="test")
        # Regex fallback should find results
        assert len(results) >= 1


class TestSafeFloat:
    """Test the _safe_float helper."""

    def test_valid_float(self) -> None:
        assert LabPdfParser._safe_float(3.14) == 3.14

    def test_valid_int(self) -> None:
        assert LabPdfParser._safe_float(100) == 100.0

    def test_string_number(self) -> None:
        assert LabPdfParser._safe_float("5.7") == 5.7

    def test_none(self) -> None:
        assert LabPdfParser._safe_float(None) is None

    def test_invalid(self) -> None:
        assert LabPdfParser._safe_float("not a number") is None
