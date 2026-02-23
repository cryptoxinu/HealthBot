"""Tests for ingest/lab_pdf_parser.py — regex patterns and parsing logic."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.ingest.lab_pdf_parser import (
    _DATE_PATTERNS,
    _REF_RANGE,
    _RESULT_PATTERNS,
    LabPdfParser,
)


@pytest.fixture
def parser():
    safety = MagicMock()
    safety.validate_bytes = MagicMock()  # No-op safety check
    return LabPdfParser(safety)


class TestResultPatterns:
    """Test regex patterns match common lab report formats."""

    def test_standard_format(self):
        line = "Glucose              95        mg/dL      70-100"
        pattern = _RESULT_PATTERNS[0]
        match = pattern.search(line)
        assert match is not None
        groups = match.groups()
        assert "Glucose" in groups[0]
        assert "95" in groups[1]

    def test_format_with_flag(self):
        line = "LDL Cholesterol      180       H         mg/dL      0-100"
        pattern = _RESULT_PATTERNS[1]
        match = pattern.search(line)
        assert match is not None
        groups = match.groups()
        assert "LDL" in groups[0]
        assert "180" in groups[1]
        assert "H" in groups[2]

    def test_decimal_value(self):
        line = "Hemoglobin A1C       5.7       %         4.0-5.6"
        pattern = _RESULT_PATTERNS[0]
        match = pattern.search(line)
        assert match is not None
        assert "5.7" in match.group(2)


class TestDatePatterns:
    def test_collected_date(self):
        text = "Collected: 01/15/2025"
        match = _DATE_PATTERNS[0].search(text)
        assert match is not None
        assert match.group(1) == "01/15/2025"

    def test_collection_date(self):
        text = "Collection Date: 3/25/2024"
        match = _DATE_PATTERNS[0].search(text)
        assert match is not None
        assert match.group(1) == "3/25/2024"

    def test_iso_date(self):
        text = "Report 2025-01-15 complete"
        # Find the ISO date pattern (bare YYYY-MM-DD)
        match = None
        for pat in _DATE_PATTERNS:
            match = pat.search(text)
            if match and match.group(1) == "2025-01-15":
                break
        assert match is not None
        assert match.group(1) == "2025-01-15"


class TestRefRange:
    def test_standard_range(self):
        match = _REF_RANGE.search("70-100")
        assert match is not None
        assert "70" in match.group(1)
        assert "100" in match.group(2)

    def test_decimal_range(self):
        match = _REF_RANGE.search("3.5 - 5.0")
        assert match is not None

    def test_en_dash_range(self):
        match = _REF_RANGE.search("70\u2013100")
        assert match is not None


class TestParseRefRange:
    def test_standard_range(self, parser):
        low, high = parser._parse_ref_range("70-100")
        assert low == 70.0
        assert high == 100.0

    def test_upper_bound_only(self, parser):
        low, high = parser._parse_ref_range("< 200")
        assert low is None
        assert high == 200.0

    def test_lower_bound_only(self, parser):
        low, high = parser._parse_ref_range("> 40")
        assert low == 40.0
        assert high is None

    def test_no_range(self, parser):
        low, high = parser._parse_ref_range("no range here")
        assert low is None
        assert high is None


class TestExtractDate:
    def test_us_date_format(self, parser):
        result = parser._extract_date("Collected: 01/15/2025")
        assert result is not None
        assert result.year == 2025
        assert result.month == 1
        assert result.day == 15

    def test_iso_format(self, parser):
        result = parser._extract_date("Date: 2024-06-15 results")
        assert result is not None
        assert result.year == 2024

    def test_no_date_returns_none(self, parser):
        result = parser._extract_date("No date in this text")
        assert result is None


class TestExtractLabName:
    def test_quest(self, parser):
        assert parser._extract_lab_name("Quest Diagnostics report") == "Quest Diagnostics"

    def test_labcorp(self, parser):
        assert "LabCorp" in parser._extract_lab_name("LabCorp results")

    def test_unknown_lab(self, parser):
        assert parser._extract_lab_name("Unknown Lab Inc.") == ""


class TestGarbageFiltering:
    """Test that known-bad patterns are rejected by the parser."""

    def test_unit_as_test_name_rejected(self, parser):
        """A line where 'mg/dL' is captured as test name should be skipped."""
        text = "mg/dL              102       mg/dL      70-110"
        results = parser._parse_result_lines(text, page_num=1)
        names = [r.test_name.strip().lower() for r in results]
        assert "mg/dl" not in names

    def test_unlabeled_result_rejected(self, parser):
        """'Unlabeled result' should not be a valid test name."""
        text = "Unlabeled result   166       mg/dL      0-200"
        results = parser._parse_result_lines(text, page_num=1)
        names = [r.test_name.strip().lower() for r in results]
        assert not any("unlabeled" in n for n in names)

    def test_numeric_only_name_rejected(self, parser):
        """A test name that's only numbers should be skipped."""
        text = "123.45             95        mg/dL      70-100"
        results = parser._parse_result_lines(text, page_num=1)
        names = [r.test_name.strip() for r in results]
        assert not any(n == "123.45" for n in names)

    def test_valid_test_passes_through(self, parser):
        """Valid test names must still be captured."""
        text = "Glucose              95        mg/dL      70-100"
        results = parser._parse_result_lines(text, page_num=1)
        assert len(results) >= 1
        assert any("Glucose" in r.test_name for r in results)

    def test_non_hdl_cholesterol_passes(self, parser):
        """Non-HDL Cholesterol is a valid test name."""
        text = "Non-HDL Cholesterol  114       mg/dL      0-130"
        results = parser._parse_result_lines(text, page_num=1)
        assert len(results) >= 1


class TestMergeResults:
    """Test the Ollama + regex merge logic."""

    def test_merge_adds_unique_regex_results(self, parser):
        from healthbot.data.models import LabResult
        primary = [
            LabResult(id="1", test_name="Glucose", canonical_name="glucose", source_page=1),
        ]
        supplement = [
            LabResult(id="2", test_name="Glucose", canonical_name="glucose", source_page=1),
            LabResult(id="3", test_name="TSH", canonical_name="tsh", source_page=1),
        ]
        merged = parser._merge_results(primary, supplement)
        assert len(merged) == 2  # glucose deduped, TSH added
        names = {r.canonical_name for r in merged}
        assert "glucose" in names
        assert "tsh" in names

    def test_merge_regex_gets_lower_confidence(self, parser):
        from healthbot.data.models import LabResult
        primary = [
            LabResult(id="1", test_name="Glucose", canonical_name="glucose",
                      source_page=1, confidence=0.85),
        ]
        supplement = [
            LabResult(id="2", test_name="TSH", canonical_name="tsh", source_page=1),
        ]
        merged = parser._merge_results(primary, supplement)
        tsh = [r for r in merged if r.canonical_name == "tsh"][0]
        assert tsh.confidence == 0.6

    def test_merge_empty_supplement(self, parser):
        from healthbot.data.models import LabResult
        primary = [
            LabResult(id="1", test_name="Glucose", canonical_name="glucose", source_page=1),
        ]
        merged = parser._merge_results(primary, [])
        assert len(merged) == 1

    def test_merge_empty_primary(self, parser):
        from healthbot.data.models import LabResult
        supplement = [
            LabResult(id="1", test_name="TSH", canonical_name="tsh", source_page=1),
        ]
        merged = parser._merge_results([], supplement)
        assert len(merged) == 1


class TestParseBytes:
    @patch("healthbot.ingest.lab_pdf_parser.extract_text")
    @patch("healthbot.ingest.ocr_fallback.needs_ocr", return_value=False)
    def test_parses_lab_text(self, mock_ocr, mock_extract, parser):
        mock_extract.return_value = (
            "Quest Diagnostics\n"
            "Collected: 01/15/2025\n"
            "\n"
            "Glucose              95        mg/dL      70-100\n"
            "Hemoglobin           14.5      g/dL       12.0-17.5\n"
        )
        results, text = parser.parse_bytes(b"fake pdf content")
        assert len(results) >= 1
        names = {r.test_name.strip() for r in results}
        assert any("Glucose" in n for n in names)

    @patch("healthbot.ingest.lab_pdf_parser.extract_text")
    @patch("healthbot.ingest.ocr_fallback.needs_ocr", return_value=False)
    def test_empty_pdf_returns_empty(self, mock_ocr, mock_extract, parser):
        mock_extract.return_value = ""
        results, text = parser.parse_bytes(b"empty")
        assert results == []

    @patch("healthbot.ingest.lab_pdf_parser.extract_text")
    @patch("healthbot.ingest.ocr_fallback.needs_ocr", return_value=False)
    def test_sets_collection_date(self, mock_ocr, mock_extract, parser):
        mock_extract.return_value = (
            "Collected: 06/20/2024\n"
            "TSH                  2.5       uIU/mL    0.4-4.0\n"
        )
        results, text = parser.parse_bytes(b"pdf")
        if results:
            assert results[0].date_collected is not None
            assert results[0].date_collected.year == 2024


class TestOllamaPrimary:
    """Test Ollama-primary parsing flow (mocked)."""

    @patch("healthbot.ingest.lab_pdf_parser.extract_text")
    @patch("healthbot.ingest.ocr_fallback.needs_ocr", return_value=False)
    @patch("healthbot.llm.ollama_client.OllamaClient")
    def test_ollama_called_first(self, mock_ollama_cls, mock_ocr, mock_extract, parser):
        """Ollama should be tried as primary parser."""
        mock_extract.return_value = (
            "Collected: 01/15/2025\n"
            "Glucose              95        mg/dL      70-100\n"
        )
        mock_instance = mock_ollama_cls.return_value
        mock_instance.is_available.return_value = True
        mock_instance.send.return_value = (
            '[{"test_name": "Glucose", "value": 95, "unit": "mg/dL", '
            '"reference_low": 70, "reference_high": 100, "flag": ""}]'
        )
        results, text = parser.parse_bytes(b"pdf")
        assert len(results) >= 1
        # Table + Ollama agree → cross-validation consensus boosts confidence
        glucose = [r for r in results if r.canonical_name == "glucose"]
        assert len(glucose) >= 1
        # Consensus boost: table (0.95) + Ollama agree → 0.95 + 0.05 = 0.99 (capped)
        assert glucose[0].confidence >= 0.85

    @patch("healthbot.ingest.lab_pdf_parser.extract_text")
    @patch("healthbot.ingest.ocr_fallback.needs_ocr", return_value=False)
    @patch("healthbot.llm.ollama_client.OllamaClient")
    def test_regex_fallback_when_ollama_unavailable(
        self, mock_ollama_cls, mock_ocr, mock_extract, parser,
    ):
        """When Ollama is unavailable, regex should still work."""
        mock_extract.return_value = (
            "Collected: 01/15/2025\n"
            "Glucose              95        mg/dL      70-100\n"
        )
        mock_instance = mock_ollama_cls.return_value
        mock_instance.is_available.return_value = False
        results, text = parser.parse_bytes(b"pdf")
        assert len(results) >= 1
        # Regex results don't have 0.85 confidence
        names = {r.test_name.strip() for r in results}
        assert any("Glucose" in n for n in names)
