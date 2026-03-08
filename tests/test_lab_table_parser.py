"""Tests for structural table extraction from lab PDFs."""
from __future__ import annotations

import pytest

from healthbot.ingest.lab_table_parser import (
    identify_columns,
    infer_columns_from_content,
    parse_table_direct,
)


class TestIdentifyColumns:
    """Test column header identification."""

    def test_labcorp_header(self) -> None:
        header = ["TESTS", "RESULT", "FLAG", "UNITS", "REFERENCE INTERVAL", "LAB"]
        col_map = identify_columns(header)
        assert col_map is not None
        assert col_map["test_name"] == 0
        assert col_map["value"] == 1
        assert col_map["flag"] == 2
        assert col_map["unit"] == 3
        assert col_map["reference"] == 4

    def test_quest_header(self) -> None:
        header = ["Test Name", "Result", "Units", "Reference Range", "Flag"]
        col_map = identify_columns(header)
        assert col_map is not None
        assert col_map["test_name"] == 0
        assert col_map["value"] == 1
        assert col_map["unit"] == 2
        assert col_map["reference"] == 3
        assert col_map["flag"] == 4

    def test_minimal_header(self) -> None:
        header = ["Component", "Your Result"]
        col_map = identify_columns(header)
        assert col_map is not None
        assert col_map["test_name"] == 0
        assert col_map["value"] == 1

    def test_no_match_returns_none(self) -> None:
        header = ["Page", "Date", "Provider", "Status"]
        col_map = identify_columns(header)
        assert col_map is None

    def test_none_cells_handled(self) -> None:
        header = [None, "Tests", None, "Result", "Units"]
        col_map = identify_columns(header)
        assert col_map is not None
        assert col_map["test_name"] == 1
        assert col_map["value"] == 3

    def test_case_insensitive(self) -> None:
        header = ["tests", "result", "units", "REFERENCE INTERVAL"]
        col_map = identify_columns(header)
        assert col_map is not None
        assert col_map["test_name"] == 0
        assert col_map["value"] == 1

    def test_empty_header(self) -> None:
        header = ["", None, ""]
        assert identify_columns(header) is None

    def test_assay_synonym(self) -> None:
        header = ["Assay", "Observed", "Unit of Measure"]
        col_map = identify_columns(header)
        assert col_map is not None
        assert col_map["test_name"] == 0
        assert col_map["value"] == 1
        assert col_map["unit"] == 2


class TestInferColumns:
    """Test content-based column inference."""

    def test_infer_from_lab_data(self) -> None:
        rows = [
            ["Glucose", "95", "mg/dL", "70-100", ""],
            ["Sodium", "140", "mmol/L", "136-145", ""],
            ["Potassium", "4.2", "mmol/L", "3.5-5.1", ""],
            ["Chloride", "102", "mmol/L", "98-106", ""],
        ]
        col_map = infer_columns_from_content(rows)
        assert col_map is not None
        assert col_map["value"] == 1
        assert col_map["test_name"] == 0

    def test_too_few_rows(self) -> None:
        rows = [["Glucose", "95"]]
        assert infer_columns_from_content(rows) is None

    def test_empty_rows(self) -> None:
        assert infer_columns_from_content([]) is None

    def test_single_column(self) -> None:
        rows = [["Glucose"], ["Sodium"], ["Potassium"]]
        assert infer_columns_from_content(rows) is None


class TestParseTableDirect:
    """Test direct table-to-LabResult conversion."""

    def test_basic_labcorp_table(self) -> None:
        rows = [
            ["TESTS", "RESULT", "FLAG", "UNITS", "REFERENCE INTERVAL"],
            ["Glucose", "95", "", "mg/dL", "70 - 100"],
            ["Hemoglobin", "14.5", "", "g/dL", "12.0 - 17.5"],
            ["LDL Cholesterol", "180", "H", "mg/dL", "0 - 100"],
        ]
        col_map = identify_columns(rows[0])
        assert col_map is not None
        results = parse_table_direct(rows, col_map, page_num=1)
        assert len(results) == 3

        glucose = [r for r in results if "glucose" in r.canonical_name][0]
        assert glucose.value == 95.0
        assert glucose.unit == "mg/dL"
        assert glucose.reference_low == 70.0
        assert glucose.reference_high == 100.0
        assert glucose.confidence == 0.95

        ldl = [r for r in results if "ldl" in r.canonical_name][0]
        assert ldl.flag == "H"
        assert ldl.value == 180.0

    def test_bad_test_names_filtered(self) -> None:
        rows = [
            ["TESTS", "RESULT", "UNITS", "REFERENCE INTERVAL"],
            ["Comment", "123", "mg/dL", "70-100"],
            ["Glucose", "95", "mg/dL", "70-100"],
        ]
        col_map = identify_columns(rows[0])
        results = parse_table_direct(rows, col_map, page_num=1)
        assert len(results) == 1
        assert results[0].canonical_name == "glucose"

    def test_none_cells_in_data_rows(self) -> None:
        rows = [
            ["TESTS", "RESULT", "UNITS"],
            ["Glucose", "95", None],
        ]
        col_map = identify_columns(rows[0])
        results = parse_table_direct(rows, col_map, page_num=1)
        assert len(results) == 1

    def test_dedup_within_table(self) -> None:
        rows = [
            ["TESTS", "RESULT", "UNITS"],
            ["Glucose", "95", "mg/dL"],
            ["Glucose", "96", "mg/dL"],
        ]
        col_map = identify_columns(rows[0])
        results = parse_table_direct(rows, col_map, page_num=1)
        assert len(results) == 1
        assert results[0].value == 95.0

    def test_non_numeric_kept_as_qualitative(self) -> None:
        """Non-numeric values (e.g. 'Non-Reactive') are kept as strings."""
        rows = [
            ["TESTS", "RESULT", "UNITS"],
            ["HIV Status", "Non-Reactive", ""],
            ["Glucose", "95", "mg/dL"],
        ]
        col_map = identify_columns(rows[0])
        results = parse_table_direct(rows, col_map, page_num=1)
        assert len(results) == 2
        hiv = [r for r in results if r.test_name == "HIV Status"]
        assert len(hiv) == 1
        assert hiv[0].value == "Non-Reactive"

    def test_header_row_1(self) -> None:
        """Title row at index 0, real headers at index 1."""
        rows = [
            ["CBC With Differential", None, None, None],
            ["TESTS", "RESULT", "UNITS", "REFERENCE INTERVAL"],
            ["WBC", "8.2", "x10E3/uL", "3.4 - 10.8"],
        ]
        col_map = identify_columns(rows[1])
        results = parse_table_direct(rows, col_map, page_num=1, header_row_idx=1)
        assert len(results) == 1
        assert results[0].value == 8.2

    def test_ref_range_parsing(self) -> None:
        rows = [
            ["Test", "Result", "Units", "Reference Range"],
            ["TSH", "2.5", "mIU/L", "0.4 - 4.0"],
        ]
        col_map = identify_columns(rows[0])
        results = parse_table_direct(rows, col_map, page_num=1)
        assert results[0].reference_low == pytest.approx(0.4)
        assert results[0].reference_high == pytest.approx(4.0)

    def test_flag_normalization(self) -> None:
        rows = [
            ["Test", "Result", "Flag"],
            ["LDL", "180", "High"],
            ["HDL", "35", "Low"],
        ]
        col_map = identify_columns(rows[0])
        results = parse_table_direct(rows, col_map, page_num=1)
        assert results[0].flag == "H"
        assert results[1].flag == "L"

    def test_comma_value(self) -> None:
        """Commas in values should be stripped."""
        rows = [
            ["Test", "Result", "Units"],
            ["Platelets", "250,000", "x10E3/uL"],
        ]
        col_map = identify_columns(rows[0])
        results = parse_table_direct(rows, col_map, page_num=1)
        assert results[0].value == 250000.0


class TestThreeWayMerge:
    """Test three-way merge of table + Ollama + regex results."""

    def test_table_takes_priority(self) -> None:
        from healthbot.data.models import LabResult
        from healthbot.ingest.lab_pdf_parser import LabPdfParser

        table = [LabResult(id="1", test_name="Glucose",
                           canonical_name="glucose", confidence=0.95)]
        ollama = [LabResult(id="2", test_name="Glucose",
                            canonical_name="glucose", confidence=0.85)]
        regex = [LabResult(id="3", test_name="TSH",
                           canonical_name="tsh")]
        merged = LabPdfParser._merge_three_way(table, ollama, regex)
        assert len(merged) == 2
        gluc = [r for r in merged if r.canonical_name == "glucose"][0]
        assert gluc.confidence == 0.95

    def test_all_three_contribute_unique(self) -> None:
        from healthbot.data.models import LabResult
        from healthbot.ingest.lab_pdf_parser import LabPdfParser

        table = [LabResult(id="1", test_name="Glucose",
                           canonical_name="glucose", confidence=0.95)]
        ollama = [LabResult(id="2", test_name="TSH",
                            canonical_name="tsh", confidence=0.85)]
        regex = [LabResult(id="3", test_name="WBC",
                           canonical_name="wbc")]
        merged = LabPdfParser._merge_three_way(table, ollama, regex)
        assert len(merged) == 3
        wbc = [r for r in merged if r.canonical_name == "wbc"][0]
        assert wbc.confidence == 0.60

    def test_empty_inputs(self) -> None:
        from healthbot.ingest.lab_pdf_parser import LabPdfParser

        merged = LabPdfParser._merge_three_way([], [], [])
        assert merged == []
