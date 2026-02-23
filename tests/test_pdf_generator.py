"""Tests for doctor packet PDF generation."""
from __future__ import annotations

import pytest

from healthbot.export.pdf_generator import DoctorPacketPdf, PrepData


@pytest.fixture
def generator() -> DoctorPacketPdf:
    return DoctorPacketPdf()


class TestPdfGeneration:
    """Verify that generated output is valid in-memory PDF bytes."""

    def test_generate_returns_pdf_bytes(self, generator: DoctorPacketPdf) -> None:
        data = PrepData(generated_date="2024-06-15 10:00 UTC")
        result = generator.generate(data)
        assert isinstance(result, bytes)
        assert result[:5] == b"%PDF-"

    def test_empty_prep_data_generates(self, generator: DoctorPacketPdf) -> None:
        """Even with no data at all, a valid PDF should be produced."""
        data = PrepData()
        result = generator.generate(data)
        assert isinstance(result, bytes)
        assert result[:5] == b"%PDF-"
        assert len(result) > 100  # Non-trivial PDF

    def test_full_prep_data_generates(self, generator: DoctorPacketPdf) -> None:
        """A packet with every section populated should produce a valid PDF."""
        data = PrepData(
            generated_date="2024-06-15 10:00 UTC",
            urgent_items=[
                {
                    "level": "CRITICAL",
                    "name": "Potassium",
                    "value": "6.2",
                    "unit": "mEq/L",
                    "date": "2024-06-10",
                    "citation": "DOC-001",
                },
                {
                    "level": "HIGH",
                    "name": "Glucose",
                    "value": "285",
                    "unit": "mg/dL",
                    "date": "2024-06-10",
                    "citation": "DOC-001",
                },
            ],
            trends=[
                {
                    "test_name": "HbA1c",
                    "direction": "increasing",
                    "pct_change": "12",
                    "first_val": "6.5",
                    "last_val": "7.3",
                    "dates": "2024-01 to 2024-06",
                },
            ],
            medications=[
                {"name": "Metformin", "dose": "500mg", "frequency": "twice daily"},
                {"name": "Lisinopril", "dose": "10mg", "frequency": "once daily"},
            ],
            overdue_items=[
                {"test_name": "Lipid Panel", "last_date": "2023-01-15", "months_overdue": 18},
            ],
            panel_gaps=[
                {
                    "panel_name": "Comprehensive Metabolic Panel",
                    "missing_tests": "Albumin, Total Protein",
                    "reason": "Only basic metabolic ordered last visit",
                },
            ],
            questions=[
                "Should we adjust metformin dosage given rising HbA1c?",
                "Is a referral to endocrinology warranted?",
            ],
            trend_tables=[
                {
                    "test_name": "HbA1c",
                    "values": [
                        ("2024-01-10", "6.5", "%"),
                        ("2024-03-15", "6.8", "%"),
                        ("2024-06-10", "7.3", "%"),
                    ],
                },
            ],
            citations=[
                {
                    "record_id": "DOC-001",
                    "source": "Quest Diagnostics PDF",
                    "page": "1",
                    "section": "Chemistry",
                    "date": "2024-06-10",
                },
            ],
        )
        result = generator.generate(data)
        assert isinstance(result, bytes)
        assert result[:5] == b"%PDF-"
        assert len(result) > 500  # Should be non-trivial with all that content

    def test_output_is_bytes_not_file(self, generator: DoctorPacketPdf) -> None:
        """The return type must be bytes, never a file path or file object."""
        data = PrepData()
        result = generator.generate(data)
        assert isinstance(result, bytes)
        # Ensure it's not accidentally a string path
        assert not isinstance(result, (str, memoryview))

    def test_trend_table_dict_rows(self, generator: DoctorPacketPdf) -> None:
        """Trend table rows can be dicts instead of tuples."""
        data = PrepData(
            trend_tables=[
                {
                    "test_name": "LDL",
                    "values": [
                        {"date": "2024-01", "value": "130", "unit": "mg/dL"},
                        {"date": "2024-06", "value": "115", "unit": "mg/dL"},
                    ],
                },
            ],
        )
        result = generator.generate(data)
        assert isinstance(result, bytes)
        assert result[:5] == b"%PDF-"
