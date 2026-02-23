"""Tests for the CSV exporter."""
from __future__ import annotations

import csv
import io
from unittest.mock import MagicMock

from healthbot.export.csv_exporter import export_labs_csv, export_medications_csv


class TestExportLabsCsv:
    """Export lab results as CSV."""

    def test_empty_labs_returns_header_only(self):
        db = MagicMock()
        db.query_observations = MagicMock(return_value=[])
        result = export_labs_csv(db, user_id=1)
        reader = csv.reader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 1  # Header only
        assert "test_name" in rows[0]

    def test_labs_with_data(self):
        db = MagicMock()
        db.query_observations = MagicMock(return_value=[
            {
                "test_name": "LDL",
                "canonical_name": "ldl",
                "value": "130",
                "unit": "mg/dL",
                "reference_low": "0",
                "reference_high": "100",
                "flag": "H",
                "_meta": {"date_effective": "2024-01-15"},
            },
        ])
        result = export_labs_csv(db, user_id=1)
        reader = csv.reader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 2  # Header + 1 row
        assert "LDL" in rows[1]
        assert "130" in rows[1]

    def test_csv_escapes_commas(self):
        db = MagicMock()
        db.query_observations = MagicMock(return_value=[
            {
                "test_name": "Test, with comma",
                "canonical_name": "test",
                "value": "10",
                "unit": "U/L",
                "reference_low": "",
                "reference_high": "",
                "flag": "",
                "_meta": {},
            },
        ])
        result = export_labs_csv(db, user_id=1)
        # Comma in test name should be properly escaped
        reader = csv.reader(io.StringIO(result))
        rows = list(reader)
        assert rows[1][1] == "Test, with comma"


class TestExportMedicationsCsv:
    """Export medications as CSV."""

    def test_empty_meds_returns_header_only(self):
        db = MagicMock()
        db.get_active_medications = MagicMock(return_value=[])
        result = export_medications_csv(db, user_id=1)
        reader = csv.reader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 1
        assert "name" in rows[0]

    def test_meds_with_data(self):
        db = MagicMock()
        db.get_active_medications = MagicMock(return_value=[
            {"name": "Metformin", "dose": "500mg", "frequency": "twice daily"},
            {"name": "Atorvastatin", "dose": "20mg", "frequency": "daily"},
        ])
        result = export_medications_csv(db, user_id=1)
        reader = csv.reader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 3  # Header + 2 rows
        assert "Metformin" in rows[1]
        assert "Atorvastatin" in rows[2]

    def test_missing_fields_default_empty(self):
        db = MagicMock()
        db.get_active_medications = MagicMock(return_value=[
            {"name": "Aspirin"},  # missing dose, frequency
        ])
        result = export_medications_csv(db, user_id=1)
        reader = csv.reader(io.StringIO(result))
        rows = list(reader)
        assert len(rows) == 2
        assert rows[1][0] == "Aspirin"
