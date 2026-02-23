"""Tests for CSV export module."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import LabResult, Medication
from healthbot.export.csv_exporter import export_labs_csv, export_medications_csv


class TestExportLabsCsv:
    def test_empty_db(self, db) -> None:
        csv_str = export_labs_csv(db, user_id=0)
        lines = csv_str.strip().split("\n")
        assert len(lines) == 1  # Header only
        assert "date" in lines[0]
        assert "test_name" in lines[0]

    def test_with_lab_results(self, db) -> None:
        lab = LabResult(
            id=uuid.uuid4().hex, test_name="Glucose",
            canonical_name="glucose", value=95,
            unit="mg/dL", flag="",
            date_collected=date(2025, 12, 1),
            reference_low=70, reference_high=100,
        )
        db.insert_observation(lab)

        csv_str = export_labs_csv(db, user_id=0)
        lines = csv_str.strip().split("\n")
        assert len(lines) == 2  # Header + 1 row
        assert "Glucose" in lines[1]
        assert "95" in lines[1]
        assert "mg/dL" in lines[1]

    def test_multiple_labs(self, db) -> None:
        for name, val in [("Glucose", 95), ("LDL", 120), ("TSH", 2.5)]:
            lab = LabResult(
                id=uuid.uuid4().hex, test_name=name,
                canonical_name=name.lower(), value=val,
                unit="mg/dL", flag="",
                date_collected=date(2025, 12, 1),
            )
            db.insert_observation(lab)

        csv_str = export_labs_csv(db, user_id=0)
        lines = csv_str.strip().split("\n")
        assert len(lines) == 4  # Header + 3 rows


class TestExportMedicationsCsv:
    def test_empty_db(self, db) -> None:
        csv_str = export_medications_csv(db, user_id=0)
        lines = csv_str.strip().split("\n")
        assert len(lines) == 1  # Header only

    def test_with_medications(self, db) -> None:
        med = Medication(
            id=uuid.uuid4().hex, name="Metformin",
            dose="1000mg", frequency="twice daily",
            status="active",
        )
        db.insert_medication(med)

        csv_str = export_medications_csv(db, user_id=0)
        lines = csv_str.strip().split("\n")
        assert len(lines) == 2
        assert "Metformin" in lines[1]
        assert "1000mg" in lines[1]
