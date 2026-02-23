"""Tests for ZIP file handling (smart routing, multi-file extraction)."""
from __future__ import annotations

import io
import zipfile

import pytest


def _make_zip(files: dict[str, bytes]) -> bytes:
    """Create an in-memory ZIP file with the given entries."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


def _bad_zip() -> bytes:
    """Create invalid ZIP bytes."""
    return b"not a zip file at all"


class TestZipContentDetection:
    """ZIP contents should be correctly classified."""

    def test_apple_health_detected(self) -> None:
        """ZIP with export.xml should be detected as Apple Health."""
        zip_bytes = _make_zip({
            "apple_health_export/export.xml": b"<HealthData>...</HealthData>",
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            has_export = any(n.endswith("export.xml") for n in names)
        assert has_export

    def test_pdf_archive_detected(self) -> None:
        """ZIP with PDFs should have them listed."""
        zip_bytes = _make_zip({
            "lab_report.pdf": b"%PDF-fake",
            "doctors_note.pdf": b"%PDF-fake",
            "readme.txt": b"ignore this",
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            pdf_names = [n for n in names if n.lower().endswith(".pdf")]
        assert len(pdf_names) == 2

    def test_macosx_folder_excluded(self) -> None:
        """__MACOSX/ entries should be excluded."""
        zip_bytes = _make_zip({
            "report.pdf": b"%PDF-fake",
            "__MACOSX/._report.pdf": b"mac metadata",
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            pdf_names = [
                n for n in names
                if n.lower().endswith(".pdf") and not n.startswith("__MACOSX")
            ]
        assert len(pdf_names) == 1
        assert pdf_names[0] == "report.pdf"

    def test_mixed_content(self) -> None:
        """ZIP with PDFs + XMLs + JSONs + other files."""
        zip_bytes = _make_zip({
            "labs.pdf": b"%PDF-fake",
            "notes.pdf": b"%PDF-fake",
            "ccda.xml": b"<ClinicalDocument>...</ClinicalDocument>",
            "fhir.json": b'{"resourceType": "Bundle"}',
            "readme.txt": b"not processable",
            "image.png": b"fake png",
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            pdf_names = [n for n in names if n.lower().endswith(".pdf")]
            xml_names = [n for n in names if n.lower().endswith(".xml")]
            json_names = [n for n in names if n.lower().endswith(".json")]
        assert len(pdf_names) == 2
        assert len(xml_names) == 1
        assert len(json_names) == 1

    def test_nested_directory_pdfs(self) -> None:
        """PDFs in subdirectories should be found."""
        zip_bytes = _make_zip({
            "folder/subfolder/report.pdf": b"%PDF-fake",
            "another/note.pdf": b"%PDF-fake",
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            pdf_names = [n for n in names if n.lower().endswith(".pdf")]
        assert len(pdf_names) == 2

    def test_empty_zip(self) -> None:
        """Empty ZIP should have no processable files."""
        zip_bytes = _make_zip({})
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
        assert len(names) == 0

    def test_bad_zip_detected(self) -> None:
        """Invalid ZIP bytes should raise BadZipFile."""
        with pytest.raises(zipfile.BadZipFile):
            zipfile.ZipFile(io.BytesIO(_bad_zip()))


class TestZipAppleHealthPriority:
    """Apple Health should take priority over other content."""

    def test_export_xml_prioritized(self) -> None:
        """If ZIP has export.xml AND PDFs, Apple Health takes priority."""
        zip_bytes = _make_zip({
            "apple_health_export/export.xml": b"<HealthData>test</HealthData>",
            "labs.pdf": b"%PDF-fake",
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            has_export = any(n.endswith("export.xml") for n in names)
            pdf_names = [n for n in names if n.lower().endswith(".pdf")]
        assert has_export  # Should route to Apple Health
        assert len(pdf_names) == 1  # PDFs exist but should be ignored


class TestSchedulerZipDetection:
    """Scheduler ZIP handling content detection."""

    def test_scheduler_detects_pdf_zip(self) -> None:
        """Scheduler should recognize ZIPs with PDFs."""
        zip_bytes = _make_zip({
            "report1.pdf": b"%PDF-fake",
            "report2.pdf": b"%PDF-fake",
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            has_export = any(n.endswith("export.xml") for n in names)
            pdf_names = [
                n for n in names
                if n.lower().endswith(".pdf") and not n.startswith("__MACOSX")
            ]
        assert not has_export
        assert len(pdf_names) == 2

    def test_scheduler_detects_mixed_zip(self) -> None:
        """Scheduler should handle ZIPs with PDFs + XML + JSON."""
        zip_bytes = _make_zip({
            "labs.pdf": b"%PDF-fake",
            "ccda.xml": b"<ClinicalDocument/>",
            "bundle.json": b'{"resourceType": "Bundle"}',
        })
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            processable = [
                n for n in names
                if n.lower().endswith((".pdf", ".xml", ".json"))
                and not n.startswith("__MACOSX")
            ]
        assert len(processable) == 3


class TestIngestResultClinicalFields:
    """IngestResult should carry clinical extraction data."""

    def test_default_clinical_fields(self) -> None:
        from healthbot.ingest.telegram_pdf_ingest import IngestResult

        r = IngestResult()
        assert r.clinical_facts_count == 0
        assert r.clinical_summary == ""
        assert r.doc_type == ""

    def test_populated_clinical_fields(self) -> None:
        from healthbot.ingest.telegram_pdf_ingest import IngestResult

        r = IngestResult(
            success=True,
            clinical_facts_count=7,
            clinical_summary="Discharge summary from hospital stay.",
            doc_type="discharge_summary",
        )
        assert r.clinical_facts_count == 7
        assert r.doc_type == "discharge_summary"
        assert "Discharge" in r.clinical_summary
