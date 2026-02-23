"""Tests for domain models."""
from __future__ import annotations

from datetime import date

from healthbot.data.models import (
    Citation,
    Concern,
    Document,
    ExternalEvidence,
    Hypothesis,
    LabResult,
    Medication,
    MemoryEntry,
    RecordType,
    TriageLevel,
    VitalSign,
    WhoopDaily,
)


class TestEnums:
    def test_record_type_values(self):
        assert RecordType.LAB_RESULT.value == "lab_result"
        assert RecordType.APPLE_HEALTH.value == "apple_health"
        assert RecordType.WHOOP_RECOVERY.value == "whoop_recovery"

    def test_triage_level_values(self):
        assert TriageLevel.NORMAL.value == "normal"
        assert TriageLevel.EMERGENCY.value == "emergency"

    def test_all_record_types(self):
        assert len(RecordType) == 10

    def test_all_triage_levels(self):
        assert len(TriageLevel) == 5


class TestLabResult:
    def test_defaults(self):
        lr = LabResult(id="lr1", test_name="glucose")
        assert lr.canonical_name == ""
        assert lr.value == ""
        assert lr.triage_level == TriageLevel.NORMAL
        assert lr.flag == ""
        assert lr.fasting is None

    def test_full_fields(self):
        lr = LabResult(
            id="lr1", test_name="Glucose", canonical_name="glucose",
            value=108.0, unit="mg/dL", reference_low=70, reference_high=100,
            date_collected=date(2025, 1, 15), flag="H",
            triage_level=TriageLevel.WATCH,
        )
        assert lr.value == 108.0
        assert lr.flag == "H"


class TestMedication:
    def test_defaults(self):
        med = Medication(id="m1", name="Metformin")
        assert med.status == "active"
        assert med.start_date is None

    def test_with_dates(self):
        med = Medication(
            id="m1", name="Lisinopril", dose="10mg",
            start_date=date(2024, 6, 1),
        )
        assert med.start_date == date(2024, 6, 1)


class TestVitalSign:
    def test_defaults(self):
        vs = VitalSign(id="v1", type="blood_pressure", value="120/80", unit="mmHg")
        assert vs.source == ""
        assert vs.timestamp is None


class TestWhoopDaily:
    def test_defaults(self):
        wd = WhoopDaily(id="w1", date=date(2025, 1, 15))
        assert wd.provider == "whoop"
        assert wd.hrv is None
        assert wd.recovery_score is None


class TestConcern:
    def test_defaults(self):
        c = Concern(id="c1", title="Elevated glucose")
        assert c.severity == TriageLevel.WATCH
        assert c.evidence == []
        assert c.status == "active"


class TestExternalEvidence:
    def test_defaults(self):
        ev = ExternalEvidence(
            id="e1", source="pubmed", query_hash="abc123",
            prompt_sanitized="cholesterol guidelines",
        )
        assert ev.result_json == {}
        assert ev.created_at is None


class TestDocument:
    def test_defaults(self):
        doc = Document(id="d1", source="telegram_pdf", sha256="abc")
        assert doc.size_bytes == 0
        assert doc.page_count == 0
        assert doc.meta == {}


class TestCitation:
    def test_format_full(self):
        c = Citation(
            record_id="r1", source_type="lab_result",
            source_blob_id="b1", page_number=2,
            section="Chemistry", date_collected="2025-01-15",
            lab_or_provider="Quest",
        )
        formatted = c.format()
        assert "lab_result" in formatted
        assert "Quest" in formatted
        assert "2025-01-15" in formatted
        assert "p.2" in formatted
        assert "Chemistry" in formatted

    def test_format_minimal(self):
        c = Citation(record_id="r1", source_type="vital_sign", source_blob_id="b1")
        assert c.format() == "[vital_sign]"

    def test_format_no_page_zero(self):
        c = Citation(
            record_id="r1", source_type="lab_result",
            source_blob_id="b1", page_number=0,
        )
        assert "p." not in c.format()


class TestMemoryEntry:
    def test_defaults(self):
        me = MemoryEntry(id="m1", user_id=1, role="user", content="hello")
        assert me.consolidated is False
        assert me.source == ""


class TestHypothesis:
    def test_defaults(self):
        h = Hypothesis(id="h1", user_id=1, title="POTS")
        assert h.confidence == 0.0
        assert h.evidence_for == []
        assert h.evidence_against == []
        assert h.missing_tests == []
        assert h.status == "active"

    def test_mutable_defaults_independent(self):
        h1 = Hypothesis(id="h1", user_id=1, title="A")
        h2 = Hypothesis(id="h2", user_id=1, title="B")
        h1.evidence_for.append("test")
        assert h2.evidence_for == []
