"""Tests for medical timeline (Phase S4)."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import Document, LabResult, Medication
from healthbot.reasoning.timeline import (
    TIMELINE_CATEGORIES,
    MedicalTimeline,
    TimelineEvent,
    format_timeline,
)


class TestTimelineBuild:
    """Test building timelines from various data sources."""

    def test_empty_timeline(self, db) -> None:
        """No data should return empty list."""
        tl = MedicalTimeline(db)
        events = tl.build(user_id=1)
        assert events == []

    def test_lab_events(self, db) -> None:
        """Lab results should appear in timeline."""
        tl = MedicalTimeline(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=130.0,
            unit="mg/dL",
            date_collected=date.today(),
            flag="H",
        )
        db.insert_observation(lab, user_id=1)

        events = tl.build(user_id=1)
        assert len(events) >= 1
        lab_events = [e for e in events if e.category == "lab"]
        assert len(lab_events) == 1
        assert "LDL" in lab_events[0].title
        assert "130" in lab_events[0].detail
        assert "[H]" in lab_events[0].detail

    def test_symptom_events(self, db) -> None:
        """User-logged symptoms should appear in timeline."""
        tl = MedicalTimeline(db)

        # Insert a user_event observation directly (EventLogger bypasses
        # user_id column, so we use raw insert for proper user_id)
        obs_id = uuid.uuid4().hex
        aad = f"observations.encrypted_data.{obs_id}"
        from datetime import UTC, datetime
        enc = db._encrypt({
            "raw_text": "severe headache today",
            "cleaned_text": "severe headache today",
            "symptom_category": "headache",
            "severity": "severe",
            "date_effective": date.today().isoformat(),
        }, aad)
        db.conn.execute(
            """INSERT INTO observations (obs_id, record_type, canonical_name,
               date_effective, triage_level, flag, source_doc_id, source_page,
               source_section, created_at, encrypted_data, user_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (obs_id, "user_event", "headache",
             date.today().isoformat(), "normal", "", "", 0, "",
             datetime.now(UTC).isoformat(), enc, 1),
        )
        db.conn.commit()

        events = tl.build(user_id=1)
        symptom_events = [e for e in events if e.category == "symptom"]
        assert len(symptom_events) == 1
        assert "headache" in symptom_events[0].title.lower()

    def test_medication_events(self, db) -> None:
        """Medication start/stop should appear in timeline."""
        tl = MedicalTimeline(db)

        med = Medication(
            id=uuid.uuid4().hex,
            name="Atorvastatin",
            dose="20",
            unit="mg",
            frequency="daily",
            start_date=date.today(),
            status="active",
        )
        db.insert_medication(med, user_id=1)

        events = tl.build(user_id=1)
        med_events = [e for e in events if e.category == "medication"]
        assert len(med_events) >= 1
        assert "Atorvastatin" in med_events[0].title

    def test_document_events(self, db) -> None:
        """Document uploads should appear in timeline."""
        tl = MedicalTimeline(db)
        doc = Document(
            id=uuid.uuid4().hex,
            source="telegram_pdf",
            sha256="abc123",
            mime_type="application/pdf",
            size_bytes=1024,
            page_count=3,
            enc_blob_path="/tmp/test.enc",
            filename="blood_work.pdf",
        )
        db.insert_document(doc, user_id=1)

        events = tl.build(user_id=1)
        doc_events = [e for e in events if e.category == "document"]
        assert len(doc_events) == 1
        assert "blood_work.pdf" in doc_events[0].title

    def test_hypothesis_events(self, db) -> None:
        """Hypotheses should appear in timeline."""
        tl = MedicalTimeline(db)
        db.insert_hypothesis(1, {
            "title": "Iron Deficiency Anemia",
            "confidence": 0.75,
            "status": "active",
            "evidence_for": ["low ferritin"],
            "evidence_against": [],
            "missing_tests": ["iron panel"],
            "notes": "",
        })

        events = tl.build(user_id=1)
        hyp_events = [e for e in events if e.category == "hypothesis"]
        assert len(hyp_events) == 1
        assert "Iron Deficiency" in hyp_events[0].title
        assert "75%" in hyp_events[0].detail

    def test_journal_events(self, db) -> None:
        """Medical journal entries should appear in timeline."""
        tl = MedicalTimeline(db)
        db.insert_journal_entry(
            user_id=1,
            speaker="user",
            content="I've been having joint pain for 2 weeks",
            category="symptom",
        )

        events = tl.build(user_id=1)
        journal_events = [e for e in events if e.category == "journal"]
        assert len(journal_events) == 1
        assert "joint pain" in journal_events[0].detail

    def test_sorted_descending(self, db) -> None:
        """Events should be sorted newest first."""
        tl = MedicalTimeline(db)
        today = date.today()
        yesterday = today - timedelta(days=1)
        last_week = today - timedelta(days=7)

        for dt in [last_week, today, yesterday]:
            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name="Glucose",
                canonical_name="glucose",
                value=100.0,
                unit="mg/dL",
                date_collected=dt,
            )
            db.insert_observation(lab, user_id=1)

        events = tl.build(user_id=1)
        dates = [e.date for e in events]
        assert dates == sorted(dates, reverse=True)

    def test_category_filter(self, db) -> None:
        """Filtering by category should exclude other types."""
        tl = MedicalTimeline(db)

        # Add a lab and a journal entry
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=4.5,
            unit="mIU/L",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)
        db.insert_journal_entry(
            user_id=1, speaker="user",
            content="Feeling tired", category="symptom",
        )

        lab_only = tl.build(user_id=1, categories={"lab"})
        assert all(e.category == "lab" for e in lab_only)

        journal_only = tl.build(user_id=1, categories={"journal"})
        assert all(e.category == "journal" for e in journal_only)

    def test_limit(self, db) -> None:
        """Limit should cap number of events."""
        tl = MedicalTimeline(db)
        for i in range(10):
            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name=f"Test{i}",
                canonical_name=f"test{i}",
                value=float(i),
                unit="mg/dL",
                date_collected=date.today() - timedelta(days=i),
            )
            db.insert_observation(lab, user_id=1)

        events = tl.build(user_id=1, limit=5)
        assert len(events) == 5

    def test_months_filter(self, db) -> None:
        """Months filter should exclude old events."""
        tl = MedicalTimeline(db)
        recent = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=90.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        old = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=85.0,
            unit="mg/dL",
            date_collected=date.today() - timedelta(days=400),
        )
        db.insert_observation(recent, user_id=1)
        db.insert_observation(old, user_id=1)

        events = tl.build(user_id=1, months=6)
        assert len(events) == 1
        assert events[0].detail.startswith("90")


class TestTimelineSeverity:
    """Test severity mapping in timeline events."""

    def test_flagged_lab_severity(self, db) -> None:
        """Flagged lab results should show watch severity."""
        tl = MedicalTimeline(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Potassium",
            canonical_name="potassium",
            value=5.8,
            unit="mEq/L",
            date_collected=date.today(),
            flag="H",
        )
        db.insert_observation(lab, user_id=1)

        events = tl.build(user_id=1)
        assert events[0].severity in ("watch", "urgent")

    def test_normal_lab_severity(self, db) -> None:
        """Normal lab results should show normal severity."""
        tl = MedicalTimeline(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=90.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        events = tl.build(user_id=1)
        assert events[0].severity == "normal"


class TestFormatTimeline:
    """Test timeline formatting."""

    def test_format_empty(self) -> None:
        """No events should show help text."""
        text = format_timeline([])
        assert "No events" in text

    def test_format_with_events(self) -> None:
        """Events should be formatted with category icons and dates."""
        events = [
            TimelineEvent(
                sort_key="2025-01-15",
                date="2025-01-15",
                category="lab",
                title="LDL",
                detail="130 mg/dL [H]",
                severity="watch",
            ),
            TimelineEvent(
                sort_key="2025-01-10",
                date="2025-01-10",
                category="medication",
                title="Started: Atorvastatin",
                detail="Atorvastatin 20 mg (daily)",
            ),
        ]
        text = format_timeline(events)
        assert "MEDICAL TIMELINE" in text
        assert "2025-01-15" in text
        assert "2025-01-10" in text
        assert "[L]" in text
        assert "[M]" in text
        assert "LDL" in text
        assert "Atorvastatin" in text
        assert "Legend" in text

    def test_format_date_grouping(self) -> None:
        """Events on the same date should share a date header."""
        events = [
            TimelineEvent(
                sort_key="2025-01-15",
                date="2025-01-15",
                category="lab",
                title="LDL",
                detail="130 mg/dL",
            ),
            TimelineEvent(
                sort_key="2025-01-15",
                date="2025-01-15",
                category="lab",
                title="HDL",
                detail="55 mg/dL",
            ),
        ]
        text = format_timeline(events)
        # Date should appear only once
        assert text.count("2025-01-15") == 1

    def test_format_severity_icons(self) -> None:
        """Urgent and watch events should have severity icons."""
        events = [
            TimelineEvent(
                sort_key="2025-01-15",
                date="2025-01-15",
                category="lab",
                title="Potassium",
                detail="5.8 mEq/L [H]",
                severity="urgent",
            ),
        ]
        text = format_timeline(events)
        assert "!" in text

    def test_categories_constant(self) -> None:
        """TIMELINE_CATEGORIES should have all expected types."""
        assert "lab" in TIMELINE_CATEGORIES
        assert "medication" in TIMELINE_CATEGORIES
        assert "symptom" in TIMELINE_CATEGORIES
        assert "wearable" in TIMELINE_CATEGORIES
        assert "document" in TIMELINE_CATEGORIES
        assert "hypothesis" in TIMELINE_CATEGORIES
        assert "journal" in TIMELINE_CATEGORIES
