"""Tests for citation manager."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import Citation, LabResult
from healthbot.retrieval.citation_manager import CitationManager


class TestCitations:
    """Test citation generation and formatting."""

    def test_cite_observation(self, db):
        """Citations should include source doc + page."""
        from healthbot.data.models import Document
        doc = Document(
            id=uuid.uuid4().hex,
            source="quest_diagnostics",
            sha256="abc123",
            enc_blob_path="blob1",
        )
        doc_id = db.insert_document(doc)

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date(2024, 1, 15),
            source_blob_id=doc_id,
            source_page=2,
            source_section="Metabolic Panel",
        )
        obs_id = db.insert_observation(lab)

        cm = CitationManager(db)
        cite = cm.cite_observation(obs_id)
        assert cite is not None
        assert cite.page_number == 2
        assert cite.source_blob_id == doc_id

    def test_citation_format(self):
        """Citation format should include key info."""
        cite = Citation(
            record_id="abc",
            source_type="lab_result",
            source_blob_id="blob1",
            page_number=3,
            section="CBC",
            date_collected="2024-01-15",
            lab_or_provider="Quest",
        )
        formatted = cite.format()
        assert "lab_result" in formatted
        assert "Quest" in formatted
        assert "p.3" in formatted
        assert "2024-01-15" in formatted
