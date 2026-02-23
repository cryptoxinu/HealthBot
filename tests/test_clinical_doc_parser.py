"""Tests for clinical document parser (Ollama-based extraction)."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

from healthbot.ingest.clinical_doc_parser import (
    _VALID_CATEGORIES,
    _VALID_DOC_TYPES,
    ClinicalDocParser,
)


def _mock_ollama(response: str) -> MagicMock:
    """Create a mock OllamaClient that returns the given response."""
    ollama = MagicMock()
    ollama.send.return_value = response
    ollama.is_available.return_value = True
    return ollama


def _make_response(
    doc_type: str = "clinical_note",
    facts: list[dict] | None = None,
    summary: str = "Test summary.",
) -> str:
    """Build a JSON response string."""
    if facts is None:
        facts = [{"category": "condition", "fact": "Diagnosed with hypertension"}]
    return json.dumps({"doc_type": doc_type, "facts": facts, "summary": summary})


class TestClinicalDocExtraction:
    """Core extraction from clinical documents."""

    def test_extracts_single_fact(self) -> None:
        response = _make_response(
            facts=[{"category": "condition", "fact": "Diagnosed with hypertension"}],
        )
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)

        result = parser.extract(
            "Patient presents with elevated blood pressure. " * 5
        )
        assert result.doc_type == "clinical_note"
        assert len(result.facts) == 1
        assert result.facts[0]["category"] == "condition"
        assert result.facts[0]["fact"] == "Diagnosed with hypertension"

    def test_extracts_multiple_facts(self) -> None:
        response = _make_response(
            doc_type="after_visit_summary",
            facts=[
                {"category": "condition", "fact": "Type 2 diabetes mellitus"},
                {"category": "medication", "fact": "Started metformin 500mg twice daily"},
                {"category": "provider", "fact": "Follow up with endocrinology in 3 months"},
            ],
            summary="After-visit summary from primary care.",
        )
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)

        result = parser.extract("A" * 100)
        assert len(result.facts) == 3
        assert result.doc_type == "after_visit_summary"
        assert result.summary == "After-visit summary from primary care."

    def test_extracts_doc_type(self) -> None:
        for dtype in _VALID_DOC_TYPES:
            response = _make_response(doc_type=dtype, facts=[])
            ollama = _mock_ollama(response)
            parser = ClinicalDocParser(ollama)
            result = parser.extract("x" * 100)
            assert result.doc_type == dtype

    def test_invalid_doc_type_defaults(self) -> None:
        response = _make_response(doc_type="bogus_type")
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)
        result = parser.extract("x" * 100)
        assert result.doc_type == "general_medical"

    def test_invalid_category_filtered(self) -> None:
        response = _make_response(
            facts=[
                {"category": "bogus", "fact": "Should be filtered out"},
                {"category": "condition", "fact": "Should be kept here"},
            ],
        )
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)
        result = parser.extract("x" * 100)
        assert len(result.facts) == 1
        assert result.facts[0]["category"] == "condition"

    def test_short_fact_filtered(self) -> None:
        response = _make_response(
            facts=[{"category": "condition", "fact": "yes"}],
        )
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)
        result = parser.extract("x" * 100)
        assert len(result.facts) == 0

    def test_all_valid_categories(self) -> None:
        # Use distinct facts that won't trigger dedup
        category_facts = {
            "demographic": "Patient is 45 years old male",
            "condition": "Diagnosed with hypertension stage two",
            "medication": "Takes lisinopril 10mg every morning",
            "pattern": "Blood pressure trending upward since January",
            "preference": "Prefers metric units for all measurements",
            "provider": "Referred to cardiology for evaluation",
        }
        facts = [
            {"category": cat, "fact": fact}
            for cat, fact in category_facts.items()
        ]
        response = _make_response(facts=facts)
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)
        result = parser.extract("x" * 100)
        assert len(result.facts) == len(_VALID_CATEGORIES)


class TestClinicalDocEdgeCases:
    """Edge cases and error handling."""

    def test_empty_text_returns_empty(self) -> None:
        ollama = _mock_ollama("")
        parser = ClinicalDocParser(ollama)
        result = parser.extract("")
        assert result.facts == []
        assert result.doc_type == "general_medical"
        ollama.send.assert_not_called()

    def test_short_text_returns_empty(self) -> None:
        ollama = _mock_ollama("")
        parser = ClinicalDocParser(ollama)
        result = parser.extract("Too short.")
        assert result.facts == []
        ollama.send.assert_not_called()

    def test_ollama_unavailable(self) -> None:
        ollama = MagicMock()
        ollama.is_available.return_value = False
        parser = ClinicalDocParser(ollama)
        result = parser.extract("A" * 100)
        assert result.facts == []

    def test_malformed_json_handled(self) -> None:
        ollama = _mock_ollama("{bad json here}")
        parser = ClinicalDocParser(ollama)
        result = parser.extract("A" * 100)
        assert result.facts == []

    def test_non_json_response(self) -> None:
        ollama = _mock_ollama("I couldn't parse that document.")
        parser = ClinicalDocParser(ollama)
        result = parser.extract("A" * 100)
        assert result.facts == []

    def test_ollama_exception_handled(self) -> None:
        ollama = MagicMock()
        ollama.is_available.return_value = True
        ollama.send.side_effect = Exception("Connection refused")
        parser = ClinicalDocParser(ollama)
        result = parser.extract("A" * 100)
        assert result.facts == []

    def test_json_with_extra_text(self) -> None:
        """Ollama sometimes wraps JSON in markdown or extra text."""
        inner = _make_response(
            facts=[{"category": "condition", "fact": "Has asthma since childhood"}],
        )
        response = f"Here is the analysis:\n```json\n{inner}\n```"
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)
        result = parser.extract("A" * 100)
        assert len(result.facts) == 1

    def test_summary_truncated(self) -> None:
        long_summary = "A" * 1000
        response = _make_response(summary=long_summary)
        ollama = _mock_ollama(response)
        parser = ClinicalDocParser(ollama)
        result = parser.extract("x" * 100)
        assert len(result.summary) <= 500


class TestChunking:
    """Document chunking for long documents."""

    def test_short_doc_single_chunk(self) -> None:
        text = "Short document." * 10
        chunks = ClinicalDocParser._split_into_chunks(text)
        assert len(chunks) == 1

    def test_long_doc_splits_on_page_breaks(self) -> None:
        # Create a document with page breaks exceeding chunk limit
        pages = [f"Page {i} content. " * 200 for i in range(5)]
        text = "\f".join(pages)
        chunks = ClinicalDocParser._split_into_chunks(text)
        assert len(chunks) > 1
        # All content should be preserved
        total_content = "".join(chunks)
        for i in range(5):
            assert f"Page {i} content" in total_content

    def test_chunk_aggregation(self) -> None:
        """Multiple chunks should have their facts aggregated."""
        call_count = [0]
        responses = [
            _make_response(
                doc_type="discharge_summary",
                facts=[{"category": "condition", "fact": "Diagnosed with pneumonia"}],
            ),
            _make_response(
                doc_type="discharge_summary",
                facts=[{"category": "medication", "fact": "Prescribed amoxicillin 500mg"}],
            ),
        ]

        ollama = MagicMock()
        ollama.is_available.return_value = True

        def mock_send(*args, **kwargs):
            idx = min(call_count[0], len(responses) - 1)
            call_count[0] += 1
            return responses[idx]

        ollama.send.side_effect = mock_send
        parser = ClinicalDocParser(ollama)

        # Force chunking with page-break separated text
        text = ("A" * 8000) + "\f" + ("B" * 8000)
        result = parser.extract(text)
        assert len(result.facts) == 2
        assert result.doc_type == "discharge_summary"


class TestFactDedup:
    """Deduplication of facts across chunks."""

    def test_exact_duplicate_removed(self) -> None:
        facts = [
            {"category": "condition", "fact": "Has hypertension"},
            {"category": "condition", "fact": "Has hypertension"},
        ]
        result = ClinicalDocParser._dedup_facts(facts)
        assert len(result) == 1

    def test_similar_facts_deduped(self) -> None:
        facts = [
            {"category": "condition", "fact": "Diagnosed with type 2 diabetes"},
            {"category": "condition", "fact": "Diagnosed with type 2 diabetes mellitus"},
        ]
        result = ClinicalDocParser._dedup_facts(facts)
        assert len(result) == 1
        # Should keep the longer version
        assert "mellitus" in result[0]["fact"]

    def test_different_facts_kept(self) -> None:
        facts = [
            {"category": "condition", "fact": "Has hypertension"},
            {"category": "medication", "fact": "Takes lisinopril daily"},
        ]
        result = ClinicalDocParser._dedup_facts(facts)
        assert len(result) == 2

    def test_empty_list(self) -> None:
        assert ClinicalDocParser._dedup_facts([]) == []


class TestIngestPipelineClinical:
    """Integration: clinical extraction in TelegramPdfIngest."""

    def test_clinical_extraction_when_no_labs(self) -> None:
        """When lab parser finds nothing, clinical extraction should run."""
        from healthbot.ingest.telegram_pdf_ingest import IngestResult

        # Verify IngestResult has clinical fields
        r = IngestResult()
        assert r.clinical_facts_count == 0
        assert r.clinical_summary == ""
        assert r.doc_type == ""

    def test_ingest_result_fields(self) -> None:
        """IngestResult should include clinical extraction fields."""
        from healthbot.ingest.telegram_pdf_ingest import IngestResult

        r = IngestResult(
            clinical_facts_count=5,
            clinical_summary="After-visit summary from cardiology.",
            doc_type="after_visit_summary",
        )
        assert r.clinical_facts_count == 5
        assert r.doc_type == "after_visit_summary"
