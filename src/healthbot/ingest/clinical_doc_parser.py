"""Clinical document parser using local Ollama LLM.

Extracts structured medical facts from non-lab documents:
doctor's notes, after-visit summaries, discharge summaries,
radiology reports, prescriptions, referrals, etc.

All processing is local (Ollama on localhost). PHI never leaves the machine.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from healthbot.llm.ollama_client import OllamaClient

logger = logging.getLogger("healthbot")

_VALID_CATEGORIES = frozenset({
    "demographic", "condition", "medication",
    "pattern", "preference", "provider",
})

_VALID_DOC_TYPES = frozenset({
    "lab_report", "clinical_note", "after_visit_summary",
    "discharge_summary", "radiology_report", "prescription",
    "referral", "imaging_report", "general_medical",
})

EXTRACTION_PROMPT = """\
You are a medical document analyzer. Identify the document type and extract \
all durable medical facts.

Document types: lab_report, clinical_note, after_visit_summary, \
discharge_summary, radiology_report, prescription, referral, \
imaging_report, general_medical

Extract facts in these categories:
- condition: diagnoses, assessments, findings, symptoms
- medication: prescriptions, changes, discontinuations (include dose/frequency)
- demographic: age, sex, height, weight, vitals mentioned
- pattern: trends, observations ("blood pressure has been elevated")
- provider: referrals, follow-up instructions, specialist recommendations
- preference: patient preferences, allergies, dietary restrictions

Return a JSON object with these fields:
{
  "doc_type": "after_visit_summary",
  "facts": [
    {"category": "condition", "fact": "Diagnosed with hypertension"},
    {"category": "medication", "fact": "Started lisinopril 10mg daily"},
    {"category": "provider", "fact": "Referred to endocrinology, follow up in 3 months"}
  ],
  "summary": "After-visit summary from cardiology discussing new hypertension diagnosis."
}

Rules:
- Extract ALL durable medical information worth remembering long-term
- Include exact values, dates, dosages where available
- NEVER include patient names, SSN, MRN, DOB, addresses, or insurance IDs
- NEVER include doctor or provider names
- If no medical facts found, return empty facts array: []
- Return ONLY JSON, nothing else"""

# Max chars per chunk sent to Ollama
_CHUNK_LIMIT = 10000


@dataclass
class ClinicalExtraction:
    """Result of clinical document analysis."""

    doc_type: str = "general_medical"
    facts: list[dict] = field(default_factory=list)
    summary: str = ""


class ClinicalDocParser:
    """Extract structured medical facts from clinical documents via Ollama."""

    def __init__(self, ollama_client: OllamaClient) -> None:
        self._ollama = ollama_client

    def extract(self, text: str) -> ClinicalExtraction:
        """Analyze document text and extract structured medical facts.

        For long documents (>10k chars), splits into page-based chunks
        and extracts from each. Facts are deduplicated across chunks.

        Args:
            text: Extracted document text (from pdfminer/OCR).

        Returns:
            ClinicalExtraction with doc_type, facts, and summary.
        """
        if not text or len(text.strip()) < 50:
            return ClinicalExtraction()

        if not self._ollama.is_available():
            logger.warning("Ollama unavailable for clinical doc extraction")
            return ClinicalExtraction()

        chunks = self._split_into_chunks(text)
        all_facts: list[dict] = []
        doc_type = "general_medical"
        summary = ""

        for chunk in chunks:
            result = self._extract_chunk(chunk)
            if result.doc_type != "general_medical":
                doc_type = result.doc_type
            if result.summary and not summary:
                summary = result.summary
            all_facts.extend(result.facts)

        # Deduplicate facts across chunks
        deduped = self._dedup_facts(all_facts)

        return ClinicalExtraction(
            doc_type=doc_type,
            facts=deduped,
            summary=summary,
        )

    def _extract_chunk(self, text: str) -> ClinicalExtraction:
        """Extract from a single chunk of text."""
        try:
            prompt = f"Analyze this medical document:\n\n{text}"
            response = self._ollama.send(
                prompt=prompt,
                system=EXTRACTION_PROMPT,
            )
            return self._parse_response(response)
        except Exception as e:
            logger.warning("Clinical doc extraction failed: %s", e)
            return ClinicalExtraction()

    def _parse_response(self, text: str) -> ClinicalExtraction:
        """Parse Ollama's JSON response into ClinicalExtraction."""
        text = text.strip()

        # Find JSON object in response
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1:
            return ClinicalExtraction()

        try:
            data = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            logger.warning("Failed to parse clinical extraction JSON")
            return ClinicalExtraction()

        if not isinstance(data, dict):
            return ClinicalExtraction()

        doc_type = data.get("doc_type", "general_medical")
        if doc_type not in _VALID_DOC_TYPES:
            doc_type = "general_medical"

        raw_facts = data.get("facts", [])
        if not isinstance(raw_facts, list):
            raw_facts = []

        facts = []
        for f in raw_facts:
            if not isinstance(f, dict):
                continue
            category = f.get("category", "").lower().strip()
            fact_text = f.get("fact", "").strip()
            if category not in _VALID_CATEGORIES:
                continue
            if len(fact_text) < 5:
                continue
            facts.append({"category": category, "fact": fact_text})

        summary = str(data.get("summary", "")).strip()

        return ClinicalExtraction(
            doc_type=doc_type,
            facts=facts,
            summary=summary[:500],
        )

    @staticmethod
    def _split_into_chunks(text: str) -> list[str]:
        """Split text into chunks for processing.

        Splits on page breaks (form feed) and groups pages to stay
        under the chunk limit. Single-chunk documents are not split.
        """
        if len(text) <= _CHUNK_LIMIT:
            return [text]

        pages = text.split("\f")
        chunks: list[str] = []
        current: list[str] = []
        current_len = 0

        for page in pages:
            page = page.strip()
            if not page:
                continue
            if current_len + len(page) > _CHUNK_LIMIT and current:
                chunks.append("\n\n".join(current))
                current = []
                current_len = 0
            current.append(page)
            current_len += len(page)

        if current:
            chunks.append("\n\n".join(current))

        return chunks if chunks else [text[:_CHUNK_LIMIT]]

    @staticmethod
    def _dedup_facts(facts: list[dict], threshold: float = 0.85) -> list[dict]:
        """Remove near-duplicate facts from combined chunk results."""
        if not facts:
            return facts

        unique: list[dict] = []
        for fact in facts:
            text = fact.get("fact", "").lower().strip()
            is_dup = False
            for existing in unique:
                ex_text = existing.get("fact", "").lower().strip()
                if text == ex_text:
                    is_dup = True
                    break
                ratio = SequenceMatcher(None, text, ex_text).ratio()
                if ratio >= threshold:
                    # Keep the longer (more detailed) version
                    if len(fact.get("fact", "")) > len(existing.get("fact", "")):
                        existing["fact"] = fact["fact"]
                        existing["category"] = fact["category"]
                    is_dup = True
                    break
            if not is_dup:
                unique.append(dict(fact))

        return unique
