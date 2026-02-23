"""Ollama-based PII detection -- 3rd anonymization layer.

Runs locally via Ollama. Optional enhancement layer on top of:
  Layer 1: GLiNER NER (contextual PII)
  Layer 2: PhiFirewall regex (health-specific patterns)

This layer uses an LLM to catch subtle, context-dependent PII that
pattern matching misses. Only runs at ingestion time (not on every query)
because it's slow. Graceful fallback: if Ollama is unavailable, returns
no findings.
"""
from __future__ import annotations

import json
import logging

logger = logging.getLogger("healthbot")

# Prompt proven to work in export/ai_export.py Layer 3
_LLM_PII_PROMPT = (
    "You are a PII detection system. Scan the following text for "
    "personally identifiable information:\n"
    "- Full names (first + last), exact date of birth, SSN, MRN\n"
    "- Phone numbers, email addresses, physical addresses\n"
    "- Insurance/member IDs, doctor/provider names, lab facility names\n\n"
    "The following are NOT PII and should be IGNORED:\n"
    "- Lab collection dates (e.g. 2024-11-15)\n"
    "- Age values (e.g. 34, age 40s)\n"
    "- Medication names and generic labels like 'Provider A'\n"
    "- [REDACTED-*] tags (already redacted)\n"
    "- Medical test names, gene names, supplement names\n\n"
    "Respond ONLY with JSON, no other text:\n"
    '{"found": false}\n'
    "or\n"
    '{"found": true, "items": [{"text": "exact text", "type": "name"}]}'
)


class OllamaAnonymizationLayer:
    """Ollama-based PII detection -- 3rd anonymization layer.

    Runs locally. If Ollama is unavailable, returns no findings (no-op).
    Intended for use at ingestion time only (slow, ~2-5s per call).
    """

    def __init__(self, ollama_client: object) -> None:
        """Accept an OllamaClient instance for local LLM calls."""
        self._ollama = ollama_client

    def scan(self, text: str) -> list[tuple[int, int, str]]:
        """Scan text for PII via Ollama LLM.

        Returns list of (start, end, tag) spans found by the LLM.
        Returns empty list if Ollama is unavailable or on error.
        """
        if not text or not text.strip():
            return []

        if not self._ollama.is_available():
            return []

        try:
            response = self._ollama.send(
                prompt=f"Scan this text for PII:\n\n{text}",
                system=_LLM_PII_PROMPT,
            )
            result = _parse_llm_response(response)

            if not result.get("found"):
                return []

            spans: list[tuple[int, int, str]] = []
            for item in result.get("items", []):
                pii_text = item.get("text", "")
                pii_type = item.get("type", "unknown")
                if not pii_text:
                    continue
                # Find all occurrences in original text
                start = 0
                while True:
                    idx = text.find(pii_text, start)
                    if idx == -1:
                        break
                    spans.append((idx, idx + len(pii_text), f"LLM-{pii_type}"))
                    start = idx + 1

            return spans

        except Exception as e:
            logger.warning("Ollama PII scan failed (non-fatal): %s", e)
            return []

    def scan_batch(
        self, texts: list[str], *, max_batch: int = 10,
    ) -> list[list[tuple[int, int, str]]]:
        """Scan multiple texts in a single Ollama call.

        Concatenates texts with numbered delimiters, sends one prompt,
        parses per-section results back to individual span lists.
        Falls back to individual scan() calls if batch parse fails.
        """
        if not texts:
            return []
        if not self._ollama.is_available():
            return [[] for _ in texts]

        # Small batches — just scan individually
        if len(texts) <= 2:
            return [self.scan(t) for t in texts]

        results: list[list[tuple[int, int, str]]] = []
        for batch_start in range(0, len(texts), max_batch):
            chunk = texts[batch_start:batch_start + max_batch]
            batch_result = self._scan_batch_chunk(chunk)
            results.extend(batch_result)
        return results

    def _scan_batch_chunk(
        self, texts: list[str],
    ) -> list[list[tuple[int, int, str]]]:
        """Scan a single batch chunk (up to max_batch texts)."""
        # Build delimited input
        parts = []
        for i, text in enumerate(texts):
            parts.append(f"[TEXT_{i}]")
            parts.append(text.strip() if text else "")
        combined = "\n---\n".join(parts)

        batch_prompt = (
            "You are a PII detection system. The input contains multiple "
            "numbered text sections separated by ---. Scan each section for "
            "personally identifiable information.\n\n"
            "The following are NOT PII and should be IGNORED:\n"
            "- Lab collection dates, age values, medication names\n"
            "- [REDACTED-*] tags, medical test names, gene names\n\n"
            "Respond ONLY with JSON:\n"
            '{"sections": [\n'
            '  {"id": 0, "found": false},\n'
            '  {"id": 1, "found": true, "items": [{"text": "exact text", "type": "name"}]}\n'
            "]}"
        )

        try:
            response = self._ollama.send(
                prompt=f"Scan these texts for PII:\n\n{combined}",
                system=batch_prompt,
            )
            return self._parse_batch_response(response, texts)
        except Exception as e:
            logger.warning("Ollama batch scan failed, falling back: %s", e)
            return [self.scan(t) for t in texts]

    def _parse_batch_response(
        self, response: str, texts: list[str],
    ) -> list[list[tuple[int, int, str]]]:
        """Parse batch response into per-text span lists."""
        result = _parse_llm_response(response)
        sections = result.get("sections", [])
        if not sections or len(sections) != len(texts):
            # Fallback: individual scans
            logger.debug("Batch parse mismatch, falling back to individual scans")
            return [self.scan(t) for t in texts]

        all_spans: list[list[tuple[int, int, str]]] = []
        for i, text in enumerate(texts):
            section = next((s for s in sections if s.get("id") == i), None)
            if not section or not section.get("found"):
                all_spans.append([])
                continue
            spans: list[tuple[int, int, str]] = []
            for item in section.get("items", []):
                pii_text = item.get("text", "")
                pii_type = item.get("type", "unknown")
                if not pii_text:
                    continue
                start = 0
                while True:
                    idx = text.find(pii_text, start)
                    if idx == -1:
                        break
                    spans.append((idx, idx + len(pii_text), f"LLM-{pii_type}"))
                    start = idx + 1
            all_spans.append(spans)
        return all_spans


def _parse_llm_response(response: str) -> dict:
    """Extract JSON from LLM response, handling markdown code blocks."""
    text = response.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(ln for ln in lines if not ln.strip().startswith("```")).strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Fallback: extract first {...} substring
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not parse LLM PII response as JSON: {text[:100]}")
