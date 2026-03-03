"""LLM-based lab result extraction via Ollama.

Uses a medical model (med42) for parsing lab reports. Falls back to
general model if med42 is unavailable. Parses JSON responses into
LabResult objects with confidence 0.85.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime

from healthbot.data.models import LabResult
from healthbot.ingest.lab_pdf_parser.helpers import DATE_FORMATS
from healthbot.ingest.lab_pdf_parser.regex_parser import (
    _BAD_TEST_NAMES,
    _COMMA_TEST_NAMES,
    _HAS_LETTER,
)
from healthbot.normalize.lab_normalizer import normalize_test_name

logger = logging.getLogger("healthbot")

# Medical model for parsing — falls back to default if unavailable
_MED_MODEL = "thewindmom/llama3-med42-70b"

# System prompt for Med42 lab parsing — provider-aware, handles garbled PDF text
_LAB_PARSE_SYSTEM = """\
You are a medical lab report parser with expertise in clinical laboratory medicine.
Extract ALL lab test results from the provided text into a structured JSON array.

For EACH result, provide:
- test_name: The exact test name as printed (e.g., "Hemoglobin A1c", "TSH")
- value: The numeric result value (number only, no units)
- unit: The unit of measurement (e.g., "mg/dL", "g/dL", "%", "mIU/L")
- reference_low: Lower bound of reference range (number or null)
- reference_high: Upper bound of reference range (number or null)
- flag: "H" for high, "L" for low, "" if normal or not flagged

Reference range formats to handle:
- "70-100" → low=70, high=100
- "< 200" → low=null, high=200
- "> 40" → low=40, high=null
- "3.5 - 5.0" → low=3.5, high=5.0
- "<= 5.7" → low=null, high=5.7
- "0.0-0.4 ng/mL" → low=0.0, high=0.4 (ignore embedded unit)

IMPORTANT — PDF text extraction often garbles the layout:
- Column headers (TESTS, RESULT, FLAG, UNITS, REFERENCE INTERVAL, LAB) may be \
separated from their values
- Test names and values may appear on different lines or out of order
- You must reconstruct the logical rows using your medical knowledge
- Common lab panels to expect: CBC with Differential (WBC, RBC, Hemoglobin, \
Hematocrit, MCV, MCH, MCHC, RDW, Platelets, Neutrophils, Lymphocytes, \
Monocytes, Eosinophils, Basophils — both % and absolute counts), \
CMP/Comprehensive Metabolic Panel (Glucose, BUN, Creatinine, eGFR, Sodium, \
Potassium, Chloride, CO2, Calcium, Total Protein, Albumin, Globulin, A/G Ratio, \
Bilirubin, Alkaline Phosphatase, AST, ALT), Lipid Panel (Total Cholesterol, \
HDL, LDL, Triglycerides, Non-HDL, VLDL), Thyroid (TSH, Free T4, Free T3), \
A1c, Iron Panel (Iron, Ferritin, TIBC, Transferrin Saturation)
- LabCorp format: columns are TESTS | RESULT | FLAG | UNITS | REFERENCE INTERVAL | LAB
- Quest format: similar columns, often with specimen ID at the end (e.g., "01")

IGNORE these — they are NOT lab results:
- Patient name, DOB, account number, specimen ID
- Page numbers, headers, footers, lab site info
- Section headers like "CBC With Differential/Platelet", "Comp. Metabolic Panel (14)"

Rules:
- Extract EVERY test result, including ALL items in panels (CBC, CMP, lipid, etc.)
- A typical CBC has ~21 results, a CMP has ~14. If you find far fewer, look harder.
- Include ALL results even if they appear normal
- Do NOT skip results just because they lack flags
- For standard tests, the value MUST be numeric (number only, no units)
- For molecular/genetic tests (JAK2, CALR, BCR-ABL, Factor V Leiden, MTHFR, \
BRAF, KRAS, EGFR mutations), infectious disease screens (HBsAg, HCV antibody, \
HIV, RPR, COVID PCR, influenza, strep), and other qualitative tests (HLA-B27, \
urine drug screen), use the text value exactly as printed \
(e.g. "Not Detected", "Positive", "Negative", "Wild Type", "Heterozygous")
- For qualitative tests, include reference_text (the expected/normal result, \
e.g. "Not Detected") and interpretation (full interpretation text if present)
- If a result has no reference range, set reference_low and reference_high to null
- Do NOT fabricate or estimate values — only extract what is printed

Also include a metadata object as the LAST item in the array with:
- "_type": "metadata"
- "collection_date": The specimen collection date (YYYY-MM-DD format, or null)
- "lab_name": The lab provider name (e.g., "LabCorp", "Quest Diagnostics", or null)

Return ONLY a JSON array, no other text. If no results, return []"""


class OllamaParserMixin:
    """Mixin providing LLM-based lab result extraction via Ollama."""

    def _ollama_parse_pages(
        self, pages: list[str], blob_id: str,
    ) -> list[LabResult]:
        """Parse lab text with Ollama in a single call.

        Uses medical model (med42) for accuracy — labs are critical.
        Falls back to general model if med42 unavailable.
        Also extracts collection date metadata from the response.
        """
        try:
            from healthbot.llm.ollama_client import OllamaClient

            kwargs = {"retry_count": 0, "timeout": 60}
            if self._config:
                base_url = getattr(self._config, "ollama_url", None)
                if base_url:
                    kwargs["base_url"] = base_url
                ollama_timeout = getattr(self._config, "ollama_timeout", None)
                if ollama_timeout:
                    kwargs["timeout"] = ollama_timeout
            ollama = OllamaClient(**kwargs)

            # Accuracy first: med42 > general (never use fast for labs)
            model = None
            for candidate in [_MED_MODEL, "llama3.3:70b-instruct-q4_K_M"]:
                if ollama.is_available(model=candidate):
                    model = candidate
                    break
            if model is None:
                logger.info("No medical/general model available for lab parsing; skipping Ollama")
                return []

            # Combine all pages into single prompt (faster than N calls)
            combined = []
            for page_num, page_text in enumerate(pages, 1):
                page_text = page_text.strip()
                if len(page_text) < 30:
                    continue
                combined.append(f"--- Page {page_num} ---\n{page_text}")

            if not combined:
                return []

            full_text = "\n\n".join(combined)
            # Truncate to ~12K chars (at line boundary) for context/speed
            if len(full_text) > 12000:
                cut = full_text.rfind("\n", 0, 12000)
                full_text = full_text[:cut] if cut > 0 else full_text[:12000]

            is_markdown = "| " in full_text[:200]
            logger.info(
                "Sending %d chars to Ollama (%s), model=%s",
                len(full_text),
                "markdown table" if is_markdown else "raw text",
                model,
            )

            prompt = (
                f"Extract ALL lab test results from this report:\n\n"
                f"{full_text}"
            )
            response = ollama.send(
                prompt=prompt,
                system=_LAB_PARSE_SYSTEM,
                model=model,
            )
            results, metadata = self._parse_ollama_response(response, blob_id)

            # Use Ollama-extracted collection date if available
            if metadata.get("collection_date"):
                self._ollama_collection_date = metadata["collection_date"]

            return results

        except Exception as exc:
            logger.warning("Ollama PDF parsing failed: %s", exc)
            return []

    def _parse_ollama_response(
        self, text: str, blob_id: str, page_num: int = 0,
    ) -> tuple[list[LabResult], dict]:
        """Parse Ollama's JSON response into LabResult objects + metadata."""
        metadata: dict = {}
        text = text.strip()
        start = text.find("[")
        end = text.rfind("]")
        if start == -1 or end == -1:
            return [], metadata

        try:
            items = json.loads(text[start : end + 1])
        except json.JSONDecodeError as exc:
            logger.warning(
                "Ollama returned invalid JSON: %s (first 200 chars: %s)",
                exc, text[:200],
            )
            return [], metadata

        results: list[LabResult] = []
        for item in items:
            if not isinstance(item, dict):
                continue

            # Extract metadata object (collection_date, lab_name)
            if item.get("_type") == "metadata":
                cdate = item.get("collection_date")
                if cdate:
                    for fmt in DATE_FORMATS:
                        try:
                            metadata["collection_date"] = (
                                datetime.strptime(cdate, fmt).date()
                            )
                            logger.info(
                                "LLM metadata collection_date: %s",
                                metadata["collection_date"],
                            )
                            break
                        except ValueError:
                            continue
                    if "collection_date" not in metadata:
                        logger.info(
                            "LLM returned unparseable collection_date: %r",
                            cdate,
                        )
                else:
                    logger.info("LLM metadata has no collection_date")
                if item.get("lab_name"):
                    metadata["lab_name"] = item["lab_name"]
                continue

            test_name = item.get("test_name", "")
            value_raw = item.get("value")
            if not test_name or value_raw is None:
                continue

            # Apply same blocklist as regex path — catches LLM hallucinations
            # like "Page", patient names, PDF metadata
            if not _HAS_LETTER.search(test_name):
                continue
            if _BAD_TEST_NAMES.search(test_name):
                if test_name.strip().lower() not in _COMMA_TEST_NAMES:
                    logger.debug("Ollama: blocked bad test name: %s", test_name)
                    continue

            try:
                value: float | str = float(value_raw)
            except (ValueError, TypeError):
                value = str(value_raw)

            canonical = normalize_test_name(test_name)

            # Capture reference_text and interpretation from LLM response
            ref_text = str(item.get("reference_text", "") or "")
            interpretation = str(item.get("interpretation", "") or "")
            if interpretation:
                ref_text = (ref_text + " | " + interpretation).strip(" |")

            # Compute flag for qualitative results as fallback
            flag = self._normalize_flag(str(item.get("flag", "")))
            if not flag and isinstance(value, str):
                from healthbot.normalize.lab_normalizer import (
                    compute_qualitative_flag,
                )
                flag = compute_qualitative_flag(value, ref_text)

            results.append(LabResult(
                id=uuid.uuid4().hex,
                test_name=test_name,
                canonical_name=canonical,
                value=value,
                unit=item.get("unit", ""),
                reference_low=self._safe_float(item.get("reference_low")),
                reference_high=self._safe_float(item.get("reference_high")),
                reference_text=ref_text,
                flag=flag,
                source_blob_id=blob_id,
                source_page=page_num,
                confidence=0.85,
            ))
        return results, metadata
