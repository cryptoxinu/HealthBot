"""Lab report PDF parsing.

Extracts structured lab results from PDF lab reports using pdfminer.six.
Dual extraction: regex patterns (fast, deterministic) + Ollama LLM (catches
non-standard formats). Results are merged — union of both, deduped by
canonical name + page. This maximizes accuracy at the cost of one LLM call.
"""
from __future__ import annotations

import io
import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime

from pdfminer.high_level import extract_text

from healthbot.data.models import LabResult
from healthbot.normalize.lab_normalizer import normalize_test_name
from healthbot.security.pdf_safety import PdfSafety

logger = logging.getLogger("healthbot")

# Medical model for parsing — falls back to default if unavailable
_MED_MODEL = "thewindmom/llama3-med42-70b"


def _parse_numeric(value: str | None) -> float | None:
    """Extract numeric value from lab result string (handles <, >, >=, etc.)."""
    if not value:
        return None
    cleaned = re.sub(r"^[<>≤≥=]+\s*", "", str(value).strip())
    cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _values_match(a: str, b: str, rel_tol: float = 0.05, abs_tol: float = 0.5) -> bool:
    """Check if two lab values match within tolerance."""
    na, nb = _parse_numeric(a), _parse_numeric(b)
    if na is None or nb is None:
        # Non-numeric — exact string match
        return str(a).strip().lower() == str(b).strip().lower()
    if na == nb == 0:
        return True
    denom = max(abs(na), abs(nb), 1e-9)
    return abs(na - nb) / denom <= rel_tol and abs(na - nb) <= abs_tol


def _adjust_confidence(
    result: LabResult, conflicts: dict[str, dict],
) -> None:
    """Adjust result confidence based on cross-validation map."""
    info = conflicts.get(result.canonical_name)
    if not info:
        return
    if info["consensus"]:
        result.confidence = min(result.confidence + 0.05, 0.99)
    elif info.get("conflict_note"):
        result.confidence = max(result.confidence - 0.15, 0.50)


def _replace_result(
    results: list[LabResult], canonical_name: str, new: LabResult,
) -> None:
    """Replace a result in-place by canonical_name."""
    for i, r in enumerate(results):
        if r.canonical_name == canonical_name:
            results[i] = new
            return


@dataclass
class ParsedPage:
    page_number: int
    text: str


# Regex for lab result lines:
# TestName    Value    Unit    RefRange    Flag
_RESULT_PATTERNS = [
    # Pattern 1: Quest/LabCorp — Name Value Unit RefRange [SpecimenNum]
    # e.g. "WBC 8.2 x10E3/uL 3.4 - 10.8 01"
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{1,44}?)\s+"
        r"([\d.,]+)\s+"
        r"([\w/%·E]+(?:/[\w]+)?)\s+"              # unit (x10E3/uL, mg/dL, etc.)
        r"(\d[\d.]*\s*[-–]\s*\d[\d.]*)"           # reference range
        r"(?:\s+\d{1,2})?\s*$",                   # optional trailing specimen ID
        re.MULTILINE,
    ),
    # Pattern 2: Name Value Flag Unit RefRange [SpecimenNum]
    # e.g. "Creatinine 0.67 Low mg/dL 0.76 - 1.27 01"
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{1,44}?)\s+"
        r"([\d.,]+)\s+"
        r"([HLhl*]+|Low|High|low|high)\s+"         # flag
        r"([\w/%·E]+(?:/[\w]+)?)\s+"               # unit
        r"(\d[\d.]*\s*[-–]\s*\d[\d.]*)"            # reference range
        r"(?:\s+\d{1,2})?\s*$",
        re.MULTILINE,
    ),
    # Pattern 3: Wide-spaced columns (2+ spaces between name and value)
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{2,44}?)\s{2,}"
        r"([\d.,]+)\s+"
        r"([\w/%·E]+(?:/[\w]+)?)\s+"
        r"(.+?)$",
        re.MULTILINE,
    ),
    # Pattern 4: Name Value Unit [SpecimenNum] (no ref range — e.g. "Neutrophils 55 % 01")
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{1,44}?)\s+"
        r"([\d.,]+)\s+"
        r"([\w/%·E]+(?:/[\w]+)?)"
        r"(?:\s+\d{1,2})?\s*$",                   # optional trailing specimen ID
        re.MULTILINE,
    ),
]

# Reference range patterns
_REF_RANGE = re.compile(
    r"([<>=]*\s*[\d.]+)\s*[-–]\s*([<>=]*\s*[\d.]+)"
)
_REF_UPPER = re.compile(r"[<≤]\s*([\d.]+)")
_REF_LOWER = re.compile(r"[>≥]\s*([\d.]+)")

# Month names for text-based date patterns
_MONTH_NAMES = (
    r"(?:January|February|March|April|May|June|July|August"
    r"|September|October|November|December"
    r"|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)"
)

# Date patterns in lab headers — ordered by specificity
_DATE_PATTERNS = [
    # Explicit collection labels — numeric dates (highest priority)
    re.compile(
        r"(?:Collected|Collection\s+Date|Date\s+Collected|Specimen\s+Collected"
        r"|Date\s+(?:of\s+)?(?:Service|Report)|Date\s+Received)"
        r"\s*[:#]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        re.IGNORECASE,
    ),
    # Explicit collection labels — text-month dates
    re.compile(
        r"(?:Collected|Collection\s+Date|Date\s+Collected|Specimen\s+Collected"
        r"|Date\s+(?:of\s+)?(?:Service|Report)|Date\s+Received)"
        r"\s*[:#]?\s*(" + _MONTH_NAMES + r"\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # Report/result date labels — numeric
    re.compile(
        r"(?:Reported|Date\s+Reported|Result\s+Date|Date\s+of\s+Report)"
        r"\s*[:#]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        re.IGNORECASE,
    ),
    # Report/result date labels — text-month
    re.compile(
        r"(?:Reported|Date\s+Reported|Result\s+Date|Date\s+of\s+Report)"
        r"\s*[:#]?\s*(" + _MONTH_NAMES + r"\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # Generic "Date:" (but NOT DOB) — numeric
    re.compile(
        r"(?<!DOB\s)(?<!Birth\s)(?<!Born\s)"
        r"Date\s*[:#]\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        re.IGNORECASE,
    ),
    # Generic "Date:" (but NOT DOB) — text-month
    re.compile(
        r"(?<!DOB\s)(?<!Birth\s)(?<!Born\s)"
        r"Date\s*[:#]\s*(" + _MONTH_NAMES + r"\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # Bare dates (last resort)
    re.compile(r"(\d{1,2}/\d{1,2}/\d{4})"),
    re.compile(r"(\d{4}-\d{2}-\d{2})"),
    # Bare text-month dates (last resort)
    re.compile(r"(" + _MONTH_NAMES + r"\s+\d{1,2},?\s+\d{4})"),
]

# Dates near these labels are NOT collection dates
_DOB_LABELS = re.compile(
    r"(?:DOB|Date\s+of\s+Birth|Birth\s*Date|Born|Patient\s+DOB)",
    re.IGNORECASE,
)

# Lab name patterns
_LAB_NAME_PATTERNS = [
    re.compile(r"(Quest Diagnostics)", re.IGNORECASE),
    re.compile(r"(LabCorp|Laboratory Corporation)", re.IGNORECASE),
    re.compile(r"(MyChart|Epic)", re.IGNORECASE),
]

# Blocklist: strings the regex captures that are NOT valid test names
_BAD_TEST_NAMES = re.compile(
    r"^("
    r"mg/d[Ll]|g/d[Ll]|mmol/[Ll]|mIU/[Ll]|uIU/m[Ll]|ng/m[Ll]|pg/m[Ll]|"
    r"U/[Ll]|fL|pg|seconds|ratio|mm/hr|mEq/[Ll]|ug/d[Ll]|umol/[Ll]|IU/[Ll]|"
    r"unlabeled.*|result[s]?|reference|range|specimen|"
    r"see note|comment|note|continued|page.*|of\s*\d+|"
    r"collected|reported|ordered|received|"
    r"date collected|date reported|unit[s]?|flag|status|"
    r"previous result.*|current result.*|standard range|"
    r"date|final report|performing lab|patient|"
    r"fasting|non-?fasting|abnormal|normal|"
    r"in range|out of range|comp.*panel|panel|"
    r"lab\s*site|lab\s*\d*|report\s*status|client|"
    r"physician|doctor|provider|npi|account|"
    r"[a-z]+\s*,\s*[a-z]+\s*[a-z]*"
    r")$",
    re.IGNORECASE,
)
# Allowlist: "WORD, WORD" patterns that ARE valid lab tests (override blocklist)
_COMMA_TEST_NAMES = {
    "glucose, serum", "creatinine, serum", "bilirubin, total",
    "bilirubin, direct", "protein, total", "iron, total",
    "cholesterol, total", "testosterone, total", "testosterone, free",
    "calcium, ionized", "insulin, fasting",
}
_HAS_LETTER = re.compile(r"[a-zA-Z]")

# Unit validation — a valid lab unit must contain a letter or % or #
_VALID_UNIT = re.compile(r"[a-zA-Z%#·]")

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


class LabPdfParser:
    """Extract structured lab results from PDF reports.

    Dual extraction: regex (fast, Quest/LabCorp patterns) + Ollama LLM
    (single call, fast model). Both always run; results merged for accuracy.
    """

    def __init__(self, pdf_safety: PdfSafety, config: object | None = None) -> None:
        self._safety = pdf_safety
        self._config = config
        self._ollama_collection_date: date | None = None

    def extract_text_and_tables(self, data: bytes) -> tuple[str, str]:
        """Extract raw text and markdown tables from PDF without parsing.

        Returns (full_text, markdown_text). Used by Claude extraction
        to get content without running the full regex/Ollama pipeline.
        """
        _, markdown_text = self._parse_tables_direct(data, "")
        full_text = self._extract_text(data)
        return full_text or "", markdown_text

    def parse_bytes(
        self, data: bytes, blob_id: str = "",
        demographics: dict | None = None,
        on_progress: object | None = None,
    ) -> tuple[list[LabResult], str]:
        """Parse a lab PDF from bytes and return (results, extracted_text).

        Uses a multi-layer extraction pipeline:

        1. **Direct table parsing** — PyMuPDF ``find_tables()`` with column
           header detection maps cells directly to LabResult (confidence 0.95).
        2. **Regex** — four patterns for Quest/LabCorp text formats.
        3. **Ollama LLM** — medical model parses the text (or markdown table).
        4. **Three-way merge** — table > Ollama > regex, deduped by canonical name.
        """
        def _progress(msg: str) -> None:
            if on_progress:
                try:
                    on_progress(msg)
                except Exception:
                    pass

        self._safety.validate_bytes(data)

        # Stage 1: direct table extraction (highest confidence)
        _progress("Reading PDF tables...")
        table_results, markdown_text = self._parse_tables_direct(data, blob_id)

        # Stage 2: text extraction (for dates, regex, Ollama fallback)
        _progress("Extracting text from PDF...")
        full_text = self._extract_text(data)

        if not full_text and not table_results:
            return [], full_text or ""
        if not full_text:
            full_text = ""

        logger.debug(
            "PDF text extracted (%d chars). First 300: %s",
            len(full_text), full_text[:300],
        )

        # Stage 3: metadata (dates, lab name) from text
        collection_date = self._extract_date(
            full_text or markdown_text, demographics,
        )
        lab_name = self._extract_lab_name(full_text or markdown_text)

        # Stage 4: regex parsing (deterministic fallback)
        _progress("Parsing with regex patterns...")
        pages = full_text.split("\f") if full_text else []
        regex_results = self._regex_parse_all_pages(pages) if pages else []

        # Stage 5: Ollama parsing — send markdown table if available
        _progress("Running local AI analysis (may take a moment)...")
        self._ollama_collection_date = None
        ollama_input = markdown_text if markdown_text else full_text
        ollama_pages = ollama_input.split("\f") if ollama_input else []
        ollama_results = self._ollama_parse_pages(ollama_pages, blob_id)

        # Prefer Ollama-extracted collection date
        if self._ollama_collection_date:
            if self._validate_date(self._ollama_collection_date, demographics):
                collection_date = self._ollama_collection_date
                logger.info(
                    "Using Ollama-extracted collection date: %s",
                    collection_date,
                )

        # Stage 5.5: cross-validate and re-run Ollama on conflicts
        _progress("Cross-validating extraction results...")
        conflicts = self._find_conflicts(
            table_results, ollama_results, regex_results,
        )
        ollama_conflicts = {
            name for name, info in conflicts.items()
            if not info["consensus"]
            and "ollama" in info["sources"]
            and info["deterministic_value"] is not None
        }
        if ollama_conflicts and ollama_results:
            rerun = self._rerun_ollama_conflicts(
                ollama_pages, blob_id, ollama_conflicts,
            )
            for name in ollama_conflicts:
                det_val = conflicts[name]["deterministic_value"]
                orig_val = conflicts[name]["sources"]["ollama"]
                rerun_r = rerun.get(name)

                if rerun_r and _values_match(rerun_r.value, det_val):
                    # Ollama self-corrected → use corrected value
                    _replace_result(ollama_results, name, rerun_r)
                    conflicts[name]["consensus"] = True
                    conflicts[name]["conflict_note"] = None
                    logger.info(
                        "Ollama self-corrected %s: %s -> %s",
                        name, orig_val, rerun_r.value,
                    )
                elif rerun_r and _values_match(rerun_r.value, orig_val):
                    # Ollama consistent but disagrees → genuine ambiguity
                    conflicts[name]["conflict_note"] = (
                        f"Ollama consistently reads {orig_val}, "
                        f"but table/regex reads {det_val}"
                    )
                    logger.warning(
                        "Persistent conflict for %s: Ollama=%s vs "
                        "deterministic=%s",
                        name, orig_val, det_val,
                    )
                elif rerun_r is None:
                    # Ollama failed to re-parse → drop it, use deterministic
                    ollama_results = [
                        r for r in ollama_results
                        if r.canonical_name != name
                    ]
                    conflicts[name]["conflict_note"] = (
                        f"Ollama failed to re-parse {name}, "
                        f"using deterministic"
                    )
                    logger.warning(
                        "Ollama re-run failed for %s, "
                        "using deterministic=%s",
                        name, det_val,
                    )
                else:
                    # Ollama gave third value → unreliable, drop it
                    ollama_results = [
                        r for r in ollama_results
                        if r.canonical_name != name
                    ]
                    conflicts[name]["conflict_note"] = (
                        f"Ollama unreliable for {name} (3 different "
                        f"values), using deterministic"
                    )
                    logger.warning(
                        "Ollama unreliable for %s: run1=%s, run2=%s, "
                        "deterministic=%s",
                        name, orig_val, rerun_r.value, det_val,
                    )

        # Stage 6: three-way merge (with cross-validation confidence)
        if table_results:
            results = self._merge_three_way(
                table_results, ollama_results, regex_results, conflicts,
            )
            logger.info(
                "Table: %d, Ollama: %d, regex: %d, merged: %d results",
                len(table_results), len(ollama_results),
                len(regex_results), len(results),
            )
        elif ollama_results:
            results = self._merge_three_way(
                [], ollama_results, regex_results, conflicts,
            )
            logger.info(
                "Ollama: %d, regex: %d, merged: %d results",
                len(ollama_results), len(regex_results), len(results),
            )
        else:
            # Regex-only: still apply any conflict adjustments
            results = self._merge_three_way(
                [], [], regex_results, conflicts,
            )
            logger.info("Regex-only: %d results", len(results))

        # Stage 7: image extraction (fills gaps missed by text extraction)
        try:
            image_results = self._extract_from_images(data, blob_id)
        except Exception as exc:
            logger.debug("Image extraction failed: %s", exc)
            image_results = []
        if image_results:
            existing_names = {r.canonical_name for r in results}
            new_image = [
                r for r in image_results
                if r.canonical_name not in existing_names
            ]
            results.extend(new_image)
            logger.info(
                "Image extraction: %d total, %d new (not in text)",
                len(image_results), len(new_image),
            )

        # Self-validation warning
        combined_len = len(full_text) + len(markdown_text)
        if combined_len > 500 and len(results) < 3:
            logger.warning(
                "EXTRACTION WARNING: only %d results from %d chars of text. "
                "First 500 chars:\n%s",
                len(results), combined_len,
                (full_text or markdown_text)[:500],
            )

        # Stamp metadata on all results
        for r in results:
            r.date_collected = collection_date
            r.lab_name = lab_name
            r.source_blob_id = blob_id

        return results, full_text or markdown_text

    def _parse_tables_direct(
        self, data: bytes, blob_id: str,
    ) -> tuple[list[LabResult], str]:
        """Extract lab results directly from PDF table structure.

        Returns ``(results, markdown_text)``.  *results* are high-confidence
        LabResult objects mapped from table cells.  *markdown_text* is a
        markdown rendering of all tables for Ollama consumption.
        """
        try:
            from healthbot.ingest.lab_table_parser import (
                extract_tables_multi_strategy,
                identify_columns,
                infer_columns_from_content,
                parse_table_direct,
                tables_to_markdown,
            )
        except ImportError:
            return [], ""

        all_results: list[LabResult] = []

        try:
            all_tables = extract_tables_multi_strategy(data)
        except Exception as exc:
            logger.debug("Table extraction failed: %s", exc)
            all_tables = []

        for rows, page_num in all_tables:
            if not rows or len(rows) < 2:
                continue

            # Try identifying columns from header row
            col_map = identify_columns(rows[0])
            header_idx = 0

            # Header might be row 1 (row 0 is a section title)
            if col_map is None and len(rows) >= 3:
                col_map = identify_columns(rows[1])
                if col_map is not None:
                    header_idx = 1

            # Fallback: infer columns from cell content
            if col_map is None:
                col_map = infer_columns_from_content(rows[1:])
                if col_map is not None:
                    header_idx = 0  # no header row to skip

            if col_map is None:
                logger.debug(
                    "Page %d: table with %d rows — columns not identified",
                    page_num, len(rows),
                )
                continue

            logger.info(
                "Page %d: table columns identified: %s",
                page_num, col_map,
            )
            table_results = parse_table_direct(
                rows, col_map, page_num, blob_id, header_row_idx=header_idx,
            )
            all_results.extend(table_results)

        # Generate markdown for Ollama
        try:
            markdown_text = tables_to_markdown(data)
        except Exception:
            markdown_text = ""

        if all_results:
            logger.info(
                "Direct table parsing: %d results extracted", len(all_results),
            )

        return all_results, markdown_text

    @staticmethod
    def _merge_three_way(
        table_results: list[LabResult],
        ollama_results: list[LabResult],
        regex_results: list[LabResult],
        conflicts: dict[str, dict] | None = None,
    ) -> list[LabResult]:
        """Merge results from three extraction methods.

        Priority: table (0.95) > Ollama (0.85) > regex (0.60).
        Deduplication by canonical_name.
        Confidence adjusted by cross-validation conflicts map.
        """
        seen: set[str] = set()
        merged: list[LabResult] = []
        conflicts = conflicts or {}

        # Table results first (highest confidence)
        for r in table_results:
            if r.canonical_name not in seen:
                _adjust_confidence(r, conflicts)
                merged.append(r)
                seen.add(r.canonical_name)

        # Ollama supplements
        for r in ollama_results:
            if r.canonical_name not in seen:
                _adjust_confidence(r, conflicts)
                merged.append(r)
                seen.add(r.canonical_name)

        # Regex fills remaining gaps
        for r in regex_results:
            if r.canonical_name not in seen:
                r.confidence = 0.60
                _adjust_confidence(r, conflicts)
                merged.append(r)
                seen.add(r.canonical_name)

        return merged

    @staticmethod
    def _find_conflicts(
        table_results: list[LabResult],
        ollama_results: list[LabResult],
        regex_results: list[LabResult],
    ) -> dict[str, dict]:
        """Compare values across extraction methods for same test.

        Returns: {canonical_name: {
            "sources": {"table": val, "ollama": val, "regex": val},
            "consensus": bool,
            "conflict_note": str | None,
            "deterministic_value": str | None,
        }}
        """
        # Build lookups by canonical_name
        table_map = {r.canonical_name: r.value for r in table_results}
        ollama_map = {r.canonical_name: r.value for r in ollama_results}
        regex_map = {r.canonical_name: r.value for r in regex_results}

        all_names = set(table_map) | set(ollama_map) | set(regex_map)
        result: dict[str, dict] = {}

        for name in all_names:
            sources: dict[str, str] = {}
            if name in table_map:
                sources["table"] = table_map[name]
            if name in ollama_map:
                sources["ollama"] = ollama_map[name]
            if name in regex_map:
                sources["regex"] = regex_map[name]

            if len(sources) < 2:
                # Single source — no cross-validation possible
                continue

            # Compare all pairs
            vals = list(sources.values())
            consensus = True
            for i in range(len(vals)):
                for j in range(i + 1, len(vals)):
                    if not _values_match(vals[i], vals[j]):
                        consensus = False
                        break
                if not consensus:
                    break

            # Deterministic value: table and regex agree
            det_val = None
            if "table" in sources and "regex" in sources:
                if _values_match(sources["table"], sources["regex"]):
                    det_val = sources["table"]
            elif "table" in sources:
                det_val = sources["table"]
            elif "regex" in sources:
                det_val = sources["regex"]

            conflict_note = None
            if not consensus:
                conflict_note = (
                    "Extraction conflict: "
                    + ", ".join(f"{k}={v}" for k, v in sources.items())
                )

            result[name] = {
                "sources": sources,
                "consensus": consensus,
                "conflict_note": conflict_note,
                "deterministic_value": det_val,
            }

        return result

    def _rerun_ollama_conflicts(
        self,
        ollama_pages: list[str],
        blob_id: str,
        conflict_names: set[str],
    ) -> dict[str, LabResult]:
        """Re-run Ollama parse for conflicting tests only.

        One additional Ollama call (not per-test). Returns only results
        whose canonical_name is in conflict_names.
        """
        logger.info(
            "Re-running Ollama for %d conflicting tests", len(conflict_names),
        )
        rerun_results = self._ollama_parse_pages(ollama_pages, blob_id)
        return {
            r.canonical_name: r
            for r in rerun_results
            if r.canonical_name in conflict_names
        }

    def _extract_text(self, data: bytes) -> str:
        """Extract text from PDF using multiple strategies.

        Priority order (short-circuits when >= 3 lab lines found):
        1. PyMuPDF table extraction (structural — best for lab tables)
        2. PyMuPDF get_text (visual reading order)
        3. pdfminer default
        4. OCR fallback
        """
        best_text = ""
        best_count = 0

        def _score(text: str) -> int:
            """Count unique lines matching ANY result pattern."""
            matched_starts: set[int] = set()
            for pattern in _RESULT_PATTERNS:
                for m in pattern.finditer(text):
                    matched_starts.add(m.start())
            return len(matched_starts)

        # Strategy 1: PyMuPDF TABLE extraction — reads actual table structure
        try:
            from healthbot.ingest.lab_table_parser import (
                extract_tables_multi_strategy,
            )
            all_tables = extract_tables_multi_strategy(data)
            if all_tables:
                lines: list[str] = []
                for rows, _pn in all_tables:
                    for row in rows:
                        cells = [str(c).strip() for c in row if c is not None]
                        cells = [c for c in cells if c]
                        if cells:
                            lines.append("  ".join(cells))
                table_text = self._normalize_text("\n".join(lines))
                count = _score(table_text)
                logger.info(
                    "PDF strategy pymupdf-tables: %d lab lines in %d chars",
                    count, len(table_text),
                )
                if count >= 3:
                    return table_text
                if count > best_count:
                    best_text, best_count = table_text, count
        except ImportError:
            pass
        except Exception as e:
            logger.debug("PyMuPDF table extraction failed: %s", e)

        # Strategy 2: PyMuPDF text (visual reading order)
        try:
            import fitz
            doc = fitz.open(stream=data, filetype="pdf")
            pages = [page.get_text(sort=True) for page in doc]
            doc.close()
            text = self._normalize_text("\n\f\n".join(pages))
            if text.strip():
                count = _score(text)
                logger.info(
                    "PDF strategy pymupdf-text: %d lab lines in %d chars",
                    count, len(text),
                )
                if count >= 3:
                    return text
                if count > best_count:
                    best_text, best_count = text, count
        except ImportError:
            pass
        except Exception as e:
            logger.debug("PyMuPDF text failed: %s", e)

        # Strategy 2b: pymupdf4llm layout-aware extraction
        try:
            import pymupdf4llm
            md_text = pymupdf4llm.to_markdown(
                fitz.open(stream=data, filetype="pdf"),
            )
            if md_text:
                text = self._normalize_text(md_text)
                count = _score(text)
                logger.info(
                    "PDF strategy pymupdf4llm: %d lab lines in %d chars",
                    count, len(text),
                )
                if count >= 3:
                    return text
                if count > best_count:
                    best_text, best_count = text, count
        except ImportError:
            pass
        except Exception as e:
            logger.debug("pymupdf4llm extraction failed: %s", e)

        # Strategy 3: pdfminer default
        try:
            text = self._normalize_text(extract_text(io.BytesIO(data)))
            if text.strip():
                count = _score(text)
                logger.info(
                    "PDF strategy pdfminer: %d lab lines in %d chars",
                    count, len(text),
                )
                if count >= 3:
                    return text
                if count > best_count:
                    best_text, best_count = text, count
        except Exception as e:
            logger.debug("pdfminer failed: %s", e)

        # Strategy 4: OCR fallback
        if best_count < 3:
            try:
                from healthbot.ingest.ocr_fallback import ocr_pdf_bytes
                ocr_text = ocr_pdf_bytes(data)
                if ocr_text:
                    ocr_text = self._normalize_text(ocr_text)
                    count = _score(ocr_text)
                    logger.info(
                        "PDF strategy OCR: %d lab lines in %d chars",
                        count, len(ocr_text),
                    )
                    if count > best_count:
                        best_text, best_count = ocr_text, count
            except Exception as e:
                logger.debug("OCR failed: %s", e)

        logger.info(
            "PDF extraction: best strategy found %d lab lines", best_count,
        )
        return best_text

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

    def _regex_parse_all_pages(self, pages: list[str]) -> list[LabResult]:
        """Parse all pages with regex patterns."""
        results: list[LabResult] = []
        for page_num, page_text in enumerate(pages, 1):
            page_results = self._parse_result_lines(page_text, page_num)
            results.extend(page_results)
        return results

    @staticmethod
    def _merge_results(
        primary: list[LabResult], supplement: list[LabResult],
    ) -> list[LabResult]:
        """Merge regex supplement into Ollama primary results.

        Deduplicates by canonical_name only (not page) because Ollama
        returns all results with source_page=0 while regex has real page
        numbers. A lab report has one result per test — page doesn't matter.
        """
        seen = {r.canonical_name for r in primary}
        merged = list(primary)
        for r in supplement:
            if r.canonical_name not in seen:
                r.confidence = 0.6  # lower confidence for regex-only
                merged.append(r)
                seen.add(r.canonical_name)
        return merged

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Normalize text extracted from PDFs.

        Fixes common pdfminer issues: non-breaking spaces, stray control
        characters, and inconsistent whitespace that break regex patterns.
        """
        # Replace non-breaking spaces and other Unicode spaces with regular space
        text = text.replace("\u00a0", " ")
        text = text.replace("\u2007", " ")  # figure space
        text = text.replace("\u202f", " ")  # narrow no-break space
        # Normalize dashes: em-dash and figure dash to en-dash (our regex uses [-–])
        text = text.replace("\u2014", "\u2013")  # em-dash → en-dash
        text = text.replace("\u2012", "\u2013")  # figure dash → en-dash
        # Strip control characters (except newline, tab, form feed)
        text = re.sub(r"[^\x09\x0a\x0c\x20-\x7e\u00a0-\uffff]", "", text)
        # Normalize runs of spaces on each line (but preserve line structure)
        lines = text.split("\n")
        cleaned = []
        for line in lines:
            # Repair broken words: pdfminer often inserts 1-2 extra spaces
            # mid-word (e.g. "Specime  n" → "Specimen", "Patie  nt" →
            # "Patient").  Only rejoin when the right fragment is 1-2
            # chars — fragments that short are never standalone words.
            # Only rejoin when preceded by 3+ lowercase chars (mid-word
            # context) to avoid merging separate short column values.
            line = re.sub(
                r"(?<=[a-z]{3}) {1,2}(?=[a-z]{1,2}(?:[^a-zA-Z]|$))",
                "", line,
            )
            # Collapse runs of 3+ spaces to 2 (preserves Pattern 3 column detection)
            line = re.sub(r" {3,}", "  ", line)
            cleaned.append(line.rstrip())
        return "\n".join(cleaned)

    # Date formats to try when parsing extracted date strings
    _DATE_FORMATS = (
        "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d",
        "%B %d, %Y", "%B %d %Y",       # January 15, 2024
        "%b %d, %Y", "%b %d %Y",       # Jan 15, 2024
        "%d %B %Y", "%d %b %Y",        # 15 January 2024
    )

    def _extract_date(
        self, text: str, demographics: dict | None = None,
    ) -> date | None:
        """Extract collection date from lab report text.

        Tries patterns in priority order (explicit collection labels first).
        Validates each candidate: rejects future dates, dates > 20 years old,
        and dates matching the patient's DOB.
        """
        for pattern in _DATE_PATTERNS:
            for match in pattern.finditer(text):
                date_str = match.group(1).strip().rstrip(",")

                # Check if this date is near a DOB label (within 30 chars)
                start = max(0, match.start() - 30)
                context = text[start:match.start()]
                if _DOB_LABELS.search(context):
                    logger.debug(
                        "Skipping date %s — near DOB label", date_str,
                    )
                    continue

                for fmt in self._DATE_FORMATS:
                    try:
                        d = datetime.strptime(date_str, fmt).date()
                    except ValueError:
                        continue

                    if self._validate_date(d, demographics):
                        logger.info(
                            "Extracted collection date: %s (from %r)",
                            d.isoformat(), date_str,
                        )
                        return d
                    logger.debug(
                        "Skipping invalid date %s (failed validation)",
                        date_str,
                    )
        logger.info("No collection date found in text (%d chars)", len(text))
        return None

    @staticmethod
    def _validate_date(
        d: date, demographics: dict | None = None,
    ) -> bool:
        """Check if a date is a plausible collection date."""
        today = date.today()
        # Reject future dates
        if d > today:
            return False
        # Reject dates > 20 years old
        if (today - d).days > 365 * 20:
            return False
        # Reject if date matches patient DOB
        if demographics and demographics.get("dob"):
            try:
                dob = date.fromisoformat(str(demographics["dob"]))
                if d == dob:
                    return False
            except (ValueError, TypeError):
                pass
        return True

    def _extract_lab_name(self, text: str) -> str:
        """Extract lab provider name."""
        for pattern in _LAB_NAME_PATTERNS:
            match = pattern.search(text)
            if match:
                return match.group(1)
        return ""

    def _parse_result_lines(self, text: str, page_num: int) -> list[LabResult]:
        """Extract individual test results from page text via regex."""
        results: list[LabResult] = []
        seen_canonical: set[str] = set()

        for pattern in _RESULT_PATTERNS:
            for match in pattern.finditer(text):
                groups = match.groups()
                test_name = re.sub(r"\s{2,}", " ", groups[0].strip())

                # Filter: skip if test name is a unit, label, or has no letters
                if not _HAS_LETTER.search(test_name):
                    continue
                if _BAD_TEST_NAMES.search(test_name):
                    # Allow known comma-style lab names (e.g. "Glucose, Serum")
                    if test_name.strip().lower() not in _COMMA_TEST_NAMES:
                        continue

                value_str = groups[1].strip().replace(",", "")
                flag = ""

                if len(groups) == 5:
                    # Pattern with flag between value and unit
                    flag = groups[2].strip()
                    unit = groups[3].strip()
                    ref_text = groups[4].strip()
                elif len(groups) == 4:
                    unit = groups[2].strip()
                    last = groups[3].strip()
                    # Check if last group is a flag (H/L/Low/High) vs ref range
                    if re.fullmatch(r"[HLhl*]+|[Ll]ow|[Hh]igh", last):
                        flag = last
                        ref_text = ""
                    else:
                        # Clean trailing garbage from Pattern 3 (specimen IDs, etc.)
                        ref_text = re.sub(r"\s+\d{1,2}\s*$", "", last)
                elif len(groups) == 3:
                    # Value + unit only (no ref range, no flag)
                    unit = groups[2].strip()
                    ref_text = ""
                else:
                    continue

                # Reject pure-number "units" (specimen IDs like "12", "58")
                if not _VALID_UNIT.search(unit):
                    continue

                # Parse value
                try:
                    value: float | str = float(value_str)
                except ValueError:
                    value = value_str

                # Parse reference range
                ref_low, ref_high = self._parse_ref_range(ref_text)

                canonical = normalize_test_name(test_name)

                # Dedup across patterns (same test on same page)
                if canonical in seen_canonical:
                    continue
                seen_canonical.add(canonical)

                results.append(LabResult(
                    id=uuid.uuid4().hex,
                    test_name=test_name,
                    canonical_name=canonical,
                    value=value,
                    unit=unit,
                    reference_low=ref_low,
                    reference_high=ref_high,
                    reference_text=ref_text,
                    flag=self._normalize_flag(flag),
                    source_page=page_num,
                ))

        return results

    def _parse_ref_range(self, text: str) -> tuple[float | None, float | None]:
        """Parse reference range text into (low, high)."""
        # Range: "70-100" or "3.5 - 5.0"
        m = _REF_RANGE.search(text)
        if m:
            try:
                low = float(re.sub(r"[<>=\s]", "", m.group(1)))
                high = float(re.sub(r"[<>=\s]", "", m.group(2)))
                return low, high
            except ValueError:
                pass

        # Upper bound: "< 200" or "<= 5.7"
        m = _REF_UPPER.search(text)
        if m:
            try:
                return None, float(m.group(1))
            except ValueError:
                pass

        # Lower bound: "> 40"
        m = _REF_LOWER.search(text)
        if m:
            try:
                return float(m.group(1)), None
            except ValueError:
                pass

        return None, None

    @staticmethod
    def _normalize_flag(flag: str) -> str:
        """Normalize flag to single-letter H/L format."""
        if not flag:
            return ""
        f = flag.strip().upper()
        if f in ("LOW", "L"):
            return "L"
        if f in ("HIGH", "H"):
            return "H"
        # Keep other flags as-is (e.g., "*", "A")
        return f

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
                    for fmt in self._DATE_FORMATS:
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

    def _extract_from_images(
        self, data: bytes, blob_id: str,
    ) -> list[LabResult]:
        """Extract lab results from embedded images in the PDF.

        Runs image extraction → OCR → optional chart vision analysis.
        Parses OCR text through existing regex patterns.
        Image results get confidence capped at 0.50.
        """
        try:
            from healthbot.ingest.image_extractor import (
                analyze_chart_with_vision,
                extract_images_from_pdf,
                ocr_images,
            )
        except ImportError:
            return []

        images = extract_images_from_pdf(data)
        if not images:
            return []

        all_results: list[LabResult] = []

        # OCR pass
        ocr_results = ocr_images(images)
        for ocr_r in ocr_results:
            page_results = self._parse_result_lines(ocr_r.text, ocr_r.page_number)
            for r in page_results:
                r.confidence = 0.50  # lower than all text methods
            all_results.extend(page_results)

        # Vision pass (chart analysis) — required per plan
        try:
            ollama_url = "http://localhost:11434"
            ollama_timeout = 120
            if self._config:
                ollama_url = getattr(self._config, "ollama_url", ollama_url)
                ollama_timeout = getattr(self._config, "ollama_timeout", ollama_timeout)

            vision_results = analyze_chart_with_vision(
                images, ollama_url=ollama_url, timeout=ollama_timeout,
            )
            for vis_r in vision_results:
                page_results = self._parse_result_lines(vis_r.text, vis_r.page_number)
                for r in page_results:
                    r.confidence = 0.50
                all_results.extend(page_results)
        except Exception as e:
            logger.debug("Chart vision analysis skipped: %s", e)

        # Deduplicate within image results by canonical name
        seen: set[str] = set()
        deduped: list[LabResult] = []
        for r in all_results:
            if r.canonical_name not in seen:
                deduped.append(r)
                seen.add(r.canonical_name)

        return deduped

    @staticmethod
    def _safe_float(val: object) -> float | None:
        """Safely convert to float or return None."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
