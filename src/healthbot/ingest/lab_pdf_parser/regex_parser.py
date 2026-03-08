"""Deterministic regex-based lab result extraction.

Four regex patterns handle Quest/LabCorp text formats. Results are parsed
into LabResult objects with confidence 0.60 (boosted by cross-validation).
"""
from __future__ import annotations

import logging
import re
import uuid

from healthbot.data.models import LabResult
from healthbot.normalize.lab_normalizer import normalize_test_name

logger = logging.getLogger("healthbot")


# Regex for lab result lines:
# TestName    Value    Unit    RefRange    Flag
_RESULT_PATTERNS = [
    # Pattern 1: Quest/LabCorp — Name Value Unit RefRange [SpecimenNum]
    # e.g. "WBC 8.2 x10E3/uL 3.4 - 10.8 01"
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{1,44}?)\s+"
        r"([<>]=?\s*[\d.,]+|[\d.,]+)\s+"           # value (supports < 0.5, >= 200)
        r"([\w/%·E]+(?:/[\w]+)?)\s+"               # unit (x10E3/uL, mg/dL, etc.)
        r"(\d[\d.]*\s*[-–]\s*\d[\d.]*)"            # reference range
        r"(?:\s+\d{1,2})?\s*$",                    # optional trailing specimen ID
        re.MULTILINE,
    ),
    # Pattern 2: Name Value Flag Unit RefRange [SpecimenNum]
    # e.g. "Creatinine 0.67 Low mg/dL 0.76 - 1.27 01"
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{1,44}?)\s+"
        r"([<>]=?\s*[\d.,]+|[\d.,]+)\s+"           # value (supports inequalities)
        r"([HLhl*]+|Low|High|low|high)\s+"          # flag
        r"([\w/%·E]+(?:/[\w]+)?)\s+"                # unit
        r"(\d[\d.]*\s*[-–]\s*\d[\d.]*)"             # reference range
        r"(?:\s+\d{1,2})?\s*$",
        re.MULTILINE,
    ),
    # Pattern 3: Wide-spaced columns (2+ spaces between name and value)
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{2,44}?)\s{2,}"
        r"([<>]=?\s*[\d.,]+|[\d.,]+)\s+"           # value (supports inequalities)
        r"([\w/%·E]+(?:/[\w]+)?)\s+"
        r"(\d[\d.]*\s*[-–]\s*[\d.]+(?:\s+\d{1,2})?|[HLhl*]+|Low|High)\s*$",
        re.MULTILINE,
    ),
    # Pattern 4: Name Value Unit [SpecimenNum] (no ref range — e.g. "Neutrophils 55 % 01")
    re.compile(
        r"^([A-Za-z][A-Za-z0-9 ,()/-]{1,44}?)\s+"
        r"([<>]=?\s*[\d.,]+|[\d.,]+)\s+"           # value (supports inequalities)
        r"([\w/%·E]+(?:/[\w]+)?)"
        r"(?:\s+\d{1,2})?\s*$",                    # optional trailing specimen ID
        re.MULTILINE,
    ),
]

# Reference range patterns
_REF_RANGE = re.compile(
    r"([<>=]*\s*[\d.]+)\s*[-–]\s*([<>=]*\s*[\d.]+)"
)
_REF_UPPER = re.compile(r"[<≤]\s*([\d.]+)")
_REF_LOWER = re.compile(r"[>≥]\s*([\d.]+)")

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


class RegexParserMixin:
    """Mixin providing deterministic regex-based lab result extraction."""

    def _regex_parse_all_pages(self, pages: list[str]) -> list[LabResult]:
        """Parse all pages with regex patterns."""
        results: list[LabResult] = []
        for page_num, page_text in enumerate(pages, 1):
            page_results = self._parse_result_lines(page_text, page_num)
            results.extend(page_results)
        return results

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

                # Parse value — strip inequality prefix for numeric, keep as string
                numeric_str = re.sub(r"^[<>]=?\s*", "", value_str)
                try:
                    value: float | str = float(numeric_str)
                    # Preserve inequality prefix in string form if present
                    if numeric_str != value_str:
                        value = value_str
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
