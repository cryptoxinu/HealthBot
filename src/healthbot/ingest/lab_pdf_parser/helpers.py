"""Helper utilities for lab PDF parsing.

Date extraction, lab name recognition, reference range parsing,
normalization utilities, and cross-validation support functions.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime

from healthbot.data.models import LabResult

logger = logging.getLogger("healthbot")


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

# Date formats to try when parsing extracted date strings
DATE_FORMATS = (
    "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d",
    "%B %d, %Y", "%B %d %Y",       # January 15, 2024
    "%b %d, %Y", "%b %d %Y",       # Jan 15, 2024
    "%d %B %Y", "%d %b %Y",        # 15 January 2024
)


class HelpersMixin:
    """Mixin providing date extraction, lab name recognition, and utilities."""

    # Date formats to try when parsing extracted date strings
    _DATE_FORMATS = DATE_FORMATS

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

    @staticmethod
    def _safe_float(val: object) -> float | None:
        """Safely convert to float or return None."""
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
