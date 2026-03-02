"""Parse medication mentions with dose modifiers from natural language.

Handles phrases like "I take 10mg metformin but break it in half"
→ actual_dose = 5mg.  Deterministic (no LLM).
"""
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class ParsedMedication:
    name: str
    prescribed_dose: str  # "10mg" — what the pill says
    actual_dose: str  # "5mg" — what they actually take
    actual_dose_mg: float | None  # numeric in mg (converted from original unit)
    actual_dose_original: float | None  # numeric in original unit
    actual_dose_unit: str  # original unit (mg, mcg, g, iu, etc.)
    frequency: str  # "daily", "twice daily", etc.
    modifier: str  # "half", "quarter", "double", or ""
    raw_text: str


_HALF_PATTERN = re.compile(
    r"(?:break|cut|split|snap)\s+(?:it\s+)?in\s+half"
    r"|half\s+(?:a\s+)?(?:pill|tablet|dose)"
    r"|take\s+half",
    re.IGNORECASE,
)
_QUARTER_PATTERN = re.compile(
    r"(?:break|cut|split)\s+.{0,30}?in(?:to)?\s+(?:quarters?|fourths?|4)",
    re.IGNORECASE,
)
_DOUBLE_PATTERN = re.compile(
    r"(?:take|taking)\s+(?:two|2)\s+(?:pills?|tablets?)"
    r"|double\s+(?:the\s+)?dose",
    re.IGNORECASE,
)
_DOSE_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?)\s*(mg|mcg|g|ml|iu|units?)\b", re.IGNORECASE,
)
_FREQ_PATTERN = re.compile(
    r"(?:once|twice|three\s+times|1x|2x|3x)\s*(?:a\s+)?(?:day|daily|week|weekly)"
    r"|daily|every\s+(?:morning|night|day|other\s+day|\d+\s*hours?)"
    r"|at\s+(?:bedtime|night)"
    r"|(?:twice|2x)\s+daily"
    r"|three\s+times\s+daily"
    r"|\d+\s+times\s+(?:daily|a\s+day)"
    r"|q(?:6|8|12)h"
    r"|prn|as\s+needed",
    re.IGNORECASE,
)


def parse_medication(text: str) -> ParsedMedication:
    """Parse a medication mention with dose modifiers."""
    # Extract dose
    dose_match = _DOSE_PATTERN.search(text)
    prescribed_dose = dose_match.group(0) if dose_match else ""
    dose_value = float(dose_match.group(1)) if dose_match else None
    dose_unit = dose_match.group(2).lower() if dose_match else ""

    # Detect modifiers — only when medication context is present
    # (a dose was found or the text mentions pills/tablets/dose).
    # This prevents matching modifiers in unrelated text like
    # "break it in half" when talking about food.
    modifier = ""
    multiplier = 1.0
    has_med_context = dose_match is not None or re.search(
        r"\b(?:pill|tablet|capsule|dose|medication|medicine|drug|supplement)\b",
        text, re.IGNORECASE,
    )
    if has_med_context:
        if _HALF_PATTERN.search(text):
            modifier = "half"
            multiplier = 0.5
        elif _QUARTER_PATTERN.search(text):
            modifier = "quarter"
            multiplier = 0.25
        elif _DOUBLE_PATTERN.search(text):
            modifier = "double"
            multiplier = 2.0

    # Calculate actual dose in original units
    actual_in_original_unit = dose_value * multiplier if dose_value else None
    if actual_in_original_unit is not None:
        # Format nicely: strip trailing .0
        if actual_in_original_unit == int(actual_in_original_unit):
            actual_dose = f"{int(actual_in_original_unit)}{dose_unit}"
        else:
            actual_dose = f"{actual_in_original_unit}{dose_unit}"
    else:
        actual_dose = prescribed_dose

    # Convert to mg for standardized storage
    actual_dose_mg = _convert_to_mg(actual_in_original_unit, dose_unit)

    # Extract frequency
    freq_match = _FREQ_PATTERN.search(text)
    frequency = freq_match.group(0).strip() if freq_match else ""

    # Extract drug name
    name = _extract_drug_name(text, dose_match)

    return ParsedMedication(
        name=name,
        prescribed_dose=prescribed_dose,
        actual_dose=actual_dose,
        actual_dose_mg=actual_dose_mg,
        actual_dose_original=actual_in_original_unit,
        actual_dose_unit=dose_unit,
        frequency=frequency,
        modifier=modifier,
        raw_text=text,
    )


def _convert_to_mg(value: float | None, unit: str) -> float | None:
    """Convert a dose value to milligrams where a standard conversion exists.

    - mcg -> mg: divide by 1000
    - g   -> mg: multiply by 1000
    - mg  -> mg: no conversion
    - iu/units -> None (no standard mg equivalent)

    Returns None when the value is None or no meaningful conversion exists.
    """
    if value is None:
        return None
    unit_lower = unit.lower()
    if unit_lower == "mg":
        return value
    if unit_lower == "mcg":
        return value / 1000.0
    if unit_lower == "g":
        return value * 1000.0
    # IU, units, ml — no standard mg conversion
    return None


def _extract_drug_name(text: str, dose_match: re.Match | None) -> str:
    """Extract drug name from text, typically before the dose."""
    # Remove common prefixes
    cleaned = re.sub(
        r"^(?:i\s+take|taking|on|started|currently\s+on)\s+",
        "", text, flags=re.IGNORECASE,
    ).strip()

    # Take everything before the dose number
    if dose_match:
        # Find the match position in the cleaned text
        dm = _DOSE_PATTERN.search(cleaned)
        if dm:
            name_part = cleaned[:dm.start()].strip()
        else:
            name_part = cleaned[:dose_match.start()].strip()
    else:
        # No dose found — strip frequency and modifiers, take what remains
        stripped = _FREQ_PATTERN.sub("", cleaned)
        stripped = _HALF_PATTERN.sub("", stripped)
        stripped = _QUARTER_PATTERN.sub("", stripped)
        stripped = _DOUBLE_PATTERN.sub("", stripped)
        name_part = stripped.strip()
        if not name_part:
            words = cleaned.split()
            name_part = words[0] if words else ""

    # Clean trailing punctuation and conjunctions
    name_part = re.sub(
        r"\s+(?:but|and|at|in)\s*$", "", name_part, flags=re.IGNORECASE,
    ).strip()

    if not name_part and cleaned:
        name_part = cleaned.split()[0]
    return name_part
