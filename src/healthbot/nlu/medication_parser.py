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
    actual_dose_mg: float | None  # 5.0 — numeric for storage
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
    r"|daily|every\s+(?:morning|night|day|other\s+day)"
    r"|at\s+(?:bedtime|night)"
    r"|(?:twice|2x)\s+daily",
    re.IGNORECASE,
)


def parse_medication(text: str) -> ParsedMedication:
    """Parse a medication mention with dose modifiers."""
    # Extract dose
    dose_match = _DOSE_PATTERN.search(text)
    prescribed_dose = dose_match.group(0) if dose_match else ""
    dose_value = float(dose_match.group(1)) if dose_match else None
    dose_unit = dose_match.group(2).lower() if dose_match else ""

    # Detect modifiers
    modifier = ""
    multiplier = 1.0
    if _HALF_PATTERN.search(text):
        modifier = "half"
        multiplier = 0.5
    elif _QUARTER_PATTERN.search(text):
        modifier = "quarter"
        multiplier = 0.25
    elif _DOUBLE_PATTERN.search(text):
        modifier = "double"
        multiplier = 2.0

    # Calculate actual dose
    actual_mg = dose_value * multiplier if dose_value else None
    if actual_mg is not None:
        # Format nicely: strip trailing .0
        if actual_mg == int(actual_mg):
            actual_dose = f"{int(actual_mg)}{dose_unit}"
        else:
            actual_dose = f"{actual_mg}{dose_unit}"
    else:
        actual_dose = prescribed_dose

    # Extract frequency
    freq_match = _FREQ_PATTERN.search(text)
    frequency = freq_match.group(0).strip() if freq_match else ""

    # Extract drug name
    name = _extract_drug_name(text, dose_match)

    return ParsedMedication(
        name=name,
        prescribed_dose=prescribed_dose,
        actual_dose=actual_dose,
        actual_dose_mg=actual_mg,
        frequency=frequency,
        modifier=modifier,
        raw_text=text,
    )


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
