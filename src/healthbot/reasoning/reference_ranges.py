"""Default reference ranges for common lab tests.

Data loaded from data/reference_ranges.json.
Ranges are population-standard adult values with age, sex, and ethnicity adjustments.
"""
from __future__ import annotations

import json
from pathlib import Path

_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "reference_ranges.json"


def _load() -> dict:
    with open(_DATA_PATH) as f:
        return json.load(f)


_data = _load()

# canonical_name -> {low, high, unit, note}
DEFAULT_RANGES: dict[str, dict] = _data["default_ranges"]

# (from_unit, to_unit) -> multiplier
UNIT_CONVERSIONS: dict[tuple[str, str], float] = {
    tuple(k.split("|")): v for k, v in _data["unit_conversions"].items()
}

FASTING_TESTS: set[str] = set(_data["fasting_tests"])

# canonical_name -> list of (age_min, age_max, {low, high, unit, ...})
AGE_STRATIFIED_RANGES: dict[str, list[tuple[int, int, dict]]] = {
    lab: [
        (b["age_min"], b["age_max"],
         {k: v for k, v in b.items() if k not in ("age_min", "age_max")})
        for b in brackets
    ]
    for lab, brackets in _data["age_stratified_ranges"].items()
}

_FEMALE_AGE_OVERRIDES: dict[str, list[tuple[int, int, dict]]] = {
    lab: [
        (b["age_min"], b["age_max"],
         {k: v for k, v in b.items() if k not in ("age_min", "age_max")})
        for b in brackets
    ]
    for lab, brackets in _data["female_age_overrides"].items()
}

_FEMALE_OVERRIDES: dict[str, dict] = _data["female_overrides"]

ETHNICITY_ADJUSTMENTS: dict[str, dict[str, dict]] = _data["ethnicity_adjustments"]
ETHNICITY_RISK_FLAGS: dict[str, list[str]] = _data["ethnicity_risk_flags"]
_ETHNICITY_ALIASES: dict[str, str] = _data["ethnicity_aliases"]

# Functional medicine ranges (narrower "optimal" windows)
# These are NOT diagnostic thresholds — label as "functional medicine perspective".
FUNCTIONAL_RANGES: dict[str, dict] = _data.get("functional_ranges", {})

del _data  # Free memory after hydration


# ---------------------------------------------------------------------------
# Lookup functions
# ---------------------------------------------------------------------------

def normalize_ethnicity(raw: str) -> str:
    """Normalize raw ethnicity text to canonical key."""
    return _ETHNICITY_ALIASES.get(raw.strip().lower(), raw.strip().lower())


def get_default_range(canonical_name: str) -> dict | None:
    """Get population-default range for a lab test."""
    return DEFAULT_RANGES.get(canonical_name)


def get_range(
    canonical_name: str,
    sex: str | None = None,
    age: int | None = None,
    ethnicity: str | None = None,
    tier: str | None = None,
) -> dict | None:
    """Get the best available reference range, layered by specificity.

    Priority: age-stratified (sex-specific) > ethnicity > sex-only > default.

    Tier values:
        None / "conventional" — standard lab reference ranges (default)
        "functional"          — functional medicine ranges (narrower)
        "optimal"             — tightest "optimal wellness" window

    When tier is "functional" or "optimal", the low/high values are replaced
    with the corresponding functional/optimal bounds if available. Falls back
    to conventional range if no functional data exists for the test.

    Functional/optimal ranges represent a functional medicine perspective and
    should always be labeled as such in user-facing output.
    """
    base = DEFAULT_RANGES.get(canonical_name)
    if base is None:
        return None

    result = dict(base)

    # Apply functional/optimal overrides first (before demographic layers)
    if tier in ("functional", "optimal"):
        func = FUNCTIONAL_RANGES.get(canonical_name)
        if func:
            if tier == "functional":
                result["low"] = func.get("functional_low", result["low"])
                result["high"] = func.get("functional_high", result["high"])
                result["tier"] = "functional"
                result["tier_note"] = "Functional medicine perspective"
            else:  # optimal
                result["low"] = func.get("optimal_low", result["low"])
                result["high"] = func.get("optimal_high", result["high"])
                result["tier"] = "optimal"
                result["tier_note"] = "Optimal wellness range (functional medicine perspective)"
            return result  # Functional/optimal ranges bypass demographic layers

    # Layer 1: Age-stratified ranges
    if age is not None:
        matched = False
        # Check sex-specific age ranges first
        if sex and sex.lower() in ("female", "f"):
            age_source = _FEMALE_AGE_OVERRIDES.get(canonical_name)
            if age_source:
                for age_min, age_max, range_data in age_source:
                    if age_min <= age <= age_max:
                        result.update(range_data)
                        matched = True
                        break

        # Fall back to general age-stratified ranges
        if not matched:
            age_source = AGE_STRATIFIED_RANGES.get(canonical_name)
            if age_source:
                for age_min, age_max, range_data in age_source:
                    if age_min <= age <= age_max:
                        result.update(range_data)
                        break

    # Layer 2: Sex-only overrides (if no age-stratified match)
    elif sex and sex.lower() in ("female", "f"):
        override = _FEMALE_OVERRIDES.get(canonical_name)
        if override:
            result.update(override)

    # Layer 3: Ethnicity adjustments
    if ethnicity:
        norm_eth = normalize_ethnicity(ethnicity)
        eth_adj = ETHNICITY_ADJUSTMENTS.get(canonical_name, {}).get(norm_eth)
        if eth_adj:
            for key in ("low", "high"):
                if key in eth_adj:
                    result[key] = eth_adj[key]
            if "note" in eth_adj:
                existing_note = result.get("note", "")
                if existing_note:
                    result["note"] = f"{existing_note}; {eth_adj['note']}"
                else:
                    result["note"] = eth_adj["note"]

    return result


def convert_unit(value: float, from_unit: str, to_unit: str) -> float | None:
    """Convert a lab value between units using known conversion factors."""
    norm_from = from_unit.lower().strip()
    norm_to = to_unit.lower().strip()
    if norm_from == norm_to:
        return value
    factor = UNIT_CONVERSIONS.get((norm_from, norm_to))
    if factor is None:
        return None
    return round(value * factor, 4)
