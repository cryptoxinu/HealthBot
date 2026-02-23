"""Condition-based lab test recommendations.

Recommends lab tests based on user's active conditions, medications, and
demographics. Cross-references with last test dates to identify gaps.
Deterministic — no LLM.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class LabRecommendation:
    """A recommended lab test with reason and urgency."""

    test_name: str
    canonical_name: str
    reason: str
    frequency_months: int
    last_tested: str  # ISO date or ""
    months_since: int  # -1 if never tested
    source: str  # "condition", "medication", "age_sex"


# Condition -> recommended labs with frequency (months)
CONDITION_LAB_SCHEDULE: dict[str, list[tuple[str, str, int]]] = {
    # (canonical_name, display_name, frequency_months)
    "diabetes": [
        ("hba1c", "HbA1c", 3),
        ("glucose", "Fasting Glucose", 3),
        ("creatinine", "Creatinine", 6),
        ("egfr", "eGFR", 6),
        ("microalbumin", "Microalbumin", 12),
        ("cholesterol_total", "Total Cholesterol", 12),
        ("ldl", "LDL", 12),
    ],
    "prediabetes": [
        ("hba1c", "HbA1c", 6),
        ("glucose", "Fasting Glucose", 6),
    ],
    "hypothyroidism": [
        ("tsh", "TSH", 6),
        ("free_t4", "Free T4", 6),
    ],
    "hyperthyroidism": [
        ("tsh", "TSH", 3),
        ("free_t4", "Free T4", 3),
        ("free_t3", "Free T3", 6),
    ],
    "hyperlipidemia": [
        ("cholesterol_total", "Total Cholesterol", 6),
        ("ldl", "LDL", 6),
        ("hdl", "HDL", 6),
        ("triglycerides", "Triglycerides", 6),
    ],
    "hypertension": [
        ("creatinine", "Creatinine", 12),
        ("egfr", "eGFR", 12),
        ("potassium", "Potassium", 12),
        ("sodium", "Sodium", 12),
    ],
    "chronic kidney disease": [
        ("creatinine", "Creatinine", 3),
        ("egfr", "eGFR", 3),
        ("potassium", "Potassium", 6),
        ("calcium", "Calcium", 6),
        ("phosphorus", "Phosphorus", 6),
        ("hemoglobin", "Hemoglobin", 6),
    ],
    "anemia": [
        ("hemoglobin", "Hemoglobin", 3),
        ("iron", "Iron", 3),
        ("ferritin", "Ferritin", 3),
        ("vitamin_b12", "Vitamin B12", 6),
        ("folate", "Folate", 6),
    ],
    "iron deficiency": [
        ("ferritin", "Ferritin", 3),
        ("iron", "Iron", 3),
        ("hemoglobin", "Hemoglobin", 3),
        ("tibc", "TIBC", 6),
    ],
    "liver disease": [
        ("alt", "ALT", 3),
        ("ast", "AST", 3),
        ("albumin", "Albumin", 6),
        ("bilirubin", "Bilirubin", 6),
    ],
    "gout": [
        ("uric_acid", "Uric Acid", 6),
        ("creatinine", "Creatinine", 12),
    ],
    "vitamin d deficiency": [
        ("vitamin_d", "Vitamin D", 6),
        ("calcium", "Calcium", 12),
    ],
}

# Medication -> recommended monitoring labs
MEDICATION_LAB_SCHEDULE: dict[str, list[tuple[str, str, int]]] = {
    "statin": [
        ("alt", "ALT (liver)", 6),
        ("ast", "AST (liver)", 6),
        ("creatine_kinase", "CK (muscle)", 12),
    ],
    "metformin": [
        ("vitamin_b12", "Vitamin B12", 12),
        ("creatinine", "Creatinine", 12),
    ],
    "ace_inhibitor": [
        ("potassium", "Potassium", 6),
        ("creatinine", "Creatinine", 6),
    ],
    "arb": [
        ("potassium", "Potassium", 6),
        ("creatinine", "Creatinine", 6),
    ],
    "lithium": [
        ("tsh", "TSH", 6),
        ("creatinine", "Creatinine", 6),
        ("lithium", "Lithium Level", 3),
    ],
    "methotrexate": [
        ("hemoglobin", "CBC", 3),
        ("alt", "ALT (liver)", 3),
        ("creatinine", "Creatinine", 3),
    ],
    "warfarin": [
        ("inr", "INR", 1),
    ],
    "thiazide": [
        ("potassium", "Potassium", 6),
        ("sodium", "Sodium", 6),
        ("glucose", "Glucose", 12),
    ],
    "corticosteroid": [
        ("glucose", "Glucose", 6),
        ("calcium", "Calcium", 12),
        ("vitamin_d", "Vitamin D", 12),
    ],
}

# Aliases for matching medication names to schedule keys
_MED_ALIASES: dict[str, str] = {
    "atorvastatin": "statin", "rosuvastatin": "statin",
    "simvastatin": "statin", "pravastatin": "statin",
    "lovastatin": "statin", "fluvastatin": "statin",
    "pitavastatin": "statin",
    "lisinopril": "ace_inhibitor", "enalapril": "ace_inhibitor",
    "ramipril": "ace_inhibitor", "benazepril": "ace_inhibitor",
    "losartan": "arb", "valsartan": "arb",
    "irbesartan": "arb", "olmesartan": "arb",
    "hydrochlorothiazide": "thiazide", "hctz": "thiazide",
    "chlorthalidone": "thiazide", "indapamide": "thiazide",
    "prednisone": "corticosteroid", "prednisolone": "corticosteroid",
    "dexamethasone": "corticosteroid",
    "trexall": "methotrexate",
}

# Condition name aliases for matching
_CONDITION_ALIASES: dict[str, str] = {
    "type 2 diabetes": "diabetes",
    "type 1 diabetes": "diabetes",
    "diabetes mellitus": "diabetes",
    "t2dm": "diabetes",
    "dm2": "diabetes",
    "hashimoto": "hypothyroidism",
    "hashimoto's": "hypothyroidism",
    "graves": "hyperthyroidism",
    "graves'": "hyperthyroidism",
    "high cholesterol": "hyperlipidemia",
    "dyslipidemia": "hyperlipidemia",
    "high blood pressure": "hypertension",
    "ckd": "chronic kidney disease",
    "kidney disease": "chronic kidney disease",
    "fatty liver": "liver disease",
    "nafld": "liver disease",
    "hepatitis": "liver disease",
    "iron deficiency anemia": "iron deficiency",
    "hyperuricemia": "gout",
}


def _normalize_condition(text: str) -> str | None:
    """Normalize a condition string to a schedule key."""
    lower = text.lower().strip()
    if lower in CONDITION_LAB_SCHEDULE:
        return lower
    return _CONDITION_ALIASES.get(lower)


def _normalize_med(name: str) -> str | None:
    """Normalize a medication name to a schedule key."""
    lower = name.lower().strip()
    if lower in MEDICATION_LAB_SCHEDULE:
        return lower
    return _MED_ALIASES.get(lower)


def recommend_labs(
    db: HealthDB, user_id: int,
) -> list[LabRecommendation]:
    """Generate lab recommendations based on conditions and medications.

    Cross-references active conditions and medications against
    recommended lab schedules. Returns overdue or never-tested labs.
    """
    now = datetime.now(UTC).date()
    recs: list[LabRecommendation] = []
    seen: set[str] = set()  # Deduplicate by canonical_name

    # 1. Condition-based recommendations
    conditions = _extract_user_conditions(db, user_id)
    for condition in conditions:
        key = _normalize_condition(condition)
        if not key or key not in CONDITION_LAB_SCHEDULE:
            continue
        for canonical, display, freq_months in CONDITION_LAB_SCHEDULE[key]:
            if canonical in seen:
                continue
            rec = _check_test_due(
                db, user_id, canonical, display, freq_months,
                f"{key}", "condition", now,
            )
            if rec:
                seen.add(canonical)
                recs.append(rec)

    # 2. Medication-based monitoring
    try:
        meds = db.get_active_medications(user_id=user_id)
        for med in meds:
            med_name = med.get("name", "")
            key = _normalize_med(med_name)
            if not key or key not in MEDICATION_LAB_SCHEDULE:
                continue
            for canonical, display, freq_months in MEDICATION_LAB_SCHEDULE[key]:
                if canonical in seen:
                    continue
                rec = _check_test_due(
                    db, user_id, canonical, display, freq_months,
                    f"on {med_name}", "medication", now,
                )
                if rec:
                    seen.add(canonical)
                    recs.append(rec)
    except Exception as e:
        logger.debug("Lab recommendations (meds): %s", e)

    # Sort: never-tested first, then by months overdue
    recs.sort(key=lambda r: (r.months_since >= 0, -r.months_since))
    return recs


def _check_test_due(
    db: HealthDB, user_id: int,
    canonical: str, display: str, freq_months: int,
    reason_context: str, source: str,
    now: datetime,
) -> LabRecommendation | None:
    """Check if a test is due and return recommendation if so."""
    rows = db.query_observations(
        record_type="lab_result",
        canonical_name=canonical,
        limit=1,
        user_id=user_id,
    )
    if not rows:
        return LabRecommendation(
            test_name=display,
            canonical_name=canonical,
            reason=f"Recommended for {reason_context} (never tested)",
            frequency_months=freq_months,
            last_tested="",
            months_since=-1,
            source=source,
        )

    row = rows[0]
    last_date_str = (
        row.get("date_collected")
        or row.get("_meta", {}).get("date_effective", "")
    )
    if not last_date_str:
        return None

    try:
        last_date = datetime.fromisoformat(last_date_str).date()
    except ValueError:
        return None

    due_date = last_date + timedelta(days=freq_months * 30)
    if now > due_date:
        months_since = (now - last_date).days // 30
        return LabRecommendation(
            test_name=display,
            canonical_name=canonical,
            reason=f"Due for {reason_context} "
                   f"(every {freq_months}mo, last {last_date_str[:10]})",
            frequency_months=freq_months,
            last_tested=last_date_str[:10],
            months_since=months_since,
            source=source,
        )
    return None


def _extract_user_conditions(db: HealthDB, user_id: int) -> list[str]:
    """Extract conditions from hypotheses and LTM facts."""
    conditions: list[str] = []

    # From active hypotheses
    try:
        hypotheses = db.get_active_hypotheses(user_id)
        for h in hypotheses:
            title = h.get("title", "")
            if title:
                conditions.append(title)
    except Exception:
        pass

    # From LTM condition facts
    try:
        facts = db.get_ltm_by_category(user_id, "condition")
        for f in facts:
            fact_text = f.get("fact", "")
            if fact_text:
                conditions.append(fact_text)
    except Exception:
        pass

    return conditions


def format_recommendations(recs: list[LabRecommendation]) -> str:
    """Format lab recommendations for display."""
    if not recs:
        return "All condition-based lab tests are up to date."

    lines = ["RECOMMENDED LAB TESTS", "-" * 30]

    never = [r for r in recs if r.months_since == -1]
    overdue = [r for r in recs if r.months_since >= 0]

    if never:
        lines.append("\nNever tested:")
        for r in never:
            lines.append(f"  * {r.test_name} — {r.reason}")

    if overdue:
        lines.append("\nOverdue:")
        for r in overdue:
            lines.append(
                f"  ! {r.test_name} — {r.months_since}mo since last test "
                f"({r.reason})"
            )

    return "\n".join(lines)
