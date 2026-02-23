"""Family history risk engine.

Maps family health history to clinical implications:
adjusted lab thresholds, screening recommendations, and risk flags.
All logic is deterministic -- no LLM calls.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class FamilyCondition:
    """A parsed family history condition."""

    condition: str  # Normalized condition keyword
    relationship: str  # "first_degree" or "second_degree"
    age_onset: int | None = None  # Age relative was diagnosed, if mentioned


@dataclass
class RiskAssessment:
    """Result of family risk evaluation for a specific lab test."""

    risk_level: str  # "elevated" or "standard"
    aggressive_range: dict | None = None  # Tighter thresholds if applicable
    clinical_notes: list[str] = field(default_factory=list)
    screening_implications: list[str] = field(default_factory=list)


# Maps family condition keywords to affected tests and clinical notes
FAMILY_RISK_RULES: list[dict] = [
    {
        "keywords": [
            "heart disease", "heart attack", "coronary",
            "cardiovascular", "cad", "mi", "cardiac",
        ],
        "affected_tests": {
            "ldl": {"aggressive_high": 100},
            "cholesterol_total": {"aggressive_high": 180},
            "triglycerides": {"aggressive_high": 130},
            "hdl": {"aggressive_low": 50},
            "crp": {"aggressive_high": 1.0},
        },
        "screening_implications": [
            "Consider earlier lipid screening (starting age 20)",
            "If LDL persistently >190: evaluate for familial "
            "hypercholesterolemia",
        ],
    },
    {
        "keywords": [
            "diabetes", "type 2 diabetes", "type 1 diabetes", "diabetic",
        ],
        "affected_tests": {
            "glucose": {"aggressive_high": 95},
            "hba1c": {"aggressive_high": 5.4},
            "triglycerides": {"aggressive_high": 130},
        },
        "screening_implications": [
            "Annual HbA1c screening recommended (ADA guidelines)",
            "Monitor fasting insulin if glucose borderline",
        ],
    },
    {
        "keywords": ["cancer", "colon cancer", "colorectal"],
        "screening_implications": [
            "Colonoscopy at age 40 or 10 years before relative's "
            "diagnosis age, whichever earlier",
        ],
    },
    {
        "keywords": ["breast cancer"],
        "screening_implications": [
            "Mammography starting 10 years before relative's "
            "diagnosis age",
            "Consider BRCA testing if multiple relatives affected",
        ],
    },
    {
        "keywords": [
            "thyroid", "hashimoto", "graves",
            "hypothyroid", "hyperthyroid",
        ],
        "affected_tests": {
            "tsh": {"watch_zone_pct": 0.15},
            "free_t4": {"watch_zone_pct": 0.15},
        },
        "screening_implications": [
            "Annual TSH screening recommended",
            "Check thyroid antibodies if TSH borderline",
        ],
    },
    {
        "keywords": ["hypertension", "high blood pressure"],
        "screening_implications": [
            "Home BP monitoring recommended starting age 25",
        ],
    },
    {
        "keywords": [
            "kidney disease", "renal", "dialysis",
            "polycystic kidney",
        ],
        "affected_tests": {
            "creatinine": {"aggressive_high": 1.1},
            "egfr": {"aggressive_low": 80},
        },
    },
    {
        "keywords": ["osteoporosis"],
        "affected_tests": {
            "vitamin_d": {"aggressive_low": 40},
        },
    },
    {
        "keywords": [
            "autoimmune", "lupus", "rheumatoid",
            "multiple sclerosis",
        ],
        "affected_tests": {
            "esr": {"aggressive_high": 15},
            "crp": {"aggressive_high": 1.5},
        },
    },
    {
        "keywords": ["prostate cancer"],
        "screening_implications": [
            "PSA screening starting at age 40 (vs 55 standard)",
        ],
    },
]

# First-degree relationship keywords
_FIRST_DEGREE = {
    "parent", "mother", "father", "mom", "dad",
    "sibling", "brother", "sister",
    "child", "son", "daughter",
}

# Second-degree relationship keywords
_SECOND_DEGREE = {
    "grandparent", "grandmother", "grandfather",
    "grandma", "grandpa",
    "uncle", "aunt", "cousin",
}


def parse_family_history(facts: list[str]) -> list[FamilyCondition]:
    """Extract structured conditions from free-text family history LTM.

    Examples:
        "Family history: father had heart attack at 55"
        -> FamilyCondition("heart disease", "first_degree", 55)

        "Family history: diabetes in mother"
        -> FamilyCondition("diabetes", "first_degree", None)
    """
    conditions: list[FamilyCondition] = []
    seen: set[str] = set()

    for fact in facts:
        text = fact.lower()

        # Determine relationship degree
        relationship = "first_degree"  # default
        for word in _SECOND_DEGREE:
            if word in text:
                relationship = "second_degree"
                break

        # Extract age of onset if mentioned
        age_onset = None
        age_match = re.search(r"at\s+(?:age\s+)?(\d{2,3})\b", text)
        if age_match:
            age_val = int(age_match.group(1))
            if 1 < age_val < 120:
                age_onset = age_val

        # Match against known condition keywords
        for rule in FAMILY_RISK_RULES:
            for keyword in rule["keywords"]:
                if keyword in text and keyword not in seen:
                    conditions.append(FamilyCondition(
                        condition=keyword,
                        relationship=relationship,
                        age_onset=age_onset,
                    ))
                    seen.add(keyword)
                    break  # One match per rule per fact

    return conditions


class FamilyRiskEngine:
    """Evaluate family history risk for lab interpretation."""

    def assess(
        self,
        family_conditions: list[FamilyCondition],
        canonical_name: str,
    ) -> RiskAssessment:
        """Assess family risk for a specific lab test.

        Returns risk level and any adjusted thresholds.
        """
        result = RiskAssessment(risk_level="standard")

        for condition in family_conditions:
            for rule in FAMILY_RISK_RULES:
                if condition.condition not in rule["keywords"]:
                    continue

                # Check if this rule affects the test
                affected = rule.get("affected_tests", {})
                if canonical_name in affected:
                    result.risk_level = "elevated"
                    adj = affected[canonical_name]
                    result.aggressive_range = adj
                    result.clinical_notes.append(
                        f"Family history of {condition.condition} "
                        f"({condition.relationship})"
                    )

                # Collect screening implications
                for imp in rule.get("screening_implications", []):
                    if imp not in result.screening_implications:
                        result.screening_implications.append(imp)

        return result

    def get_all_screening_implications(
        self,
        family_conditions: list[FamilyCondition],
    ) -> list[str]:
        """Get all screening recommendations based on family history."""
        implications: list[str] = []
        seen: set[str] = set()

        for condition in family_conditions:
            for rule in FAMILY_RISK_RULES:
                if condition.condition in rule["keywords"]:
                    for imp in rule.get("screening_implications", []):
                        if imp not in seen:
                            implications.append(imp)
                            seen.add(imp)

        return implications
