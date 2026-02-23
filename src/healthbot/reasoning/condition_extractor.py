"""Extract user's active conditions for research monitoring.

Deterministic extraction from:
- Active hypotheses (hypothesis_tracker)
- LTM condition facts (clinical_doc_parser extractions)
- Abnormal lab patterns (latest flagged labs)

Returns anonymized condition keywords suitable for PubMed queries.
No PHI in output.
"""
from __future__ import annotations

import logging
import re

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")

# Map common hypothesis titles and LTM facts to PubMed search terms
_CONDITION_KEYWORDS: dict[str, str] = {
    "iron deficiency": "iron deficiency",
    "iron deficiency anemia": "iron deficiency anemia",
    "b12 deficiency": "vitamin B12 deficiency",
    "vitamin b12": "vitamin B12 deficiency",
    "hypothyroidism": "hypothyroidism",
    "hyperthyroidism": "hyperthyroidism",
    "prediabetes": "prediabetes glucose intolerance",
    "diabetes": "type 2 diabetes mellitus",
    "metabolic syndrome": "metabolic syndrome",
    "kidney disease": "chronic kidney disease",
    "liver inflammation": "hepatitis elevated transaminases",
    "vitamin d deficiency": "vitamin D deficiency",
    "hemochromatosis": "hemochromatosis iron overload",
    "polycythemia": "polycythemia erythrocytosis",
    "chronic inflammation": "chronic inflammation elevated CRP ESR",
    "folate deficiency": "folate deficiency",
    "hyperuricemia": "hyperuricemia gout",
    "dyslipidemia": "dyslipidemia hyperlipidemia",
    "anemia of chronic disease": "anemia of chronic disease",
    "hypertension": "hypertension",
    "high blood pressure": "hypertension",
    "heart disease": "cardiovascular disease",
    "pots": "postural orthostatic tachycardia syndrome",
    "hashimoto": "hashimoto thyroiditis",
    "graves": "graves disease",
}

# Lab tests that when flagged suggest a research-worthy condition
_FLAGGED_LAB_TO_CONDITION: dict[str, str] = {
    "ferritin": "iron deficiency",
    "hemoglobin": "anemia",
    "tsh": "thyroid dysfunction",
    "glucose": "glucose metabolism disorder",
    "hba1c": "diabetes mellitus HbA1c",
    "vitamin_d": "vitamin D deficiency",
    "alt": "elevated liver enzymes",
    "ast": "elevated liver enzymes",
    "creatinine": "kidney function",
    "egfr": "kidney function eGFR",
    "ldl": "dyslipidemia LDL cholesterol",
    "triglycerides": "hypertriglyceridemia",
    "vitamin_b12": "vitamin B12 deficiency",
    "crp": "chronic inflammation CRP",
    "uric_acid": "hyperuricemia",
    "psa": "prostate specific antigen screening",
}


def extract_conditions(
    db: HealthDB,
    user_id: int,
    max_conditions: int = 10,
) -> list[str]:
    """Extract user's active conditions as anonymized search keywords.

    Returns a list of condition strings suitable for PubMed queries.
    No PHI included — only medical condition names.
    """
    conditions: set[str] = set()

    # 1. Active hypotheses
    try:
        hypotheses = db.get_active_hypotheses(user_id)
        for h in hypotheses:
            title = h.get("title", "").lower()
            for key, pubmed_term in _CONDITION_KEYWORDS.items():
                if key in title:
                    conditions.add(pubmed_term)
                    break
            else:
                # Use the hypothesis title directly if no mapping found
                # Strip common prefixes
                cleaned = re.sub(
                    r"^(possible|probable|suspected|early|chronic)\s+",
                    "", title,
                )
                if cleaned and len(cleaned) > 3:
                    conditions.add(cleaned)
    except Exception as e:
        logger.debug("Condition extraction (hypotheses): %s", e)

    # 2. LTM condition facts
    try:
        condition_facts = db.get_ltm_by_category(user_id, "condition")
        for fact in condition_facts:
            fact_text = fact.get("fact", "").lower()
            for key, pubmed_term in _CONDITION_KEYWORDS.items():
                if key in fact_text:
                    conditions.add(pubmed_term)
    except Exception as e:
        logger.debug("Condition extraction (LTM): %s", e)

    # 3. Flagged (abnormal) lab results
    try:
        labs = db.query_observations(
            record_type="lab_result",
            user_id=user_id,
            limit=50,
        )
        for lab in labs:
            flag = lab.get("flag", "")
            canonical = lab.get("canonical_name", "")
            if flag and flag.upper().startswith(("H", "L")):
                condition = _FLAGGED_LAB_TO_CONDITION.get(canonical)
                if condition:
                    conditions.add(condition)
    except Exception as e:
        logger.debug("Condition extraction (flagged labs): %s", e)

    # Return top N conditions (sorted for deterministic ordering)
    return sorted(conditions)[:max_conditions]
