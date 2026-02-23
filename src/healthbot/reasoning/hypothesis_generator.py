"""Auto-generate medical hypotheses from lab result patterns.

Deterministic pattern matching — no LLM calls. Compares the user's
latest lab values against known multi-test patterns for common conditions.
Each pattern specifies trigger tests (must be abnormal) and optional
confirmatory tests. Confidence starts at a base and increases with
each optional match.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB
from healthbot.reasoning.reference_ranges import get_range

logger = logging.getLogger("healthbot")


@dataclass
class GeneratedHypothesis:
    """A hypothesis generated from lab pattern matching."""

    title: str
    confidence: float
    evidence_for: list[str] = field(default_factory=list)
    evidence_against: list[str] = field(default_factory=list)
    missing_tests: list[str] = field(default_factory=list)
    pattern_id: str = ""
    specialist_referral: str = ""


# Each rule: triggers = must ALL be abnormal; optional = boost confidence
# direction: "low" = below ref low, "high" = above ref high
PATTERN_RULES: list[dict] = [
    {
        "id": "iron_deficiency_anemia",
        "title": "Iron deficiency anemia",
        "triggers": {"ferritin": "low", "hemoglobin": "low"},
        "optional": {
            "mcv": "low",
            "rdw": "high",
            "iron": "low",
            "tibc": "high",
            "transferrin_saturation": "low",
        },
        "confidence_base": 0.55,
        "confidence_per_optional": 0.08,
        "missing_tests": [
            "iron", "tibc", "transferrin_saturation",
            "reticulocyte_count",
        ],
        "specialist": "Hematologist",
        "referral_threshold": 0.6,
    },
    {
        "id": "b12_deficiency",
        "title": "Vitamin B12 deficiency",
        "triggers": {"vitamin_b12": "low"},
        "optional": {
            "mcv": "high",
            "homocysteine": "high",
            "methylmalonic_acid": "high",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.12,
        "missing_tests": ["homocysteine", "methylmalonic_acid"],
        "specialist": "Hematologist",
        "referral_threshold": 0.6,
    },
    {
        "id": "hypothyroidism",
        "title": "Hypothyroidism",
        "triggers": {"tsh": "high"},
        "optional": {
            "free_t4": "low",
            "free_t3": "low",
            "cholesterol_total": "high",
            "ldl": "high",
        },
        "confidence_base": 0.50,
        "confidence_per_optional": 0.10,
        "missing_tests": [
            "free_t4", "free_t3", "tpo_antibodies",
            "thyroglobulin_ab",
        ],
        "specialist": "Endocrinologist",
        "referral_threshold": 0.5,
    },
    {
        "id": "hyperthyroidism",
        "title": "Hyperthyroidism",
        "triggers": {"tsh": "low"},
        "optional": {
            "free_t4": "high",
            "free_t3": "high",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.12,
        "missing_tests": ["free_t4", "free_t3", "tsi"],
        "specialist": "Endocrinologist",
        "referral_threshold": 0.5,
    },
    {
        "id": "prediabetes",
        "title": "Prediabetes / insulin resistance",
        "triggers": {"hba1c": "high"},
        "optional": {
            "glucose": "high",
            "triglycerides": "high",
            "hdl": "low",
        },
        "confidence_base": 0.50,
        "confidence_per_optional": 0.10,
        "missing_tests": ["insulin", "c_peptide", "ogtt"],
        "specialist": "Endocrinologist",
        "referral_threshold": 0.6,
    },
    {
        "id": "metabolic_syndrome",
        "title": "Metabolic syndrome",
        "triggers": {
            "glucose": "high",
            "triglycerides": "high",
            "hdl": "low",
        },
        "optional": {
            "hba1c": "high",
            "uric_acid": "high",
            "alt": "high",
        },
        "confidence_base": 0.60,
        "confidence_per_optional": 0.08,
        "missing_tests": ["insulin", "uric_acid"],
        "specialist": "Endocrinologist",
        "referral_threshold": 0.65,
    },
    {
        "id": "kidney_disease_early",
        "title": "Early kidney disease (CKD)",
        "triggers": {"egfr": "low"},
        "optional": {
            "creatinine": "high",
            "bun": "high",
            "albumin": "low",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.10,
        "missing_tests": [
            "urine_albumin", "urine_creatinine_ratio", "cystatin_c",
        ],
        "specialist": "Nephrologist",
        "referral_threshold": 0.5,
    },
    {
        "id": "liver_inflammation",
        "title": "Liver inflammation / hepatitis",
        "triggers": {"alt": "high", "ast": "high"},
        "optional": {
            "alkaline_phosphatase": "high",
            "bilirubin": "high",
            "ggt": "high",
            "albumin": "low",
        },
        "confidence_base": 0.50,
        "confidence_per_optional": 0.08,
        "missing_tests": [
            "ggt", "hepatitis_panel", "ferritin",
            "ceruloplasmin",
        ],
        "specialist": "Gastroenterologist / Hepatologist",
        "referral_threshold": 0.55,
    },
    {
        "id": "vitamin_d_deficiency",
        "title": "Vitamin D deficiency",
        "triggers": {"vitamin_d": "low"},
        "optional": {
            "calcium": "low",
            "alkaline_phosphatase": "high",
            "pth": "high",
        },
        "confidence_base": 0.55,
        "confidence_per_optional": 0.10,
        "missing_tests": ["pth", "calcium"],
        "specialist": "Endocrinologist",
        "referral_threshold": 0.6,
    },
    {
        "id": "hemochromatosis",
        "title": "Hemochromatosis (iron overload)",
        "triggers": {"ferritin": "high", "iron": "high"},
        "optional": {
            "transferrin_saturation": "high",
            "alt": "high",
            "ast": "high",
        },
        "confidence_base": 0.50,
        "confidence_per_optional": 0.10,
        "missing_tests": [
            "transferrin_saturation", "hfe_gene_test",
        ],
        "specialist": "Hematologist",
        "referral_threshold": 0.55,
    },
    {
        "id": "polycythemia",
        "title": "Polycythemia",
        "triggers": {"hemoglobin": "high", "hematocrit": "high"},
        "optional": {
            "wbc": "high",
            "platelets": "high",
            "epo": "low",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.10,
        "missing_tests": ["epo", "jak2_mutation"],
        "specialist": "Hematologist",
        "referral_threshold": 0.5,
    },
    {
        "id": "inflammation_chronic",
        "title": "Chronic inflammation",
        "triggers": {"crp": "high"},
        "optional": {
            "esr": "high",
            "ferritin": "high",
            "wbc": "high",
            "albumin": "low",
        },
        "confidence_base": 0.40,
        "confidence_per_optional": 0.08,
        "missing_tests": ["il_6", "tnf_alpha"],
        "specialist": "Rheumatologist",
        "referral_threshold": 0.6,
    },
    {
        "id": "folate_deficiency",
        "title": "Folate deficiency",
        "triggers": {"folate": "low"},
        "optional": {
            "mcv": "high",
            "homocysteine": "high",
            "hemoglobin": "low",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.10,
        "missing_tests": ["homocysteine"],
        "specialist": "Hematologist",
        "referral_threshold": 0.6,
    },
    {
        "id": "hyperuricemia",
        "title": "Hyperuricemia / gout risk",
        "triggers": {"uric_acid": "high"},
        "optional": {
            "creatinine": "high",
            "egfr": "low",
            "triglycerides": "high",
        },
        "confidence_base": 0.40,
        "confidence_per_optional": 0.10,
        "missing_tests": [],
        "specialist": "Rheumatologist",
        "referral_threshold": 0.55,
    },
    {
        "id": "dyslipidemia",
        "title": "Dyslipidemia",
        "triggers": {"ldl": "high"},
        "optional": {
            "cholesterol_total": "high",
            "triglycerides": "high",
            "hdl": "low",
            "apob": "high",
        },
        "confidence_base": 0.50,
        "confidence_per_optional": 0.08,
        "missing_tests": ["apob", "lp_a"],
        "specialist": "Cardiologist",
        "referral_threshold": 0.6,
    },
    {
        "id": "anemia_chronic_disease",
        "title": "Anemia of chronic disease",
        "triggers": {"hemoglobin": "low", "ferritin": "high"},
        "optional": {
            "iron": "low",
            "tibc": "low",
            "crp": "high",
            "esr": "high",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.10,
        "missing_tests": [
            "iron", "tibc", "reticulocyte_count", "haptoglobin",
        ],
        "specialist": "Hematologist",
        "referral_threshold": 0.6,
    },
    # --- Patterns 17-24 (added) ---
    {
        "id": "magnesium_deficiency",
        "title": "Magnesium deficiency",
        "triggers": {"magnesium": "low"},
        "optional": {
            "calcium": "low",
            "potassium": "low",
            "phosphorus": "low",
        },
        "confidence_base": 0.50,
        "confidence_per_optional": 0.10,
        "missing_tests": ["rbc_magnesium", "urine_magnesium"],
        "specialist": "Internal Medicine",
        "referral_threshold": 0.6,
    },
    {
        "id": "zinc_deficiency",
        "title": "Zinc deficiency",
        "triggers": {"zinc": "low"},
        "optional": {
            "alkaline_phosphatase": "low",
            "albumin": "low",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.12,
        "missing_tests": ["copper", "ceruloplasmin"],
        "specialist": "Internal Medicine",
        "referral_threshold": 0.6,
    },
    {
        "id": "testosterone_deficiency",
        "title": "Testosterone deficiency (hypogonadism)",
        "triggers": {"testosterone_total": "low"},
        "optional": {
            "shbg": "high",
            "lh": "high",
            "fsh": "high",
            "free_testosterone": "low",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.10,
        "missing_tests": [
            "free_testosterone", "lh", "fsh", "shbg", "prolactin",
        ],
        "sex_filter": "M",
        "specialist": "Endocrinologist / Urologist",
        "referral_threshold": 0.55,
    },
    {
        "id": "pcos",
        "title": "Polycystic ovary syndrome (PCOS)",
        "triggers": {"testosterone_total": "high"},
        "optional": {
            "dhea_s": "high",
            "lh": "high",
            "insulin": "high",
            "glucose": "high",
        },
        "confidence_base": 0.40,
        "confidence_per_optional": 0.10,
        "missing_tests": [
            "dhea_s", "lh", "fsh", "insulin",
            "androstenedione", "17_oh_progesterone",
        ],
        "sex_filter": "F",
        "specialist": "OB/GYN / Endocrinologist",
        "referral_threshold": 0.5,
    },
    {
        "id": "malabsorption",
        "title": "Malabsorption / celiac disease",
        "triggers": {
            "iron": "low",
            "vitamin_b12": "low",
            "vitamin_d": "low",
        },
        "optional": {
            "folate": "low",
            "ferritin": "low",
            "calcium": "low",
            "albumin": "low",
        },
        "confidence_base": 0.45,
        "confidence_per_optional": 0.08,
        "missing_tests": [
            "ttg_iga", "total_iga", "anti_endomysial",
        ],
        "specialist": "Gastroenterologist",
        "referral_threshold": 0.55,
    },
    {
        "id": "acute_infection",
        "title": "Acute infection / inflammatory response",
        "triggers": {"wbc": "high", "crp": "high"},
        "optional": {
            "neutrophils": "high",
            "procalcitonin": "high",
            "esr": "high",
            "albumin": "low",
        },
        "confidence_base": 0.50,
        "confidence_per_optional": 0.08,
        "missing_tests": ["procalcitonin", "blood_culture"],
        "specialist": "Infectious Disease",
        "referral_threshold": 0.65,
    },
    {
        "id": "dehydration",
        "title": "Dehydration",
        "triggers": {"bun": "high"},
        "optional": {
            "sodium": "high",
            "hematocrit": "high",
            "albumin": "high",
            "urine_specific_gravity": "high",
        },
        "confidence_base": 0.40,
        "confidence_per_optional": 0.10,
        "missing_tests": ["urine_specific_gravity", "urine_osmolality"],
        # Note: BUN/creatinine ratio >20:1 is classic, but we check BUN high
        # as a simpler trigger since creatinine may be normal in pre-renal
        "specialist": "",
        "referral_threshold": 1.0,
    },
    {
        "id": "hyperparathyroidism",
        "title": "Primary hyperparathyroidism",
        "triggers": {"calcium": "high", "pth": "high"},
        "optional": {
            "phosphorus": "low",
            "vitamin_d": "low",
            "alkaline_phosphatase": "high",
        },
        "confidence_base": 0.55,
        "confidence_per_optional": 0.10,
        "missing_tests": [
            "ionized_calcium", "urine_calcium_24hr",
        ],
        "specialist": "Endocrinologist",
        "referral_threshold": 0.55,
    },
]


class HypothesisGenerator:
    """Generate hypotheses from lab result patterns."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def scan_all(
        self,
        user_id: int,
        sex: str | None = None,
        age: int | None = None,
    ) -> list[GeneratedHypothesis]:
        """Scan all lab results for pattern matches.

        Returns hypotheses sorted by confidence (highest first).
        """
        latest = self._get_latest_values(user_id)
        if not latest:
            return []

        hypotheses: list[GeneratedHypothesis] = []
        for rule in PATTERN_RULES:
            hyp = self._check_rule(rule, latest, sex, age)
            if hyp:
                hypotheses.append(hyp)

        hypotheses.sort(key=lambda h: h.confidence, reverse=True)
        return hypotheses

    def _get_latest_values(self, user_id: int) -> dict[str, float]:
        """Get the most recent value for each canonical test name."""
        sql = (
            "SELECT DISTINCT canonical_name "
            "FROM observations WHERE record_type = 'lab_result'"
        )
        params: list = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        sql += " LIMIT 500"
        rows = self._db.conn.execute(sql, params).fetchall()

        latest: dict[str, float] = {}
        for row in rows:
            name = row["canonical_name"]
            if not name:
                continue
            obs = self._db.query_observations(
                record_type="lab_result",
                canonical_name=name,
                limit=1,
                user_id=user_id,
            )
            if obs:
                val = obs[0].get("value")
                try:
                    latest[name] = float(val)
                except (ValueError, TypeError):
                    continue
        return latest

    def _check_rule(
        self,
        rule: dict,
        latest: dict[str, float],
        sex: str | None,
        age: int | None,
    ) -> GeneratedHypothesis | None:
        """Check a single pattern rule against latest values."""
        # Sex-specific patterns: skip if sex doesn't match
        sex_filter = rule.get("sex_filter")
        if sex_filter and sex:
            sex_initial = sex.strip()[0].upper()
            if sex_initial != sex_filter.upper():
                return None

        triggers: dict[str, str] = rule["triggers"]
        evidence_for: list[str] = []

        # All triggers must be present AND abnormal
        for test_name, direction in triggers.items():
            if test_name not in latest:
                return None  # Missing trigger test — can't evaluate
            if not self._is_abnormal(
                test_name, latest[test_name], direction, sex, age,
            ):
                return None  # Trigger test is normal — pattern doesn't match

            evidence_for.append(
                f"{test_name} is {direction} ({latest[test_name]})"
            )

        # Count optional matches for confidence boost
        confidence = rule["confidence_base"]
        optional: dict[str, str] = rule.get("optional", {})
        for test_name, direction in optional.items():
            if test_name in latest:
                if self._is_abnormal(
                    test_name, latest[test_name], direction, sex, age,
                ):
                    confidence += rule.get("confidence_per_optional", 0.08)
                    evidence_for.append(
                        f"{test_name} is {direction} ({latest[test_name]})"
                    )

        confidence = min(confidence, 0.95)

        # Identify missing tests that would help confirm/rule out
        missing = [
            t for t in rule.get("missing_tests", [])
            if t not in latest
        ]

        # Specialist referral if confidence exceeds threshold
        specialist = ""
        referral_threshold = rule.get("referral_threshold", 1.0)
        if confidence >= referral_threshold and rule.get("specialist"):
            specialist = rule["specialist"]

        return GeneratedHypothesis(
            title=rule["title"],
            confidence=confidence,
            evidence_for=evidence_for,
            missing_tests=missing,
            pattern_id=rule["id"],
            specialist_referral=specialist,
        )

    @staticmethod
    def _is_abnormal(
        canonical_name: str,
        value: float,
        direction: str,
        sex: str | None,
        age: int | None,
    ) -> bool:
        """Check if a value is abnormal in the given direction."""
        ref = get_range(canonical_name, sex=sex, age=age)
        if not ref:
            return False

        if direction == "low":
            low = ref.get("low")
            return low is not None and value < low
        if direction == "high":
            high = ref.get("high")
            return high is not None and value > high
        return False
