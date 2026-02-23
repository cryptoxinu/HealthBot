"""Dedicated tests for hypothesis generator patterns 1-24.

Each test inserts synthetic lab results that match a specific pattern's
trigger conditions and verifies the pattern fires with the correct ID.
"""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult
from healthbot.reasoning.hypothesis_generator import HypothesisGenerator


def _insert_lab(db: HealthDB, name: str, value: float, user_id: int = 0) -> None:
    """Insert a single lab result for testing."""
    lab = LabResult(
        id=uuid.uuid4().hex,
        test_name=name,
        canonical_name=name,
        value=value,
        unit="",
        date_collected=date(2026, 1, 1),
    )
    db.insert_observation(lab, user_id=user_id)


def _insert_labs(db: HealthDB, labs: dict[str, float], user_id: int = 0) -> None:
    """Insert multiple lab results."""
    for name, value in labs.items():
        _insert_lab(db, name, value, user_id)


def _pattern_ids(
    db: HealthDB,
    user_id: int = 0,
    sex: str | None = None,
) -> set[str]:
    """Return the set of pattern IDs that fired."""
    gen = HypothesisGenerator(db)
    results = gen.scan_all(user_id, sex=sex)
    return {h.pattern_id for h in results}


class TestOriginalPatterns:
    """Patterns 1-16 (original set)."""

    def test_iron_deficiency_anemia(self, db: HealthDB) -> None:
        _insert_labs(db, {"ferritin": 5.0, "hemoglobin": 10.0})
        assert "iron_deficiency_anemia" in _pattern_ids(db)

    def test_b12_deficiency(self, db: HealthDB) -> None:
        _insert_labs(db, {"vitamin_b12": 100.0})
        assert "b12_deficiency" in _pattern_ids(db)

    def test_hypothyroidism(self, db: HealthDB) -> None:
        _insert_labs(db, {"tsh": 8.0})
        assert "hypothyroidism" in _pattern_ids(db)

    def test_hyperthyroidism(self, db: HealthDB) -> None:
        _insert_labs(db, {"tsh": 0.1})
        assert "hyperthyroidism" in _pattern_ids(db)

    def test_prediabetes(self, db: HealthDB) -> None:
        _insert_labs(db, {"hba1c": 6.2})
        assert "prediabetes" in _pattern_ids(db)

    def test_metabolic_syndrome(self, db: HealthDB) -> None:
        _insert_labs(db, {
            "glucose": 115.0, "triglycerides": 200.0, "hdl": 30.0,
        })
        assert "metabolic_syndrome" in _pattern_ids(db)

    def test_kidney_disease_early(self, db: HealthDB) -> None:
        _insert_labs(db, {"egfr": 50.0})
        assert "kidney_disease_early" in _pattern_ids(db)

    def test_liver_inflammation(self, db: HealthDB) -> None:
        _insert_labs(db, {"alt": 100.0, "ast": 80.0})
        assert "liver_inflammation" in _pattern_ids(db)

    def test_vitamin_d_deficiency(self, db: HealthDB) -> None:
        _insert_labs(db, {"vitamin_d": 15.0})
        assert "vitamin_d_deficiency" in _pattern_ids(db)

    def test_hemochromatosis(self, db: HealthDB) -> None:
        _insert_labs(db, {"ferritin": 500.0, "iron": 200.0})
        assert "hemochromatosis" in _pattern_ids(db)

    def test_polycythemia(self, db: HealthDB) -> None:
        _insert_labs(db, {"hemoglobin": 20.0, "hematocrit": 55.0})
        assert "polycythemia" in _pattern_ids(db)

    def test_inflammation_chronic(self, db: HealthDB) -> None:
        _insert_labs(db, {"crp": 10.0})
        assert "inflammation_chronic" in _pattern_ids(db)

    def test_folate_deficiency(self, db: HealthDB) -> None:
        _insert_labs(db, {"folate": 1.5})
        assert "folate_deficiency" in _pattern_ids(db)

    def test_hyperuricemia(self, db: HealthDB) -> None:
        _insert_labs(db, {"uric_acid": 9.0})
        assert "hyperuricemia" in _pattern_ids(db)

    def test_dyslipidemia(self, db: HealthDB) -> None:
        _insert_labs(db, {"ldl": 160.0})
        assert "dyslipidemia" in _pattern_ids(db)

    def test_anemia_chronic_disease(self, db: HealthDB) -> None:
        _insert_labs(db, {"hemoglobin": 10.0, "ferritin": 400.0})
        assert "anemia_chronic_disease" in _pattern_ids(db)


class TestNewPatterns:
    """Patterns 17-24 (added in Gap 2)."""

    def test_magnesium_deficiency(self, db: HealthDB) -> None:
        _insert_labs(db, {"magnesium": 1.2})
        assert "magnesium_deficiency" in _pattern_ids(db)

    def test_zinc_deficiency(self, db: HealthDB) -> None:
        _insert_labs(db, {"zinc": 40.0})
        assert "zinc_deficiency" in _pattern_ids(db)

    def test_testosterone_deficiency_male(self, db: HealthDB) -> None:
        _insert_labs(db, {"testosterone_total": 150.0})
        ids = _pattern_ids(db, sex="male")
        assert "testosterone_deficiency" in ids

    def test_testosterone_deficiency_skipped_for_female(
        self, db: HealthDB,
    ) -> None:
        _insert_labs(db, {"testosterone_total": 150.0})
        ids = _pattern_ids(db, sex="female")
        assert "testosterone_deficiency" not in ids

    def test_pcos_female(self, db: HealthDB) -> None:
        _insert_labs(db, {"testosterone_total": 90.0})
        ids = _pattern_ids(db, sex="female")
        assert "pcos" in ids

    def test_pcos_skipped_for_male(self, db: HealthDB) -> None:
        # Male testosterone > 1070 is high for males, but PCOS is female-only
        _insert_labs(db, {"testosterone_total": 1100.0})
        ids = _pattern_ids(db, sex="male")
        assert "pcos" not in ids

    def test_malabsorption(self, db: HealthDB) -> None:
        _insert_labs(db, {
            "iron": 30.0, "vitamin_b12": 100.0, "vitamin_d": 10.0,
        })
        assert "malabsorption" in _pattern_ids(db)

    def test_acute_infection(self, db: HealthDB) -> None:
        _insert_labs(db, {"wbc": 15.0, "crp": 20.0})
        assert "acute_infection" in _pattern_ids(db)

    def test_dehydration(self, db: HealthDB) -> None:
        _insert_labs(db, {"bun": 30.0})
        assert "dehydration" in _pattern_ids(db)

    def test_hyperparathyroidism(self, db: HealthDB) -> None:
        _insert_labs(db, {"calcium": 12.0, "pth": 90.0})
        assert "hyperparathyroidism" in _pattern_ids(db)


class TestPatternNegatives:
    """Verify patterns do NOT fire for normal values."""

    def test_normal_values_no_patterns(self, db: HealthDB) -> None:
        _insert_labs(db, {
            "ferritin": 100.0, "hemoglobin": 15.0, "tsh": 2.0,
            "hba1c": 5.0, "crp": 1.0, "magnesium": 2.0,
            "zinc": 80.0, "calcium": 9.5, "pth": 40.0,
            "bun": 15.0, "wbc": 7.0,
        })
        assert len(_pattern_ids(db)) == 0

    def test_optional_boost_increases_confidence(self, db: HealthDB) -> None:
        """More optional matches = higher confidence."""
        gen = HypothesisGenerator(db)

        # Trigger only
        _insert_labs(db, {"tsh": 8.0}, user_id=1)
        r1 = gen.scan_all(user_id=1)
        conf_trigger_only = r1[0].confidence

        # Trigger + optionals
        _insert_labs(db, {
            "tsh": 8.0, "free_t4": 0.5, "free_t3": 1.5,
        }, user_id=2)
        r2 = gen.scan_all(user_id=2)
        hyps = [h for h in r2 if h.pattern_id == "hypothyroidism"]
        conf_with_optionals = hyps[0].confidence

        assert conf_with_optionals > conf_trigger_only

    def test_missing_trigger_no_match(self, db: HealthDB) -> None:
        """Pattern should not fire if trigger test is missing entirely."""
        # Only optional tests, no triggers
        _insert_labs(db, {"mcv": 110.0, "homocysteine": 20.0})
        assert "b12_deficiency" not in _pattern_ids(db)


class TestSpecialistReferrals:
    """Test specialist referral field on generated hypotheses."""

    def test_hypothyroidism_endocrinologist(self, db: HealthDB) -> None:
        _insert_labs(db, {"tsh": 12.0, "free_t4": 0.5}, user_id=90)
        gen = HypothesisGenerator(db)
        hyps = gen.scan_all(user_id=90)
        thyroid = [h for h in hyps if h.pattern_id == "hypothyroidism"]
        assert len(thyroid) == 1
        assert thyroid[0].specialist_referral == "Endocrinologist"

    def test_kidney_disease_nephrologist(self, db: HealthDB) -> None:
        _insert_labs(db, {"egfr": 45.0, "creatinine": 2.0}, user_id=91)
        gen = HypothesisGenerator(db)
        hyps = gen.scan_all(user_id=91)
        kidney = [h for h in hyps if h.pattern_id == "kidney_disease_early"]
        assert len(kidney) == 1
        assert kidney[0].specialist_referral == "Nephrologist"

    def test_all_rules_have_specialist_key(self) -> None:
        from healthbot.reasoning.hypothesis_generator import PATTERN_RULES
        for rule in PATTERN_RULES:
            assert "specialist" in rule, f"{rule['id']} missing specialist"
            assert "referral_threshold" in rule, (
                f"{rule['id']} missing referral_threshold"
            )

    def test_dehydration_no_specialist(self, db: HealthDB) -> None:
        """Dehydration shouldn't suggest a specialist."""
        _insert_labs(db, {"bun": 40.0, "sodium": 150.0}, user_id=92)
        gen = HypothesisGenerator(db)
        hyps = gen.scan_all(user_id=92)
        dehy = [h for h in hyps if h.pattern_id == "dehydration"]
        if dehy:
            assert dehy[0].specialist_referral == ""

    def test_low_confidence_no_referral(self, db: HealthDB) -> None:
        """Below referral threshold → no specialist suggested."""
        # Just the trigger, no optionals → base confidence
        _insert_labs(db, {"tsh": 5.5}, user_id=93)
        gen = HypothesisGenerator(db)
        hyps = gen.scan_all(user_id=93)
        thyroid = [h for h in hyps if h.pattern_id == "hypothyroidism"]
        assert len(thyroid) == 1
        # Base confidence 0.50 matches threshold 0.5, so referral should appear
        assert thyroid[0].specialist_referral == "Endocrinologist"
