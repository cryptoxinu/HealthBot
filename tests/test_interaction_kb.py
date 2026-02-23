"""Tests for the medication interaction knowledge base integrity."""
from __future__ import annotations

from healthbot.reasoning.interaction_kb import (
    CONDITION_ALIASES,
    DRUG_CONDITION_INTERACTIONS,
    DRUG_LAB_INTERACTIONS,
    INTERACTIONS,
    SUBSTANCE_ALIASES,
    TIMING_RULES,
)

VALID_SEVERITIES = {"minor", "moderate", "major", "contraindicated"}
VALID_EVIDENCE = {"established", "probable", "theoretical"}


class TestInteractionKBIntegrity:
    """Validate the static interaction knowledge base."""

    def test_no_duplicate_pairs(self) -> None:
        """Each substance pair should appear at most once."""
        seen: set[tuple[str, str]] = set()
        for ix in INTERACTIONS:
            pair = tuple(sorted((ix.substance_a, ix.substance_b)))
            assert pair not in seen, (
                f"Duplicate interaction pair: {pair[0]} + {pair[1]}"
            )
            seen.add(pair)

    def test_all_interactions_have_required_fields(self) -> None:
        """Every interaction must have non-empty required fields."""
        for ix in INTERACTIONS:
            assert ix.substance_a, "substance_a must not be empty"
            assert ix.substance_b, "substance_b must not be empty"
            assert ix.severity, "severity must not be empty"
            assert ix.mechanism, "mechanism must not be empty"
            assert ix.recommendation, "recommendation must not be empty"
            assert ix.evidence, "evidence must not be empty"

    def test_severity_values_valid(self) -> None:
        """Severity must be one of the defined levels."""
        for ix in INTERACTIONS:
            assert ix.severity in VALID_SEVERITIES, (
                f"Invalid severity '{ix.severity}' for "
                f"{ix.substance_a} + {ix.substance_b}"
            )

    def test_evidence_values_valid(self) -> None:
        """Evidence level must be one of the defined levels."""
        for ix in INTERACTIONS:
            assert ix.evidence in VALID_EVIDENCE, (
                f"Invalid evidence '{ix.evidence}' for "
                f"{ix.substance_a} + {ix.substance_b}"
            )

    def test_all_interactions_have_citations(self) -> None:
        """Every interaction should have at least one citation."""
        for ix in INTERACTIONS:
            assert len(ix.citations) >= 1, (
                f"Missing citation for {ix.substance_a} + {ix.substance_b}"
            )

    def test_substances_are_lowercase(self) -> None:
        """KB keys should be lowercase with underscores (no spaces)."""
        for ix in INTERACTIONS:
            assert ix.substance_a == ix.substance_a.lower(), (
                f"substance_a '{ix.substance_a}' should be lowercase"
            )
            assert ix.substance_b == ix.substance_b.lower(), (
                f"substance_b '{ix.substance_b}' should be lowercase"
            )
            assert " " not in ix.substance_a, (
                f"substance_a '{ix.substance_a}' should not contain spaces"
            )
            assert " " not in ix.substance_b, (
                f"substance_b '{ix.substance_b}' should not contain spaces"
            )

    def test_aliases_are_lowercase(self) -> None:
        """All alias keys should be lowercase."""
        for alias in SUBSTANCE_ALIASES:
            assert alias == alias.lower(), (
                f"Alias '{alias}' should be lowercase"
            )

    def test_minimum_interaction_count(self) -> None:
        """KB should have at least 100 drug-drug interactions."""
        assert len(INTERACTIONS) >= 100, (
            f"Expected >= 100 interactions, got {len(INTERACTIONS)}"
        )

    def test_minimum_alias_count(self) -> None:
        """Alias map should have at least 150 entries."""
        assert len(SUBSTANCE_ALIASES) >= 150, (
            f"Expected >= 150 aliases, got {len(SUBSTANCE_ALIASES)}"
        )


class TestDrugConditionKBIntegrity:
    """Validate the drug-condition interaction knowledge base."""

    def test_no_duplicate_drug_condition_pairs(self) -> None:
        """Each drug-condition pair should appear at most once."""
        seen: set[tuple[str, str]] = set()
        for ix in DRUG_CONDITION_INTERACTIONS:
            pair = (ix.drug, ix.condition)
            assert pair not in seen, (
                f"Duplicate drug-condition pair: {pair[0]} + {pair[1]}"
            )
            seen.add(pair)

    def test_drug_condition_required_fields(self) -> None:
        """Every drug-condition interaction must have non-empty required fields."""
        for ix in DRUG_CONDITION_INTERACTIONS:
            assert ix.drug, "drug must not be empty"
            assert ix.condition, "condition must not be empty"
            assert ix.severity in VALID_SEVERITIES, (
                f"Invalid severity '{ix.severity}' for {ix.drug} + {ix.condition}"
            )
            assert ix.mechanism, "mechanism must not be empty"
            assert ix.recommendation, "recommendation must not be empty"
            assert ix.evidence in VALID_EVIDENCE, (
                f"Invalid evidence '{ix.evidence}' for {ix.drug} + {ix.condition}"
            )

    def test_minimum_drug_condition_count(self) -> None:
        """KB should have at least 25 drug-condition interactions."""
        assert len(DRUG_CONDITION_INTERACTIONS) >= 25, (
            f"Expected >= 25 drug-condition interactions, "
            f"got {len(DRUG_CONDITION_INTERACTIONS)}"
        )

    def test_condition_aliases_are_lowercase(self) -> None:
        """All condition alias keys should be lowercase."""
        for alias in CONDITION_ALIASES:
            assert alias == alias.lower(), (
                f"Condition alias '{alias}' should be lowercase"
            )

    def test_condition_aliases_minimum_count(self) -> None:
        """Condition aliases should have at least 40 entries."""
        assert len(CONDITION_ALIASES) >= 40, (
            f"Expected >= 40 condition aliases, got {len(CONDITION_ALIASES)}"
        )

    def test_drug_condition_keys_exist_in_substance_or_interactions(self) -> None:
        """Drug keys in drug-condition KB should exist as substance aliases or interaction keys."""
        all_kb_keys = set()
        for ix in INTERACTIONS:
            all_kb_keys.add(ix.substance_a)
            all_kb_keys.add(ix.substance_b)
        for alias_key in SUBSTANCE_ALIASES.values():
            all_kb_keys.add(alias_key)

        for ix in DRUG_CONDITION_INTERACTIONS:
            assert ix.drug in all_kb_keys, (
                f"Drug key '{ix.drug}' in drug-condition KB is not a known substance"
            )


class TestNewCategories:
    """Test that new interaction categories are present and well-formed."""

    def test_doac_interactions_present(self) -> None:
        """DOACs should have interactions with NSAIDs and aspirin."""
        doac_pairs = {(ix.substance_a, ix.substance_b) for ix in INTERACTIONS
                      if "doac" in (ix.substance_a, ix.substance_b)}
        assert ("doac", "nsaid") in doac_pairs or ("nsaid", "doac") in doac_pairs
        assert ("doac", "aspirin") in doac_pairs or ("aspirin", "doac") in doac_pairs

    def test_supplement_interactions_present(self) -> None:
        """Key supplement interactions should be in the KB."""
        all_substances = set()
        for ix in INTERACTIONS:
            all_substances.add(ix.substance_a)
            all_substances.add(ix.substance_b)
        assert "cbd" in all_substances
        assert "berberine" in all_substances
        assert "five_htp" in all_substances
        assert "same" in all_substances
        assert "ashwagandha" in all_substances
        assert "melatonin" in all_substances

    def test_food_interactions_present(self) -> None:
        """Food-drug interactions should be in the KB."""
        all_substances = set()
        for ix in INTERACTIONS:
            all_substances.add(ix.substance_a)
            all_substances.add(ix.substance_b)
        assert "dairy" in all_substances
        assert "soy" in all_substances
        assert "tyramine_foods" in all_substances

    def test_antibiotic_interactions_present(self) -> None:
        """Antibiotic interactions should be present."""
        all_substances = set()
        for ix in INTERACTIONS:
            all_substances.add(ix.substance_a)
            all_substances.add(ix.substance_b)
        assert "fluoroquinolone" in all_substances
        assert "macrolide" in all_substances
        assert "tetracycline" in all_substances

    def test_timing_rules_expanded(self) -> None:
        """Timing rules should include new drug classes."""
        timing_substances = {r.substance for r in TIMING_RULES}
        assert "doac" in timing_substances
        assert "fluoroquinolone" in timing_substances
        assert "melatonin" in timing_substances
        assert "glp1_agonist" in timing_substances

    def test_drug_lab_expanded(self) -> None:
        """Drug-lab interactions should include new drug classes."""
        dl_drugs = {ix.drug for ix in DRUG_LAB_INTERACTIONS}
        assert "sglt2i" in dl_drugs
        assert "doac" in dl_drugs
        assert "snri" in dl_drugs

    def test_new_alias_resolution(self) -> None:
        """New substance aliases should resolve correctly."""
        assert SUBSTANCE_ALIASES["apixaban"] == "doac"
        assert SUBSTANCE_ALIASES["ozempic"] == "glp1_agonist"
        assert SUBSTANCE_ALIASES["ciprofloxacin"] == "fluoroquinolone"
        assert SUBSTANCE_ALIASES["azithromycin"] == "macrolide"
        assert SUBSTANCE_ALIASES["doxycycline"] == "tetracycline"
        assert SUBSTANCE_ALIASES["fluconazole"] == "azole_antifungal"
        assert SUBSTANCE_ALIASES["cbd"] == "cbd"
        assert SUBSTANCE_ALIASES["berberine"] == "berberine"
        assert SUBSTANCE_ALIASES["5-htp"] == "five_htp"

    def test_condition_alias_resolution(self) -> None:
        """Condition aliases should resolve to canonical keys."""
        assert CONDITION_ALIASES["ckd"] == "kidney_disease"
        assert CONDITION_ALIASES["chf"] == "heart_failure"
        assert CONDITION_ALIASES["copd"] == "copd"
        assert CONDITION_ALIASES["type 2 diabetes"] == "diabetes"
        assert CONDITION_ALIASES["gout"] == "gout"
        assert CONDITION_ALIASES["epilepsy"] == "seizure_disorder"
