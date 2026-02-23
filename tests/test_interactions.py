"""Tests for medication interaction checker."""
from __future__ import annotations

import uuid
from datetime import date
from unittest.mock import MagicMock

from healthbot.data.models import Medication
from healthbot.reasoning.interactions import (
    DrugConditionResult,
    DrugLabResult,
    InteractionChecker,
    InteractionResult,
)


def _make_med(name: str) -> dict:
    """Create a mock decrypted medication dict (as returned by get_active_medications)."""
    return {
        "name": name,
        "dose": "10mg",
        "frequency": "daily",
        "prescriber": "Dr. Test",
        "start_date": "2024-01-01",
        "end_date": None,
        "status": "active",
    }


class TestInteractionChecker:
    """Test the interaction checking engine."""

    def test_statin_grapefruit_detected(self, db) -> None:
        """Atorvastatin + grapefruit should flag a major interaction."""
        # Insert two medications
        med_a = Medication(id=uuid.uuid4().hex, name="Atorvastatin", dose="20mg",
                           frequency="daily", status="active", start_date=date(2024, 1, 1))
        med_b = Medication(id=uuid.uuid4().hex, name="Grapefruit", dose="daily",
                           frequency="daily", status="active", start_date=date(2024, 1, 1))
        db.insert_medication(med_a)
        db.insert_medication(med_b)

        checker = InteractionChecker(db)
        results = checker.check_all()

        assert len(results) >= 1
        found = any(
            r.interaction.severity == "major"
            and "CYP3A4" in r.interaction.mechanism
            for r in results
        )
        assert found, "Statin + grapefruit major interaction not detected"

    def test_no_interactions_empty_meds(self, db) -> None:
        """Empty medication list should return no interactions."""
        checker = InteractionChecker(db)
        results = checker.check_all()
        assert results == []

    def test_no_interactions_single_med(self, db) -> None:
        """A single medication cannot have interactions with itself."""
        med = Medication(id=uuid.uuid4().hex, name="Atorvastatin", dose="20mg",
                         frequency="daily", status="active")
        db.insert_medication(med)

        checker = InteractionChecker(db)
        results = checker.check_all()
        assert results == []

    def test_severity_sorting(self, db) -> None:
        """Results should be sorted with major interactions first."""
        # warfarin + aspirin (major), warfarin + fish oil (moderate)
        meds = [
            Medication(id=uuid.uuid4().hex, name="Warfarin", dose="5mg",
                       frequency="daily", status="active"),
            Medication(id=uuid.uuid4().hex, name="Aspirin", dose="81mg",
                       frequency="daily", status="active"),
            Medication(id=uuid.uuid4().hex, name="Fish Oil", dose="1000mg",
                       frequency="daily", status="active"),
        ]
        for m in meds:
            db.insert_medication(m)

        checker = InteractionChecker(db)
        results = checker.check_all()

        assert len(results) >= 2
        # First result should be major (warfarin + aspirin)
        assert results[0].interaction.severity == "major"
        # Moderate interactions come after major
        major_idx = [i for i, r in enumerate(results) if r.interaction.severity == "major"]
        moderate_idx = [i for i, r in enumerate(results) if r.interaction.severity == "moderate"]
        if major_idx and moderate_idx:
            assert max(major_idx) < min(moderate_idx)

    def test_alias_resolution(self) -> None:
        """Drug brand/generic names should resolve to KB keys."""
        # Use a mock DB to test the normalizer directly
        mock_db = MagicMock()
        checker = InteractionChecker(mock_db)

        assert checker._normalize_to_kb("Atorvastatin") == "statin"
        assert checker._normalize_to_kb("simvastatin") == "statin"
        assert checker._normalize_to_kb("LIPITOR") == "statin"
        assert checker._normalize_to_kb("coumadin") == "warfarin"
        assert checker._normalize_to_kb("Synthroid") == "levothyroxine"
        assert checker._normalize_to_kb("Omeprazole") == "ppi"
        assert checker._normalize_to_kb("Fish Oil") == "fish_oil"
        assert checker._normalize_to_kb("vitamin d3") == "vitamin_d"
        assert checker._normalize_to_kb("st john's wort") == "st_johns_wort"

    def test_alias_resolution_unknown(self) -> None:
        """Unknown medications should return None."""
        mock_db = MagicMock()
        checker = InteractionChecker(mock_db)

        assert checker._normalize_to_kb("xyznotamedicine") is None

    def test_check_against_new_med(self, db) -> None:
        """Check a new medication against existing active meds."""
        # User is on warfarin
        med = Medication(id=uuid.uuid4().hex, name="Warfarin", dose="5mg",
                         frequency="daily", status="active")
        db.insert_medication(med)

        checker = InteractionChecker(db)
        results = checker.check_against("Ibuprofen")

        assert len(results) >= 1
        found = any(
            r.interaction.severity == "major"
            and "bleeding" in r.interaction.mechanism.lower()
            for r in results
        )
        assert found, "Warfarin + NSAID interaction not detected via check_against"

    def test_check_against_no_interactions(self, db) -> None:
        """check_against with unrecognized med should return empty."""
        med = Medication(id=uuid.uuid4().hex, name="Warfarin", dose="5mg",
                         frequency="daily", status="active")
        db.insert_medication(med)

        checker = InteractionChecker(db)
        results = checker.check_against("xyznotamedicine")
        assert results == []

    def test_format_output(self) -> None:
        """format_results should produce readable output."""
        from healthbot.reasoning.interaction_kb import Interaction

        results = [
            InteractionResult(
                med_a_name="Atorvastatin",
                med_b_name="Grapefruit Juice",
                interaction=Interaction(
                    substance_a="statin",
                    substance_b="grapefruit",
                    severity="major",
                    mechanism="CYP3A4 inhibition increases statin levels.",
                    recommendation="Avoid grapefruit while on statins.",
                    evidence="established",
                    citations=("Lilja JJ et al. 1998.",),
                ),
            ),
        ]
        output = InteractionChecker.format_results(results)

        assert "!!! MAJOR" in output
        assert "Atorvastatin" in output
        assert "Grapefruit Juice" in output
        assert "CYP3A4" in output
        assert "Avoid grapefruit" in output
        assert "established" in output
        assert "Lilja" in output

    def test_format_output_empty(self) -> None:
        """format_results with no results should produce friendly message."""
        output = InteractionChecker.format_results([])
        assert "No known interactions" in output

    def test_duplicate_med_class_not_self_interact(self, db) -> None:
        """Two statins should not flag an interaction with each other."""
        meds = [
            Medication(id=uuid.uuid4().hex, name="Atorvastatin", dose="20mg",
                       frequency="daily", status="active"),
            Medication(id=uuid.uuid4().hex, name="Rosuvastatin", dose="10mg",
                       frequency="daily", status="active"),
        ]
        for m in meds:
            db.insert_medication(m)

        checker = InteractionChecker(db)
        results = checker.check_all()
        # Both resolve to "statin" so no self-interaction
        assert len(results) == 0


class TestDrugLabInteractions:
    """Test drug-lab interaction checking."""

    def test_metformin_b12_low_detected(self, db) -> None:
        """Metformin + low B12 should flag drug-lab interaction."""
        from healthbot.data.models import LabResult

        med = Medication(id=uuid.uuid4().hex, name="Metformin", dose="1000mg",
                         frequency="twice daily", status="active")
        db.insert_medication(med)

        lab = LabResult(id=uuid.uuid4().hex, test_name="Vitamin B12",
                        canonical_name="vitamin_b12", value=180,
                        unit="pg/mL", flag="L",
                        date_collected=date(2025, 12, 1))
        db.insert_observation(lab)

        checker = InteractionChecker(db)
        results = checker.check_drug_lab()

        b12_hits = [r for r in results if r.lab_name == "vitamin_b12"]
        assert len(b12_hits) >= 1
        assert b12_hits[0].lab_flag == "L"
        assert b12_hits[0].interaction.effect == "decrease"
        assert "B12" in b12_hits[0].interaction.mechanism

    def test_statin_monitoring_gap(self, db) -> None:
        """Statin without CK or liver enzyme results should flag monitoring gap."""
        med = Medication(id=uuid.uuid4().hex, name="Atorvastatin", dose="20mg",
                         frequency="daily", status="active")
        db.insert_medication(med)

        checker = InteractionChecker(db)
        results = checker.check_drug_lab()

        # Should have monitoring gaps for CK, ALT, AST, CoQ10
        gap_labs = {r.lab_name for r in results if not r.lab_value}
        assert "creatine_kinase" in gap_labs or "alt" in gap_labs, \
            f"Expected monitoring gaps for statin, got: {gap_labs}"

    def test_ace_inhibitor_potassium_high(self, db) -> None:
        """ACE inhibitor + high potassium should flag major interaction."""
        from healthbot.data.models import LabResult

        med = Medication(id=uuid.uuid4().hex, name="Lisinopril", dose="10mg",
                         frequency="daily", status="active")
        db.insert_medication(med)

        lab = LabResult(id=uuid.uuid4().hex, test_name="Potassium",
                        canonical_name="potassium", value=5.8,
                        unit="mEq/L", flag="H",
                        date_collected=date(2025, 12, 1))
        db.insert_observation(lab)

        checker = InteractionChecker(db)
        results = checker.check_drug_lab()

        k_hits = [r for r in results if r.lab_name == "potassium"]
        assert len(k_hits) >= 1
        assert k_hits[0].interaction.severity == "major"
        assert k_hits[0].lab_flag == "H"

    def test_no_drug_lab_without_meds(self, db) -> None:
        """No medications should return empty drug-lab results."""
        checker = InteractionChecker(db)
        results = checker.check_drug_lab()
        assert results == []

    def test_normal_lab_not_flagged(self, db) -> None:
        """Metformin + normal B12 should NOT flag (only low/missing flagged)."""
        from healthbot.data.models import LabResult

        med = Medication(id=uuid.uuid4().hex, name="Metformin", dose="500mg",
                         frequency="daily", status="active")
        db.insert_medication(med)

        lab = LabResult(id=uuid.uuid4().hex, test_name="Vitamin B12",
                        canonical_name="vitamin_b12", value=450,
                        unit="pg/mL", flag="",
                        date_collected=date(2025, 12, 1))
        db.insert_observation(lab)

        checker = InteractionChecker(db)
        results = checker.check_drug_lab()

        b12_hits = [r for r in results if r.lab_name == "vitamin_b12"]
        assert len(b12_hits) == 0, "Normal B12 should not be flagged"

    def test_severity_sorting_drug_lab(self, db) -> None:
        """Drug-lab results should be sorted by severity (major first)."""
        from healthbot.data.models import LabResult

        # ACE inhibitor (major for K+) + PPI (moderate for Mg)
        meds = [
            Medication(id=uuid.uuid4().hex, name="Lisinopril", dose="10mg",
                       frequency="daily", status="active"),
            Medication(id=uuid.uuid4().hex, name="Omeprazole", dose="20mg",
                       frequency="daily", status="active"),
        ]
        for m in meds:
            db.insert_medication(m)

        labs = [
            LabResult(id=uuid.uuid4().hex, test_name="Potassium",
                      canonical_name="potassium", value=5.9,
                      unit="mEq/L", flag="H",
                      date_collected=date(2025, 12, 1)),
            LabResult(id=uuid.uuid4().hex, test_name="Magnesium",
                      canonical_name="magnesium", value=1.3,
                      unit="mg/dL", flag="L",
                      date_collected=date(2025, 12, 1)),
        ]
        for lab in labs:
            db.insert_observation(lab)

        checker = InteractionChecker(db)
        results = checker.check_drug_lab()

        findings = [r for r in results if r.lab_value]
        if len(findings) >= 2:
            from healthbot.reasoning.interactions import _SEVERITY_ORDER
            for i in range(len(findings) - 1):
                sev_i = _SEVERITY_ORDER.get(findings[i].interaction.severity, 99)
                sev_next = _SEVERITY_ORDER.get(findings[i + 1].interaction.severity, 99)
                assert sev_i <= sev_next

    def test_format_drug_lab_output(self) -> None:
        """format_drug_lab_results should produce readable output."""
        from healthbot.reasoning.interaction_kb import DrugLabInteraction

        results = [
            DrugLabResult(
                med_name="Metformin",
                lab_name="vitamin_b12",
                lab_value="180",
                lab_flag="L",
                interaction=DrugLabInteraction(
                    drug="metformin", lab="vitamin_b12",
                    effect="decrease",
                    mechanism="Metformin reduces B12 absorption.",
                    monitor="Check B12 annually.",
                    severity="moderate", evidence="established",
                    citation="Aroda VR et al. 2016.",
                ),
            ),
        ]
        output = InteractionChecker.format_drug_lab_results(results)
        assert "Metformin" in output
        assert "Vitamin B12" in output  # title-cased
        assert "(LOW)" in output
        assert "B12 absorption" in output

    def test_format_drug_lab_empty(self) -> None:
        """format_drug_lab_results with empty list should return empty string."""
        output = InteractionChecker.format_drug_lab_results([])
        assert output == ""

    def test_thiazide_alias_resolution(self) -> None:
        """Thiazide aliases should resolve correctly."""
        mock_db = MagicMock()
        checker = InteractionChecker(mock_db)
        assert checker._normalize_to_kb("Hydrochlorothiazide") == "thiazide"
        assert checker._normalize_to_kb("HCTZ") == "thiazide"
        assert checker._normalize_to_kb("chlorthalidone") == "thiazide"

    def test_corticosteroid_alias_resolution(self) -> None:
        """Corticosteroid aliases should resolve correctly."""
        mock_db = MagicMock()
        checker = InteractionChecker(mock_db)
        assert checker._normalize_to_kb("Prednisone") == "corticosteroid"
        assert checker._normalize_to_kb("dexamethasone") == "corticosteroid"
        assert checker._normalize_to_kb("methylprednisolone") == "corticosteroid"

    def test_new_alias_classes(self) -> None:
        """New drug class aliases should resolve correctly."""
        mock_db = MagicMock()
        checker = InteractionChecker(mock_db)
        assert checker._normalize_to_kb("Apixaban") == "doac"
        assert checker._normalize_to_kb("Eliquis") == "doac"
        assert checker._normalize_to_kb("semaglutide") == "glp1_agonist"
        assert checker._normalize_to_kb("Ozempic") == "glp1_agonist"
        assert checker._normalize_to_kb("empagliflozin") == "sglt2i"
        assert checker._normalize_to_kb("ciprofloxacin") == "fluoroquinolone"
        assert checker._normalize_to_kb("azithromycin") == "macrolide"
        assert checker._normalize_to_kb("CBD") == "cbd"
        assert checker._normalize_to_kb("berberine") == "berberine"
        assert checker._normalize_to_kb("5-HTP") == "five_htp"


class TestDrugConditionInteractions:
    """Test drug-condition interaction checking."""

    def test_nsaid_heart_failure_detected(self, db) -> None:
        """NSAIDs + heart failure should flag major interaction."""
        med = Medication(id=uuid.uuid4().hex, name="Ibuprofen", dose="400mg",
                         frequency="as needed", status="active")
        db.insert_medication(med)
        db.insert_ltm(
            user_id=0, category="condition",
            fact="Known condition: Heart Failure",
            source="mychart_import",
        )

        checker = InteractionChecker(db)
        results = checker.check_drug_condition()

        assert len(results) >= 1
        hf_hits = [r for r in results if r.condition_name == "Heart Failure"]
        assert len(hf_hits) >= 1
        assert hf_hits[0].interaction.severity == "major"
        assert "sodium" in hf_hits[0].interaction.mechanism.lower() or \
               "retention" in hf_hits[0].interaction.mechanism.lower()

    def test_beta_blocker_asthma_detected(self, db) -> None:
        """Beta-blocker + asthma should flag major interaction."""
        med = Medication(id=uuid.uuid4().hex, name="Propranolol", dose="40mg",
                         frequency="twice daily", status="active")
        db.insert_medication(med)
        db.insert_ltm(
            user_id=0, category="condition",
            fact="Known condition: Asthma",
            source="onboarding",
        )

        checker = InteractionChecker(db)
        results = checker.check_drug_condition()

        assert len(results) >= 1
        assert any(r.interaction.severity == "major" for r in results)

    def test_metformin_ckd_detected(self, db) -> None:
        """Metformin + CKD should flag major interaction."""
        med = Medication(id=uuid.uuid4().hex, name="Metformin", dose="1000mg",
                         frequency="twice daily", status="active")
        db.insert_medication(med)
        db.insert_ltm(
            user_id=0, category="condition",
            fact="Known condition: Chronic Kidney Disease",
            source="mychart_import",
        )

        checker = InteractionChecker(db)
        results = checker.check_drug_condition()

        assert len(results) >= 1
        ckd_hits = [r for r in results if "kidney" in r.condition_name.lower()
                    or "ckd" in r.condition_name.lower()]
        assert len(ckd_hits) >= 1
        assert ckd_hits[0].interaction.severity == "major"

    def test_no_conditions_returns_empty(self, db) -> None:
        """No conditions on file should return empty results."""
        med = Medication(id=uuid.uuid4().hex, name="Ibuprofen", dose="400mg",
                         frequency="daily", status="active")
        db.insert_medication(med)

        checker = InteractionChecker(db)
        results = checker.check_drug_condition()
        assert results == []

    def test_no_meds_returns_empty(self, db) -> None:
        """No medications should return empty results."""
        db.insert_ltm(
            user_id=0, category="condition",
            fact="Known condition: Heart Failure",
            source="onboarding",
        )

        checker = InteractionChecker(db)
        results = checker.check_drug_condition()
        assert results == []

    def test_allergy_facts_skipped(self, db) -> None:
        """Allergy facts should not be treated as conditions."""
        med = Medication(id=uuid.uuid4().hex, name="Ibuprofen", dose="400mg",
                         frequency="daily", status="active")
        db.insert_medication(med)
        db.insert_ltm(
            user_id=0, category="condition",
            fact="Known allergy: Penicillin",
            source="mychart_import",
        )

        checker = InteractionChecker(db)
        results = checker.check_drug_condition()
        assert results == []

    def test_severity_sorting_drug_condition(self, db) -> None:
        """Drug-condition results should be sorted by severity."""
        meds = [
            Medication(id=uuid.uuid4().hex, name="Ibuprofen", dose="400mg",
                       frequency="daily", status="active"),
            Medication(id=uuid.uuid4().hex, name="Prednisone", dose="10mg",
                       frequency="daily", status="active"),
        ]
        for m in meds:
            db.insert_medication(m)
        # NSAIDs + heart failure = major, corticosteroid + diabetes = major
        db.insert_ltm(user_id=0, category="condition",
                       fact="Known condition: Heart Failure", source="test")
        db.insert_ltm(user_id=0, category="condition",
                       fact="Known condition: Type 2 Diabetes", source="test")

        checker = InteractionChecker(db)
        results = checker.check_drug_condition()
        assert len(results) >= 2

        from healthbot.reasoning.interactions import _SEVERITY_ORDER
        for i in range(len(results) - 1):
            sev_i = _SEVERITY_ORDER.get(results[i].interaction.severity, 99)
            sev_next = _SEVERITY_ORDER.get(results[i + 1].interaction.severity, 99)
            assert sev_i <= sev_next

    def test_format_drug_condition_output(self) -> None:
        """format_drug_condition_results should produce readable output."""
        from healthbot.reasoning.interaction_kb import DrugConditionInteraction

        results = [
            DrugConditionResult(
                med_name="Ibuprofen",
                condition_name="Heart Failure",
                interaction=DrugConditionInteraction(
                    drug="nsaid", condition="heart_failure",
                    severity="major",
                    mechanism="NSAIDs worsen heart failure.",
                    recommendation="Avoid. Use acetaminophen.",
                    evidence="established",
                    citation="Gislason GH et al. 2009.",
                ),
            ),
        ]
        output = InteractionChecker.format_drug_condition_results(results)
        assert "!!! MAJOR" in output
        assert "Ibuprofen" in output
        assert "Heart Failure" in output
        assert "acetaminophen" in output

    def test_format_drug_condition_empty(self) -> None:
        """format_drug_condition_results with empty list should return empty."""
        output = InteractionChecker.format_drug_condition_results([])
        assert output == ""

    def test_condition_name_extraction(self) -> None:
        """_extract_condition_name should parse LTM fact patterns."""
        result = InteractionChecker._extract_condition_name(
            "Known condition: Type 2 Diabetes"
        )
        assert result == "Type 2 Diabetes"

        result = InteractionChecker._extract_condition_name(
            "Known condition: Hypertension (status: active)"
        )
        assert result == "Hypertension"

        # Allergies should be skipped
        result = InteractionChecker._extract_condition_name(
            "Known allergy: Penicillin"
        )
        assert result == ""

    def test_condition_normalization(self) -> None:
        """_normalize_condition should map names to KB keys."""
        assert InteractionChecker._normalize_condition("Heart Failure") == "heart_failure"
        assert InteractionChecker._normalize_condition("CKD") == "kidney_disease"
        assert InteractionChecker._normalize_condition("asthma") == "asthma"
        assert InteractionChecker._normalize_condition("Type 2 Diabetes") == "diabetes"
        assert InteractionChecker._normalize_condition("xyznotacondition") is None
