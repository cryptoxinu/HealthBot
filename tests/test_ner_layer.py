"""Tests for the GLiNER NER layer.

Requires gliner to be installed: pip install -e ".[nlp]"
Tests are skipped if gliner is not available.
"""
from __future__ import annotations

import pytest

try:
    from gliner import GLiNER  # noqa: F401

    GLINER_AVAILABLE = True
except ImportError:
    GLINER_AVAILABLE = False

pytestmark = pytest.mark.skipif(not GLINER_AVAILABLE, reason="gliner not installed")


@pytest.fixture(scope="module")
def ner():
    """Load NER layer once for all tests (model loading is slow)."""
    from healthbot.security.ner_layer import NerLayer

    return NerLayer()


@pytest.mark.slow
class TestNerDetection:
    """Test that NER catches PII that regex would miss."""

    def test_catches_unlabeled_person_name(self, ner):
        entities = ner.detect("Sarah Johnson called about her results")
        names = [e for e in entities if e.label == "person"]
        assert any("Sarah" in e.text for e in names)

    def test_catches_city_name(self, ner):
        entities = ner.detect("The patient lives in Cleveland, Ohio")
        locations = [e for e in entities if e.label == "location"]
        assert any("Cleveland" in e.text for e in locations)

    def test_catches_organization(self, ner):
        entities = ner.detect("She was referred to Cleveland Clinic")
        orgs = [e for e in entities if e.label == "organization"]
        assert any("Cleveland Clinic" in e.text for e in orgs)

    def test_catches_doctor_name(self, ner):
        entities = ner.detect("Dr. Robert Chen is her endocrinologist")
        names = [e for e in entities if e.label == "person"]
        assert any("Robert" in e.text or "Chen" in e.text for e in names)

    def test_catches_ssn(self, ner):
        entities = ner.detect("Her SSN is 123-45-6789")
        found = [e for e in entities if "123-45-6789" in e.text]
        assert len(found) > 0

    def test_catches_email(self, ner):
        entities = ner.detect("Contact at sarah@gmail.com for info")
        emails = [e for e in entities if e.label == "email"]
        assert any("sarah@gmail.com" in e.text for e in emails)


class TestMedicalValuePreservation:
    """Medical values must NOT be flagged as PII."""

    def test_preserves_glucose(self, ner):
        entities = ner.detect("Glucose: 108 mg/dL")
        assert len(entities) == 0

    def test_preserves_hba1c(self, ner):
        entities = ner.detect("HbA1c: 5.7%")
        assert len(entities) == 0

    def test_preserves_cholesterol(self, ner):
        entities = ner.detect("Total Cholesterol: 210 mg/dL, LDL: 135 mg/dL")
        assert len(entities) == 0

    def test_preserves_platelet_count(self, ner):
        entities = ner.detect("Platelets: 250000 /uL")
        assert len(entities) == 0

    def test_preserves_tsh(self, ner):
        entities = ner.detect("TSH: 2.15 mIU/L")
        assert len(entities) == 0

    def test_preserves_wbc(self, ner):
        entities = ner.detect("WBC: 7200 cells/uL")
        assert len(entities) == 0

    def test_preserves_wearable_metrics(self, ner):
        entities = ner.detect("HRV:42, RHR:68, Recovery:65, Sleep:72")
        assert len(entities) == 0

    def test_preserves_medications(self, ner):
        entities = ner.detect("Metformin 500mg twice daily, Atorvastatin 20mg")
        assert len(entities) == 0

    def test_preserves_reference_ranges(self, ner):
        entities = ner.detect("Glucose: 105 mg/dL (ref 70-100) [HIGH]")
        assert len(entities) == 0


class TestConversationLabelFiltering:
    """Conversation labels like 'User:' must not be flagged."""

    def test_ignores_user_label(self, ner):
        entities = ner.detect("User: What is my glucose level?")
        names = [e for e in entities if e.text.strip() == "User"]
        assert len(names) == 0

    def test_ignores_you_label(self, ner):
        entities = ner.detect("You: Your glucose is slightly elevated.")
        names = [e for e in entities if e.text.strip() == "You"]
        assert len(names) == 0


class TestNerRedaction:
    """Test the redact method produces clean text."""

    def test_redacts_name(self, ner):
        cleaned, had_pii = ner.redact("Sarah Johnson has glucose 108")
        assert "Sarah" not in cleaned
        assert "108" in cleaned
        assert had_pii is True

    def test_clean_text_unchanged(self, ner):
        text = "Glucose: 108 mg/dL and HRV: 55ms"
        cleaned, had_pii = ner.redact(text)
        assert cleaned == text
        assert had_pii is False


@pytest.mark.slow
class TestTwoLayerIntegration:
    """Test NER + regex working together via Anonymizer."""

    def test_ner_catches_name_regex_catches_ssn(self):
        from healthbot.llm.anonymizer import Anonymizer

        anon = Anonymizer(use_ner=True)
        assert anon.has_ner

        text = "Sarah Johnson SSN 123-45-6789 glucose 108"
        cleaned, had_phi = anon.anonymize(text)
        assert "Sarah" not in cleaned
        assert "123-45-6789" not in cleaned
        assert "108" in cleaned
        assert had_phi is True

    def test_ner_catches_city_regex_catches_mrn(self):
        from healthbot.llm.anonymizer import Anonymizer

        anon = Anonymizer(use_ner=True)
        text = "Lives in Cleveland MRN: 12345678 glucose 108"
        cleaned, had_phi = anon.anonymize(text)
        assert "Cleveland" not in cleaned
        assert "12345678" not in cleaned
        assert "108" in cleaned

    def test_assert_safe_checks_both_layers(self):
        from healthbot.llm.anonymizer import AnonymizationError, Anonymizer

        anon = Anonymizer(use_ner=True)
        # NER should catch this (unlabeled name)
        with pytest.raises(AnonymizationError):
            anon.assert_safe("Tell Sarah Johnson her results are ready")


class TestIsAvailable:
    """Test availability check."""

    def test_is_available_when_installed(self):
        from healthbot.security.ner_layer import NerLayer

        assert NerLayer.is_available() is True


class TestNerFalsePositiveSuppression:
    """Medical terms from MEDICAL_TERMS must never be flagged as PII.

    Parametrized over a subset of MEDICAL_TERMS most commonly misclassified.
    """

    STANDALONE_TERMS = [
        "Iron", "Calcium", "Zinc", "Selenium", "Magnesium",
        "Vitamin D", "Vitamin B12", "Vitamin K", "Folate",
        "Cortisol", "Insulin", "Ferritin", "Hemoglobin",
        "Cholesterol", "Creatinine", "Albumin", "Fibrinogen",
        "JAK2", "MTHFR", "BRCA", "APOE",
    ]

    @pytest.mark.parametrize("term", STANDALONE_TERMS)
    def test_standalone_medical_term_not_flagged(self, ner, term):
        entities = ner.detect(term)
        assert len(entities) == 0, f"NER falsely flagged '{term}' as PII: {entities}"

    CONTEXTUAL_SENTENCES = [
        "Lab result: Iron is within normal range",
        "Vitamin D insufficiency noted on latest panel",
        "MTHFR C677T heterozygous variant detected",
        "Factor V Leiden heterozygous — consider anticoagulation",
        "JAK2 V617F positive — polycythemia vera confirmed",
        "Ferritin trending down from 45 to 12 ng/mL",
        "Cortisol 15.2 mcg/dL at 8AM is appropriate",
        "Calcium supplementation 600mg with Vitamin D3",
    ]

    @pytest.mark.parametrize("sentence", CONTEXTUAL_SENTENCES)
    def test_medical_term_in_context_not_flagged(self, ner, sentence):
        entities = ner.detect(sentence)
        # Allow location/org entities for real locations, but no person entities
        # from medical terms
        person_entities = [e for e in entities if e.label == "person"]
        assert len(person_entities) == 0, (
            f"NER falsely flagged person in medical text: {person_entities}"
        )


class TestNerChunking:
    """Test chunking behavior for texts exceeding the chunk size."""

    def test_long_text_chunked(self, ner):
        """500-char text forces chunking. Results should still be valid."""
        text = (
            "Glucose 108 mg/dL. " * 15
            + "Blood draw for routine checkup. "
            + "HbA1c 5.7% within normal limits. " * 5
        )
        assert len(text) > ner._CHUNK_SIZE
        entities = ner.detect(text)
        # Should find no PII in pure medical text
        assert len(entities) == 0

    def test_entity_at_chunk_boundary_preserved(self, ner):
        """An entity near the boundary should still be detected."""
        # Build text so a name appears around char 300 (near chunk boundary)
        padding = "Lab results show glucose 108 mg/dL. " * 8  # ~288 chars
        text = padding + "Sarah Johnson called about results. More lab data follows."
        assert len(text) > ner._CHUNK_SIZE
        entities = ner.detect(text)
        names = [e for e in entities if e.label == "person"]
        assert any("Sarah" in e.text for e in names), (
            "Name near chunk boundary should be detected"
        )

    def test_sentence_boundary_splitting(self, ner):
        """Chunking should prefer sentence boundaries over arbitrary splits."""
        # Each sentence is ~50 chars; 7 sentences = ~350 chars > chunk size
        sentences = [
            "The patient had glucose of 108 mg/dL.",
            "Her HbA1c was measured at 5.7 percent.",
            "Total cholesterol came back at 210.",
            "LDL was elevated at 145 mg/dL range.",
            "HDL was within normal limits at 55.",
            "Triglycerides measured 150 mg/dL today.",
            "All liver enzymes were within range.",
        ]
        text = " ".join(sentences)
        entities = ner.detect(text)
        # No false positives expected in pure medical text
        assert len(entities) == 0


class TestOverlapRatioDedup:
    """Test the overlap-ratio deduplication logic."""

    def test_overlapping_same_label_merged(self):
        from healthbot.security.ner_layer import NerEntity, NerLayer

        entities = [
            NerEntity(label="person", text="Sarah", start=0, end=5, score=0.7),
            NerEntity(label="person", text="Sarah Johnson", start=0, end=13, score=0.9),
        ]
        deduped = NerLayer._dedup_entities(entities)
        assert len(deduped) == 1
        assert deduped[0].score == 0.9
        assert deduped[0].end == 13  # Widest span

    def test_non_overlapping_not_merged(self):
        from healthbot.security.ner_layer import NerEntity, NerLayer

        entities = [
            NerEntity(label="person", text="Sarah", start=0, end=5, score=0.8),
            NerEntity(label="person", text="John", start=20, end=24, score=0.8),
        ]
        deduped = NerLayer._dedup_entities(entities)
        assert len(deduped) == 2

    def test_overlapping_different_label_not_merged(self):
        from healthbot.security.ner_layer import NerEntity, NerLayer

        entities = [
            NerEntity(label="person", text="Cleveland", start=0, end=9, score=0.7),
            NerEntity(label="location", text="Cleveland", start=0, end=9, score=0.9),
        ]
        deduped = NerLayer._dedup_entities(entities)
        assert len(deduped) == 2  # Different labels, not merged
