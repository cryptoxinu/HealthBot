"""Tests for the deterministic medical classifier."""
from __future__ import annotations

from healthbot.nlu.medical_classifier import (
    classify_medical_category,
    is_medically_relevant,
)


class TestIsMedicallyRelevant:
    """Deterministic medical relevance detection."""

    def test_symptom_text_is_relevant(self):
        assert is_medically_relevant("I've been having a headache lately")

    def test_medication_text_is_relevant(self):
        assert is_medically_relevant("Started taking a new medication 500 mg")

    def test_lab_text_is_relevant(self):
        assert is_medically_relevant("My cholesterol results came back")

    def test_condition_text_is_relevant(self):
        assert is_medically_relevant("I was diagnosed with hypothyroidism")

    def test_greeting_not_relevant(self):
        assert not is_medically_relevant("hello")

    def test_empty_text_not_relevant(self):
        assert not is_medically_relevant("")

    def test_short_text_not_relevant(self):
        assert not is_medically_relevant("ok")


class TestClassifyCategory:
    """Category classification for medical messages."""

    def test_classify_symptom(self):
        assert classify_medical_category("I have chest pain and nausea") == "symptom_report"

    def test_classify_medication_change(self):
        assert classify_medical_category("I stopped taking my statin") == "medication_change"

    def test_classify_doctor_visit(self):
        assert classify_medical_category("My doctor told me to recheck TSH") == "doctor_visit"

    def test_classify_lab_discussion(self):
        assert classify_medical_category("My blood test results are abnormal") == "lab_discussion"

    def test_classify_general_fallback(self):
        assert classify_medical_category("thinking about nutrition") == "general"
