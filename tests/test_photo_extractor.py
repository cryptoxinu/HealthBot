"""Tests for smart photo content extraction (Phase U1)."""
from __future__ import annotations

from healthbot.reasoning.photo_extractor import (
    PhotoClassification,
    classify_photo,
    extract_lab_values,
    extract_medications,
    format_extraction_summary,
)


class TestPhotoClassification:
    """Test photo content type classification."""

    def test_medication_bottle(self) -> None:
        """Description of medication should classify as medication_bottle."""
        description = (
            "A white prescription bottle labeled 'Atorvastatin Calcium Tablets'. "
            "The dosage shows 20 mg per tablet. Pharmacy label on the side "
            "with instructions: 'Take one tablet daily'. "
            "NDC number and Rx information visible."
        )
        result = classify_photo(description)
        assert result.content_type == "medication_bottle"
        assert result.confidence > 0.5

    def test_lab_printout(self) -> None:
        """Description of lab results should classify as lab_printout."""
        description = (
            "A printed lab result document showing 'Chemistry Panel'. "
            "Patient name is redacted. Test results listed in a table format. "
            "Reference range column visible. Specimen type: blood. "
            "Collected: 2025-01-15. Reported: 2025-01-16. "
            "Tests include Glucose 95 mg/dL, BUN 15 mg/dL, Creatinine 0.9 mg/dL."
        )
        result = classify_photo(description)
        assert result.content_type == "lab_printout"
        assert result.confidence > 0.5

    def test_general_health(self) -> None:
        """Description without med/lab keywords should be general_health."""
        description = (
            "A close-up photo of a person's forearm showing "
            "a small red rash approximately 2 cm in diameter."
        )
        result = classify_photo(description)
        assert result.content_type == "general_health"

    def test_classification_returns_extracted_data(self) -> None:
        """Classification should auto-extract when type matches."""
        description = (
            "A prescription pill bottle containing Metformin tablets. "
            "The label reads 500 mg. Pharmacy sticker visible."
        )
        result = classify_photo(description)
        assert result.content_type == "medication_bottle"
        assert len(result.extracted_meds) >= 1
        assert any("Metformin" in m.name for m in result.extracted_meds)


class TestMedicationExtraction:
    """Test medication name and dose extraction."""

    def test_extract_drug_name(self) -> None:
        """Should extract known drug names."""
        text = "The bottle contains Lisinopril 10 mg tablets."
        meds = extract_medications(text)
        assert len(meds) == 1
        assert meds[0].name == "Lisinopril"

    def test_extract_dose(self) -> None:
        """Should extract dose and unit."""
        text = "Metformin 500 mg extended release tablet"
        meds = extract_medications(text)
        assert len(meds) == 1
        assert meds[0].dose == "500"
        assert meds[0].unit == "mg"

    def test_extract_form(self) -> None:
        """Should detect tablet/capsule form."""
        text = "Omeprazole 20 mg capsule for oral use"
        meds = extract_medications(text)
        assert len(meds) == 1
        assert meds[0].form == "capsule"

    def test_extract_multiple_meds(self) -> None:
        """Should extract multiple medications."""
        text = (
            "Patient medications: Metformin 1000 mg, "
            "Lisinopril 20 mg, Atorvastatin 40 mg"
        )
        meds = extract_medications(text)
        assert len(meds) == 3
        names = {m.name for m in meds}
        assert "Metformin" in names
        assert "Lisinopril" in names
        assert "Atorvastatin" in names

    def test_no_duplicates(self) -> None:
        """Should not extract the same drug twice."""
        text = "Metformin bottle. Take Metformin with food."
        meds = extract_medications(text)
        assert len(meds) == 1

    def test_supplement_extraction(self) -> None:
        """Should extract vitamin/supplement names."""
        text = "Bottle of Vitamin D 5000 IU supplement"
        meds = extract_medications(text)
        assert len(meds) == 1
        assert "Vitamin D" in meds[0].name

    def test_no_match(self) -> None:
        """No drugs should return empty list."""
        text = "A photo of a sunset over the ocean."
        meds = extract_medications(text)
        assert meds == []


class TestLabValueExtraction:
    """Test lab value extraction from descriptions."""

    def test_extract_glucose(self) -> None:
        """Should extract glucose value."""
        text = "Glucose 105 mg/dL shown on the report."
        labs = extract_lab_values(text)
        assert len(labs) >= 1
        glucose = [x for x in labs if "glucose" in x.test_name.lower()]
        assert len(glucose) == 1
        assert glucose[0].value == "105"

    def test_extract_multiple_values(self) -> None:
        """Should extract multiple lab values."""
        text = (
            "Results: Sodium 140 mEq/L, Potassium 4.2 mEq/L, "
            "Creatinine 0.9 mg/dL, BUN 15 mg/dL"
        )
        labs = extract_lab_values(text)
        assert len(labs) >= 3
        names = {lab.test_name.lower() for lab in labs}
        assert "sodium" in names
        assert "potassium" in names
        assert "creatinine" in names

    def test_extract_tsh(self) -> None:
        """Should extract TSH value."""
        text = "TSH: 2.5 mIU/L within normal limits"
        labs = extract_lab_values(text)
        tsh = [x for x in labs if "tsh" in x.test_name.lower()]
        assert len(tsh) == 1
        assert tsh[0].value == "2.5"

    def test_extract_hba1c(self) -> None:
        """Should extract HbA1c value."""
        text = "HbA1c 6.2 % indicates prediabetes."
        labs = extract_lab_values(text)
        hba1c = [x for x in labs if "a1c" in x.test_name.lower()]
        assert len(hba1c) == 1
        assert hba1c[0].value == "6.2"

    def test_no_duplicates(self) -> None:
        """Should not extract the same test twice."""
        text = "Glucose: 105 mg/dL. Reference: Glucose 70-100 mg/dL"
        labs = extract_lab_values(text)
        glucose = [x for x in labs if "glucose" in x.test_name.lower()]
        assert len(glucose) == 1

    def test_no_match(self) -> None:
        """No lab values should return empty list."""
        text = "A blurry photo of text that is not readable."
        labs = extract_lab_values(text)
        assert labs == []


class TestFormatExtractionSummary:
    """Test formatting extracted data."""

    def test_format_meds(self) -> None:
        """Medication extraction should be formatted."""
        from healthbot.reasoning.photo_extractor import ExtractedMedication

        classification = PhotoClassification(
            content_type="medication_bottle",
            confidence=0.8,
            extracted_meds=[
                ExtractedMedication(
                    name="Metformin", dose="500", unit="mg", form="tablet",
                ),
            ],
        )
        text = format_extraction_summary(classification)
        assert "Metformin" in text
        assert "500" in text
        assert "store this" in text.lower()

    def test_format_labs(self) -> None:
        """Lab extraction should be formatted."""
        from healthbot.reasoning.photo_extractor import ExtractedLabValue

        classification = PhotoClassification(
            content_type="lab_printout",
            confidence=0.8,
            extracted_labs=[
                ExtractedLabValue(test_name="Glucose", value="105", unit="mg/dL"),
                ExtractedLabValue(test_name="TSH", value="2.5", unit="mIU/L"),
            ],
        )
        text = format_extraction_summary(classification)
        assert "Glucose" in text
        assert "TSH" in text
        assert "2 lab values" in text

    def test_format_general_empty(self) -> None:
        """General health photos should not produce extraction summary."""
        classification = PhotoClassification(
            content_type="general_health",
            confidence=0.5,
        )
        text = format_extraction_summary(classification)
        assert text == ""

    def test_format_no_extractions(self) -> None:
        """No extracted data should return empty string."""
        classification = PhotoClassification(
            content_type="medication_bottle",
            confidence=0.6,
        )
        text = format_extraction_summary(classification)
        assert text == ""
