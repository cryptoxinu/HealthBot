"""Tests for Fasten Health FHIR import with de-identification."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from healthbot.ingest.fasten_import import FastenImporter


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.insert_observation = MagicMock(return_value="obs-id-001")
    db.insert_medication = MagicMock(return_value="med-id-001")
    db.insert_ltm = MagicMock(return_value="ltm-id-001")
    return db


@pytest.fixture
def mock_vault():
    return MagicMock()


@pytest.fixture
def importer(mock_db, mock_vault):
    return FastenImporter(mock_db, mock_vault)


def _make_ndjson(*resources: dict) -> bytes:
    """Create NDJSON bytes from resources."""
    return "\n".join(json.dumps(r) for r in resources).encode()


def _make_bundle(*resources: dict) -> bytes:
    """Create FHIR Bundle bytes from resources."""
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [{"resource": r} for r in resources],
    }
    return json.dumps(bundle).encode()


SAMPLE_PATIENT = {
    "resourceType": "Patient",
    "id": "pt-12345",
    "name": [{"family": "Doe", "given": ["Jane"]}],
    "gender": "male",
    "birthDate": "1995-03-15",
    "telecom": [{"value": "555-123-4567"}],
    "address": [{"city": "Anytown", "state": "CA"}],
}

SAMPLE_LAB = {
    "resourceType": "Observation",
    "id": "obs-001",
    "status": "final",
    "category": [{
        "coding": [{
            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
            "code": "laboratory",
        }],
    }],
    "code": {
        "coding": [{
            "system": "http://loinc.org",
            "code": "2345-7",
            "display": "Glucose [Mass/volume] in Serum or Plasma",
        }],
    },
    "subject": {"reference": "Patient/pt-12345", "display": "Jane Doe"},
    "performer": [{"reference": "Practitioner/dr-smith"}],
    "effectiveDateTime": "2025-12-01",
    "valueQuantity": {"value": 95.0, "unit": "mg/dL"},
    "referenceRange": [{"low": {"value": 70}, "high": {"value": 100}}],
}

SAMPLE_VITAL = {
    "resourceType": "Observation",
    "id": "obs-002",
    "status": "final",
    "category": [{
        "coding": [{"code": "vital-signs"}],
    }],
    "code": {"coding": [{"display": "Blood Pressure Systolic"}]},
    "subject": {"reference": "Patient/pt-12345"},
    "effectiveDateTime": "2025-12-01T10:00:00Z",
    "valueQuantity": {"value": 120, "unit": "mmHg"},
}

SAMPLE_MEDICATION = {
    "resourceType": "MedicationRequest",
    "id": "med-001",
    "status": "active",
    "intent": "order",
    "medicationCodeableConcept": {
        "coding": [{"display": "Metformin 500 MG Oral Tablet"}],
    },
    "subject": {"reference": "Patient/pt-12345"},
    "requester": {"reference": "Practitioner/dr-smith"},
    "dosageInstruction": [{"text": "Take 1 tablet twice daily"}],
    "authoredOn": "2025-06-01",
}

SAMPLE_CONDITION = {
    "resourceType": "Condition",
    "id": "cond-001",
    "clinicalStatus": {"coding": [{"code": "active"}]},
    "code": {"coding": [{"display": "Type 2 diabetes mellitus"}]},
    "subject": {"reference": "Patient/pt-12345"},
    "onsetDateTime": "2023-01-15",
}

SAMPLE_ALLERGY = {
    "resourceType": "AllergyIntolerance",
    "id": "allergy-001",
    "code": {"coding": [{"display": "Penicillin"}]},
    "criticality": "high",
    "subject": {"reference": "Patient/pt-12345"},
    "reaction": [{"manifestation": [{"text": "Hives"}]}],
}

SAMPLE_IMMUNIZATION = {
    "resourceType": "Immunization",
    "id": "imm-001",
    "status": "completed",
    "vaccineCode": {"coding": [{"display": "COVID-19 mRNA Vaccine"}]},
    "subject": {"reference": "Patient/pt-12345"},
    "occurrenceDateTime": "2025-01-15",
}


class TestNdjsonImport:

    def test_import_lab(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_LAB)
        result = importer.import_ndjson(data)
        assert result.labs == 1
        assert mock_db.insert_observation.called

    def test_import_vital(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_VITAL)
        result = importer.import_ndjson(data)
        assert result.vitals == 1

    def test_import_medication(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_MEDICATION)
        result = importer.import_ndjson(data)
        assert result.medications == 1
        assert mock_db.insert_medication.called

    def test_import_condition(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_CONDITION)
        result = importer.import_ndjson(data)
        assert result.conditions == 1
        assert mock_db.insert_ltm.called

    def test_import_allergy(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_ALLERGY)
        result = importer.import_ndjson(data)
        assert result.allergies == 1
        # Should be stored as LTM fact
        assert mock_db.insert_ltm.called
        fact_arg = mock_db.insert_ltm.call_args
        assert "Penicillin" in str(fact_arg)

    def test_import_immunization(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_IMMUNIZATION)
        result = importer.import_ndjson(data)
        assert result.immunizations == 1
        assert mock_db.insert_ltm.called

    def test_patient_extracts_demographics(self, importer):
        data = _make_ndjson(SAMPLE_PATIENT)
        result = importer.import_ndjson(data)
        assert result.demographics is not None
        assert result.demographics.get("sex") == "male"
        assert "age" in result.demographics

    def test_patient_not_stored(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_PATIENT)
        result = importer.import_ndjson(data)
        assert result.skipped == 1  # Patient counted as skipped

    def test_empty_ndjson(self, importer):
        result = importer.import_ndjson(b"")
        assert result.labs == 0
        assert result.medications == 0

    def test_malformed_json_skipped(self, importer):
        data = b'{"resourceType": "Observation"}\n{invalid json}\n'
        result = importer.import_ndjson(data)
        assert result.skipped >= 1
        assert len(result.errors) >= 1

    def test_mixed_resources(self, importer, mock_db):
        data = _make_ndjson(
            SAMPLE_PATIENT, SAMPLE_LAB, SAMPLE_MEDICATION,
            SAMPLE_CONDITION, SAMPLE_ALLERGY, SAMPLE_IMMUNIZATION,
        )
        result = importer.import_ndjson(data)
        assert result.labs == 1
        assert result.medications == 1
        assert result.conditions == 1
        assert result.allergies == 1
        assert result.immunizations == 1
        assert result.demographics is not None


class TestBundleImport:

    def test_import_bundle(self, importer, mock_db):
        data = _make_bundle(SAMPLE_LAB, SAMPLE_MEDICATION)
        result = importer.import_bundle(data)
        assert result.labs == 1
        assert result.medications == 1

    def test_invalid_json_bundle(self, importer):
        result = importer.import_bundle(b"not json")
        assert len(result.errors) >= 1

    def test_unknown_resource_skipped(self, importer):
        unknown = {"resourceType": "Questionnaire", "id": "q1"}
        data = _make_bundle(unknown)
        result = importer.import_bundle(data)
        assert result.skipped == 1


class TestPiiStripping:

    def test_lab_no_patient_name(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_LAB)
        importer.import_ndjson(data)
        # Check the LabResult passed to insert_observation
        call_args = mock_db.insert_observation.call_args
        lab = call_args[0][0]  # First positional arg
        # Verify no PII fields
        assert lab.ordering_provider == ""
        assert lab.lab_name == ""

    def test_med_no_prescriber(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_MEDICATION)
        importer.import_ndjson(data)
        call_args = mock_db.insert_medication.call_args
        med = call_args[0][0]
        assert med.prescriber == ""

    def test_no_patient_name_in_any_output(self, importer, mock_db):
        """Verify patient name doesn't appear in any stored data."""
        data = _make_ndjson(SAMPLE_PATIENT, SAMPLE_LAB, SAMPLE_MEDICATION)
        importer.import_ndjson(data)

        # Check all observation calls
        for call in mock_db.insert_observation.call_args_list:
            obs = call[0][0]
            assert "Jane" not in str(obs.__dict__)
            assert "Doe" not in str(obs.__dict__)

        # Check all medication calls
        for call in mock_db.insert_medication.call_args_list:
            med = call[0][0]
            assert "Jane" not in str(med.__dict__)

    def test_practitioner_not_stored(self, importer):
        pract = {
            "resourceType": "Practitioner",
            "id": "dr-smith",
            "name": [{"family": "Smith"}],
        }
        data = _make_ndjson(pract)
        result = importer.import_ndjson(data)
        assert result.skipped == 1


class TestLabMapping:

    def test_preserves_loinc_code(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_LAB)
        importer.import_ndjson(data)
        lab = mock_db.insert_observation.call_args[0][0]
        assert lab.test_name == "Glucose [Mass/volume] in Serum or Plasma"

    def test_preserves_value_and_unit(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_LAB)
        importer.import_ndjson(data)
        lab = mock_db.insert_observation.call_args[0][0]
        assert lab.value == 95.0
        assert lab.unit == "mg/dL"

    def test_preserves_reference_range(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_LAB)
        importer.import_ndjson(data)
        lab = mock_db.insert_observation.call_args[0][0]
        assert lab.reference_low == 70.0
        assert lab.reference_high == 100.0

    def test_preserves_date(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_LAB)
        importer.import_ndjson(data)
        lab = mock_db.insert_observation.call_args[0][0]
        from datetime import date
        assert lab.date_collected == date(2025, 12, 1)


class TestMedicationMapping:

    def test_preserves_med_name(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_MEDICATION)
        importer.import_ndjson(data)
        med = mock_db.insert_medication.call_args[0][0]
        assert "Metformin" in med.name

    def test_preserves_dosage(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_MEDICATION)
        importer.import_ndjson(data)
        med = mock_db.insert_medication.call_args[0][0]
        assert "twice daily" in med.dose

    def test_preserves_status(self, importer, mock_db):
        data = _make_ndjson(SAMPLE_MEDICATION)
        importer.import_ndjson(data)
        med = mock_db.insert_medication.call_args[0][0]
        assert med.status == "active"
