"""Tests for FHIR de-identification (HIPAA Safe Harbor)."""
from __future__ import annotations

import pytest

from healthbot.security.deidentifier import FhirDeidentifier


@pytest.fixture
def deid():
    return FhirDeidentifier(anon_patient_id="anon-test-001")


@pytest.fixture
def patient_resource():
    return {
        "resourceType": "Patient",
        "id": "pt-12345",
        "name": [{"family": "Doe", "given": ["Jane"]}],
        "gender": "male",
        "birthDate": "1995-03-15",
        "telecom": [
            {"system": "phone", "value": "555-123-4567"},
            {"system": "email", "value": "jane@example.com"},
        ],
        "address": [{"line": ["123 Main St"], "city": "Anytown", "state": "CA"}],
        "identifier": [
            {"system": "http://hl7.org/fhir/sid/us-ssn", "value": "123-45-6789"},
            {"system": "http://hospital.org/mrn", "value": "MRN12345678"},
        ],
        "photo": [{"contentType": "image/jpeg", "data": "base64data"}],
        "extension": [
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                "extension": [
                    {
                        "url": "text",
                        "valueString": "Asian",
                    }
                ],
            },
            {
                "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                "extension": [
                    {
                        "url": "text",
                        "valueString": "Not Hispanic or Latino",
                    }
                ],
            },
        ],
    }


@pytest.fixture
def observation_lab():
    return {
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
            "text": "Glucose",
        },
        "subject": {
            "reference": "Patient/pt-12345",
            "display": "Jane Doe",
        },
        "performer": [{
            "reference": "Practitioner/dr-smith",
            "display": "Dr. Sarah Smith",
        }],
        "effectiveDateTime": "2025-12-01T08:00:00Z",
        "valueQuantity": {
            "value": 95.0,
            "unit": "mg/dL",
            "system": "http://unitsofmeasure.org",
        },
        "referenceRange": [{
            "low": {"value": 70, "unit": "mg/dL"},
            "high": {"value": 100, "unit": "mg/dL"},
        }],
    }


@pytest.fixture
def medication_request():
    return {
        "resourceType": "MedicationRequest",
        "id": "med-001",
        "status": "active",
        "intent": "order",
        "medicationCodeableConcept": {
            "coding": [{
                "system": "http://www.nlm.nih.gov/research/umls/rxnorm",
                "code": "860975",
                "display": "Metformin 500 MG Oral Tablet",
            }],
        },
        "subject": {"reference": "Patient/pt-12345", "display": "Jane Doe"},
        "requester": {"reference": "Practitioner/dr-smith", "display": "Dr. Smith"},
        "dosageInstruction": [{"text": "Take 1 tablet by mouth twice daily"}],
        "authoredOn": "2025-06-01",
    }


@pytest.fixture
def condition_resource():
    return {
        "resourceType": "Condition",
        "id": "cond-001",
        "clinicalStatus": {
            "coding": [{"code": "active"}],
        },
        "code": {
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "44054006",
                "display": "Type 2 diabetes mellitus",
            }],
        },
        "subject": {"reference": "Patient/pt-12345"},
        "recorder": {"reference": "Practitioner/dr-jones"},
        "asserter": {"reference": "Practitioner/dr-jones"},
        "onsetDateTime": "2023-01-15",
        "note": [{"text": "Patient: Jane Doe diagnosed with T2DM at age 28."}],
    }


class TestPatientResource:
    """Patient resources should return None (pure PII)."""

    def test_patient_returns_none(self, deid, patient_resource):
        result, report = deid.deidentify_resource(patient_resource)
        assert result is None
        assert report.resource_type == "Patient"

    def test_extract_demographics_age(self, deid, patient_resource):
        demo = deid.extract_demographics(patient_resource)
        assert "age" in demo
        assert isinstance(demo["age"], int)
        assert demo["age"] > 0

    def test_extract_demographics_sex(self, deid, patient_resource):
        demo = deid.extract_demographics(patient_resource)
        assert demo["sex"] == "male"

    def test_extract_demographics_race(self, deid, patient_resource):
        demo = deid.extract_demographics(patient_resource)
        assert demo["race"] == "Asian"

    def test_extract_demographics_ethnicity(self, deid, patient_resource):
        demo = deid.extract_demographics(patient_resource)
        assert demo["ethnicity"] == "Not Hispanic or Latino"

    def test_extract_demographics_no_name(self, deid, patient_resource):
        demo = deid.extract_demographics(patient_resource)
        assert "name" not in demo
        # Verify name doesn't leak into any value
        for val in demo.values():
            assert "Jane" not in str(val)
            assert "Doe" not in str(val)


class TestObservationDeidentification:
    """Lab observations should keep clinical data, strip PII."""

    def test_keeps_loinc_code(self, deid, observation_lab):
        result, _ = deid.deidentify_resource(observation_lab)
        coding = result["code"]["coding"][0]
        assert coding["code"] == "2345-7"
        assert coding["system"] == "http://loinc.org"

    def test_keeps_value_and_unit(self, deid, observation_lab):
        result, _ = deid.deidentify_resource(observation_lab)
        assert result["valueQuantity"]["value"] == 95.0
        assert result["valueQuantity"]["unit"] == "mg/dL"

    def test_keeps_reference_range(self, deid, observation_lab):
        result, _ = deid.deidentify_resource(observation_lab)
        ref = result["referenceRange"][0]
        assert ref["low"]["value"] == 70
        assert ref["high"]["value"] == 100

    def test_keeps_effective_date(self, deid, observation_lab):
        result, _ = deid.deidentify_resource(observation_lab)
        assert result["effectiveDateTime"] == "2025-12-01T08:00:00Z"

    def test_replaces_patient_reference(self, deid, observation_lab):
        result, report = deid.deidentify_resource(observation_lab)
        assert result["subject"]["reference"] == "Patient/anon-test-001"
        assert report.patient_ref_replaced

    def test_strips_patient_display_name(self, deid, observation_lab):
        result, _ = deid.deidentify_resource(observation_lab)
        assert "display" not in result["subject"]

    def test_removes_performer(self, deid, observation_lab):
        result, report = deid.deidentify_resource(observation_lab)
        assert "performer" not in result
        assert report.pii_refs_removed > 0

    def test_no_pii_in_result(self, deid, observation_lab):
        result, _ = deid.deidentify_resource(observation_lab)
        result_str = str(result)
        assert "Jane" not in result_str
        assert "Doe" not in result_str
        assert "Dr. Sarah Smith" not in result_str
        assert "pt-12345" not in result_str


class TestMedicationDeidentification:

    def test_keeps_medication_name(self, deid, medication_request):
        result, _ = deid.deidentify_resource(medication_request)
        coding = result["medicationCodeableConcept"]["coding"][0]
        assert "Metformin" in coding["display"]

    def test_keeps_dosage(self, deid, medication_request):
        result, _ = deid.deidentify_resource(medication_request)
        assert result["dosageInstruction"][0]["text"] == "Take 1 tablet by mouth twice daily"

    def test_keeps_authored_date(self, deid, medication_request):
        result, _ = deid.deidentify_resource(medication_request)
        assert result["authoredOn"] == "2025-06-01"

    def test_removes_requester(self, deid, medication_request):
        result, _ = deid.deidentify_resource(medication_request)
        assert "requester" not in result

    def test_replaces_subject(self, deid, medication_request):
        result, _ = deid.deidentify_resource(medication_request)
        assert result["subject"]["reference"] == "Patient/anon-test-001"


class TestConditionDeidentification:

    def test_keeps_clinical_code(self, deid, condition_resource):
        result, _ = deid.deidentify_resource(condition_resource)
        coding = result["code"]["coding"][0]
        assert coding["code"] == "44054006"
        assert "diabetes" in coding["display"].lower()

    def test_keeps_onset_date(self, deid, condition_resource):
        result, _ = deid.deidentify_resource(condition_resource)
        assert result["onsetDateTime"] == "2023-01-15"

    def test_removes_recorder(self, deid, condition_resource):
        result, _ = deid.deidentify_resource(condition_resource)
        assert "recorder" not in result

    def test_removes_asserter(self, deid, condition_resource):
        result, _ = deid.deidentify_resource(condition_resource)
        assert "asserter" not in result

    def test_redacts_name_in_note(self, deid, condition_resource):
        result, report = deid.deidentify_resource(condition_resource)
        note_text = result["note"][0]["text"]
        # The name "Jane Doe" should be caught by PhiFirewall
        # (it appears as "Patient Jane Doe" which matches name_labeled pattern)
        assert "Jane Doe" not in note_text or "REDACTED" in note_text


class TestPractitionerResource:
    """Practitioner resources should be filtered out entirely."""

    def test_practitioner_returns_none(self, deid):
        resource = {
            "resourceType": "Practitioner",
            "id": "dr-smith",
            "name": [{"family": "Smith", "given": ["Sarah"]}],
        }
        result, _ = deid.deidentify_resource(resource)
        assert result is None

    def test_organization_returns_none(self, deid):
        resource = {
            "resourceType": "Organization",
            "id": "org-001",
            "name": "City General Hospital",
        }
        result, _ = deid.deidentify_resource(resource)
        assert result is None

    def test_location_returns_none(self, deid):
        resource = {
            "resourceType": "Location",
            "id": "loc-001",
            "name": "Main Campus, Building A",
        }
        result, _ = deid.deidentify_resource(resource)
        assert result is None


class TestBundleDeidentification:

    def test_bundle_filters_patient(self, deid, patient_resource, observation_lab):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {"resource": patient_resource},
                {"resource": observation_lab},
            ],
        }
        cleaned, reports = deid.deidentify_bundle(bundle)
        # Patient filtered out, Observation kept
        assert len(cleaned) == 1
        assert cleaned[0]["resourceType"] == "Observation"

    def test_bundle_strips_all_pii(
        self, deid, patient_resource, observation_lab, medication_request,
    ):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {"resource": patient_resource},
                {"resource": observation_lab},
                {"resource": medication_request},
            ],
        }
        cleaned, _ = deid.deidentify_bundle(bundle)
        bundle_str = str(cleaned)
        assert "Jane" not in bundle_str
        assert "pt-12345" not in bundle_str
        assert "dr-smith" not in bundle_str


class TestIdentifierStripping:

    def test_strips_identifiers_from_observation(self, deid):
        resource = {
            "resourceType": "Observation",
            "id": "obs-002",
            "identifier": [{"value": "ACC-2025-12345"}],
            "status": "final",
            "code": {"text": "Hemoglobin"},
            "valueQuantity": {"value": 14.5, "unit": "g/dL"},
        }
        result, report = deid.deidentify_resource(resource)
        assert "identifier" not in result
        assert "identifier" in report.fields_removed


class TestFreeTextScrubbing:

    def test_redacts_ssn_in_text(self, deid):
        resource = {
            "resourceType": "Observation",
            "id": "obs-003",
            "status": "final",
            "code": {"text": "Note"},
            "valueString": "Patient SSN: 123-45-6789, glucose normal",
        }
        result, report = deid.deidentify_resource(resource)
        assert "123-45-6789" not in result["valueString"]

    def test_redacts_phone_in_text(self, deid):
        resource = {
            "resourceType": "Observation",
            "id": "obs-004",
            "status": "final",
            "code": {"text": "Note"},
            "valueString": "Call patient at 555-123-4567 for results",
        }
        result, _ = deid.deidentify_resource(resource)
        assert "555-123-4567" not in result["valueString"]

    def test_redacts_email_in_text(self, deid):
        resource = {
            "resourceType": "Observation",
            "id": "obs-005",
            "status": "final",
            "code": {"text": "Note"},
            "valueString": "Send report to patient@example.com",
        }
        result, _ = deid.deidentify_resource(resource)
        assert "patient@example.com" not in result["valueString"]


class TestNarrativeText:

    def test_removes_text_div(self, deid):
        resource = {
            "resourceType": "Observation",
            "id": "obs-006",
            "text": {
                "status": "generated",
                "div": "<div>Patient: Jane Doe, Glucose 95 mg/dL</div>",
            },
            "status": "final",
            "code": {"text": "Glucose"},
            "valueQuantity": {"value": 95},
        }
        result, report = deid.deidentify_resource(resource)
        assert "text" not in result
        assert "text" in report.fields_removed


class TestMetaSource:

    def test_strips_meta_source(self, deid):
        resource = {
            "resourceType": "Observation",
            "id": "obs-007",
            "meta": {
                "source": "https://cityhospital.org/fhir",
                "lastUpdated": "2025-12-01",
            },
            "status": "final",
            "code": {"text": "Glucose"},
            "valueQuantity": {"value": 95},
        }
        result, _ = deid.deidentify_resource(resource)
        assert "source" not in result.get("meta", {})
        # lastUpdated should remain
        assert result["meta"]["lastUpdated"] == "2025-12-01"


class TestContainedResources:

    def test_strips_contained_practitioner(self, deid):
        resource = {
            "resourceType": "Observation",
            "id": "obs-008",
            "contained": [
                {"resourceType": "Practitioner", "id": "p1", "name": [{"family": "Smith"}]},
                {"resourceType": "Device", "id": "d1", "type": {"text": "Glucometer"}},
            ],
            "status": "final",
            "code": {"text": "Glucose"},
            "valueQuantity": {"value": 95},
        }
        result, _ = deid.deidentify_resource(resource)
        # Practitioner removed, Device kept
        assert len(result["contained"]) == 1
        assert result["contained"][0]["resourceType"] == "Device"


class TestAllergyIntolerance:

    def test_keeps_allergy_code(self, deid):
        resource = {
            "resourceType": "AllergyIntolerance",
            "id": "allergy-001",
            "clinicalStatus": {"coding": [{"code": "active"}]},
            "code": {
                "coding": [{"display": "Penicillin"}],
            },
            "type": "allergy",
            "criticality": "high",
            "subject": {"reference": "Patient/pt-12345"},
            "recorder": {"reference": "Practitioner/dr-smith"},
            "reaction": [{"manifestation": [{"text": "Hives"}]}],
        }
        result, _ = deid.deidentify_resource(resource)
        assert result["code"]["coding"][0]["display"] == "Penicillin"
        assert result["criticality"] == "high"
        assert "recorder" not in result
        assert result["subject"]["reference"] == "Patient/anon-test-001"


class TestImmunization:

    def test_keeps_vaccine_code(self, deid):
        resource = {
            "resourceType": "Immunization",
            "id": "imm-001",
            "status": "completed",
            "vaccineCode": {
                "coding": [{"display": "COVID-19 mRNA Vaccine"}],
            },
            "subject": {"reference": "Patient/pt-12345"},
            "performer": [{"actor": {"reference": "Practitioner/dr-smith"}}],
            "occurrenceDateTime": "2025-01-15",
        }
        result, _ = deid.deidentify_resource(resource)
        assert "COVID-19" in result["vaccineCode"]["coding"][0]["display"]
        assert result["occurrenceDateTime"] == "2025-01-15"
        assert "performer" not in result


class TestCalculateAge:

    def test_full_date(self):
        # Use a date far enough in the past for stability
        age = FhirDeidentifier._calculate_age("1990-01-01")
        assert age is not None
        assert age >= 35

    def test_year_only(self):
        age = FhirDeidentifier._calculate_age("1990")
        assert age is not None
        assert age >= 35

    def test_year_month(self):
        age = FhirDeidentifier._calculate_age("1990-06")
        assert age is not None
        assert age >= 35

    def test_invalid_date(self):
        assert FhirDeidentifier._calculate_age("not-a-date") is None

    def test_future_date(self):
        assert FhirDeidentifier._calculate_age("2090-01-01") is None


class TestEdgeCases:

    def test_empty_resource(self, deid):
        result, report = deid.deidentify_resource({"resourceType": "Observation"})
        assert result is not None
        assert result["resourceType"] == "Observation"

    def test_resource_without_type(self, deid):
        result, report = deid.deidentify_resource({})
        assert result is not None

    def test_deeply_nested_patient_ref(self, deid):
        resource = {
            "resourceType": "DiagnosticReport",
            "id": "dr-001",
            "status": "final",
            "code": {"text": "CBC"},
            "subject": {"reference": "Patient/pt-12345"},
            "result": [
                {"reference": "Observation/obs-001"},
            ],
        }
        result, report = deid.deidentify_resource(resource)
        assert result["subject"]["reference"] == "Patient/anon-test-001"
        # Observation references should remain (not PII)
        assert result["result"][0]["reference"] == "Observation/obs-001"

    def test_original_not_mutated(self, deid, observation_lab):
        original_str = str(observation_lab)
        deid.deidentify_resource(observation_lab)
        assert str(observation_lab) == original_str
