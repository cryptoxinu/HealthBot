"""Tests for MyChart CCDA/FHIR import."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from healthbot.ingest.mychart_import import CCDA_NS, MyChartImporter


@pytest.fixture()
def importer():
    db = MagicMock()
    db.insert_ltm = MagicMock(return_value="fake-id")
    vault = MagicMock()
    return MyChartImporter(db, vault)


class TestCcdaImport:
    """Test CCDA XML import."""

    def _make_ccda(
        self,
        labs: list[tuple] | None = None,
        meds: list[str] | None = None,
        conditions: list[str] | None = None,
        allergies: list[dict] | None = None,
        immunizations: list[dict] | None = None,
    ) -> bytes:
        """Build a minimal CCDA XML document."""
        ns = CCDA_NS["cda"]
        parts = [
            f'<ClinicalDocument xmlns="{ns}">',
            "<component><structuredBody>",
        ]
        if labs:
            parts.append('<component><section>')
            parts.append('<code code="30954-2"/>')
            for item in labs:
                if len(item) == 3:
                    name, value, unit = item
                    parts.append(
                        f'<entry><observation>'
                        f'<code displayName="{name}"/>'
                        f'<value value="{value}" unit="{unit}"/>'
                        f'</observation></entry>'
                    )
                elif len(item) == 6:
                    name, value, unit, ref_low, ref_high, flag = item
                    parts.append(
                        f'<entry><observation>'
                        f'<code displayName="{name}"/>'
                        f'<value value="{value}" unit="{unit}"/>'
                        f'<referenceRange><observationRange>'
                        f'<value><low value="{ref_low}"/><high value="{ref_high}"/></value>'
                        f'</observationRange></referenceRange>'
                    )
                    if flag:
                        parts.append(f'<interpretationCode code="{flag}"/>')
                    parts.append('</observation></entry>')
            parts.append('</section></component>')
        if meds:
            parts.append('<component><section>')
            parts.append('<code code="10160-0"/>')
            for name in meds:
                parts.append(
                    '<entry><substanceAdministration>'
                    f'<consumable><manufacturedProduct>'
                    f'<material><code displayName="{name}"/></material>'
                    f'</manufacturedProduct></consumable>'
                    f'</substanceAdministration></entry>'
                )
            parts.append('</section></component>')
        if conditions:
            parts.append('<component><section>')
            parts.append('<code code="11450-4"/>')
            for name in conditions:
                parts.append(
                    f'<entry><act><entryRelationship><observation>'
                    f'<value displayName="{name}"/>'
                    f'<statusCode code="active"/>'
                    f'</observation></entryRelationship></act></entry>'
                )
            parts.append('</section></component>')
        if allergies:
            parts.append('<component><section>')
            parts.append('<code code="48765-2"/>')
            for allergy in allergies:
                allergen = allergy["allergen"]
                reaction = allergy.get("reaction", "")
                parts.append(
                    f'<entry><act><entryRelationship><observation>'
                    f'<participant><participantRole><playingEntity>'
                    f'<code displayName="{allergen}"/>'
                    f'</playingEntity></participantRole></participant>'
                )
                if reaction:
                    parts.append(
                        f'<entryRelationship><observation>'
                        f'<value displayName="{reaction}"/>'
                        f'</observation></entryRelationship>'
                    )
                parts.append('</observation></entryRelationship></act></entry>')
            parts.append('</section></component>')
        if immunizations:
            parts.append('<component><section>')
            parts.append('<code code="11369-6"/>')
            for imm in immunizations:
                name = imm["name"]
                date = imm.get("date", "")
                parts.append(
                    '<entry><substanceAdministration>'
                )
                if date:
                    parts.append(f'<effectiveTime value="{date}"/>')
                parts.append(
                    f'<consumable><manufacturedProduct>'
                    f'<manufacturedMaterial>'
                    f'<code displayName="{name}"/>'
                    f'</manufacturedMaterial>'
                    f'</manufacturedProduct></consumable>'
                    f'</substanceAdministration></entry>'
                )
            parts.append('</section></component>')
        parts.append("</structuredBody></component></ClinicalDocument>")
        return "".join(parts).encode()

    def test_import_labs(self, importer):
        xml = self._make_ccda(labs=[("Glucose", "95", "mg/dL"), ("LDL", "110", "mg/dL")])
        result = importer.import_ccda_bytes(xml)
        assert result["labs"] == 2
        assert importer._db.insert_observation.call_count == 2
        importer._vault.store_blob.assert_called_once()

    def test_import_meds(self, importer):
        xml = self._make_ccda(meds=["Metformin", "Atorvastatin"])
        result = importer.import_ccda_bytes(xml)
        assert result["meds"] == 2
        assert importer._db.insert_medication.call_count == 2

    def test_import_labs_and_meds(self, importer):
        xml = self._make_ccda(
            labs=[("TSH", "2.5", "mIU/L")],
            meds=["Levothyroxine"],
        )
        result = importer.import_ccda_bytes(xml)
        assert result["labs"] == 1
        assert result["meds"] == 1

    def test_empty_ccda(self, importer):
        xml = self._make_ccda()
        result = importer.import_ccda_bytes(xml)
        assert result["labs"] == 0
        assert result["meds"] == 0

    def test_skips_incomplete_observations(self, importer):
        """Observations missing code or value are skipped."""
        ns = CCDA_NS["cda"]
        xml = (
            f'<ClinicalDocument xmlns="{ns}"><component><structuredBody>'
            f'<component><section><code code="30954-2"/>'
            f'<entry><observation><code displayName="Glucose"/></observation></entry>'
            f'<entry><observation><value value="95" unit="mg/dL"/></observation></entry>'
            f'</section></component>'
            f'</structuredBody></component></ClinicalDocument>'
        ).encode()
        result = importer.import_ccda_bytes(xml)
        assert result["labs"] == 0

    def test_import_labs_with_reference_ranges(self, importer):
        """Labs should extract reference range low/high."""
        xml = self._make_ccda(
            labs=[("Glucose", "95", "mg/dL", "70", "100", "")],
        )
        result = importer.import_ccda_bytes(xml)
        assert result["labs"] == 1
        lab = importer._db.insert_observation.call_args[0][0]
        assert lab.reference_low == 70.0
        assert lab.reference_high == 100.0

    def test_import_labs_with_flag(self, importer):
        """Labs should extract interpretation flags."""
        xml = self._make_ccda(
            labs=[("Glucose", "130", "mg/dL", "70", "100", "H")],
        )
        result = importer.import_ccda_bytes(xml)
        assert result["labs"] == 1
        lab = importer._db.insert_observation.call_args[0][0]
        assert lab.flag == "H"

    def test_import_conditions(self, importer):
        """Conditions from Problem List section should be stored as LTM facts."""
        xml = self._make_ccda(
            conditions=["Type 2 Diabetes", "Hypertension"],
        )
        result = importer.import_ccda_bytes(xml)
        assert result["conditions"] == 2
        assert importer._db.insert_ltm.call_count == 2
        first_call = importer._db.insert_ltm.call_args_list[0]
        assert "Type 2 Diabetes" in first_call[1]["fact"]
        assert first_call[1]["category"] == "condition"
        assert first_call[1]["source"] == "mychart_import"

    def test_import_allergies(self, importer):
        """Allergies should be stored as LTM facts with reactions."""
        xml = self._make_ccda(
            allergies=[
                {"allergen": "Penicillin", "reaction": "Hives"},
                {"allergen": "Sulfa"},
            ],
        )
        result = importer.import_ccda_bytes(xml)
        assert result["allergies"] == 2
        first_call = importer._db.insert_ltm.call_args_list[0]
        assert "Penicillin" in first_call[1]["fact"]
        assert "Hives" in first_call[1]["fact"]

    def test_import_immunizations(self, importer):
        """Immunizations should be stored as LTM facts."""
        xml = self._make_ccda(
            immunizations=[
                {"name": "COVID-19 Vaccine", "date": "20210415"},
                {"name": "Influenza Vaccine"},
            ],
        )
        result = importer.import_ccda_bytes(xml)
        assert result["immunizations"] == 2
        first_call = importer._db.insert_ltm.call_args_list[0]
        assert "COVID-19 Vaccine" in first_call[1]["fact"]
        assert "20210415" in first_call[1]["fact"]

    def test_comprehensive_ccda(self, importer):
        """Full CCDA with all section types."""
        xml = self._make_ccda(
            labs=[("TSH", "4.5", "mIU/L")],
            meds=["Levothyroxine"],
            conditions=["Hypothyroidism"],
            allergies=[{"allergen": "Iodine"}],
            immunizations=[{"name": "Tdap", "date": "20230101"}],
        )
        result = importer.import_ccda_bytes(xml)
        assert result["labs"] == 1
        assert result["meds"] == 1
        assert result["conditions"] == 1
        assert result["allergies"] == 1
        assert result["immunizations"] == 1


class TestFhirImport:
    """Test FHIR JSON bundle import."""

    def test_import_observations(self, importer):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Observation",
                        "code": {"coding": [{"display": "Glucose"}]},
                        "valueQuantity": {"value": 95, "unit": "mg/dL"},
                    }
                },
                {
                    "resource": {
                        "resourceType": "Observation",
                        "code": {"coding": [{"display": "HbA1c"}]},
                        "valueQuantity": {"value": 5.7, "unit": "%"},
                    }
                },
            ],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["labs"] == 2
        assert importer._db.insert_observation.call_count == 2

    def test_import_medications(self, importer):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "MedicationStatement",
                        "medicationCodeableConcept": {
                            "coding": [{"display": "Metformin 500mg"}]
                        },
                    }
                },
            ],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["meds"] == 1
        assert importer._db.insert_medication.call_count == 1

    def test_empty_bundle(self, importer):
        bundle = {"resourceType": "Bundle", "entry": []}
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["labs"] == 0
        assert result["meds"] == 0

    def test_mixed_resources(self, importer):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Observation",
                        "code": {"coding": [{"display": "LDL"}]},
                        "valueQuantity": {"value": 110, "unit": "mg/dL"},
                    }
                },
                {
                    "resource": {
                        "resourceType": "MedicationStatement",
                        "medicationCodeableConcept": {
                            "coding": [{"display": "Atorvastatin"}]
                        },
                    }
                },
                {
                    "resource": {
                        "resourceType": "Patient",
                        "name": [{"family": "Doe"}],
                    }
                },
            ],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["labs"] == 1
        assert result["meds"] == 1

    def test_skips_observation_without_value(self, importer):
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Observation",
                        "code": {"coding": [{"display": "Glucose"}]},
                    }
                },
            ],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["labs"] == 0

    def test_fhir_observation_with_reference_range(self, importer):
        """FHIR observations should extract reference range."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [{
                "resource": {
                    "resourceType": "Observation",
                    "code": {"coding": [{"display": "Glucose"}]},
                    "valueQuantity": {"value": 130, "unit": "mg/dL"},
                    "referenceRange": [{
                        "low": {"value": 70, "unit": "mg/dL"},
                        "high": {"value": 100, "unit": "mg/dL"},
                        "text": "70-100 mg/dL",
                    }],
                }
            }],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["labs"] == 1
        lab = importer._db.insert_observation.call_args[0][0]
        assert lab.reference_low == 70.0
        assert lab.reference_high == 100.0
        assert lab.reference_text == "70-100 mg/dL"

    def test_fhir_observation_with_interpretation(self, importer):
        """FHIR observations should extract interpretation flags."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [{
                "resource": {
                    "resourceType": "Observation",
                    "code": {"coding": [{"display": "Glucose"}]},
                    "valueQuantity": {"value": 130, "unit": "mg/dL"},
                    "interpretation": [
                        {"coding": [{"code": "H", "display": "High"}]}
                    ],
                }
            }],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["labs"] == 1
        lab = importer._db.insert_observation.call_args[0][0]
        assert lab.flag == "H"

    def test_fhir_condition_import(self, importer):
        """FHIR Condition should be stored as LTM fact."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [{
                "resource": {
                    "resourceType": "Condition",
                    "code": {"coding": [{"display": "Type 2 Diabetes Mellitus"}]},
                    "clinicalStatus": {
                        "coding": [{"code": "active"}],
                    },
                    "onsetDateTime": "2020-03-15",
                }
            }],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["conditions"] == 1
        call = importer._db.insert_ltm.call_args
        assert "Type 2 Diabetes" in call[1]["fact"]
        assert "2020-03-15" in call[1]["fact"]
        assert call[1]["category"] == "condition"

    def test_fhir_allergy_import(self, importer):
        """FHIR AllergyIntolerance should be stored as LTM fact."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [{
                "resource": {
                    "resourceType": "AllergyIntolerance",
                    "code": {"coding": [{"display": "Penicillin"}]},
                    "criticality": "high",
                    "reaction": [{
                        "manifestation": [
                            {"text": "Anaphylaxis"},
                            {"coding": [{"display": "Urticaria"}]},
                        ]
                    }],
                }
            }],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["allergies"] == 1
        call = importer._db.insert_ltm.call_args
        assert "Penicillin" in call[1]["fact"]
        assert "high" in call[1]["fact"]
        assert "Anaphylaxis" in call[1]["fact"]

    def test_fhir_immunization_import(self, importer):
        """FHIR Immunization should be stored as LTM fact."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [{
                "resource": {
                    "resourceType": "Immunization",
                    "vaccineCode": {
                        "coding": [{"display": "Influenza Vaccine"}],
                    },
                    "occurrenceDateTime": "2024-10-01",
                }
            }],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["immunizations"] == 1
        call = importer._db.insert_ltm.call_args
        assert "Influenza Vaccine" in call[1]["fact"]
        assert "2024-10-01" in call[1]["fact"]

    def test_fhir_medication_request(self, importer):
        """MedicationRequest resources should also be imported."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [{
                "resource": {
                    "resourceType": "MedicationRequest",
                    "medicationCodeableConcept": {
                        "coding": [{"display": "Lisinopril 10mg"}],
                    },
                }
            }],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["meds"] == 1

    def test_fhir_comprehensive_bundle(self, importer):
        """Bundle with all resource types."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [
                {"resource": {
                    "resourceType": "Observation",
                    "code": {"coding": [{"display": "TSH"}]},
                    "valueQuantity": {"value": 4.5, "unit": "mIU/L"},
                }},
                {"resource": {
                    "resourceType": "MedicationStatement",
                    "medicationCodeableConcept": {
                        "coding": [{"display": "Levothyroxine"}],
                    },
                }},
                {"resource": {
                    "resourceType": "Condition",
                    "code": {"coding": [{"display": "Hypothyroidism"}]},
                    "clinicalStatus": {"coding": [{"code": "active"}]},
                }},
                {"resource": {
                    "resourceType": "AllergyIntolerance",
                    "code": {"coding": [{"display": "Aspirin"}]},
                }},
                {"resource": {
                    "resourceType": "Immunization",
                    "vaccineCode": {"coding": [{"display": "Tdap"}]},
                    "occurrenceDateTime": "2023-06-01",
                }},
            ],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["labs"] == 1
        assert result["meds"] == 1
        assert result["conditions"] == 1
        assert result["allergies"] == 1
        assert result["immunizations"] == 1

    def test_fhir_condition_resolved_status(self, importer):
        """Resolved conditions should include status in fact."""
        bundle = {
            "resourceType": "Bundle",
            "entry": [{
                "resource": {
                    "resourceType": "Condition",
                    "code": {"coding": [{"display": "Pneumonia"}]},
                    "clinicalStatus": {"coding": [{"code": "resolved"}]},
                }
            }],
        }
        result = importer.import_fhir_bundle(json.dumps(bundle).encode())
        assert result["conditions"] == 1
        call = importer._db.insert_ltm.call_args
        assert "resolved" in call[1]["fact"]
