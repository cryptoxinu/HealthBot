"""Tests for FHIR R4 export."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

from healthbot.export.fhir_export import FhirExporter


def _make_exporter(labs=None, meds=None, vitals=None, events=None, wearables=None):
    """Create FhirExporter with mocked DB."""
    db = MagicMock()
    db.query_observations.side_effect = lambda **kwargs: {
        "lab_result": labs or [],
        "vital_sign": vitals or [],
        "user_event": events or [],
    }.get(kwargs.get("record_type"), [])
    db.get_active_medications.return_value = meds or []
    db.query_wearable_daily.return_value = wearables or []
    db.conn.execute.return_value.fetchall.return_value = []
    return FhirExporter(db)


class TestLabToObservation:
    """Lab results should map to FHIR Observation resources."""

    def test_basic_lab(self) -> None:
        exporter = _make_exporter()
        lab = {
            "test_name": "Glucose",
            "canonical_name": "glucose",
            "value": 108,
            "unit": "mg/dL",
            "reference_low": 70,
            "reference_high": 100,
            "_meta": {"obs_id": "abc123", "date_effective": "2025-06-15"},
        }
        obs = exporter._lab_to_observation(lab)
        assert obs is not None
        assert obs["resourceType"] == "Observation"
        assert obs["status"] == "final"
        assert obs["valueQuantity"]["value"] == 108
        assert "referenceRange" in obs
        # Glucose should have LOINC code
        assert obs["code"]["coding"][0]["system"] == "http://loinc.org"
        assert obs["code"]["coding"][0]["code"] == "2345-7"

    def test_lab_without_loinc(self) -> None:
        exporter = _make_exporter()
        lab = {
            "test_name": "Unknown Test",
            "canonical_name": "unknown_test",
            "value": 5.0,
            "unit": "units",
            "_meta": {},
        }
        obs = exporter._lab_to_observation(lab)
        assert obs is not None
        assert "coding" not in obs["code"]
        assert obs["code"]["text"] == "Unknown Test"

    def test_lab_without_value(self) -> None:
        exporter = _make_exporter()
        lab = {"test_name": "Test", "value": None}
        assert exporter._lab_to_observation(lab) is None

    def test_string_value(self) -> None:
        exporter = _make_exporter()
        lab = {
            "test_name": "Blood Type",
            "canonical_name": "blood_type",
            "value": "A+",
            "_meta": {},
        }
        obs = exporter._lab_to_observation(lab)
        assert obs is not None
        assert obs["valueString"] == "A+"


class TestMedToStatement:
    """Medications should map to MedicationStatement."""

    def test_active_med(self) -> None:
        exporter = _make_exporter()
        med = {
            "name": "Metformin",
            "dose": "500",
            "unit": "mg",
            "frequency": "twice daily",
            "status": "active",
            "start_date": "2025-01-15",
        }
        stmt = exporter._med_to_medication_statement(med)
        assert stmt is not None
        assert stmt["resourceType"] == "MedicationStatement"
        assert stmt["status"] == "active"
        assert stmt["medicationCodeableConcept"]["text"] == "Metformin"
        assert stmt["dosage"][0]["text"] == "500 mg"

    def test_empty_name(self) -> None:
        exporter = _make_exporter()
        assert exporter._med_to_medication_statement({"name": ""}) is None


class TestVitalToObservation:
    """Vitals should map to FHIR Observation with vital-signs category."""

    def test_heart_rate(self) -> None:
        exporter = _make_exporter()
        vital = {
            "type": "heart_rate",
            "value": 72,
            "unit": "bpm",
            "_meta": {"obs_id": "v1", "date_effective": "2025-06-15"},
        }
        obs = exporter._vital_to_observation(vital)
        assert obs is not None
        assert obs["category"][0]["coding"][0]["code"] == "vital-signs"
        assert obs["valueQuantity"]["value"] == 72

    def test_missing_type(self) -> None:
        exporter = _make_exporter()
        assert exporter._vital_to_observation({"value": 72}) is None


class TestExportBundle:
    """Full bundle export should produce valid FHIR structure."""

    def test_empty_db(self) -> None:
        exporter = _make_exporter()
        bundle = exporter.export_bundle()
        assert bundle["resourceType"] == "Bundle"
        assert bundle["type"] == "collection"
        assert bundle["entry"] == []

    def test_mixed_bundle(self) -> None:
        labs = [{
            "test_name": "TSH",
            "canonical_name": "tsh",
            "value": 2.5,
            "unit": "mIU/L",
            "_meta": {"obs_id": "l1"},
        }]
        meds = [{
            "name": "Levothyroxine",
            "dose": "50",
            "unit": "mcg",
            "frequency": "daily",
            "status": "active",
        }]
        exporter = _make_exporter(labs=labs, meds=meds)
        bundle = exporter.export_bundle()
        assert len(bundle["entry"]) == 2
        types = {e["resource"]["resourceType"] for e in bundle["entry"]}
        assert "Observation" in types
        assert "MedicationStatement" in types

    def test_json_export(self) -> None:
        exporter = _make_exporter()
        json_str = exporter.export_json()
        parsed = json.loads(json_str)
        assert parsed["resourceType"] == "Bundle"

    def test_labs_only(self) -> None:
        labs = [{
            "test_name": "Glucose",
            "canonical_name": "glucose",
            "value": 100,
            "_meta": {},
        }]
        meds = [{"name": "Test Med", "status": "active"}]
        exporter = _make_exporter(labs=labs, meds=meds)
        bundle = exporter.export_bundle(
            include_labs=True, include_meds=False, include_vitals=False,
            include_symptoms=False, include_wearables=False, include_concerns=False,
        )
        assert len(bundle["entry"]) == 1
        assert bundle["entry"][0]["resource"]["resourceType"] == "Observation"


class TestEventToObservation:
    """Symptom events should map to FHIR Observation with survey category."""

    def test_symptom_event(self) -> None:
        exporter = _make_exporter()
        event = {
            "symptom_category": "headache",
            "cleaned_text": "headache since yesterday moderate",
            "severity": "moderate",
            "_meta": {"obs_id": "e1", "date_effective": "2025-06-15"},
        }
        obs = exporter._event_to_observation(event)
        assert obs is not None
        assert obs["resourceType"] == "Observation"
        assert obs["category"][0]["coding"][0]["code"] == "survey"
        assert obs["code"]["text"] == "headache"
        assert obs["valueString"] == "headache since yesterday moderate"
        assert obs["interpretation"] == [{"text": "moderate"}]
        assert obs["effectiveDateTime"] == "2025-06-15"

    def test_event_without_severity(self) -> None:
        exporter = _make_exporter()
        event = {
            "symptom_category": "nausea",
            "raw_text": "feeling nauseous",
            "_meta": {"obs_id": "e2"},
        }
        obs = exporter._event_to_observation(event)
        assert obs is not None
        assert "interpretation" not in obs

    def test_empty_event(self) -> None:
        exporter = _make_exporter()
        assert exporter._event_to_observation({}) is None


class TestWearableToObservation:
    """Wearable daily data should map to FHIR Observation with components."""

    def test_wearable_daily(self) -> None:
        exporter = _make_exporter()
        wd = {
            "_date": "2025-06-15",
            "hrv": 45.0,
            "rhr": 58,
            "recovery_score": 82,
            "sleep_score": 75,
        }
        obs = exporter._wearable_to_observation(wd)
        assert obs is not None
        assert obs["resourceType"] == "Observation"
        assert obs["category"][0]["coding"][0]["code"] == "activity"
        assert obs["code"]["text"] == "wearable_daily_summary"
        assert obs["effectiveDateTime"] == "2025-06-15"
        assert len(obs["component"]) == 4
        codes = {c["code"]["text"] for c in obs["component"]}
        assert "hrv" in codes
        assert "rhr" in codes

    def test_wearable_no_date(self) -> None:
        exporter = _make_exporter()
        assert exporter._wearable_to_observation({"hrv": 50}) is None

    def test_wearable_no_metrics(self) -> None:
        exporter = _make_exporter()
        assert exporter._wearable_to_observation({"_date": "2025-06-15"}) is None


class TestConcernToCondition:
    """Concerns should map to FHIR Condition resources."""

    def test_active_concern(self) -> None:
        exporter = _make_exporter()
        concern = {
            "title": "Elevated liver enzymes",
            "notes": "ALT and AST both elevated",
            "_concern_id": "c1",
            "_severity": "watch",
            "_status": "active",
            "_created_at": "2025-06-15T10:00:00",
        }
        cond = exporter._concern_to_condition(concern)
        assert cond is not None
        assert cond["resourceType"] == "Condition"
        assert cond["code"]["text"] == "Elevated liver enzymes"
        status_code = cond["clinicalStatus"]["coding"][0]["code"]
        assert status_code == "active"
        assert cond["severity"]["coding"][0]["display"] == "mild"
        assert cond["onsetDateTime"] == "2025-06-15T10:00:00"
        assert cond["note"][0]["text"] == "ALT and AST both elevated"

    def test_urgent_concern(self) -> None:
        exporter = _make_exporter()
        concern = {
            "title": "Critical glucose",
            "_concern_id": "c2",
            "_severity": "urgent",
            "_status": "active",
        }
        cond = exporter._concern_to_condition(concern)
        assert cond is not None
        assert cond["severity"]["coding"][0]["display"] == "severe"

    def test_empty_title(self) -> None:
        exporter = _make_exporter()
        assert exporter._concern_to_condition({"title": ""}) is None


class TestBundleWithNewTypes:
    """Bundle should include symptoms, wearables, and concerns."""

    def test_bundle_with_events(self) -> None:
        events = [{
            "symptom_category": "headache",
            "cleaned_text": "bad headache",
            "_meta": {"obs_id": "e1"},
        }]
        exporter = _make_exporter(events=events)
        bundle = exporter.export_bundle(
            include_labs=False, include_meds=False, include_vitals=False,
            include_symptoms=True, include_wearables=False, include_concerns=False,
        )
        assert len(bundle["entry"]) == 1
        assert bundle["entry"][0]["resource"]["code"]["text"] == "headache"

    def test_bundle_with_wearables(self) -> None:
        wearables = [{"_date": "2025-06-15", "hrv": 50, "rhr": 60}]
        exporter = _make_exporter(wearables=wearables)
        bundle = exporter.export_bundle(
            include_labs=False, include_meds=False, include_vitals=False,
            include_symptoms=False, include_wearables=True, include_concerns=False,
        )
        assert len(bundle["entry"]) == 1
        assert bundle["entry"][0]["resource"]["code"]["text"] == "wearable_daily_summary"
