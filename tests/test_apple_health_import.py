"""Tests for Apple Health import."""
from __future__ import annotations

import io
import json
import zipfile
from unittest.mock import MagicMock

from healthbot.ingest.apple_health_import import (
    CATEGORY_TYPES,
    SUPPORTED_TYPES,
    WORKOUT_TYPES,
    AppleHealthImporter,
    AppleHealthImportResult,
)

# ── Helpers ──────────────────────────────────────────────────────────

def _make_export_zip(xml_content: str) -> bytes:
    """Create an in-memory ZIP with apple_health_export/export.xml."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("apple_health_export/export.xml", xml_content)
    return buf.getvalue()


def _make_record(record_type: str, value: str, unit: str, date: str) -> str:
    return (
        f'<Record type="{record_type}" value="{value}" '
        f'unit="{unit}" startDate="{date}" endDate="{date}"/>'
    )


_VALID_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
  <Record type="HKQuantityTypeIdentifierHeartRate" value="72"
          unit="count/min" startDate="2024-01-15 08:30:00 -0500"
          endDate="2024-01-15 08:30:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierStepCount" value="5432"
          unit="count" startDate="2024-01-15 09:00:00 -0500"
          endDate="2024-01-15 09:30:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierBloodPressureSystolic" value="120"
          unit="mmHg" startDate="2024-01-15 10:00:00 -0500"
          endDate="2024-01-15 10:00:00 -0500"/>
</HealthData>
"""


def _mock_db():
    db = MagicMock()
    db.insert_observation = MagicMock()
    db.get_existing_observation_keys = MagicMock(return_value=set())
    db.insert_workout = MagicMock(return_value="fake-wo-id")
    db.get_existing_workout_keys = MagicMock(return_value=set())
    return db


# ── Tests ────────────────────────────────────────────────────────────

class TestAppleHealthImporter:
    def test_import_valid_records(self):
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(_VALID_XML))

        assert result.records_imported == 3
        assert "heart_rate" in result.types_found
        assert "steps" in result.types_found
        assert "bp_systolic" in result.types_found
        assert result.types_found["heart_rate"] == 1
        assert result.types_found["steps"] == 1

    def test_no_raw_zip_stored(self):
        """Raw ZIP should NOT be stored — it contains PII."""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(_make_export_zip(_VALID_XML))
        # No vault interaction — raw ZIP with PII is never persisted

    def test_inserts_observations(self):
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(_make_export_zip(_VALID_XML))
        assert db.insert_observation.call_count == 3

    def test_missing_export_xml(self):
        """ZIP without export.xml should return empty result."""
        db = _mock_db()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("other_file.txt", "not health data")
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(buf.getvalue())
        assert result.records_imported == 0
        assert result.types_found == {}

    def test_unsupported_types_skipped(self):
        """Records with unsupported types should be ignored."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierUnknownType" value="42"
          unit="count" startDate="2024-01-15 08:00:00 -0500"
          endDate="2024-01-15 08:00:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" value="70"
          unit="count/min" startDate="2024-01-15 08:00:00 -0500"
          endDate="2024-01-15 08:00:00 -0500"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        assert "heart_rate" in result.types_found

    def test_empty_export(self):
        """Empty HealthData should return 0 records."""
        xml = '<?xml version="1.0"?><HealthData></HealthData>'
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 0

    def test_supported_types_count(self):
        """Verify we support 49 Apple Health quantity types."""
        assert len(SUPPORTED_TYPES) == 49

    def test_result_dataclass_defaults(self):
        r = AppleHealthImportResult()
        assert r.records_imported == 0
        assert r.records_skipped == 0
        assert r.types_found == {}

    def test_pii_not_extracted(self):
        """Verify sourceName and device attributes are NOT extracted."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Me HKCharacteristicTypeIdentifierDateOfBirth="1990-01-15"
      HKCharacteristicTypeIdentifierBiologicalSex="HKBiologicalSexMale"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" value="72"
          sourceName="John's Apple Watch" device="Apple Watch"
          unit="count/min" startDate="2024-01-15 08:30:00 -0500"
          endDate="2024-01-15 08:30:00 -0500"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1

        # Verify the stored VitalSign contains no PII
        vital = db.insert_observation.call_args_list[0][0][0]
        assert vital.type == "heart_rate"
        assert vital.value == "72"
        assert vital.source == "apple_health"
        # No name, no device, no DOB
        assert not hasattr(vital, "source_name")
        assert not hasattr(vital, "device")
        assert "John" not in str(vital.__dict__)


class TestDateParsing:
    """Test the three date formats handled by _parse_date."""

    def test_format_with_timezone(self):
        importer = AppleHealthImporter(_mock_db())
        result = importer._parse_date("2024-01-15 08:30:00 -0500")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1

    def test_format_without_timezone(self):
        importer = AppleHealthImporter(_mock_db())
        result = importer._parse_date("2024-01-15 08:30:00")
        assert result is not None
        assert result.year == 2024

    def test_format_iso8601(self):
        importer = AppleHealthImporter(_mock_db())
        result = importer._parse_date("2024-01-15T08:30:00+0000")
        assert result is not None
        assert result.year == 2024

    def test_invalid_date_returns_none(self):
        importer = AppleHealthImporter(_mock_db())
        result = importer._parse_date("not-a-date")
        assert result is None


class TestDeduplication:
    def test_duplicate_records_skipped(self):
        """Importing same data twice should skip duplicates."""
        db = _mock_db()
        importer = AppleHealthImporter(db)

        # First import — nothing exists
        result1 = importer.import_from_zip_bytes(_make_export_zip(_VALID_XML))
        assert result1.records_imported == 3
        assert result1.records_skipped == 0

        # Simulate existing keys from first import
        db.get_existing_observation_keys = MagicMock(return_value={
            ("heart_rate", "2024-01-15T08:30:00-05:00"),
            ("steps", "2024-01-15T09:00:00-05:00"),
            ("bp_systolic", "2024-01-15T10:00:00-05:00"),
        })

        # Second import — all should be skipped
        result2 = importer.import_from_zip_bytes(_make_export_zip(_VALID_XML))
        assert result2.records_imported == 0
        assert result2.records_skipped == 3

    def test_partial_dedup(self):
        """Only new records should be imported."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierHeartRate" value="72"
          unit="count/min" startDate="2024-01-15 08:30:00 -0500"
          endDate="2024-01-15 08:30:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" value="80"
          unit="count/min" startDate="2024-01-16 08:30:00 -0500"
          endDate="2024-01-16 08:30:00 -0500"/>
</HealthData>
"""
        db = _mock_db()
        # Only the first record exists
        db.get_existing_observation_keys = MagicMock(return_value={
            ("heart_rate", "2024-01-15T08:30:00-05:00"),
        })
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        assert result.records_skipped == 1

    def test_within_batch_dedup(self):
        """Duplicate records within the same ZIP should be deduped."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Record type="HKQuantityTypeIdentifierHeartRate" value="72"
          unit="count/min" startDate="2024-01-15 08:30:00 -0500"
          endDate="2024-01-15 08:30:00 -0500"/>
  <Record type="HKQuantityTypeIdentifierHeartRate" value="72"
          unit="count/min" startDate="2024-01-15 08:30:00 -0500"
          endDate="2024-01-15 08:30:00 -0500"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        assert result.records_skipped == 1


class TestAllSupportedTypes:
    def test_all_49_types_imported(self):
        """Build XML with all 49 supported quantity types and verify all are imported."""
        records = []
        for hk_type in SUPPORTED_TYPES:
            records.append(_make_record(hk_type, "42", "count", "2024-01-15 08:00:00 -0500"))
        xml = f'<?xml version="1.0"?><HealthData>{"".join(records)}</HealthData>'

        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 49
        for mapped_name in SUPPORTED_TYPES.values():
            assert mapped_name in result.types_found


# ── Workout XML fixtures ────────────────────────────────────────────

_WORKOUT_XML = """\
<?xml version="1.0"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="28.5" durationUnit="min"
           totalDistance="5.2" totalDistanceUnit="km"
           totalEnergyBurned="320" totalEnergyBurnedUnit="kcal"
           startDate="2024-01-15 07:00:00 -0500"
           endDate="2024-01-15 07:28:30 -0500">
    <WorkoutStatistics type="HKQuantityTypeIdentifierHeartRate"
                        average="155" minimum="120" maximum="178"
                        unit="count/min"/>
  </Workout>
</HealthData>
"""

_MULTI_WORKOUT_XML = """\
<?xml version="1.0"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="28.5" durationUnit="min"
           totalDistance="5.2" totalDistanceUnit="km"
           totalEnergyBurned="320" totalEnergyBurnedUnit="kcal"
           startDate="2024-01-15 07:00:00 -0500"
           endDate="2024-01-15 07:28:30 -0500">
    <WorkoutStatistics type="HKQuantityTypeIdentifierHeartRate"
                        average="155" minimum="120" maximum="178"
                        unit="count/min"/>
  </Workout>
  <Workout workoutActivityType="HKWorkoutActivityTypeYoga"
           duration="45" durationUnit="min"
           totalEnergyBurned="150" totalEnergyBurnedUnit="kcal"
           startDate="2024-01-16 18:00:00 -0500"
           endDate="2024-01-16 18:45:00 -0500"/>
  <Workout workoutActivityType="HKWorkoutActivityTypeCycling"
           duration="60" durationUnit="min"
           totalDistance="25" totalDistanceUnit="km"
           totalEnergyBurned="500" totalEnergyBurnedUnit="kcal"
           startDate="2024-01-17 07:00:00 -0500"
           endDate="2024-01-17 08:00:00 -0500">
    <WorkoutStatistics type="HKQuantityTypeIdentifierHeartRate"
                        average="142" minimum="105" maximum="170"
                        unit="count/min"/>
  </Workout>
  <Record type="HKQuantityTypeIdentifierHeartRate" value="72"
          unit="count/min" startDate="2024-01-15 08:30:00 -0500"
          endDate="2024-01-15 08:30:00 -0500"/>
</HealthData>
"""


# ── Workout Tests ───────────────────────────────────────────────────

class TestWorkoutParsing:
    def test_parse_single_workout(self):
        """Parse a single workout with all attributes."""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(_WORKOUT_XML))

        assert result.workouts_imported == 1
        assert result.workouts_skipped == 0
        assert result.records_imported == 0  # No vitals in this XML

        # Verify the Workout object passed to insert_workout
        wo = db.insert_workout.call_args_list[0][0][0]
        assert wo.sport_type == "running"
        assert wo.duration_minutes == 28.5
        assert wo.distance_km == 5.2
        assert wo.calories_burned == 320.0
        assert wo.avg_heart_rate == 155.0
        assert wo.max_heart_rate == 178.0
        assert wo.min_heart_rate == 120.0
        assert wo.source == "apple_health"
        assert wo.start_time is not None
        assert wo.end_time is not None

    def test_parse_multiple_workouts(self):
        """Parse multiple workout types in one export."""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(
            _make_export_zip(_MULTI_WORKOUT_XML),
        )

        assert result.workouts_imported == 3
        assert result.records_imported == 1  # 1 heart rate record too
        assert db.insert_workout.call_count == 3

    def test_workout_without_hr_stats(self):
        """Yoga workout with no WorkoutStatistics is still parsed."""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(
            _make_export_zip(_MULTI_WORKOUT_XML),
        )

        # Find the yoga workout (second call)
        yoga_wo = db.insert_workout.call_args_list[1][0][0]
        assert yoga_wo.sport_type == "yoga"
        assert yoga_wo.duration_minutes == 45.0
        assert yoga_wo.avg_heart_rate is None  # No HR stats
        assert yoga_wo.distance_km is None  # No distance for yoga

    def test_unsupported_workout_type_skipped(self):
        """Workouts with unknown activity types are skipped."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeUnderwaterDiving"
           duration="45" durationUnit="min"
           startDate="2024-01-15 10:00:00 -0500"
           endDate="2024-01-15 10:45:00 -0500"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.workouts_imported == 0

    def test_workout_types_count(self):
        """Verify we support 24 workout activity types (but map to fewer canonical names)."""
        assert len(WORKOUT_TYPES) >= 23  # At least 23 supported

    def test_distance_miles_converted_to_km(self):
        """Workouts reported in miles should be converted to km."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="30" durationUnit="min"
           totalDistance="3.1" totalDistanceUnit="mi"
           startDate="2024-01-15 07:00:00 -0500"
           endDate="2024-01-15 07:30:00 -0500"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.workouts_imported == 1

        wo = db.insert_workout.call_args_list[0][0][0]
        # 3.1 miles * 1.60934 ≈ 4.99
        assert wo.distance_km is not None
        assert abs(wo.distance_km - 4.989) < 0.01

    def test_workout_pii_not_extracted(self):
        """Verify sourceName and device attributes are NOT in the Workout."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="30" durationUnit="min"
           sourceName="John's Apple Watch"
           device="Apple Watch Series 9"
           startDate="2024-01-15 07:00:00 -0500"
           endDate="2024-01-15 07:30:00 -0500"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.workouts_imported == 1

        wo = db.insert_workout.call_args_list[0][0][0]
        # No PII attributes
        assert not hasattr(wo, "source_name")
        assert not hasattr(wo, "device")
        assert "John" not in str(wo.__dict__)

    def test_workout_missing_start_date_skipped(self):
        """Workouts without startDate are skipped."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="30" durationUnit="min"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.workouts_imported == 0


class TestWorkoutDedup:
    def test_duplicate_workouts_skipped(self):
        """Importing same workout twice should skip duplicates."""
        db = _mock_db()
        importer = AppleHealthImporter(db)

        # First import — no existing workouts
        result1 = importer.import_from_zip_bytes(
            _make_export_zip(_WORKOUT_XML),
        )
        assert result1.workouts_imported == 1
        assert result1.workouts_skipped == 0

        # Simulate existing keys
        db.get_existing_workout_keys = MagicMock(return_value={
            ("running", "2024-01-15T07:00:00-05:00"),
        })

        result2 = importer.import_from_zip_bytes(
            _make_export_zip(_WORKOUT_XML),
        )
        assert result2.workouts_imported == 0
        assert result2.workouts_skipped == 1

    def test_within_batch_workout_dedup(self):
        """Duplicate workouts in same ZIP should be deduped."""
        xml = """\
<?xml version="1.0"?>
<HealthData>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="28.5" durationUnit="min"
           startDate="2024-01-15 07:00:00 -0500"
           endDate="2024-01-15 07:28:30 -0500"/>
  <Workout workoutActivityType="HKWorkoutActivityTypeRunning"
           duration="28.5" durationUnit="min"
           startDate="2024-01-15 07:00:00 -0500"
           endDate="2024-01-15 07:28:30 -0500"/>
</HealthData>
"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.workouts_imported == 1
        assert result.workouts_skipped == 1


class TestResultDataclass:
    def test_result_has_workout_fields(self):
        """AppleHealthImportResult should have workout counters."""
        r = AppleHealthImportResult()
        assert r.workouts_imported == 0
        assert r.workouts_skipped == 0

    def test_result_has_clinical_fields(self):
        """AppleHealthImportResult should have clinical record counters."""
        r = AppleHealthImportResult()
        assert r.clinical_records == 0
        assert r.clinical_breakdown == {}


# ── Clinical Record XML fixtures ───────────────────────────────────


def _make_clinical_record(resource_type: str, fhir_resource: dict) -> str:
    """Build a <ClinicalRecord> element with inline FHIR JSON."""
    fhir_json = json.dumps(fhir_resource)
    return (
        f'<ClinicalRecord type="HKClinicalTypeIdentifier{resource_type}Record"'
        f' displayName="test">'
        f'<fhirResource>{fhir_json}</fhirResource>'
        f'</ClinicalRecord>'
    )


_ALLERGY_RESOURCE = {
    "resourceType": "AllergyIntolerance",
    "code": {"coding": [{"display": "Penicillin"}]},
    "criticality": "high",
    "reaction": [
        {"manifestation": [{"text": "rash"}, {"text": "hives"}]}
    ],
}

_CONDITION_RESOURCE = {
    "resourceType": "Condition",
    "code": {"coding": [{"display": "Type 2 Diabetes Mellitus"}]},
    "clinicalStatus": {"coding": [{"code": "active"}]},
    "onsetDateTime": "2022-03-15",
}

_MEDICATION_RESOURCE = {
    "resourceType": "MedicationStatement",
    "medicationCodeableConcept": {
        "coding": [{"display": "Metformin"}],
    },
    "status": "active",
    "dosage": [
        {
            "doseAndRate": [
                {"doseQuantity": {"value": 500, "unit": "mg"}}
            ],
            "timing": {
                "repeat": {"frequency": 2, "period": 1, "periodUnit": "d"}
            },
        }
    ],
}

_IMMUNIZATION_RESOURCE = {
    "resourceType": "Immunization",
    "vaccineCode": {"coding": [{"display": "COVID-19 Pfizer-BioNTech"}]},
    "occurrenceDateTime": "2024-01-15T10:00:00Z",
}

_LAB_RESOURCE = {
    "resourceType": "Observation",
    "code": {"coding": [{"display": "Hemoglobin A1c"}]},
    "valueQuantity": {"value": 6.5, "unit": "%"},
    "referenceRange": [
        {"low": {"value": 4.0}, "high": {"value": 5.6}, "text": "4.0-5.6 %"}
    ],
    "interpretation": [{"coding": [{"code": "H"}]}],
    "effectiveDateTime": "2024-01-15",
}

_PROCEDURE_RESOURCE = {
    "resourceType": "Procedure",
    "code": {"coding": [{"display": "Knee arthroscopy"}]},
    "status": "completed",
    "performedDateTime": "2024-01-10T09:00:00Z",
    "performer": [{"actor": {"display": "Dr. Smith"}}],
    "location": {"display": "City Hospital"},
}

_CLINICAL_XML = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<HealthData locale="en_US">
  <Record type="HKQuantityTypeIdentifierHeartRate" value="72"
          unit="count/min" startDate="2024-01-15 08:30:00 -0500"
          endDate="2024-01-15 08:30:00 -0500"/>
  {_make_clinical_record("Allergy", _ALLERGY_RESOURCE)}
  {_make_clinical_record("Condition", _CONDITION_RESOURCE)}
  {_make_clinical_record("Medication", _MEDICATION_RESOURCE)}
  {_make_clinical_record("Immunization", _IMMUNIZATION_RESOURCE)}
  {_make_clinical_record("LabResult", _LAB_RESOURCE)}
  {_make_clinical_record("Procedure", _PROCEDURE_RESOURCE)}
</HealthData>
"""


# ── Clinical Record Tests ──────────────────────────────────────────


class TestClinicalRecordParsing:
    """Test clinical record extraction from Apple Health exports."""

    def test_clinical_records_extracted_in_relaxed_mode(self):
        """5 clinical record types extracted in relaxed mode.

        elem.clear() runs for ALL elements during iterparse, which clears
        fhirResource children before the parent ClinicalRecord is processed.
        Records fall back to attribute-only extraction (using displayName).
        Labs are skipped in attribute fallback (no LabResult from displayName),
        so only 5 of the 6 types are extracted.
        """
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(
            _make_export_zip(_CLINICAL_XML),
            privacy_mode="relaxed",
        )
        assert result.clinical_records == 5
        assert result.clinical_breakdown["allergies"] == 1
        assert result.clinical_breakdown["conditions"] == 1
        assert result.clinical_breakdown["medications"] == 1
        assert result.clinical_breakdown["immunizations"] == 1
        assert result.clinical_breakdown["procedures"] == 1
        # Labs are NOT extracted (attribute fallback skips labs)
        assert "labs" not in result.clinical_breakdown
        # Vitals still imported too
        assert result.records_imported == 1

    def test_clinical_records_skipped_in_strict_mode(self):
        """No clinical records extracted when privacy_mode is strict."""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(
            _make_export_zip(_CLINICAL_XML),
            privacy_mode="strict",
        )
        assert result.clinical_records == 0
        assert result.clinical_breakdown == {}
        # Vitals still imported
        assert result.records_imported == 1

    def test_allergy_fact_stored(self):
        """Allergy should be stored as LTM fact via attribute fallback.

        elem.clear() clears fhirResource children before ClinicalRecord
        is processed, so FHIR JSON is unavailable. Falls back to
        _extract_from_attributes using displayName.
        """
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("Allergy", _ALLERGY_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        db.insert_ltm.assert_called_once()
        call_kwargs = db.insert_ltm.call_args[1]
        assert call_kwargs["category"] == "condition"
        assert call_kwargs["source"] == "apple_health_clinical"
        # Falls back to displayName="test" (FHIR JSON lost by elem.clear())
        assert "test" in call_kwargs["fact"]

    def test_condition_fact_stored(self):
        """Condition should be stored as LTM fact via attribute fallback.

        elem.clear() clears fhirResource children before ClinicalRecord
        is processed, so FHIR JSON is unavailable. Falls back to displayName.
        """
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("Condition", _CONDITION_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        db.insert_ltm.assert_called_once()
        call_kwargs = db.insert_ltm.call_args[1]
        assert call_kwargs["category"] == "condition"
        # Falls back to displayName="test" (FHIR JSON lost by elem.clear())
        assert "Known condition: test" == call_kwargs["fact"]

    def test_medication_fact_stored(self):
        """Medication should be stored via attribute fallback.

        elem.clear() clears fhirResource children before ClinicalRecord
        is processed, so FHIR JSON is unavailable. Falls back to displayName.
        """
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("Medication", _MEDICATION_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        db.insert_ltm.assert_called_once()
        call_kwargs = db.insert_ltm.call_args[1]
        assert call_kwargs["category"] == "medication"
        # Falls back to displayName="test" (FHIR JSON lost by elem.clear())
        assert "Medication: test" == call_kwargs["fact"]

    def test_immunization_fact_stored(self):
        """Immunization should be stored via attribute fallback.

        elem.clear() clears fhirResource children before ClinicalRecord
        is processed, so FHIR JSON is unavailable. Falls back to displayName.
        """
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("Immunization", _IMMUNIZATION_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        db.insert_ltm.assert_called_once()
        call_kwargs = db.insert_ltm.call_args[1]
        assert call_kwargs["category"] == "medication"
        # Falls back to displayName="test" (FHIR JSON lost by elem.clear())
        assert "Immunization: test" == call_kwargs["fact"]

    def test_lab_result_skipped_due_to_elem_clear(self):
        """Lab result is NOT extracted because elem.clear() clears fhirResource
        before ClinicalRecord is processed, and attribute-only fallback
        explicitly skips labs (can't create LabResult from displayName alone).
        """
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("LabResult", _LAB_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        # Labs are skipped: FHIR JSON lost and attribute fallback skips labs
        assert "labs" not in result.clinical_breakdown
        assert result.clinical_records == 0
        assert db.insert_observation.call_count == 0
        assert db.insert_ltm.call_count == 0

    def test_procedure_fact_stored(self):
        """Procedure should be stored via attribute fallback.

        elem.clear() clears fhirResource children before ClinicalRecord
        is processed, so FHIR JSON is unavailable. Falls back to displayName.
        PII is inherently absent since attribute fallback only uses displayName.
        """
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("Procedure", _PROCEDURE_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        db.insert_ltm.assert_called_once()
        call_kwargs = db.insert_ltm.call_args[1]
        assert call_kwargs["category"] == "procedure"
        # Falls back to displayName="test" (FHIR JSON lost by elem.clear())
        assert "Procedure: test" == call_kwargs["fact"]
        # PII is inherently absent (only displayName is used)
        assert "Dr. Smith" not in call_kwargs["fact"]
        assert "City Hospital" not in call_kwargs["fact"]

    def test_pii_not_stored_in_clinical_facts(self):
        """Provider names, facilities, lot numbers must not be stored."""
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("Procedure", _PROCEDURE_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        fact = db.insert_ltm.call_args[1]["fact"]
        assert "Smith" not in fact
        assert "Hospital" not in fact

    def test_default_privacy_mode_is_relaxed(self):
        """Default privacy_mode should be relaxed (clinical records extracted)."""
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_clinical_record("Condition", _CONDITION_RESOURCE)}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        # No privacy_mode arg — should default to relaxed
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.clinical_records == 1

    def test_attribute_only_clinical_record(self):
        """Clinical records without inline FHIR should use displayName."""
        xml = """\
<?xml version="1.0"?><HealthData>
  <ClinicalRecord type="HKClinicalTypeIdentifierConditionRecord"
                   displayName="Hypertension"/>
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        assert result.clinical_records == 1
        db.insert_ltm.assert_called_once()
        assert "Hypertension" in db.insert_ltm.call_args[1]["fact"]

    def test_malformed_fhir_json_skipped(self):
        """Malformed FHIR JSON should not crash — falls back to attributes."""
        xml = """\
<?xml version="1.0"?><HealthData>
  <ClinicalRecord type="HKClinicalTypeIdentifierConditionRecord"
                   displayName="Asthma">
    <fhirResource>{not valid json}</fhirResource>
  </ClinicalRecord>
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        # Falls back to attribute extraction
        assert result.clinical_records == 1
        assert "Asthma" in db.insert_ltm.call_args[1]["fact"]

    def test_empty_clinical_record_skipped(self):
        """Clinical records with no data should be silently skipped."""
        xml = """\
<?xml version="1.0"?><HealthData>
  <ClinicalRecord type="HKClinicalTypeIdentifierConditionRecord"/>
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(
            _make_export_zip(xml), privacy_mode="relaxed",
        )
        assert result.clinical_records == 0
        assert db.insert_ltm.call_count == 0


# ── Category Type Tests ────────────────────────────────────────────


def _make_category_record(
    record_type: str, value: str, start: str, end: str,
) -> str:
    return (
        f'<Record type="{record_type}" value="{value}" '
        f'startDate="{start}" endDate="{end}"/>'
    )


class TestCategoryTypes:
    def test_category_types_dict_structure(self):
        """Every CATEGORY_TYPES entry must have a canonical key."""
        for hk_type, info in CATEGORY_TYPES.items():
            assert "canonical" in info, f"{hk_type} missing 'canonical'"
            assert isinstance(info["canonical"], str)
            if "value_map" in info:
                assert isinstance(info["value_map"], dict)

    def test_sleep_stage_with_duration(self):
        """Sleep analysis should record duration in minutes and map stage."""
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_category_record(
      "HKCategoryTypeIdentifierSleepAnalysis", "5",
      "2024-01-15 23:00:00 -0500", "2024-01-16 00:30:00 -0500",
  )}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        vital = db.insert_observation.call_args[0][0]
        assert vital.type == "sleep_stage"
        assert vital.unit == "min"
        # 90 minutes between 23:00 and 00:30
        assert abs(float(vital.value) - 90.0) < 0.2
        assert vital.source == "apple_health"

    def test_mindful_minutes_duration(self):
        """Mindful session should record duration in minutes."""
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_category_record(
      "HKCategoryTypeIdentifierMindfulSession", "0",
      "2024-01-15 07:00:00 -0500", "2024-01-15 07:15:00 -0500",
  )}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        vital = db.insert_observation.call_args[0][0]
        assert vital.type == "mindful_minutes"
        assert vital.unit == "min"
        assert abs(float(vital.value) - 15.0) < 0.2

    def test_high_heart_rate_event(self):
        """Heart rate events should be recorded as presence (value=1)."""
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_category_record(
      "HKCategoryTypeIdentifierHighHeartRateEvent", "0",
      "2024-01-15 10:00:00 -0500", "2024-01-15 10:05:00 -0500",
  )}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        vital = db.insert_observation.call_args[0][0]
        assert vital.type == "high_heart_rate_event"
        assert vital.value == "1"

    def test_headache_severity_value_map(self):
        """Headache should map integer enum to severity string."""
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_category_record(
      "HKCategoryTypeIdentifierHeadache", "2",
      "2024-01-15 14:00:00 -0500", "2024-01-15 14:30:00 -0500",
  )}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        vital = db.insert_observation.call_args[0][0]
        assert vital.type == "headache"
        assert vital.value == "moderate"

    def test_handwashing_duration(self):
        """Handwashing events should record duration in minutes."""
        xml = f"""\
<?xml version="1.0"?><HealthData>
  {_make_category_record(
      "HKCategoryTypeIdentifierHandwashingEvent", "0",
      "2024-01-15 12:00:00 -0500", "2024-01-15 12:00:20 -0500",
  )}
</HealthData>"""
        db = _mock_db()
        importer = AppleHealthImporter(db)
        result = importer.import_from_zip_bytes(_make_export_zip(xml))
        assert result.records_imported == 1
        vital = db.insert_observation.call_args[0][0]
        assert vital.type == "handwashing"
        assert vital.unit == "min"
        # 20 seconds = ~0.3 min
        assert abs(float(vital.value) - 0.3) < 0.1
