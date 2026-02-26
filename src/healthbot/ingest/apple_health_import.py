"""Apple Health export.zip parser.

Parses the XML export from Apple Health app. Uses iterparse for
memory efficiency with large exports.

Clinical records (allergies, conditions, medications, immunizations,
labs, procedures) are extracted when privacy_mode == "relaxed".
PII (provider names, facility, lot numbers) is stripped at extraction
time. Clinical facts are stored as LTM entries; labs as LabResult
observations. Clean Sync handles anonymization automatically.
"""
from __future__ import annotations

import io
import json
import logging
import uuid
import zipfile
from dataclasses import dataclass, field
from datetime import datetime

import defusedxml.ElementTree as ET  # noqa: N817

from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, VitalSign, Workout
from healthbot.normalize.lab_normalizer import normalize_test_name

logger = logging.getLogger("healthbot")

# HealthKit quantity types we extract
SUPPORTED_TYPES: dict[str, str] = {
    "HKQuantityTypeIdentifierHeartRate": "heart_rate",
    "HKQuantityTypeIdentifierRestingHeartRate": "resting_heart_rate",
    "HKQuantityTypeIdentifierHeartRateVariabilitySDNN": "hrv",
    "HKQuantityTypeIdentifierBloodPressureSystolic": "bp_systolic",
    "HKQuantityTypeIdentifierBloodPressureDiastolic": "bp_diastolic",
    "HKQuantityTypeIdentifierBodyMass": "weight",
    "HKQuantityTypeIdentifierHeight": "height",
    "HKQuantityTypeIdentifierStepCount": "steps",
    "HKQuantityTypeIdentifierActiveEnergyBurned": "active_calories",
    "HKQuantityTypeIdentifierOxygenSaturation": "spo2",
    "HKQuantityTypeIdentifierBodyTemperature": "body_temperature",
    "HKQuantityTypeIdentifierBloodGlucose": "blood_glucose",
    "HKQuantityTypeIdentifierVO2Max": "vo2_max",
    "HKQuantityTypeIdentifierRespiratoryRate": "respiratory_rate",
    # Body composition
    "HKQuantityTypeIdentifierBodyFatPercentage": "body_fat_pct",
    "HKQuantityTypeIdentifierLeanBodyMass": "lean_body_mass",
    "HKQuantityTypeIdentifierWaistCircumference": "waist_circumference",
    "HKQuantityTypeIdentifierBodyMassIndex": "bmi",
    # Nutrition
    "HKQuantityTypeIdentifierDietaryEnergyConsumed": "dietary_calories",
    "HKQuantityTypeIdentifierDietaryProtein": "dietary_protein",
    "HKQuantityTypeIdentifierDietaryCarbohydrates": "dietary_carbs",
    "HKQuantityTypeIdentifierDietaryFatTotal": "dietary_fat",
    "HKQuantityTypeIdentifierDietaryWater": "water_intake",
    "HKQuantityTypeIdentifierDietaryFiber": "dietary_fiber",
    "HKQuantityTypeIdentifierDietarySugar": "dietary_sugar",
    "HKQuantityTypeIdentifierDietarySodium": "dietary_sodium",
    "HKQuantityTypeIdentifierDietaryCholesterol": "dietary_cholesterol",
    "HKQuantityTypeIdentifierDietaryCaffeine": "dietary_caffeine",
    "HKQuantityTypeIdentifierDietaryFatSaturated": "dietary_fat_saturated",
    "HKQuantityTypeIdentifierDietaryFatPolyunsaturated": "dietary_fat_polyunsaturated",
    "HKQuantityTypeIdentifierDietaryFatMonounsaturated": "dietary_fat_monounsaturated",
    "HKQuantityTypeIdentifierDietaryPotassium": "dietary_potassium",
    # Respiratory
    "HKQuantityTypeIdentifierPeakExpiratoryFlowRate": "peak_expiratory_flow",
    "HKQuantityTypeIdentifierForcedVitalCapacity": "forced_vital_capacity",
    "HKQuantityTypeIdentifierInhalerUsage": "inhaler_usage",
    # Hearing
    "HKQuantityTypeIdentifierEnvironmentalAudioExposure": "environmental_audio_exposure",
    "HKQuantityTypeIdentifierHeadphoneAudioExposure": "headphone_audio_exposure",
    # Fitness / activity
    "HKQuantityTypeIdentifierBasalEnergyBurned": "basal_calories",
    "HKQuantityTypeIdentifierFlightsClimbed": "flights_climbed",
    "HKQuantityTypeIdentifierSwimmingStrokeCount": "swimming_stroke_count",
    "HKQuantityTypeIdentifierCyclingCadence": "cycling_cadence",
    "HKQuantityTypeIdentifierRunningPower": "running_power",
    "HKQuantityTypeIdentifierDistanceWalkingRunning": "distance",
    "HKQuantityTypeIdentifierDistanceSwimming": "distance_swimming",
    "HKQuantityTypeIdentifierUVExposure": "uv_exposure",
    # Other useful
    "HKQuantityTypeIdentifierAppleExerciseTime": "exercise_minutes",
    "HKQuantityTypeIdentifierAppleStandTime": "stand_minutes",
    "HKQuantityTypeIdentifierWalkingHeartRateAverage": "walking_hr_avg",
    "HKQuantityTypeIdentifierWalkingSpeed": "walking_speed",
}


# Apple Health workout activity types -> canonical sport names
WORKOUT_TYPES: dict[str, str] = {
    "HKWorkoutActivityTypeRunning": "running",
    "HKWorkoutActivityTypeCycling": "cycling",
    "HKWorkoutActivityTypeSwimming": "swimming",
    "HKWorkoutActivityTypeWalking": "walking",
    "HKWorkoutActivityTypeHiking": "hiking",
    "HKWorkoutActivityTypeYoga": "yoga",
    "HKWorkoutActivityTypeFunctionalStrengthTraining": "strength_training",
    "HKWorkoutActivityTypeTraditionalStrengthTraining": "strength_training",
    "HKWorkoutActivityTypeCoreTraining": "core_training",
    "HKWorkoutActivityTypeHighIntensityIntervalTraining": "hiit",
    "HKWorkoutActivityTypePilates": "pilates",
    "HKWorkoutActivityTypeElliptical": "elliptical",
    "HKWorkoutActivityTypeRowing": "rowing",
    "HKWorkoutActivityTypeDance": "dance",
    "HKWorkoutActivityTypeCooldown": "cooldown",
    "HKWorkoutActivityTypeMindAndBody": "mind_and_body",
    "HKWorkoutActivityTypeSoccer": "soccer",
    "HKWorkoutActivityTypeBasketball": "basketball",
    "HKWorkoutActivityTypeTennis": "tennis",
    "HKWorkoutActivityTypeStairClimbing": "stair_climbing",
    "HKWorkoutActivityTypeCrossTraining": "cross_training",
    "HKWorkoutActivityTypePlay": "play",
    "HKWorkoutActivityTypeMixedCardio": "mixed_cardio",
    "HKWorkoutActivityTypeOther": "other",
}

# HealthKit category types — integer enum values or duration-based
CATEGORY_TYPES: dict[str, dict] = {
    "HKCategoryTypeIdentifierSleepAnalysis": {
        "canonical": "sleep_stage",
        "use_duration": True,
        "value_map": {
            "0": "InBed",
            "1": "Asleep",
            "2": "Awake",
            "3": "AsleepCore",
            "4": "AsleepDeep",
            "5": "AsleepREM",
        },
    },
    "HKCategoryTypeIdentifierMindfulSession": {
        "canonical": "mindful_minutes",
        "use_duration": True,
    },
    "HKCategoryTypeIdentifierHighHeartRateEvent": {
        "canonical": "high_heart_rate_event",
    },
    "HKCategoryTypeIdentifierLowHeartRateEvent": {
        "canonical": "low_heart_rate_event",
    },
    "HKCategoryTypeIdentifierIrregularHeartRhythmEvent": {
        "canonical": "irregular_heart_rhythm_event",
    },
    "HKCategoryTypeIdentifierHeadache": {
        "canonical": "headache",
        "value_map": {
            "1": "mild",
            "2": "moderate",
            "3": "severe",
        },
    },
    "HKCategoryTypeIdentifierAppetiteChanges": {
        "canonical": "appetite_changes",
        "value_map": {
            "1": "decreased",
            "2": "increased",
            "3": "no_change",
        },
    },
    "HKCategoryTypeIdentifierMenstrualFlow": {
        "canonical": "menstrual_flow",
        "value_map": {
            "1": "unspecified",
            "2": "light",
            "3": "medium",
            "4": "heavy",
        },
    },
    "HKCategoryTypeIdentifierOvulationTestResult": {
        "canonical": "ovulation_test",
        "value_map": {
            "1": "negative",
            "2": "luteinizing_hormone_surge",
            "3": "indeterminate",
            "4": "estrogen_surge",
        },
    },
    "HKCategoryTypeIdentifierSexualActivity": {
        "canonical": "sexual_activity",
    },
    "HKCategoryTypeIdentifierIntermenstrualBleeding": {
        "canonical": "intermenstrual_bleeding",
    },
    "HKCategoryTypeIdentifierHandwashingEvent": {
        "canonical": "handwashing",
        "use_duration": True,
    },
}

# Apple Health clinical record type identifiers -> category names
_CLINICAL_TYPE_MAP: dict[str, str] = {
    "HKClinicalTypeIdentifierAllergyRecord": "allergy",
    "HKClinicalTypeIdentifierConditionRecord": "condition",
    "HKClinicalTypeIdentifierMedicationRecord": "medication",
    "HKClinicalTypeIdentifierImmunizationRecord": "immunization",
    "HKClinicalTypeIdentifierLabResultRecord": "lab",
    "HKClinicalTypeIdentifierProcedureRecord": "procedure",
}


@dataclass
class AppleHealthImportResult:
    records_imported: int = 0
    records_skipped: int = 0
    types_found: dict[str, int] = field(default_factory=dict)
    workouts_imported: int = 0
    workouts_skipped: int = 0
    clinical_records: int = 0
    clinical_breakdown: dict[str, int] = field(default_factory=dict)


class AppleHealthImporter:
    """Parse and import Apple Health export data."""

    def __init__(self, db: HealthDB, vault: object = None) -> None:
        self._db = db

    # ── Batch-friendly API (parse + insert separately) ────────────

    def parse_zip_bytes(
        self, zip_bytes: bytes, privacy_mode: str = "relaxed",
    ) -> tuple[list[VitalSign], list[Workout], bytes | None]:
        """Parse ZIP and return (vitals, workouts, xml_bytes). No DB writes.

        Returns xml_bytes only when privacy_mode == "relaxed" (needed for
        clinical record extraction later). Callers can then insert records
        in batches using insert_vitals_batch / insert_workouts_batch.
        """
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            xml_name = next(
                (n for n in zf.namelist() if n.endswith("export.xml")), None,
            )
            if not xml_name:
                return [], [], None
            xml_bytes_raw = zf.read(xml_name)

        vitals, workouts = self._parse_xml_streaming(xml_bytes_raw)
        return vitals, workouts, xml_bytes_raw if privacy_mode == "relaxed" else None

    def insert_vitals_batch(
        self,
        vitals: list[VitalSign],
        existing_keys: set[tuple[str, str | None]],
        user_id: int,
        result: AppleHealthImportResult,
    ) -> int:
        """Insert a slice of vitals, updating *result* and *existing_keys* in place.

        Returns the number of newly inserted records in this batch.
        """
        inserted = 0
        for vital in vitals:
            date_key = vital.timestamp.isoformat() if vital.timestamp else None
            if (vital.type, date_key) in existing_keys:
                result.records_skipped += 1
                continue
            self._db.insert_observation(vital, user_id=user_id)
            existing_keys.add((vital.type, date_key))
            result.records_imported += 1
            result.types_found[vital.type] = result.types_found.get(vital.type, 0) + 1
            inserted += 1
        return inserted

    def insert_workouts_batch(
        self,
        workouts: list[Workout],
        existing_keys: set[tuple[str, str | None]],
        user_id: int,
        result: AppleHealthImportResult,
    ) -> int:
        """Insert a slice of workouts, updating *result* and *existing_keys* in place.

        Returns the number of newly inserted workouts in this batch.
        """
        inserted = 0
        for wo in workouts:
            date_key = wo.start_time.isoformat() if wo.start_time else None
            if (wo.sport_type, date_key) in existing_keys:
                result.workouts_skipped += 1
                continue
            self._db.insert_workout(wo, user_id=user_id)
            existing_keys.add((wo.sport_type, date_key))
            result.workouts_imported += 1
            inserted += 1
        return inserted

    # ── Original all-in-one API (still used by scheduler) ────────

    def import_from_zip_bytes(
        self, zip_bytes: bytes, user_id: int = 0,
        privacy_mode: str = "relaxed",
    ) -> AppleHealthImportResult:
        """Import from Apple Health export ZIP bytes (in memory).

        PII handling: The raw ZIP contains identifiable information
        (user name in sourceName/device attributes, DOB in <Me> element,
        GPS in workout routes, patient name in clinical records).
        We do NOT store the raw ZIP. Only de-identified metric data
        (type, value, unit, timestamp) is extracted and stored.

        When privacy_mode == "relaxed", clinical records (allergies,
        conditions, medications, immunizations, labs, procedures) are
        also extracted with PII stripped at extraction time.
        """
        result = AppleHealthImportResult()

        # NOTE: We intentionally do NOT store the raw ZIP in the vault.
        # Apple Health exports contain PII (name, DOB, GPS, clinical records)
        # that we don't need. Only the extracted vitals are stored.

        # Unzip in memory
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Find export.xml
            xml_name = None
            for name in zf.namelist():
                if name.endswith("export.xml"):
                    xml_name = name
                    break
            if not xml_name:
                return result

            xml_bytes = zf.read(xml_name)

        # Stream-parse vitals + workouts
        vitals, workouts = self._parse_xml_streaming(xml_bytes)
        result.records_imported = 0

        # Load existing keys for dedup — skip records already in DB
        canonical_names = list(SUPPORTED_TYPES.values()) + [
            c["canonical"] for c in CATEGORY_TYPES.values()
        ]
        existing_keys = self._db.get_existing_observation_keys(
            record_type="vital_sign",
            canonical_names=canonical_names,
        )

        for vital in vitals:
            date_key = vital.timestamp.isoformat() if vital.timestamp else None
            if (vital.type, date_key) in existing_keys:
                result.records_skipped += 1
                continue
            self._db.insert_observation(vital, user_id=user_id)
            existing_keys.add((vital.type, date_key))  # Dedup within batch too
            result.records_imported += 1
            result.types_found[vital.type] = result.types_found.get(vital.type, 0) + 1

        # Import workouts with dedup
        existing_wo_keys = self._db.get_existing_workout_keys(user_id=user_id)
        for wo in workouts:
            date_key = wo.start_time.isoformat() if wo.start_time else None
            if (wo.sport_type, date_key) in existing_wo_keys:
                result.workouts_skipped += 1
                continue
            self._db.insert_workout(wo, user_id=user_id)
            existing_wo_keys.add((wo.sport_type, date_key))
            result.workouts_imported += 1

        # Extract clinical records (relaxed mode only)
        if privacy_mode == "relaxed":
            self._parse_clinical_records(xml_bytes, user_id, result)

        return result

    def _parse_xml_streaming(
        self, xml_bytes: bytes,
    ) -> tuple[list[VitalSign], list[Workout]]:
        """Parse export.xml using iterparse for memory efficiency.

        Returns (vitals, workouts). Does NOT extract GPS route data
        (PII) or sourceName/device attributes from workouts.
        """
        vitals: list[VitalSign] = []
        workouts: list[Workout] = []
        context = ET.iterparse(io.BytesIO(xml_bytes), events=("end",))

        for _, elem in context:
            if elem.tag == "Record":
                record_type = elem.get("type", "")
                if record_type in SUPPORTED_TYPES:
                    value = elem.get("value", "")
                    unit = elem.get("unit", "")
                    start_date = elem.get("startDate", "")

                    if value and start_date:
                        ts = self._parse_date(start_date)
                        vital = VitalSign(
                            id=uuid.uuid4().hex,
                            type=SUPPORTED_TYPES[record_type],
                            value=value,
                            unit=unit,
                            timestamp=ts,
                            source="apple_health",
                        )
                        vitals.append(vital)

                elif record_type in CATEGORY_TYPES:
                    cat_info = CATEGORY_TYPES[record_type]
                    start_date = elem.get("startDate", "")
                    if start_date:
                        ts = self._parse_date(start_date)
                        value_map = cat_info.get("value_map")
                        use_duration = cat_info.get("use_duration", False)

                        if use_duration:
                            end_date = elem.get("endDate", "")
                            ts_end = self._parse_date(end_date) if end_date else None
                            if ts and ts_end:
                                dur_min = (ts_end - ts).total_seconds() / 60.0
                                val = str(round(dur_min, 1))
                            else:
                                val = "0"
                            unit = "min"
                        elif value_map:
                            raw_val = elem.get("value", "")
                            val = value_map.get(raw_val, raw_val)
                            unit = ""
                        else:
                            val = "1"  # presence event
                            unit = ""

                        vital = VitalSign(
                            id=uuid.uuid4().hex,
                            type=cat_info["canonical"],
                            value=val,
                            unit=unit,
                            timestamp=ts,
                            source="apple_health",
                        )
                        vitals.append(vital)

                elem.clear()  # Free memory

            elif elem.tag == "Workout":
                wo = self._parse_workout(elem)
                if wo:
                    workouts.append(wo)
                elem.clear()

        return vitals, workouts

    def _parse_workout(self, elem: ET.Element) -> Workout | None:
        """Parse a <Workout> element into a Workout model.

        Extracts: activity type, duration, distance, calories, HR stats.
        Does NOT extract: GPS routes, sourceName, device (PII).
        """
        activity_type = elem.get("workoutActivityType", "")
        sport = WORKOUT_TYPES.get(activity_type)
        if not sport:
            return None

        start_date = elem.get("startDate", "")
        end_date = elem.get("endDate", "")
        if not start_date:
            return None

        start_time = self._parse_date(start_date)
        end_time = self._parse_date(end_date) if end_date else None

        # Duration
        duration = self._safe_float(elem.get("duration"))

        # Distance (convert miles to km if needed)
        distance = self._safe_float(elem.get("totalDistance"))
        dist_unit = elem.get("totalDistanceUnit", "km")
        if distance and dist_unit == "mi":
            distance = distance * 1.60934

        # Calories
        calories = self._safe_float(elem.get("totalEnergyBurned"))

        # Heart rate from WorkoutStatistics child elements
        avg_hr = None
        max_hr = None
        min_hr = None
        for stat in elem.iter("WorkoutStatistics"):
            stat_type = stat.get("type", "")
            if stat_type == "HKQuantityTypeIdentifierHeartRate":
                avg_hr = self._safe_float(stat.get("average"))
                max_hr = self._safe_float(stat.get("maximum"))
                min_hr = self._safe_float(stat.get("minimum"))
                break

        return Workout(
            id=uuid.uuid4().hex,
            sport_type=sport,
            start_time=start_time,
            end_time=end_time,
            duration_minutes=duration,
            distance_km=distance,
            calories_burned=calories,
            avg_heart_rate=avg_hr,
            max_heart_rate=max_hr,
            min_heart_rate=min_hr,
            source="apple_health",
        )

    @staticmethod
    def _safe_float(val: str | None) -> float | None:
        """Convert string to float, returning None on failure."""
        if not val:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_str: str) -> datetime | None:
        """Parse Apple Health date format."""
        # Format: 2024-01-15 08:30:00 -0500
        for fmt in (
            "%Y-%m-%d %H:%M:%S %z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S%z",
        ):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None

    # ── Clinical record extraction (relaxed mode) ─────────────────

    def _parse_clinical_records(
        self,
        xml_bytes: bytes,
        user_id: int,
        result: AppleHealthImportResult,
    ) -> None:
        """Parse ClinicalRecord elements from export.xml.

        Apple Health clinical records contain FHIR R4 JSON in the
        resourceFilePath attribute or inline fhirResource element.
        PII (provider names, facilities, lot numbers, patient IDs)
        is stripped at extraction time — never stored.
        """
        context = ET.iterparse(io.BytesIO(xml_bytes), events=("end",))
        for _, elem in context:
            if elem.tag == "ClinicalRecord":
                try:
                    self._process_clinical_record(elem, user_id, result)
                except Exception as e:
                    logger.debug("Clinical record parse error: %s", e)
                elem.clear()

    def _process_clinical_record(
        self,
        elem: ET.Element,
        user_id: int,
        result: AppleHealthImportResult,
    ) -> None:
        """Process a single <ClinicalRecord> element."""
        record_type = elem.get("type", "")

        # Try inline FHIR resource (fhirResource child element)
        fhir_json = None
        fhir_elem = elem.find("fhirResource")
        if fhir_elem is not None and fhir_elem.text:
            fhir_json = fhir_elem.text.strip()

        # Also check direct JSON in the element text
        if not fhir_json and elem.text and elem.text.strip().startswith("{"):
            fhir_json = elem.text.strip()

        if not fhir_json or not fhir_json.startswith("{"):
            # No inline FHIR JSON — extract from attributes only
            self._extract_from_attributes(elem, record_type, user_id, result)
            return

        try:
            resource = json.loads(fhir_json)
        except (json.JSONDecodeError, ValueError):
            self._extract_from_attributes(elem, record_type, user_id, result)
            return

        rtype = resource.get("resourceType", "")
        extracted = False

        if rtype == "AllergyIntolerance":
            extracted = self._extract_allergy(resource, user_id)
            if extracted:
                result.clinical_breakdown["allergies"] = (
                    result.clinical_breakdown.get("allergies", 0) + 1
                )
        elif rtype == "Condition":
            extracted = self._extract_condition(resource, user_id)
            if extracted:
                result.clinical_breakdown["conditions"] = (
                    result.clinical_breakdown.get("conditions", 0) + 1
                )
        elif rtype in ("MedicationStatement", "MedicationRequest"):
            extracted = self._extract_medication(resource, user_id)
            if extracted:
                result.clinical_breakdown["medications"] = (
                    result.clinical_breakdown.get("medications", 0) + 1
                )
        elif rtype == "Immunization":
            extracted = self._extract_immunization(resource, user_id)
            if extracted:
                result.clinical_breakdown["immunizations"] = (
                    result.clinical_breakdown.get("immunizations", 0) + 1
                )
        elif rtype == "Observation":
            extracted = self._extract_lab(resource, user_id=user_id)
            if extracted:
                result.clinical_breakdown["labs"] = (
                    result.clinical_breakdown.get("labs", 0) + 1
                )
        elif rtype == "Procedure":
            extracted = self._extract_procedure(resource, user_id)
            if extracted:
                result.clinical_breakdown["procedures"] = (
                    result.clinical_breakdown.get("procedures", 0) + 1
                )

        if extracted:
            result.clinical_records += 1

    def _extract_from_attributes(
        self,
        elem: ET.Element,
        record_type: str,
        user_id: int,
        result: AppleHealthImportResult,
    ) -> None:
        """Fallback: extract clinical data from XML attributes.

        Some Apple Health exports store clinical records as simple
        attributes (type, identifier, displayName) without inline FHIR.
        """
        display = elem.get("displayName", "")
        if not display:
            return

        category = _CLINICAL_TYPE_MAP.get(record_type)
        if not category:
            return

        if category == "lab":
            # Can't create a proper LabResult from just a display name
            return

        prefix_map = {
            "allergy": "Known allergy",
            "condition": "Known condition",
            "medication": "Medication",
            "immunization": "Immunization",
            "procedure": "Procedure",
        }
        fact = f"{prefix_map[category]}: {display}"
        ltm_category = "condition" if category in ("allergy", "condition") else (
            "medication" if category in ("medication", "immunization") else "procedure"
        )

        self._db.insert_ltm(
            user_id=user_id,
            category=ltm_category,
            fact=fact,
            source="apple_health_clinical",
        )
        breakdown_key = f"{category}s" if category != "allergy" else "allergies"
        result.clinical_breakdown[breakdown_key] = (
            result.clinical_breakdown.get(breakdown_key, 0) + 1
        )
        result.clinical_records += 1

    def _extract_allergy(self, resource: dict, user_id: int) -> bool:
        """Extract allergy from FHIR AllergyIntolerance. PII stripped."""
        code_display = self._fhir_display(resource)
        if not code_display:
            return False

        criticality = resource.get("criticality", "")
        reactions = []
        for r in resource.get("reaction", []):
            for m in r.get("manifestation", []):
                text = m.get("text", "")
                m_coding = m.get("coding", [])
                if text:
                    reactions.append(text)
                elif m_coding:
                    reactions.append(m_coding[0].get("display", ""))

        fact = f"Known allergy: {code_display}"
        if criticality:
            fact += f" (criticality: {criticality})"
        if reactions:
            fact += f" — reactions: {', '.join(reactions)}"

        self._db.insert_ltm(
            user_id=user_id,
            category="condition",
            fact=fact,
            source="apple_health_clinical",
        )
        return True

    def _extract_condition(self, resource: dict, user_id: int) -> bool:
        """Extract condition from FHIR Condition. PII stripped."""
        code_display = self._fhir_display(resource)
        if not code_display:
            return False

        clinical_status = ""
        cs = resource.get("clinicalStatus", {})
        cs_coding = cs.get("coding", [])
        if cs_coding:
            clinical_status = cs_coding[0].get("code", "")

        onset = resource.get("onsetDateTime", "")
        fact = f"Known condition: {code_display}"
        if clinical_status and clinical_status != "active":
            fact += f" (status: {clinical_status})"
        if onset:
            fact += f" (onset: {onset[:10]})"

        self._db.insert_ltm(
            user_id=user_id,
            category="condition",
            fact=fact,
            source="apple_health_clinical",
        )
        return True

    def _extract_medication(self, resource: dict, user_id: int) -> bool:
        """Extract medication from FHIR MedicationStatement. PII stripped.

        Strips: prescriber, pharmacy, dispenser (PII fields).
        Keeps: drug name, dose, frequency, status.
        """
        coding = resource.get("medicationCodeableConcept", {}).get("coding", [{}])
        name = coding[0].get("display", "") if coding else ""
        if not name:
            name = resource.get("medicationCodeableConcept", {}).get("text", "")
        if not name:
            return False

        # Dosage info
        dosage_parts = []
        for d in resource.get("dosage", []):
            dose_qty = d.get("doseAndRate", [{}])
            if dose_qty:
                dr = dose_qty[0].get("doseQuantity", {})
                val = dr.get("value")
                unit = dr.get("unit", "")
                if val:
                    dosage_parts.append(f"{val}{unit}")
            timing = d.get("timing", {})
            repeat = timing.get("repeat", {})
            freq = repeat.get("frequency")
            period = repeat.get("period")
            period_unit = repeat.get("periodUnit", "")
            if freq and period:
                dosage_parts.append(f"{freq}x per {period} {period_unit}")
            text = d.get("text", "")
            if text and not dosage_parts:
                dosage_parts.append(text)

        status = resource.get("status", "")
        fact = f"Medication: {name}"
        if dosage_parts:
            fact += f" {' '.join(dosage_parts)}"
        if status:
            fact += f" ({status})"

        self._db.insert_ltm(
            user_id=user_id,
            category="medication",
            fact=fact,
            source="apple_health_clinical",
        )
        return True

    def _extract_immunization(self, resource: dict, user_id: int) -> bool:
        """Extract immunization from FHIR Immunization. PII stripped.

        Strips: lot number, clinic/facility name (PII).
        Keeps: vaccine name, date, route.
        """
        vaccine = resource.get("vaccineCode", {})
        coding = vaccine.get("coding", [])
        name = coding[0].get("display", "") if coding else ""
        if not name:
            name = vaccine.get("text", "")
        if not name:
            return False

        occ = resource.get("occurrenceDateTime", "")
        fact = f"Immunization: {name}"
        if occ:
            fact += f" (date: {occ[:10]})"

        self._db.insert_ltm(
            user_id=user_id,
            category="medication",
            fact=fact,
            source="apple_health_clinical",
        )
        return True

    def _extract_lab(self, resource: dict, user_id: int = 0) -> bool:
        """Extract lab result from FHIR Observation. PII stripped.

        Strips: performer (lab name), provider.
        Keeps: test name, value, unit, reference range, flag.
        """
        coding = resource.get("code", {}).get("coding", [{}])
        test_name = coding[0].get("display", "") if coding else ""
        if not test_name:
            test_name = resource.get("code", {}).get("text", "")
        value_qty = resource.get("valueQuantity", {})
        value = value_qty.get("value")
        unit = value_qty.get("unit", "")

        if not test_name or value is None:
            return False

        # Reference range
        ref_low = None
        ref_high = None
        ref_text = ""
        ref_ranges = resource.get("referenceRange", [])
        if ref_ranges:
            rr = ref_ranges[0]
            low = rr.get("low", {})
            high = rr.get("high", {})
            if low.get("value") is not None:
                ref_low = float(low["value"])
            if high.get("value") is not None:
                ref_high = float(high["value"])
            ref_text = rr.get("text", "")

        # Interpretation flag
        flag = ""
        interp = resource.get("interpretation", [])
        if interp:
            interp_coding = interp[0].get("coding", [{}])
            flag = interp_coding[0].get("code", "") if interp_coding else ""

        # Effective date
        date_collected = None
        eff = resource.get("effectiveDateTime", "")
        if eff:
            try:
                date_collected = datetime.fromisoformat(eff[:10]).date()
            except ValueError:
                pass

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name=test_name,
            canonical_name=normalize_test_name(test_name),
            value=value,
            unit=unit,
            reference_low=ref_low,
            reference_high=ref_high,
            reference_text=ref_text,
            flag=flag,
            date_collected=date_collected,
        )
        self._db.insert_observation(lab, user_id=user_id)
        return True

    def _extract_procedure(self, resource: dict, user_id: int) -> bool:
        """Extract procedure from FHIR Procedure. PII stripped.

        Strips: performer (surgeon), location (facility).
        Keeps: procedure name, code, status, date.
        """
        code_display = self._fhir_display(resource)
        if not code_display:
            return False

        status = resource.get("status", "")
        performed = resource.get("performedDateTime", "")
        if not performed:
            period = resource.get("performedPeriod", {})
            performed = period.get("start", "")

        fact = f"Procedure: {code_display}"
        if status and status != "completed":
            fact += f" ({status})"
        if performed:
            fact += f" (date: {performed[:10]})"

        self._db.insert_ltm(
            user_id=user_id,
            category="procedure",
            fact=fact,
            source="apple_health_clinical",
        )
        return True

    @staticmethod
    def _fhir_display(resource: dict) -> str:
        """Extract display name from resource.code, stripping PII."""
        code_obj = resource.get("code", {})
        coding = code_obj.get("coding", [])
        if coding:
            display = coding[0].get("display", "")
            if display:
                return display
        return code_obj.get("text", "")
