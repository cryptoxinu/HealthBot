"""Apple Health auto-import from Health Auto Export JSON files.

Parses JSON files produced by the 'Health Auto Export' iOS app
(iCloud Drive export). Supports metrics, medications, and symptoms.
Deduplicates by (canonical_name, timestamp) to prevent double-imports.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime

from healthbot.data.db import HealthDB
from healthbot.data.models import VitalSign

logger = logging.getLogger("healthbot")

# Health Auto Export metric name → HealthBot canonical name
METRIC_MAP: dict[str, str] = {
    "active_energy": "active_calories",
    "apple_exercise_time": "exercise_minutes",
    "basal_energy_burned": "basal_calories",
    "blood_glucose": "blood_glucose",
    "blood_oxygen": "spo2",
    "blood_pressure_diastolic": "bp_diastolic",
    "blood_pressure_systolic": "bp_systolic",
    "body_fat_percentage": "body_fat_pct",
    "body_mass_index": "bmi",
    "body_temperature": "body_temperature",
    "dietary_water": "water_intake",
    "flights_climbed": "flights_climbed",
    "heart_rate": "heart_rate",
    "heart_rate_variability": "hrv",
    "height": "height",
    "resting_heart_rate": "resting_heart_rate",
    "respiratory_rate": "respiratory_rate",
    "sleep_analysis": "sleep",
    "stand_time": "stand_minutes",
    "step_count": "steps",
    "vo2_max": "vo2_max",
    "walking_heart_rate_average": "walking_hr_avg",
    "walking_running_distance": "distance",
    "weight_body_mass": "weight",
    # Display name variants (the app may use either)
    "Active Energy": "active_calories",
    "Apple Exercise Time": "exercise_minutes",
    "Basal Energy Burned": "basal_calories",
    "Blood Glucose": "blood_glucose",
    "Blood Oxygen": "spo2",
    "Blood Pressure - Diastolic": "bp_diastolic",
    "Blood Pressure - Systolic": "bp_systolic",
    "Body Fat Percentage": "body_fat_pct",
    "Body Mass Index": "bmi",
    "Body Temperature": "body_temperature",
    "Flights Climbed": "flights_climbed",
    "Heart Rate": "heart_rate",
    "Heart Rate Variability": "hrv",
    "Height": "height",
    "Resting Heart Rate": "resting_heart_rate",
    "Respiratory Rate": "respiratory_rate",
    "Sleep Analysis": "sleep",
    "Stand Time": "stand_minutes",
    "Step Count": "steps",
    "Steps": "steps",
    "VO2 Max": "vo2_max",
    "Walking Heart Rate Average": "walking_hr_avg",
    "Walking + Running Distance": "distance",
    "Weight": "weight",
    "Oxygen Saturation": "spo2",
}

# Units normalization
UNIT_MAP: dict[str, str] = {
    "count": "",
    "count/min": "bpm",
    "ms": "ms",
    "kcal": "kcal",
    "mi": "mi",
    "km": "km",
    "lb": "lb",
    "kg": "kg",
    "°F": "°F",
    "°C": "°C",
    "mg/dL": "mg/dL",
    "%": "%",
    "mL/(kg·min)": "mL/kg/min",
    "min": "min",
    "hr": "hr",
}

# Date formats used by Health Auto Export
_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S %z",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%d %H:%M:%S %Z",
]


@dataclass
class AppleHealthImportResult:
    imported: int = 0
    skipped: int = 0
    types_found: dict[str, int] = field(default_factory=dict)


class AppleHealthAutoImporter:
    """Parse Health Auto Export JSON files and import into HealthBot DB."""

    def import_from_json(
        self,
        json_bytes: bytes,
        db: HealthDB,
        user_id: int = 0,
        last_sync_iso: str | None = None,
    ) -> AppleHealthImportResult:
        """Parse a Health Auto Export JSON file and import new records.

        Args:
            json_bytes: Raw JSON file content.
            db: Encrypted health database.
            user_id: Telegram user ID.
            last_sync_iso: ISO timestamp of last sync (skip older records).

        Returns:
            Import result with counts.
        """
        result = AppleHealthImportResult()
        data = json.loads(json_bytes)

        # Handle both top-level formats:
        # Format A: {"data": {"metrics": [...]}}
        # Format B: {"metrics": [...]}
        if "data" in data:
            data = data["data"]

        # Load existing keys for dedup — skip records already in DB
        canonical_names = list(set(METRIC_MAP.values())) + ["sleep_duration"]
        existing_keys = db.get_existing_observation_keys(
            record_type="vital_sign",
            canonical_names=canonical_names,
        )
        self._existing_keys = existing_keys

        # Parse metrics
        for metric in data.get("metrics", []):
            self._import_metric(metric, db, user_id, last_sync_iso, result)

        # Parse sleep (special structure)
        for sleep in data.get("sleepAnalysis", data.get("sleep", [])):
            self._import_sleep(sleep, db, user_id, last_sync_iso, result)

        logger.info(
            "Apple Health import: %d imported, %d skipped. Types: %s",
            result.imported,
            result.skipped,
            ", ".join(f"{k}: {v}" for k, v in sorted(result.types_found.items())),
        )
        return result

    def _import_metric(
        self,
        metric: dict,
        db: HealthDB,
        user_id: int,
        last_sync_iso: str | None,
        result: AppleHealthImportResult,
    ) -> None:
        """Import a single metric block (name + data array)."""
        name = metric.get("name", "")
        units = metric.get("units", "")
        canonical = METRIC_MAP.get(name)
        if canonical is None:
            # Try lowercase/underscore variant
            normalized_name = name.lower().replace(" ", "_").replace("-", "_")
            canonical = METRIC_MAP.get(normalized_name)
        if canonical is None:
            logger.debug("Apple Health: skipping unknown metric: %s", name)
            return

        unit = UNIT_MAP.get(units, units)

        for point in metric.get("data", []):
            ts = self._parse_timestamp(point.get("date", ""))
            if ts is None:
                result.skipped += 1
                continue

            # Skip records older than last sync
            if last_sync_iso and ts.isoformat() <= last_sync_iso:
                result.skipped += 1
                continue

            # Extract value — format varies by metric type
            value = self._extract_value(point, canonical)
            if value is None:
                result.skipped += 1
                continue

            # Dedup: skip if same metric + timestamp already in DB
            date_key = ts.isoformat() if ts else None
            if (canonical, date_key) in self._existing_keys:
                result.skipped += 1
                continue

            vital = VitalSign(
                id=uuid.uuid4().hex,
                type=canonical,
                value=str(value),
                unit=unit,
                timestamp=ts,
                source="apple_health",
            )
            db.insert_observation(vital, user_id=user_id)
            self._existing_keys.add((canonical, date_key))
            result.imported += 1
            result.types_found[canonical] = result.types_found.get(canonical, 0) + 1

    def _import_sleep(
        self,
        sleep: dict,
        db: HealthDB,
        user_id: int,
        last_sync_iso: str | None,
        result: AppleHealthImportResult,
    ) -> None:
        """Import a sleep analysis record."""
        ts = self._parse_timestamp(sleep.get("date", sleep.get("sleepStart", "")))
        if ts is None:
            return
        if last_sync_iso and ts.isoformat() <= last_sync_iso:
            result.skipped += 1
            return

        # Dedup: skip if same sleep timestamp already in DB
        date_key = ts.isoformat() if ts else None
        if ("sleep_duration", date_key) in self._existing_keys:
            result.skipped += 1
            return

        # Store total sleep duration in minutes
        total = sleep.get("asleep") or sleep.get("totalSleep")
        if total is None:
            return

        vital = VitalSign(
            id=uuid.uuid4().hex,
            type="sleep_duration",
            value=str(total),
            unit="min",
            timestamp=ts,
            source="apple_health",
        )
        db.insert_observation(vital, user_id=user_id)
        self._existing_keys.add(("sleep_duration", date_key))
        result.imported += 1
        result.types_found["sleep_duration"] = (
            result.types_found.get("sleep_duration", 0) + 1
        )

    @staticmethod
    def _extract_value(point: dict, canonical: str) -> float | None:
        """Extract numeric value from a data point.

        Health Auto Export uses different field names:
        - qty: most metrics (steps, heart rate, etc.)
        - Avg: aggregated metrics (heart rate over interval)
        - systolic/diastolic: blood pressure
        """
        # Direct quantity
        if "qty" in point:
            try:
                return float(point["qty"])
            except (ValueError, TypeError):
                return None

        # Aggregated (Avg/Min/Max)
        if "Avg" in point:
            try:
                return float(point["Avg"])
            except (ValueError, TypeError):
                return None

        # Blood pressure special handling
        if canonical == "bp_systolic" and "systolic" in point:
            try:
                return float(point["systolic"])
            except (ValueError, TypeError):
                return None
        if canonical == "bp_diastolic" and "diastolic" in point:
            try:
                return float(point["diastolic"])
            except (ValueError, TypeError):
                return None

        return None

    @staticmethod
    def _parse_timestamp(date_str: str) -> datetime | None:
        """Parse Health Auto Export date formats."""
        if not date_str:
            return None
        for fmt in _DATE_FORMATS:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return None
