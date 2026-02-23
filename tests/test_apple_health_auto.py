"""Tests for Apple Health Auto Export JSON importer."""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

from healthbot.importers.apple_health_auto import (
    METRIC_MAP,
    AppleHealthAutoImporter,
)


def _make_db():
    """Create a mock HealthDB."""
    db = MagicMock()
    db.insert_observation = MagicMock()
    db.get_existing_observation_keys = MagicMock(return_value=set())
    return db


def _make_json(metrics=None, sleep=None):
    """Build a Health Auto Export JSON payload."""
    data = {"data": {"metrics": metrics or []}}
    if sleep:
        data["data"]["sleepAnalysis"] = sleep
    return json.dumps(data).encode()


class TestMetricMapping:
    def test_known_metrics_mapped(self):
        assert METRIC_MAP["Heart Rate"] == "heart_rate"
        assert METRIC_MAP["Steps"] == "steps"
        assert METRIC_MAP["Blood Glucose"] == "blood_glucose"
        assert METRIC_MAP["VO2 Max"] == "vo2_max"

    def test_snake_case_variants(self):
        assert METRIC_MAP["heart_rate"] == "heart_rate"
        assert METRIC_MAP["step_count"] == "steps"
        assert METRIC_MAP["blood_oxygen"] == "spo2"


class TestImportMetrics:
    def test_import_steps(self):
        db = _make_db()
        payload = _make_json(metrics=[{
            "name": "Steps",
            "units": "count",
            "data": [
                {"qty": 8500, "date": "2025-01-20 14:30:00 +0000"},
                {"qty": 3200, "date": "2025-01-20 10:00:00 +0000"},
            ],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=123)

        assert result.imported == 2
        assert result.types_found["steps"] == 2
        assert db.insert_observation.call_count == 2

        vital = db.insert_observation.call_args_list[0][0][0]
        assert vital.type == "steps"
        assert vital.value == "8500.0"
        assert vital.source == "apple_health"

    def test_import_heart_rate_with_avg(self):
        db = _make_db()
        payload = _make_json(metrics=[{
            "name": "Heart Rate",
            "units": "count/min",
            "data": [
                {"Avg": 72, "Min": 65, "Max": 80,
                 "date": "2025-01-20 08:00:00 +0000"},
            ],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 1
        vital = db.insert_observation.call_args_list[0][0][0]
        assert vital.type == "heart_rate"
        assert vital.value == "72.0"
        assert vital.unit == "bpm"

    def test_import_blood_pressure(self):
        db = _make_db()
        payload = _make_json(metrics=[
            {
                "name": "Blood Pressure - Systolic",
                "units": "mmHg",
                "data": [{"qty": 120, "date": "2025-01-20 08:00:00 +0000"}],
            },
            {
                "name": "Blood Pressure - Diastolic",
                "units": "mmHg",
                "data": [{"qty": 80, "date": "2025-01-20 08:00:00 +0000"}],
            },
        ])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 2
        assert result.types_found["bp_systolic"] == 1
        assert result.types_found["bp_diastolic"] == 1

    def test_skip_unknown_metric(self):
        db = _make_db()
        payload = _make_json(metrics=[{
            "name": "Mindful Minutes",
            "units": "min",
            "data": [{"qty": 15, "date": "2025-01-20 08:00:00 +0000"}],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 0
        assert db.insert_observation.call_count == 0

    def test_skip_bad_date(self):
        db = _make_db()
        payload = _make_json(metrics=[{
            "name": "Steps",
            "units": "count",
            "data": [{"qty": 100, "date": "not-a-date"}],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 0
        assert result.skipped == 1

    def test_empty_payload(self):
        db = _make_db()
        payload = json.dumps({"data": {"metrics": []}}).encode()
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 0
        assert result.skipped == 0

    def test_last_sync_filter(self):
        db = _make_db()
        payload = _make_json(metrics=[{
            "name": "Steps",
            "units": "count",
            "data": [
                {"qty": 100, "date": "2025-01-18 10:00:00 +0000"},
                {"qty": 200, "date": "2025-01-20 10:00:00 +0000"},
            ],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(
            payload, db, user_id=0,
            last_sync_iso="2025-01-19T00:00:00+00:00",
        )

        assert result.imported == 1
        assert result.skipped == 1
        vital = db.insert_observation.call_args_list[0][0][0]
        assert vital.value == "200.0"


class TestImportSleep:
    def test_import_sleep_analysis(self):
        db = _make_db()
        payload = json.dumps({"data": {
            "metrics": [],
            "sleepAnalysis": [{
                "date": "2025-01-20 06:00:00 +0000",
                "asleep": 420,
                "core": 200,
                "deep": 100,
                "rem": 120,
            }],
        }}).encode()
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 1
        assert result.types_found["sleep_duration"] == 1
        vital = db.insert_observation.call_args_list[0][0][0]
        assert vital.type == "sleep_duration"
        assert vital.value == "420"
        assert vital.unit == "min"


class TestFormatVariants:
    def test_format_without_data_wrapper(self):
        """Handle JSON without the 'data' wrapper."""
        db = _make_db()
        payload = json.dumps({
            "metrics": [{
                "name": "Steps",
                "units": "count",
                "data": [{"qty": 500, "date": "2025-01-20 08:00:00 +0000"}],
            }],
        }).encode()
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)
        assert result.imported == 1

    def test_iso_date_format(self):
        db = _make_db()
        payload = _make_json(metrics=[{
            "name": "Steps",
            "units": "count",
            "data": [{"qty": 300, "date": "2025-01-20T08:00:00Z"}],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)
        assert result.imported == 1

    def test_snake_case_metric_name(self):
        db = _make_db()
        payload = _make_json(metrics=[{
            "name": "resting_heart_rate",
            "units": "count/min",
            "data": [{"qty": 58, "date": "2025-01-20 08:00:00 +0000"}],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)
        assert result.imported == 1
        assert result.types_found["resting_heart_rate"] == 1


class TestDeduplication:
    def test_db_dedup_skips_existing(self):
        """Records already in DB should be skipped."""
        db = _make_db()
        db.get_existing_observation_keys = MagicMock(return_value={
            ("steps", "2025-01-20T14:30:00+00:00"),
        })
        payload = _make_json(metrics=[{
            "name": "Steps",
            "units": "count",
            "data": [
                {"qty": 8500, "date": "2025-01-20 14:30:00 +0000"},
                {"qty": 3200, "date": "2025-01-20 10:00:00 +0000"},
            ],
        }])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 1
        assert result.skipped == 1
        assert db.insert_observation.call_count == 1

    def test_reimport_same_data_zero_new(self):
        """Re-importing identical data should import nothing."""
        db = _make_db()
        db.get_existing_observation_keys = MagicMock(return_value={
            ("steps", "2025-01-20T08:00:00+00:00"),
            ("heart_rate", "2025-01-20T08:00:00+00:00"),
        })
        payload = _make_json(metrics=[
            {
                "name": "Steps",
                "units": "count",
                "data": [{"qty": 500, "date": "2025-01-20 08:00:00 +0000"}],
            },
            {
                "name": "Heart Rate",
                "units": "count/min",
                "data": [{"qty": 72, "date": "2025-01-20 08:00:00 +0000"}],
            },
        ])
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 0
        assert result.skipped == 2
        assert db.insert_observation.call_count == 0

    def test_sleep_dedup(self):
        """Sleep records already in DB should be skipped."""
        db = _make_db()
        db.get_existing_observation_keys = MagicMock(return_value={
            ("sleep_duration", "2025-01-20T06:00:00+00:00"),
        })
        payload = json.dumps({"data": {
            "metrics": [],
            "sleepAnalysis": [{
                "date": "2025-01-20 06:00:00 +0000",
                "asleep": 420,
            }],
        }}).encode()
        importer = AppleHealthAutoImporter()
        result = importer.import_from_json(payload, db, user_id=0)

        assert result.imported == 0
        assert result.skipped == 1


class TestAppleHealthZipDetection:
    """ZIP file detection for Apple Health exports."""

    def test_valid_apple_health_zip(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "export.zip"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("apple_health_export/export.xml", "<HealthData></HealthData>")
        zip_path.write_bytes(buf.getvalue())
        with zipfile.ZipFile(str(zip_path)) as zf:
            has_export = any(name.endswith("export.xml") for name in zf.namelist())
        assert has_export is True

    def test_non_apple_health_zip(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "random.zip"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("document.txt", "Hello world")
        zip_path.write_bytes(buf.getvalue())
        with zipfile.ZipFile(str(zip_path)) as zf:
            has_export = any(name.endswith("export.xml") for name in zf.namelist())
        assert has_export is False
