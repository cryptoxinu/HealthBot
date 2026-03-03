"""FHIR R4 Bundle export for health record interoperability.

Exports lab results, medications, vitals, symptom observations,
wearable data, and concerns as a FHIR R4 JSON Bundle.
Uses LOINC codes where available from lab_normalizer.
All data processed in memory -- never written to disk unencrypted.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

from healthbot.data.db import HealthDB
from healthbot.normalize.lab_normalizer import get_loinc
from healthbot.security.phi_firewall import PhiFirewall


class FhirExporter:
    """Export health data as FHIR R4 JSON Bundle.

    All text fields are validated through PhiFirewall.redact() before inclusion
    in the FHIR bundle to prevent PII leakage from Tier 1 data.
    """

    def __init__(self, db: HealthDB, phi_firewall: PhiFirewall | None = None) -> None:
        self._db = db
        self._fw = phi_firewall or PhiFirewall()

    def _safe(self, value: str) -> str:
        """Redact any PHI from a string value before export."""
        if not value:
            return value
        return self._fw.redact(str(value))

    def export_bundle(
        self,
        include_labs: bool = True,
        include_meds: bool = True,
        include_vitals: bool = True,
        include_symptoms: bool = True,
        include_wearables: bool = True,
        include_concerns: bool = True,
        start_date: str | None = None,
        end_date: str | None = None,
        user_id: int | None = None,
    ) -> dict:
        """Build a FHIR R4 Bundle containing requested resources."""
        entries: list[dict] = []

        if include_labs:
            labs = self._db.query_observations(
                record_type="lab_result",
                start_date=start_date,
                end_date=end_date,
                user_id=user_id,
            )
            for lab in labs:
                resource = self._lab_to_observation(lab)
                if resource:
                    entries.append({"resource": resource})

        if include_meds:
            meds = self._db.get_active_medications(user_id=user_id)
            for med in meds:
                resource = self._med_to_medication_statement(med)
                if resource:
                    entries.append({"resource": resource})

        if include_vitals:
            vitals = self._db.query_observations(
                record_type="vital_sign",
                start_date=start_date,
                end_date=end_date,
                user_id=user_id,
            )
            for vital in vitals:
                resource = self._vital_to_observation(vital)
                if resource:
                    entries.append({"resource": resource})

        if include_symptoms:
            events = self._db.query_observations(
                record_type="user_event",
                start_date=start_date,
                end_date=end_date,
                user_id=user_id,
            )
            for event in events:
                resource = self._event_to_observation(event)
                if resource:
                    entries.append({"resource": resource})

        if include_wearables:
            wearable_rows = self._db.query_wearable_daily(
                start_date=start_date,
                end_date=end_date,
                user_id=user_id,
            )
            for wd in wearable_rows:
                resource = self._wearable_to_observation(wd)
                if resource:
                    entries.append({"resource": resource})

        if include_concerns:
            concerns = self._query_concerns()
            for concern in concerns:
                resource = self._concern_to_condition(concern)
                if resource:
                    entries.append({"resource": resource})

        return {
            "resourceType": "Bundle",
            "id": uuid.uuid4().hex,
            "type": "collection",
            "timestamp": datetime.now(UTC).isoformat(),
            "entry": entries,
        }

    def export_json(self, **kwargs: Any) -> str:
        """Export as formatted JSON string."""
        bundle = self.export_bundle(**kwargs)
        return json.dumps(bundle, indent=2, default=str)

    def _lab_to_observation(self, lab: dict) -> dict | None:
        """Map a decrypted lab result to FHIR Observation resource."""
        canonical = lab.get("canonical_name", "")
        value = lab.get("value")
        if value is None:
            return None

        obs: dict[str, Any] = {
            "resourceType": "Observation",
            "id": lab.get("_meta", {}).get("obs_id", uuid.uuid4().hex),
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "laboratory",
                    "display": "Laboratory",
                }]
            }],
        }

        # LOINC code if available
        loinc = get_loinc(canonical)
        test_name = self._safe(lab.get("test_name", canonical))
        if loinc:
            obs["code"] = {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": loinc,
                    "display": test_name,
                }],
                "text": test_name,
            }
        else:
            obs["code"] = {"text": test_name}

        # Value
        try:
            numeric_value = float(value)
            obs["valueQuantity"] = {
                "value": numeric_value,
                "unit": lab.get("unit", ""),
                "system": "http://unitsofmeasure.org",
            }
        except (ValueError, TypeError):
            obs["valueString"] = self._safe(str(value))

        # Reference range
        ref_low = lab.get("reference_low")
        ref_high = lab.get("reference_high")
        if ref_low is not None or ref_high is not None:
            ref_range: dict[str, Any] = {}
            if ref_low is not None:
                ref_range["low"] = {
                    "value": float(ref_low),
                    "unit": lab.get("unit", ""),
                }
            if ref_high is not None:
                ref_range["high"] = {
                    "value": float(ref_high),
                    "unit": lab.get("unit", ""),
                }
            obs["referenceRange"] = [ref_range]

        # Effective date
        meta = lab.get("_meta", {})
        date_eff = meta.get("date_effective")
        if date_eff:
            obs["effectiveDateTime"] = date_eff

        return obs

    def _med_to_medication_statement(self, med: dict) -> dict | None:
        """Map a decrypted medication to FHIR MedicationStatement."""
        name = self._safe(med.get("name", ""))
        if not name:
            return None

        status = "active" if med.get("status") == "active" else "stopped"
        stmt: dict[str, Any] = {
            "resourceType": "MedicationStatement",
            "id": uuid.uuid4().hex,
            "status": status,
            "medicationCodeableConcept": {"text": name},
        }

        dose = self._safe(med.get("dose", ""))
        freq = self._safe(med.get("frequency", ""))
        if dose or freq:
            dosage: dict[str, Any] = {}
            if dose:
                dosage["text"] = f"{dose} {self._safe(med.get('unit', ''))}".strip()
            if freq:
                dosage["timing"] = {"code": {"text": freq}}
            stmt["dosage"] = [dosage]

        start = med.get("start_date")
        if start:
            stmt["effectivePeriod"] = {"start": str(start)}
            end = med.get("end_date")
            if end:
                stmt["effectivePeriod"]["end"] = str(end)

        return stmt

    def _vital_to_observation(self, vital: dict) -> dict | None:
        """Map a vital sign to FHIR Observation resource."""
        vital_type = self._safe(vital.get("type", vital.get("canonical_name", "")))
        value = vital.get("value")
        if not vital_type or value is None:
            return None

        obs: dict[str, Any] = {
            "resourceType": "Observation",
            "id": vital.get("_meta", {}).get("obs_id", uuid.uuid4().hex),
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "vital-signs",
                    "display": "Vital Signs",
                }]
            }],
            "code": {"text": vital_type},
        }

        try:
            numeric = float(value)
            obs["valueQuantity"] = {
                "value": numeric,
                "unit": self._safe(vital.get("unit", "")),
            }
        except (ValueError, TypeError):
            obs["valueString"] = self._safe(str(value))

        meta = vital.get("_meta", {})
        date_eff = meta.get("date_effective")
        if date_eff:
            obs["effectiveDateTime"] = date_eff

        return obs

    def _event_to_observation(self, event: dict) -> dict | None:
        """Map a user-logged symptom event to FHIR Observation.

        Only ``cleaned_text`` is used — ``raw_text`` is never included in FHIR
        output because it may contain the original user input with PII.
        """
        symptom = self._safe(event.get("symptom_category", ""))
        # Never fall back to raw_text — it contains original user input and may
        # include PII.  Only use cleaned_text which has already been sanitized.
        text = self._safe(event.get("cleaned_text", ""))
        if not symptom and not text:
            return None

        obs: dict[str, Any] = {
            "resourceType": "Observation",
            "id": event.get("_meta", {}).get("obs_id", uuid.uuid4().hex),
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "survey",
                    "display": "Survey",
                }]
            }],
            "code": {"text": symptom or "user_event"},
            "valueString": text,
        }

        severity = self._safe(event.get("severity", ""))
        if severity:
            obs["interpretation"] = [{"text": severity}]

        meta = event.get("_meta", {})
        date_eff = meta.get("date_effective") or event.get("date_effective")
        if date_eff:
            obs["effectiveDateTime"] = str(date_eff)

        return obs

    def _wearable_to_observation(self, wd: dict) -> dict | None:
        """Map a wearable daily record to FHIR Observation."""
        date_str = wd.get("_date", "")
        if not date_str:
            return None

        # Collect key wearable metrics into a single observation
        metrics: dict[str, Any] = {}
        for key in ("hrv", "rhr", "recovery_score", "sleep_score",
                     "strain", "sleep_duration_min", "spo2", "skin_temp"):
            val = wd.get(key)
            if val is not None:
                metrics[key] = val

        if not metrics:
            return None

        obs: dict[str, Any] = {
            "resourceType": "Observation",
            "id": uuid.uuid4().hex,
            "status": "final",
            "category": [{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "activity",
                    "display": "Activity",
                }]
            }],
            "code": {"text": "wearable_daily_summary"},
            "effectiveDateTime": date_str,
        }

        # Store individual metrics as FHIR components
        components: list[dict] = []
        for key, val in metrics.items():
            component: dict[str, Any] = {"code": {"text": key}}
            try:
                component["valueQuantity"] = {"value": float(val)}
            except (ValueError, TypeError):
                component["valueString"] = str(val)
            components.append(component)
        obs["component"] = components

        return obs

    def _query_concerns(self) -> list[dict]:
        """Query active concerns from the DB."""
        try:
            rows = self._db.conn.execute(
                "SELECT * FROM concerns WHERE status = 'active'"
            ).fetchall()
        except Exception:
            return []
        results = []
        for row in rows:
            aad = f"concerns.encrypted_data.{row['concern_id']}"
            try:
                data = self._db._decrypt(row["encrypted_data"], aad)
                data["_concern_id"] = row["concern_id"]
                data["_severity"] = row["severity"]
                data["_status"] = row["status"]
                data["_created_at"] = row["created_at"]
                results.append(data)
            except Exception:
                continue
        return results

    def _concern_to_condition(self, concern: dict) -> dict | None:
        """Map a health concern to FHIR Condition resource."""
        title = self._safe(concern.get("title", ""))
        if not title:
            return None

        severity_map = {
            "watch": "mild",
            "active": "moderate",
            "urgent": "severe",
        }
        fhir_severity = severity_map.get(
            concern.get("_severity", "watch"), "mild"
        )

        condition: dict[str, Any] = {
            "resourceType": "Condition",
            "id": concern.get("_concern_id", uuid.uuid4().hex),
            "clinicalStatus": {
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": "active",
                }]
            },
            "severity": {
                "coding": [{
                    "system": "http://snomed.info/sct",
                    "display": fhir_severity,
                }]
            },
            "code": {"text": title},
        }

        onset = concern.get("_created_at")
        if onset:
            condition["onsetDateTime"] = onset

        notes = self._safe(concern.get("notes", ""))
        if notes:
            condition["note"] = [{"text": notes}]

        return condition
