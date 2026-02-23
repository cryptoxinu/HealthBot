"""Fasten Health FHIR import with de-identification.

Reads FHIR R4 NDJSON exports or JSON Bundles from Fasten Health.
Each resource is de-identified (HIPAA Safe Harbor) before mapping
to HealthBot models. PII is stripped BEFORE storage — encrypted
data never contains personally identifiable information.
"""
from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime

from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, Medication, VitalSign
from healthbot.normalize.lab_normalizer import normalize_test_name
from healthbot.security.deidentifier import FhirDeidentifier
from healthbot.security.phi_firewall import PhiFirewall
from healthbot.security.vault import Vault

logger = logging.getLogger("healthbot")


@dataclass
class FastenImportResult:
    """Summary of a Fasten import operation."""

    labs: int = 0
    medications: int = 0
    vitals: int = 0
    conditions: int = 0
    allergies: int = 0
    immunizations: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)
    demographics: dict | None = None


class FastenImporter:
    """Import de-identified FHIR data from Fasten Health exports."""

    def __init__(
        self,
        db: HealthDB,
        vault: Vault,
        phi_firewall: PhiFirewall | None = None,
    ) -> None:
        self._db = db
        self._vault = vault
        self._fw = phi_firewall or PhiFirewall()
        self._deid = FhirDeidentifier(phi_firewall=self._fw)

    def import_ndjson(self, data: bytes, user_id: int = 0) -> FastenImportResult:
        """Import NDJSON (newline-delimited JSON) from Fasten.

        Each line is a single FHIR R4 resource.
        """
        result = FastenImportResult()
        text = data.decode("utf-8", errors="replace")

        # Store de-identified source as encrypted blob
        blob_id = uuid.uuid4().hex

        for line_num, line in enumerate(text.splitlines(), 1):
            line = line.strip()
            if not line:
                continue
            try:
                resource = json.loads(line)
            except json.JSONDecodeError as e:
                result.errors.append(f"Line {line_num}: invalid JSON: {e}")
                result.skipped += 1
                continue

            self._process_resource(resource, user_id, blob_id, result)

        logger.info(
            "Fasten NDJSON import: %d labs, %d meds, %d vitals, "
            "%d conditions, %d allergies, %d immunizations, %d skipped",
            result.labs, result.medications, result.vitals,
            result.conditions, result.allergies, result.immunizations,
            result.skipped,
        )
        return result

    def import_bundle(self, data: bytes, user_id: int = 0) -> FastenImportResult:
        """Import a FHIR R4 Bundle JSON from Fasten."""
        result = FastenImportResult()

        try:
            bundle = json.loads(data)
        except json.JSONDecodeError as e:
            result.errors.append(f"Invalid JSON bundle: {e}")
            return result

        blob_id = uuid.uuid4().hex

        entries = bundle.get("entry", [])
        for entry in entries:
            resource = entry.get("resource", entry)
            if not isinstance(resource, dict):
                result.skipped += 1
                continue
            self._process_resource(resource, user_id, blob_id, result)

        logger.info(
            "Fasten Bundle import: %d labs, %d meds, %d vitals, "
            "%d conditions, %d allergies, %d immunizations, %d skipped",
            result.labs, result.medications, result.vitals,
            result.conditions, result.allergies, result.immunizations,
            result.skipped,
        )
        return result

    def _process_resource(
        self, resource: dict, user_id: int, blob_id: str,
        result: FastenImportResult,
    ) -> None:
        """De-identify a resource and route to the appropriate mapper."""
        rtype = resource.get("resourceType", "")

        # Extract demographics from Patient before de-identification
        if rtype == "Patient":
            result.demographics = self._deid.extract_demographics(resource)
            result.skipped += 1  # Patient resource itself not stored
            return

        # De-identify the resource
        cleaned, report = self._deid.deidentify_resource(resource)
        if cleaned is None:
            result.skipped += 1
            return

        try:
            if rtype == "Observation":
                category = self._get_observation_category(cleaned)
                if category == "laboratory":
                    if self._map_lab(cleaned, user_id, blob_id):
                        result.labs += 1
                    else:
                        result.skipped += 1
                elif category == "vital-signs":
                    if self._map_vital(cleaned, user_id, blob_id):
                        result.vitals += 1
                    else:
                        result.skipped += 1
                else:
                    # Default: treat as lab if has valueQuantity
                    if "valueQuantity" in cleaned:
                        if self._map_lab(cleaned, user_id, blob_id):
                            result.labs += 1
                        else:
                            result.skipped += 1
                    else:
                        result.skipped += 1

            elif rtype in ("MedicationRequest", "MedicationStatement"):
                if self._map_medication(cleaned, user_id, blob_id):
                    result.medications += 1
                else:
                    result.skipped += 1

            elif rtype == "Condition":
                if self._map_condition(cleaned, user_id, blob_id):
                    result.conditions += 1
                else:
                    result.skipped += 1

            elif rtype == "AllergyIntolerance":
                if self._map_allergy(cleaned, user_id):
                    result.allergies += 1
                else:
                    result.skipped += 1

            elif rtype == "Immunization":
                if self._map_immunization(cleaned, user_id):
                    result.immunizations += 1
                else:
                    result.skipped += 1

            else:
                result.skipped += 1

        except Exception as e:
            result.errors.append(f"{rtype}/{resource.get('id', '?')}: {e}")
            result.skipped += 1

    def _map_lab(self, resource: dict, user_id: int, blob_id: str) -> bool:
        """Map a de-identified FHIR Observation to LabResult."""
        code_display, loinc_code = self._extract_code(resource)
        if not code_display:
            return False

        value_qty = resource.get("valueQuantity", {})
        value = value_qty.get("value")
        value_str = resource.get("valueString")

        if value is None and value_str is None:
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
            coding = interp[0].get("coding", [{}])
            flag = coding[0].get("code", "") if coding else ""

        date_collected = self._parse_fhir_date(
            resource.get("effectiveDateTime")
            or resource.get("effectivePeriod", {}).get("start")
        )

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name=code_display,
            canonical_name=normalize_test_name(code_display),
            value=value if value is not None else value_str,
            unit=value_qty.get("unit", ""),
            reference_low=ref_low,
            reference_high=ref_high,
            reference_text=ref_text,
            date_collected=date_collected,
            fasting=None,
            confidence=0.95,
            source_blob_id=blob_id,
            flag=flag,
            ordering_provider="",  # PII stripped
            lab_name="",  # PII stripped
        )
        self._db.insert_observation(lab, user_id=user_id)
        return True

    def _map_vital(self, resource: dict, user_id: int, blob_id: str) -> bool:
        """Map a de-identified FHIR Observation to VitalSign."""
        code_display, _ = self._extract_code(resource)
        if not code_display:
            return False

        value_qty = resource.get("valueQuantity", {})
        value = value_qty.get("value")
        value_str = resource.get("valueString")
        if value is None and value_str is None:
            return False

        timestamp = None
        dt_str = resource.get("effectiveDateTime")
        if dt_str:
            try:
                timestamp = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            except ValueError:
                pass

        vital = VitalSign(
            id=uuid.uuid4().hex,
            type=code_display.lower(),
            value=str(value) if value is not None else str(value_str),
            unit=value_qty.get("unit", ""),
            timestamp=timestamp,
            source="fasten",
            source_blob_id=blob_id,
        )
        self._db.insert_observation(vital, user_id=user_id)
        return True

    def _map_medication(
        self, resource: dict, user_id: int, blob_id: str,
    ) -> bool:
        """Map MedicationRequest/MedicationStatement to Medication."""
        # Try medicationCodeableConcept first, then medicationReference
        med_concept = resource.get("medicationCodeableConcept", {})
        name = ""
        if med_concept:
            coding = med_concept.get("coding", [])
            name = coding[0].get("display", "") if coding else ""
            if not name:
                name = med_concept.get("text", "")
        if not name:
            return False

        # Dosage
        dose = ""
        frequency = ""
        dosage_list = resource.get("dosageInstruction", resource.get("dosage", []))
        if dosage_list:
            d = dosage_list[0]
            dose = d.get("text", "")
            timing = d.get("timing", {}).get("code", {}).get("text", "")
            if timing:
                frequency = timing

        # Dates
        start_date = self._parse_fhir_date(
            resource.get("authoredOn")
            or resource.get("effectivePeriod", {}).get("start")
        )
        end_date = self._parse_fhir_date(
            resource.get("effectivePeriod", {}).get("end")
        )

        status = resource.get("status", "active")
        if status in ("completed", "stopped", "cancelled"):
            status = "stopped"
        else:
            status = "active"

        med = Medication(
            id=uuid.uuid4().hex,
            name=name,
            dose=dose,
            frequency=frequency,
            start_date=start_date,
            end_date=end_date,
            status=status,
            source_blob_id=blob_id,
            prescriber="",  # PII stripped
        )
        self._db.insert_medication(med, user_id=user_id)
        return True

    def _map_condition(self, resource: dict, user_id: int, blob_id: str) -> bool:
        """Map FHIR Condition to LTM fact (condition category)."""
        code_display, _ = self._extract_code(resource)
        if not code_display:
            return False

        # Store as LTM condition fact
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
            fact += f" (onset: {onset})"

        self._db.insert_ltm(
            user_id=user_id,
            category="condition",
            fact=fact,
            source="fasten_import",
        )
        return True

    def _map_allergy(self, resource: dict, user_id: int) -> bool:
        """Map AllergyIntolerance to LTM fact."""
        code_display, _ = self._extract_code(resource)
        if not code_display:
            return False

        criticality = resource.get("criticality", "")
        reactions = []
        for r in resource.get("reaction", []):
            for m in r.get("manifestation", []):
                text = m.get("text", "")
                coding = m.get("coding", [])
                if text:
                    reactions.append(text)
                elif coding:
                    reactions.append(coding[0].get("display", ""))

        fact = f"Known allergy: {code_display}"
        if criticality:
            fact += f" (criticality: {criticality})"
        if reactions:
            fact += f" — reactions: {', '.join(reactions)}"

        self._db.insert_ltm(
            user_id=user_id,
            category="condition",
            fact=fact,
            source="fasten_import",
        )
        return True

    def _map_immunization(self, resource: dict, user_id: int) -> bool:
        """Map Immunization to LTM fact."""
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
            fact += f" (date: {occ})"

        self._db.insert_ltm(
            user_id=user_id,
            category="medication",
            fact=fact,
            source="fasten_import",
        )
        return True

    # --- Helpers ---

    @staticmethod
    def _extract_code(resource: dict) -> tuple[str, str]:
        """Extract (display_name, code) from resource.code."""
        code_obj = resource.get("code", {})
        coding = code_obj.get("coding", [])
        if coding:
            display = coding[0].get("display", "")
            code = coding[0].get("code", "")
            if display:
                return display, code
        text = code_obj.get("text", "")
        return text, ""

    @staticmethod
    def _get_observation_category(resource: dict) -> str:
        """Extract observation category code."""
        categories = resource.get("category", [])
        for cat in categories:
            codings = cat.get("coding", [])
            for c in codings:
                code = c.get("code", "")
                if code:
                    return code
        return ""

    @staticmethod
    def _parse_fhir_date(date_str: str | None) -> date | None:
        """Parse FHIR date or dateTime to Python date."""
        if not date_str:
            return None
        try:
            # Handle full dateTime (2025-01-15T08:00:00Z)
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return dt.date()
            # Handle date only (2025-01-15)
            parts = date_str.split("-")
            if len(parts) >= 3:
                return date(int(parts[0]), int(parts[1]), int(parts[2]))
            if len(parts) == 2:
                return date(int(parts[0]), int(parts[1]), 1)
            if len(parts) == 1:
                return date(int(parts[0]), 1, 1)
        except (ValueError, IndexError):
            pass
        return None
