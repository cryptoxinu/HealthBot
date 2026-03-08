"""MyChart / CCDA / FHIR import.

Parses CCDA XML documents and FHIR JSON bundles from MyChart/Epic
exports into canonical schema. Supports labs, medications, conditions,
allergies, and immunizations with reference ranges and flags.
"""
from __future__ import annotations

import io
import json
import logging
import uuid

import defusedxml.ElementTree as ET  # noqa: N817

from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, Medication
from healthbot.normalize.lab_normalizer import normalize_test_name
from healthbot.security.deidentifier import FhirDeidentifier
from healthbot.security.phi_firewall import PhiFirewall
from healthbot.security.vault import Vault

logger = logging.getLogger("healthbot")


def _try_float(value: str) -> float | str:
    """Try to parse a numeric value (handles negatives, scientific notation).

    Returns float if parseable, otherwise the original string.
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return value


# CCDA XML namespaces
CCDA_NS = {
    "cda": "urn:hl7-org:v3",
    "sdtc": "urn:hl7-org:sdtc",
}

# CCDA section codes (LOINC)
_SECTION_LABS = "30954-2"
_SECTION_MEDS = "10160-0"
_SECTION_PROBLEMS = "11450-4"
_SECTION_ALLERGIES = "48765-2"
_SECTION_IMMUNIZATIONS = "11369-6"


class MyChartImporter:
    """Import health data from MyChart/CCDA/FHIR exports."""

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

    def import_ccda_bytes(
        self, xml_bytes: bytes, user_id: int = 0,
    ) -> dict:
        """Import CCDA XML document."""
        blob_id = uuid.uuid4().hex
        self._vault.store_blob(xml_bytes, blob_id=blob_id)

        tree = ET.parse(io.BytesIO(xml_bytes))
        root = tree.getroot()
        results = {
            "labs": 0, "meds": 0,
            "conditions": 0, "allergies": 0, "immunizations": 0,
        }

        for section in root.iter(f"{{{CCDA_NS['cda']}}}section"):
            code = section.find(f"{{{CCDA_NS['cda']}}}code")
            if code is not None:
                section_code = code.get("code", "")
                if section_code == _SECTION_LABS:
                    results["labs"] = self._parse_ccda_labs(section, blob_id)
                elif section_code == _SECTION_MEDS:
                    results["meds"] = self._parse_ccda_meds(section, blob_id)
                elif section_code == _SECTION_PROBLEMS:
                    results["conditions"] = self._parse_ccda_conditions(
                        section, user_id,
                    )
                elif section_code == _SECTION_ALLERGIES:
                    results["allergies"] = self._parse_ccda_allergies(
                        section, user_id,
                    )
                elif section_code == _SECTION_IMMUNIZATIONS:
                    results["immunizations"] = self._parse_ccda_immunizations(
                        section, user_id,
                    )

        return results

    def import_fhir_bundle(
        self, json_bytes: bytes, user_id: int = 0,
    ) -> dict:
        """Import FHIR JSON bundle."""
        blob_id = uuid.uuid4().hex
        self._vault.store_blob(json_bytes, blob_id=blob_id)

        bundle = json.loads(json_bytes)
        results = {
            "labs": 0, "meds": 0,
            "conditions": 0, "allergies": 0, "immunizations": 0,
        }

        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})

            # Strip PII from FHIR resource before processing
            cleaned, _report = self._deid.deidentify_resource(resource)
            if cleaned is None:
                continue  # Pure-PII resource (e.g. Patient) — skip
            resource = cleaned

            rtype = resource.get("resourceType", "")

            if rtype == "Observation":
                if self._import_fhir_observation(resource, blob_id):
                    results["labs"] += 1
            elif rtype in ("MedicationStatement", "MedicationRequest"):
                if self._import_fhir_medication(resource, blob_id):
                    results["meds"] += 1
            elif rtype == "Condition":
                if self._import_fhir_condition(resource, user_id):
                    results["conditions"] += 1
            elif rtype == "AllergyIntolerance":
                if self._import_fhir_allergy(resource, user_id):
                    results["allergies"] += 1
            elif rtype == "Immunization":
                if self._import_fhir_immunization(resource, user_id):
                    results["immunizations"] += 1

        return results

    # --- CCDA parsers ---

    def _parse_ccda_labs(self, section: ET.Element, blob_id: str) -> int:
        """Parse lab results from CCDA section with reference ranges and flags."""
        count = 0
        ns = CCDA_NS["cda"]
        for entry in section.iter(f"{{{ns}}}observation"):
            code_elem = entry.find(f"{{{ns}}}code")
            value_elem = entry.find(f"{{{ns}}}value")
            if code_elem is None or value_elem is None:
                continue

            test_name = code_elem.get("displayName", "")
            value = value_elem.get("value", "")
            unit = value_elem.get("unit", "")

            if not test_name or not value:
                continue

            # Reference range
            ref_low = None
            ref_high = None
            ref_text = ""
            ref_range = entry.find(f"{{{ns}}}referenceRange")
            if ref_range is not None:
                obs_range = ref_range.find(f"{{{ns}}}observationRange")
                if obs_range is not None:
                    rr_value = obs_range.find(f"{{{ns}}}value")
                    if rr_value is not None:
                        low = rr_value.find(f"{{{ns}}}low")
                        high = rr_value.find(f"{{{ns}}}high")
                        if low is not None and low.get("value"):
                            try:
                                ref_low = float(low.get("value"))
                            except (ValueError, TypeError):
                                pass  # Skip malformed reference range
                        if high is not None and high.get("value"):
                            try:
                                ref_high = float(high.get("value"))
                            except (ValueError, TypeError):
                                pass  # Skip malformed reference range
                    text_elem = obs_range.find(f"{{{ns}}}text")
                    if text_elem is not None and text_elem.text:
                        ref_text = text_elem.text

            # Interpretation flag
            flag = ""
            interp = entry.find(f"{{{ns}}}interpretationCode")
            if interp is not None:
                flag = interp.get("code", "")

            lab = LabResult(
                id=uuid.uuid4().hex,
                test_name=test_name,
                canonical_name=normalize_test_name(test_name),
                value=_try_float(value),
                unit=unit,
                reference_low=ref_low,
                reference_high=ref_high,
                reference_text=ref_text,
                flag=flag,
                source_blob_id=blob_id,
            )
            self._db.insert_observation(lab)
            count += 1
        return count

    def _parse_ccda_meds(self, section: ET.Element, blob_id: str) -> int:
        """Parse medications from CCDA section."""
        count = 0
        ns = CCDA_NS["cda"]
        for entry in section.iter(f"{{{ns}}}substanceAdministration"):
            product = entry.find(f".//{{{ns}}}manufacturedProduct")
            if product is None:
                continue
            material = product.find(f".//{{{ns}}}material/{{{ns}}}code")
            if material is None:
                continue

            name = material.get("displayName", "")
            if name:
                med = Medication(
                    id=uuid.uuid4().hex,
                    name=name,
                    source_blob_id=blob_id,
                )
                self._db.insert_medication(med)
                count += 1
        return count

    def _parse_ccda_conditions(
        self, section: ET.Element, user_id: int,
    ) -> int:
        """Parse conditions from CCDA Problem List section."""
        count = 0
        ns = CCDA_NS["cda"]
        for obs in section.iter(f"{{{ns}}}observation"):
            code_elem = obs.find(f".//{{{ns}}}value")
            if code_elem is None:
                continue
            display = code_elem.get("displayName", "")
            if not display:
                continue

            # Clinical status from observation status
            status_elem = obs.find(f"{{{ns}}}statusCode")
            status = status_elem.get("code", "") if status_elem is not None else ""

            fact = f"Known condition: {display}"
            if status and status != "active":
                fact += f" (status: {status})"

            self._db.insert_ltm(
                user_id=user_id,
                category="condition",
                fact=fact,
                source="mychart_import",
            )
            count += 1
        return count

    def _parse_ccda_allergies(
        self, section: ET.Element, user_id: int,
    ) -> int:
        """Parse allergies from CCDA Allergies section."""
        count = 0
        ns = CCDA_NS["cda"]
        for entry in section.findall(f"{{{ns}}}entry"):
            # Navigate to allergy observation (skip nested reaction obs)
            obs = entry.find(
                f".//{{{ns}}}act/{{{ns}}}entryRelationship/{{{ns}}}observation",
            )
            if obs is None:
                obs = entry.find(f"{{{ns}}}observation")
            if obs is None:
                continue

            # Allergen is in participant/playingEntity/code
            participant = obs.find(
                f".//{{{ns}}}participant/{{{ns}}}participantRole"
                f"/{{{ns}}}playingEntity/{{{ns}}}code"
            )
            if participant is not None:
                allergen = participant.get("displayName", "")
            else:
                # Fallback: value element
                value_elem = obs.find(f".//{{{ns}}}value")
                allergen = value_elem.get("displayName", "") if value_elem is not None else ""

            if not allergen:
                continue

            # Reaction manifestations from nested observations
            reactions = []
            for er in obs.findall(f"{{{ns}}}entryRelationship"):
                react_obs = er.find(f"{{{ns}}}observation")
                if react_obs is not None:
                    react_value = react_obs.find(f"{{{ns}}}value")
                    if react_value is not None:
                        react_display = react_value.get("displayName", "")
                        if react_display and react_display != allergen:
                            reactions.append(react_display)

            fact = f"Known allergy: {allergen}"
            if reactions:
                fact += f" — reactions: {', '.join(reactions)}"

            self._db.insert_ltm(
                user_id=user_id,
                category="condition",
                fact=fact,
                source="mychart_import",
            )
            count += 1
        return count

    def _parse_ccda_immunizations(
        self, section: ET.Element, user_id: int,
    ) -> int:
        """Parse immunizations from CCDA Immunizations section."""
        count = 0
        ns = CCDA_NS["cda"]
        for admin in section.iter(f"{{{ns}}}substanceAdministration"):
            product = admin.find(
                f".//{{{ns}}}manufacturedProduct/{{{ns}}}manufacturedMaterial"
                f"/{{{ns}}}code"
            )
            if product is None:
                continue
            vaccine_name = product.get("displayName", "")
            if not vaccine_name:
                continue

            # Effective time
            time_elem = admin.find(f"{{{ns}}}effectiveTime")
            date_str = time_elem.get("value", "") if time_elem is not None else ""

            fact = f"Immunization: {vaccine_name}"
            if date_str:
                fact += f" (date: {date_str})"

            self._db.insert_ltm(
                user_id=user_id,
                category="medication",
                fact=fact,
                source="mychart_import",
            )
            count += 1
        return count

    # --- FHIR parsers ---

    def _import_fhir_observation(self, resource: dict, blob_id: str) -> bool:
        """Import a FHIR Observation with reference ranges and flags."""
        coding = resource.get("code", {}).get("coding", [{}])
        test_name = coding[0].get("display", "") if coding else ""
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
                try:
                    ref_low = float(low["value"])
                except (ValueError, TypeError):
                    pass  # Skip malformed reference range
            if high.get("value") is not None:
                try:
                    ref_high = float(high["value"])
                except (ValueError, TypeError):
                    pass  # Skip malformed reference range
            ref_text = rr.get("text", "")

        # Interpretation flag
        flag = ""
        interp = resource.get("interpretation", [])
        if interp:
            interp_coding = interp[0].get("coding", [{}])
            flag = interp_coding[0].get("code", "") if interp_coding else ""

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
            source_blob_id=blob_id,
        )
        # Dedup: skip if same test/date/value already exists
        if lab.date_collected and lab.canonical_name:
            existing = self._db.query_observations(
                record_type="lab_result",
                canonical_name=lab.canonical_name,
                start_date=lab.date_collected.isoformat(),
                end_date=lab.date_collected.isoformat(),
                limit=1,
            )
            if existing:
                for e in existing:
                    try:
                        if abs(float(e.get("value", "")) - float(lab.value)) < 0.001:
                            logger.debug(
                                "MyChart FHIR dedup: skipping %s on %s",
                                lab.canonical_name, lab.date_collected,
                            )
                            return False
                    except (ValueError, TypeError):
                        pass

        self._db.insert_observation(lab)
        return True

    def _import_fhir_medication(self, resource: dict, blob_id: str) -> bool:
        """Import a FHIR MedicationStatement/MedicationRequest."""
        coding = resource.get("medicationCodeableConcept", {}).get("coding", [{}])
        name = coding[0].get("display", "") if coding else ""
        if not name:
            name = resource.get("medicationCodeableConcept", {}).get("text", "")
        if not name:
            return False
        med = Medication(
            id=uuid.uuid4().hex,
            name=name,
            source_blob_id=blob_id,
        )
        self._db.insert_medication(med)
        return True

    def _import_fhir_condition(self, resource: dict, user_id: int) -> bool:
        """Import a FHIR Condition resource as LTM fact."""
        code_display = self._extract_fhir_display(resource)
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
            fact += f" (onset: {onset})"

        self._db.insert_ltm(
            user_id=user_id,
            category="condition",
            fact=fact,
            source="mychart_import",
        )
        return True

    def _import_fhir_allergy(self, resource: dict, user_id: int) -> bool:
        """Import a FHIR AllergyIntolerance as LTM fact."""
        code_display = self._extract_fhir_display(resource)
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
            source="mychart_import",
        )
        return True

    def _import_fhir_immunization(self, resource: dict, user_id: int) -> bool:
        """Import a FHIR Immunization as LTM fact."""
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
            source="mychart_import",
        )
        return True

    # --- Helpers ---

    @staticmethod
    def _extract_fhir_display(resource: dict) -> str:
        """Extract display name from resource.code."""
        code_obj = resource.get("code", {})
        coding = code_obj.get("coding", [])
        if coding:
            display = coding[0].get("display", "")
            if display:
                return display
        return code_obj.get("text", "")
