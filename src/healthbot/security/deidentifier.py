"""FHIR R4 resource de-identification (HIPAA Safe Harbor).

Strips all 18 HIPAA Safe Harbor identifiers from FHIR resources.
Deterministic only: regex + PhiFirewall. No LLM involvement.

Used by importers (Fasten, MyChart) to strip PII before storage
so that any AI model can analyze the data without knowing whose
records these are.
"""
from __future__ import annotations

import copy
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date

from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")

# FHIR resource fields that may contain free-text PII
_FREE_TEXT_PATHS = (
    "text",           # Narrative
    "note",           # Annotation[]
    "conclusion",     # DiagnosticReport
    "comment",        # Observation
    "description",    # Various
)

# Fields that are always PII and should be removed entirely
_PATIENT_PII_FIELDS = (
    "name", "telecom", "address", "photo", "identifier",
    "contact", "communication", "link", "managingOrganization",
    "generalPractitioner",
)

# Reference types that reveal identity (provider names, locations)
_PII_REFERENCE_TYPES = (
    "Practitioner", "PractitionerRole", "Organization",
    "Location", "RelatedPerson", "Person",
)

# Fields containing references to strip
_PII_REFERENCE_FIELDS = (
    "performer", "requester", "recorder", "asserter",
    "resultsInterpreter", "author", "informationSource",
    "participant", "careManager", "managingOrganization",
    "generalPractitioner", "sender", "recipient",
)


@dataclass
class DeidentificationReport:
    """Track what was stripped from a resource."""

    resource_type: str
    fields_removed: list[str] = field(default_factory=list)
    fields_redacted: list[str] = field(default_factory=list)
    patient_ref_replaced: bool = False
    pii_refs_removed: int = 0


class FhirDeidentifier:
    """Strip PII from FHIR R4 resources per HIPAA Safe Harbor.

    All detection is deterministic (regex/pattern). No LLM calls.
    """

    def __init__(
        self,
        phi_firewall: PhiFirewall | None = None,
        anon_patient_id: str | None = None,
    ) -> None:
        self._fw = phi_firewall or PhiFirewall()
        self._anon_id = anon_patient_id or uuid.uuid4().hex

    @property
    def anon_patient_id(self) -> str:
        """The anonymous patient ID used for this session."""
        return self._anon_id

    def deidentify_resource(
        self, resource: dict,
    ) -> tuple[dict | None, DeidentificationReport]:
        """De-identify a single FHIR resource.

        Returns (cleaned_resource, report).
        Patient resources return None (pure PII) but demographics
        can be extracted separately via extract_demographics().
        """
        resource = copy.deepcopy(resource)
        rtype = resource.get("resourceType", "")
        report = DeidentificationReport(resource_type=rtype)

        # Patient resources are pure PII — don't store them
        if rtype == "Patient":
            return None, report

        # Skip non-clinical reference resources (pure PII)
        if rtype in _PII_REFERENCE_TYPES:
            return None, report

        # Strip identifier arrays (SSN, MRN, insurance IDs)
        self._strip_identifiers(resource, report)

        # Replace Patient references with anonymous UUID
        self._replace_patient_refs(resource, report)

        # Remove PII reference fields (practitioner, organization, etc.)
        self._strip_pii_ref_fields(resource, report)

        # Scrub free-text fields through PhiFirewall
        self._scrub_free_text(resource, report)

        # Remove narrative HTML (often contains patient name)
        if "text" in resource:
            del resource["text"]
            report.fields_removed.append("text")

        # Remove contained resources (may include Patient/Practitioner)
        if "contained" in resource:
            resource["contained"] = [
                r for r in resource["contained"]
                if r.get("resourceType") not in (
                    "Patient", *_PII_REFERENCE_TYPES
                )
            ]
            if not resource["contained"]:
                del resource["contained"]

        # Remove meta.source (may contain institution URL)
        meta = resource.get("meta", {})
        if "source" in meta:
            del meta["source"]
            report.fields_removed.append("meta.source")

        return resource, report

    def deidentify_bundle(
        self, bundle: dict,
    ) -> tuple[list[dict], list[DeidentificationReport]]:
        """De-identify all resources in a FHIR Bundle.

        Returns (cleaned_resources, reports).
        Patient resources are filtered out (return None from deidentify_resource).
        """
        reports: list[DeidentificationReport] = []
        cleaned: list[dict] = []

        entries = bundle.get("entry", [])
        for entry in entries:
            resource = entry.get("resource", entry)
            result, report = self.deidentify_resource(resource)
            reports.append(report)
            if result is not None:
                cleaned.append(result)

        return cleaned, reports

    def deidentify_ndjson(
        self, lines: list[dict],
    ) -> tuple[list[dict], list[DeidentificationReport]]:
        """De-identify a list of FHIR resources (parsed NDJSON).

        Returns (cleaned_resources, reports).
        """
        reports: list[DeidentificationReport] = []
        cleaned: list[dict] = []

        for resource in lines:
            result, report = self.deidentify_resource(resource)
            reports.append(report)
            if result is not None:
                cleaned.append(result)

        return cleaned, reports

    def extract_demographics(self, patient: dict) -> dict:
        """Extract safe demographics from a Patient resource.

        Returns dict with age, sex, ethnicity — no PII.
        """
        demographics: dict = {}

        # Gender/sex
        gender = patient.get("gender")
        if gender:
            demographics["sex"] = gender

        # Birth date -> age (strip exact date)
        birth_date = patient.get("birthDate")
        if birth_date:
            age = self._calculate_age(birth_date)
            if age is not None:
                demographics["age"] = age

        # Ethnicity from US Core extension
        for ext in patient.get("extension", []):
            url = ext.get("url", "")
            if "us-core-race" in url or "race" in url.lower():
                race_text = self._extract_extension_text(ext)
                if race_text:
                    demographics["race"] = race_text
            elif "us-core-ethnicity" in url or "ethnicity" in url.lower():
                eth_text = self._extract_extension_text(ext)
                if eth_text:
                    demographics["ethnicity"] = eth_text

        return demographics

    # --- Internal methods ---

    def _strip_identifiers(
        self, resource: dict, report: DeidentificationReport,
    ) -> None:
        """Remove all identifier arrays (SSN, MRN, insurance IDs)."""
        if "identifier" in resource:
            del resource["identifier"]
            report.fields_removed.append("identifier")

    def _replace_patient_refs(
        self, resource: dict, report: DeidentificationReport,
    ) -> None:
        """Replace all Patient references with anonymous UUID."""
        self._walk_and_replace_refs(resource, report)

    def _walk_and_replace_refs(
        self, obj: object, report: DeidentificationReport,
    ) -> None:
        """Recursively walk a FHIR structure and replace Patient references."""
        if isinstance(obj, dict):
            # Check 'reference' field
            ref = obj.get("reference", "")
            if isinstance(ref, str) and ref.startswith("Patient/"):
                obj["reference"] = f"Patient/{self._anon_id}"
                report.patient_ref_replaced = True
            # Check 'subject' shorthand
            if "subject" in obj and isinstance(obj["subject"], dict):
                subj_ref = obj["subject"].get("reference", "")
                if isinstance(subj_ref, str) and subj_ref.startswith("Patient/"):
                    obj["subject"]["reference"] = f"Patient/{self._anon_id}"
                    # Clear display (may contain patient name)
                    obj["subject"].pop("display", None)
                    report.patient_ref_replaced = True

            for value in obj.values():
                self._walk_and_replace_refs(value, report)

        elif isinstance(obj, list):
            for item in obj:
                self._walk_and_replace_refs(item, report)

    def _strip_pii_ref_fields(
        self, resource: dict, report: DeidentificationReport,
    ) -> None:
        """Remove fields that reference Practitioners, Organizations, etc."""
        for field_name in _PII_REFERENCE_FIELDS:
            if field_name in resource:
                del resource[field_name]
                report.pii_refs_removed += 1

        # Also check encounter.participant (nested provider refs)
        if "participant" in resource:
            del resource["participant"]
            report.pii_refs_removed += 1

    def _scrub_free_text(
        self, resource: dict, report: DeidentificationReport,
    ) -> None:
        """Run PhiFirewall.redact() on free-text fields."""
        self._scrub_obj(resource, report, depth=0)

    def _scrub_obj(
        self, obj: object, report: DeidentificationReport, depth: int,
    ) -> None:
        """Recursively scrub free-text values in a dict/list."""
        if depth > 10:  # prevent infinite recursion
            return

        if isinstance(obj, dict):
            for key, value in list(obj.items()):
                if isinstance(value, str) and len(value) > 5:
                    # Check if this text field contains PII
                    if self._fw.contains_phi(value):
                        obj[key] = self._fw.redact(value)
                        report.fields_redacted.append(key)
                elif isinstance(value, (dict, list)):
                    self._scrub_obj(value, report, depth + 1)

            # Scrub 'note' arrays specifically
            notes = obj.get("note", [])
            if isinstance(notes, list):
                for note in notes:
                    if isinstance(note, dict) and "text" in note:
                        text = note["text"]
                        if isinstance(text, str) and self._fw.contains_phi(text):
                            note["text"] = self._fw.redact(text)
                            report.fields_redacted.append("note.text")

        elif isinstance(obj, list):
            for item in obj:
                self._scrub_obj(item, report, depth + 1)

    @staticmethod
    def _calculate_age(birth_date_str: str) -> int | None:
        """Calculate age from FHIR birthDate string (YYYY, YYYY-MM, or YYYY-MM-DD)."""
        try:
            parts = birth_date_str.split("-")
            year = int(parts[0])
            month = int(parts[1]) if len(parts) > 1 else 1
            day = int(parts[2]) if len(parts) > 2 else 1
            born = date(year, month, day)
            today = date.today()
            age = today.year - born.year
            if (today.month, today.day) < (born.month, born.day):
                age -= 1
            return age if 0 <= age <= 150 else None
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _extract_extension_text(ext: dict) -> str:
        """Extract display text from a US Core race/ethnicity extension."""
        # Look in nested extensions for the 'text' value
        for sub in ext.get("extension", []):
            url = sub.get("url", "")
            if url == "text":
                return sub.get("valueString", "")
        # Fallback: check for valueCoding
        for sub in ext.get("extension", []):
            coding = sub.get("valueCoding", {})
            display = coding.get("display", "")
            if display:
                return display
        return ""
