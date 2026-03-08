"""Data quality validation for lab results.

Deterministic checks run at ingestion time. No LLM involvement.
Catches fasting flags, unit mismatches, duplicates, missing references.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult
from healthbot.reasoning.reference_ranges import (
    FASTING_TESTS,
    convert_unit,
    get_default_range,
    get_range,
)

# A valid unit must contain at least one letter, %, #, or ·
# Pure numbers (e.g., "12", "62") are specimen IDs from old broken parses
_VALID_UNIT_RE = re.compile(r"[a-zA-Z%#·]")


@dataclass
class DataQualityIssue:
    obs_id: str
    test_name: str
    canonical_name: str
    issue_type: str  # "missing_fasting_flag", "unit_mismatch", etc.
    severity: str  # "info", "warning", "error"
    message: str
    suggestion: str = ""


class DataQualityEngine:
    """Deterministic data quality checks for lab results."""

    def __init__(self, db: HealthDB, user_id: int = 0) -> None:
        self._db = db
        self._user_id = user_id

    def check_batch(
        self,
        labs: list[LabResult],
        sex: str | None = None,
        age: int | None = None,
    ) -> list[DataQualityIssue]:
        """Run all quality checks on a batch of lab results."""
        issues: list[DataQualityIssue] = []
        for lab in labs:
            issue = self.check_fasting(lab)
            if issue:
                issues.append(issue)
            issue = self.check_unit_mismatch(lab)
            if issue:
                issues.append(issue)
            issue = self.check_duplicate(lab)
            if issue:
                issues.append(issue)
            issue = self.check_reference_range(lab, sex=sex, age=age)
            if issue:
                issues.append(issue)
        return issues

    def check_fasting(self, lab: LabResult) -> DataQualityIssue | None:
        """Check if fasting-required test is missing fasting flag."""
        canonical = lab.canonical_name or lab.test_name.lower()
        if canonical not in FASTING_TESTS:
            return None
        if lab.fasting is None:
            return DataQualityIssue(
                obs_id=lab.id,
                test_name=lab.test_name,
                canonical_name=canonical,
                issue_type="missing_fasting_flag",
                severity="warning",
                message=f"{lab.test_name}: fasting status not recorded",
                suggestion="Confirm with your lab if this was a fasting sample.",
            )
        return None

    def check_unit_mismatch(self, lab: LabResult) -> DataQualityIssue | None:
        """Check if this result uses a different unit than historical data."""
        if not lab.unit or not lab.canonical_name:
            return None
        if not _VALID_UNIT_RE.search(lab.unit):
            return None  # Current unit is garbage (pure number)

        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=lab.canonical_name,
            limit=5,
            user_id=self._user_id or None,
        )
        if not rows:
            return None

        # Find the most common historical unit (skip garbage)
        hist_units: dict[str, int] = {}
        for row in rows:
            u = row.get("unit", "")
            if u and _VALID_UNIT_RE.search(u):
                hist_units[u.strip().lower()] = hist_units.get(u.strip().lower(), 0) + 1

        if not hist_units:
            return None

        most_common = max(hist_units, key=hist_units.get)
        current = lab.unit.strip().lower()

        if current == most_common:
            return None

        # Try to convert
        try:
            val = float(lab.value) if not isinstance(lab.value, (int, float)) else lab.value
        except (ValueError, TypeError):
            return None

        converted = convert_unit(val, current, most_common)
        suggestion = ""
        if converted is not None:
            suggestion = f"Converted: {val} {lab.unit} = {converted} {most_common}"

        return DataQualityIssue(
            obs_id=lab.id,
            test_name=lab.test_name,
            canonical_name=lab.canonical_name,
            issue_type="unit_mismatch",
            severity="warning",
            message=(
                f"{lab.test_name}: unit '{lab.unit}' differs from "
                f"historical '{most_common}'"
            ),
            suggestion=suggestion,
        )

    def check_duplicate(self, lab: LabResult) -> DataQualityIssue | None:
        """Check for duplicate results (same test + same date)."""
        if not lab.canonical_name or not lab.date_collected:
            return None

        date_str = lab.date_collected.isoformat()
        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=lab.canonical_name,
            start_date=date_str,
            end_date=date_str,
            limit=5,
            user_id=self._user_id or None,
        )

        if not rows:
            return None

        for row in rows:
            if row.get("_meta", {}).get("obs_id") == lab.id:
                continue  # Skip self
            existing_val = row.get("value")
            try:
                existing_num = float(existing_val)
                new_num = float(lab.value)
                if abs(existing_num - new_num) < 0.001:
                    return DataQualityIssue(
                        obs_id=lab.id,
                        test_name=lab.test_name,
                        canonical_name=lab.canonical_name,
                        issue_type="duplicate_exact",
                        severity="info",
                        message=f"{lab.test_name}: duplicate result on {date_str}",
                    )
                else:
                    return DataQualityIssue(
                        obs_id=lab.id,
                        test_name=lab.test_name,
                        canonical_name=lab.canonical_name,
                        issue_type="duplicate_conflict",
                        severity="error",
                        message=(
                            f"{lab.test_name}: conflicting values on {date_str} "
                            f"(new: {lab.value}, existing: {existing_val})"
                        ),
                    )
            except (ValueError, TypeError):
                # Qualitative values: compare as strings
                if (
                    existing_val is not None
                    and str(existing_val).strip().lower()
                    == str(lab.value).strip().lower()
                ):
                    return DataQualityIssue(
                        obs_id=lab.id,
                        test_name=lab.test_name,
                        canonical_name=lab.canonical_name,
                        issue_type="duplicate_exact",
                        severity="info",
                        message=f"{lab.test_name}: duplicate result on {date_str}",
                    )
                elif existing_val is not None:
                    return DataQualityIssue(
                        obs_id=lab.id,
                        test_name=lab.test_name,
                        canonical_name=lab.canonical_name,
                        issue_type="duplicate_conflict",
                        severity="error",
                        message=(
                            f"{lab.test_name}: conflicting values on {date_str} "
                            f"(new: {lab.value}, existing: {existing_val})"
                        ),
                    )

        return None

    def check_reference_range(
        self,
        lab: LabResult,
        sex: str | None = None,
        age: int | None = None,
    ) -> DataQualityIssue | None:
        """Check if reference range is missing and provide defaults.

        Uses age/sex-adjusted ranges when demographics are available,
        falling back to population defaults otherwise.
        """
        if lab.reference_low is not None or lab.reference_high is not None:
            return None
        if lab.reference_text:
            return None

        canonical = lab.canonical_name or lab.test_name.lower()

        # Try age/sex-adjusted range first, then generic default
        adjusted = get_range(canonical, sex=sex, age=age) if (sex or age) else None
        default = adjusted or get_default_range(canonical)

        if default:
            note = default.get("note", "")
            if adjusted and (sex or age):
                context = []
                if sex:
                    context.append(sex)
                if age is not None:
                    context.append(f"age {age}")
                note = ", ".join(context)
            note_str = f" ({note})" if note else ""
            return DataQualityIssue(
                obs_id=lab.id,
                test_name=lab.test_name,
                canonical_name=canonical,
                issue_type="missing_reference_range",
                severity="info",
                message=f"{lab.test_name}: no reference range from lab report",
                suggestion=(
                    f"Using {'adjusted' if adjusted else 'population'} default: "
                    f"{default['low']}-{default['high']} {default['unit']}{note_str}"
                ),
            )

        return DataQualityIssue(
            obs_id=lab.id,
            test_name=lab.test_name,
            canonical_name=canonical,
            issue_type="missing_reference_range",
            severity="warning",
            message=f"{lab.test_name}: no reference range available",
        )

    def compute_completeness(self, labs: list[LabResult]) -> float:
        """Compute data completeness score (0.0–1.0) for a batch."""
        if not labs:
            return 0.0

        total_checks = 0
        passed = 0

        for lab in labs:
            # Has value
            total_checks += 1
            if lab.value is not None and str(lab.value).strip():
                passed += 1

            # Has unit
            total_checks += 1
            if lab.unit:
                passed += 1

            # Has date
            total_checks += 1
            if lab.date_collected:
                passed += 1

            # Has reference range
            total_checks += 1
            if lab.reference_low is not None or lab.reference_high is not None:
                passed += 1

            # Has fasting flag (only for fasting-required tests)
            canonical = lab.canonical_name or lab.test_name.lower()
            if canonical in FASTING_TESTS:
                total_checks += 1
                if lab.fasting is not None:
                    passed += 1

        return round(passed / total_checks, 3) if total_checks > 0 else 0.0

    def format_issues(self, issues: list[DataQualityIssue]) -> str:
        """Format issues for display."""
        if not issues:
            return "No data quality issues detected."

        lines = ["DATA QUALITY", "-" * 30]
        for issue in issues:
            icon = {"info": "i", "warning": "!", "error": "X"}.get(issue.severity, "?")
            lines.append(f"  [{icon}] {issue.message}")
            if issue.suggestion:
                lines.append(f"      -> {issue.suggestion}")
        return "\n".join(lines)
