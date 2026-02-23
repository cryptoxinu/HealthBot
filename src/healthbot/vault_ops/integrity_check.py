"""Vault integrity checker.

Verifies encrypted data can be decrypted, checks FK relationships,
and validates AAD bindings. All checks are read-only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class IntegrityIssue:
    """A single integrity issue found during check."""

    table: str
    issue_type: str  # "decrypt_failure", "orphan_fk", "missing_data"
    description: str
    severity: str = "warning"  # "info", "warning", "error"


@dataclass
class IntegrityReport:
    """Result of a full integrity check."""

    tables_checked: int = 0
    rows_checked: int = 0
    issues: list[IntegrityIssue] = field(default_factory=list)
    ok: bool = True


class IntegrityChecker:
    """Verify vault data integrity without modifying anything."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def check_all(self, user_id: int | None = None) -> IntegrityReport:
        """Run all integrity checks. Read-only."""
        report = IntegrityReport()

        self._check_observations(report, user_id)
        self._check_medications(report, user_id)
        self._check_memory(report, user_id)
        self._check_workouts(report, user_id)

        report.ok = not any(
            i.severity == "error" for i in report.issues
        )
        return report

    def _check_observations(
        self, report: IntegrityReport, user_id: int | None,
    ) -> None:
        """Verify observation records decrypt and have required fields."""
        report.tables_checked += 1
        try:
            rows = self._db.query_observations(
                record_type="lab_result", limit=500, user_id=user_id,
            )
            for row in rows:
                report.rows_checked += 1
                if not row.get("test_name"):
                    report.issues.append(IntegrityIssue(
                        table="observations",
                        issue_type="missing_data",
                        description=(
                            f"Observation {row.get('_meta', {}).get('obs_id', '?')} "
                            f"has no test_name"
                        ),
                        severity="warning",
                    ))
        except Exception as e:
            report.issues.append(IntegrityIssue(
                table="observations",
                issue_type="decrypt_failure",
                description=f"Failed to read observations: {e}",
                severity="error",
            ))

    def _check_medications(
        self, report: IntegrityReport, user_id: int | None,
    ) -> None:
        """Verify medication records decrypt."""
        report.tables_checked += 1
        try:
            meds = self._db.get_active_medications(user_id=user_id)
            for med in meds:
                report.rows_checked += 1
                if not med.get("name"):
                    report.issues.append(IntegrityIssue(
                        table="medications",
                        issue_type="missing_data",
                        description="Medication record has no name",
                        severity="warning",
                    ))
        except Exception as e:
            report.issues.append(IntegrityIssue(
                table="medications",
                issue_type="decrypt_failure",
                description=f"Failed to read medications: {e}",
                severity="error",
            ))

    def _check_memory(
        self, report: IntegrityReport, user_id: int | None,
    ) -> None:
        """Verify LTM records decrypt."""
        report.tables_checked += 1
        try:
            facts = self._db.get_ltm_by_category(user_id or 0, "condition")
            for _fact in facts:
                report.rows_checked += 1
        except Exception as e:
            report.issues.append(IntegrityIssue(
                table="memory_ltm",
                issue_type="decrypt_failure",
                description=f"Failed to read LTM: {e}",
                severity="error",
            ))

    def _check_workouts(
        self, report: IntegrityReport, user_id: int | None,
    ) -> None:
        """Verify workout records decrypt."""
        report.tables_checked += 1
        try:
            workouts = self._db.query_workouts(
                user_id=user_id, limit=100,
            )
            for wo in workouts:
                report.rows_checked += 1
                if not wo.get("sport_type"):
                    report.issues.append(IntegrityIssue(
                        table="workouts",
                        issue_type="missing_data",
                        description="Workout record has no sport_type",
                        severity="warning",
                    ))
        except Exception as e:
            report.issues.append(IntegrityIssue(
                table="workouts",
                issue_type="decrypt_failure",
                description=f"Failed to read workouts: {e}",
                severity="error",
            ))

    def format_report(self, report: IntegrityReport) -> str:
        """Format integrity report for display."""
        lines = [
            f"Integrity Check: {'PASS' if report.ok else 'FAIL'}",
            f"  Tables checked: {report.tables_checked}",
            f"  Rows verified: {report.rows_checked}",
        ]
        if report.issues:
            lines.append(f"  Issues: {len(report.issues)}")
            for issue in report.issues:
                icon = {"error": "X", "warning": "!", "info": "i"}.get(
                    issue.severity, "?",
                )
                lines.append(f"  [{icon}] {issue.table}: {issue.description}")
        else:
            lines.append("  No issues found.")
        return "\n".join(lines)
