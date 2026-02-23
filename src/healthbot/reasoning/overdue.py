"""Overdue test and screening detection.

Rule-based scheduling: common tests have recommended frequencies.
Compares last test date to recommended interval.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from healthbot.data.db import HealthDB

# Standard screening intervals (months)
TEST_INTERVALS: dict[str, int] = {
    "glucose": 12,
    "hba1c": 6,
    "cholesterol_total": 12,
    "ldl": 12,
    "hdl": 12,
    "triglycerides": 12,
    "hemoglobin": 12,
    "wbc": 12,
    "platelets": 12,
    "tsh": 12,
    "vitamin_d": 12,
    "vitamin_b12": 12,
    "creatinine": 12,
    "egfr": 12,
    "alt": 12,
    "ast": 12,
    "iron": 12,
    "ferritin": 12,
    "psa": 12,
    "bun": 12,
    "calcium": 12,
}


@dataclass
class OverdueItem:
    test_name: str
    canonical_name: str
    last_date: str
    interval_months: int
    days_overdue: int


# Family history modifiers for screening schedules
FAMILY_SCREENING_MODIFIERS: dict[str, dict] = {
    "colon_cancer": {
        "family_keywords": ["colon cancer", "colorectal", "colon polyps"],
        "tests": ["colonoscopy"],
        "modified_interval_months": 60,  # 5 years vs 10 standard
    },
    "diabetes_screening": {
        "family_keywords": ["diabetes", "type 2 diabetes"],
        "tests": ["glucose", "hba1c"],
        "modified_interval_months": 6,  # vs 12 standard
    },
    "lipid_panel": {
        "family_keywords": [
            "heart disease", "heart attack", "stroke", "cardiovascular",
        ],
        "tests": [
            "cholesterol_total", "ldl", "hdl", "triglycerides",
        ],
        "modified_interval_months": 6,  # vs 12 standard
    },
    "thyroid": {
        "family_keywords": [
            "thyroid", "hashimoto", "graves", "hypothyroid",
        ],
        "tests": ["tsh"],
        "modified_interval_months": 6,  # vs 12 standard
    },
    "kidney": {
        "family_keywords": ["kidney disease", "renal", "dialysis"],
        "tests": ["creatinine", "egfr", "bun"],
        "modified_interval_months": 6,
    },
}


class OverdueDetector:
    """Detect overdue health screenings."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def check_overdue(
        self,
        user_id: int | None = None,
        family_conditions: list[str] | None = None,
    ) -> list[OverdueItem]:
        """Check all known test types for overdue status."""
        now = datetime.now(UTC).date()
        overdue: list[OverdueItem] = []

        # Build family-modified intervals
        family_intervals = dict(TEST_INTERVALS)
        if family_conditions:
            family_intervals = self._apply_family_modifiers(
                family_intervals, family_conditions,
            )

        for canonical_name, interval_months in family_intervals.items():
            rows = self._db.query_observations(
                record_type="lab_result",
                canonical_name=canonical_name,
                limit=1,
                user_id=user_id,
            )
            if not rows:
                continue  # Never tested; don't flag as overdue

            row = rows[0]
            last_date_str = (
                row.get("date_collected")
                or row.get("_meta", {}).get("date_effective", "")
            )
            if not last_date_str:
                continue

            try:
                last_date = datetime.fromisoformat(last_date_str).date()
            except ValueError:
                continue

            due_date = last_date + timedelta(days=interval_months * 30)
            if now > due_date:
                days_over = (now - due_date).days
                test_name = row.get("test_name", canonical_name)
                overdue.append(OverdueItem(
                    test_name=test_name,
                    canonical_name=canonical_name,
                    last_date=last_date_str,
                    interval_months=interval_months,
                    days_overdue=days_over,
                ))

        overdue.sort(key=lambda x: x.days_overdue, reverse=True)
        return overdue

    @staticmethod
    def _apply_family_modifiers(
        intervals: dict[str, int],
        family_conditions: list[str],
    ) -> dict[str, int]:
        """Shorten screening intervals based on family history."""
        result = dict(intervals)
        family_text = " ".join(c.lower() for c in family_conditions)

        for _name, modifier in FAMILY_SCREENING_MODIFIERS.items():
            matched = any(
                kw in family_text for kw in modifier["family_keywords"]
            )
            if not matched:
                continue

            for test in modifier["tests"]:
                current = result.get(test)
                modified = modifier["modified_interval_months"]
                if current is not None and modified < current:
                    result[test] = modified

        return result

    def format_reminders(self, items: list[OverdueItem]) -> str:
        """Format overdue items for display."""
        if not items:
            return "All screenings are up to date."

        lines = ["OVERDUE SCREENINGS", "-" * 30]
        for item in items:
            months_over = item.days_overdue // 30
            lines.append(
                f"! {item.test_name}: last done {item.last_date} "
                f"(~{months_over} months overdue, recommended every "
                f"{item.interval_months} months)"
            )
        lines.append("")
        lines.append("Tip: Say 'pause notifications for 2 weeks' to snooze.")
        return "\n".join(lines)
