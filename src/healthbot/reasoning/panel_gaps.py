"""Lab panel gap detection and companion test recommendations.

Identifies incomplete lab panels and suggests missing companion tests
based on clinical context. All recommendations phrased as 'consider discussing.'
All logic is deterministic -- no LLM calls.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB

# ---------------------------------------------------------------------------
# Panel definitions
# ---------------------------------------------------------------------------

PANELS: dict[str, list[str]] = {
    "lipid_panel": [
        "cholesterol_total", "ldl", "hdl", "triglycerides",
    ],
    "cmp": [
        "glucose", "bun", "creatinine", "sodium", "potassium", "chloride",
        "carbon_dioxide", "calcium", "total_protein", "albumin", "bilirubin",
        "alkaline_phosphatase", "ast", "alt",
    ],
    "cbc": [
        "wbc", "rbc", "hemoglobin", "hematocrit", "platelets",
        "mcv", "mch", "mchc", "rdw",
    ],
    "thyroid_panel": [
        "tsh", "free_t4", "free_t3",
    ],
    "iron_studies": [
        "iron", "ferritin", "tibc", "transferrin",
    ],
    "metabolic_diabetes": [
        "glucose", "hba1c",
    ],
    "liver_panel": [
        "alt", "ast", "alkaline_phosphatase", "bilirubin", "albumin",
    ],
    "kidney_panel": [
        "bun", "creatinine", "egfr",
    ],
}

# ---------------------------------------------------------------------------
# Conditional rules
# ---------------------------------------------------------------------------

_FLAGGED_TRIAGE = {"urgent", "critical", "watch"}


@dataclass
class ConditionalRule:
    """A rule that triggers companion-test recommendations when a lab is abnormal."""

    trigger_test: str
    recommended: list[str]
    description: str


CONDITIONAL_RULES: list[ConditionalRule] = [
    ConditionalRule(
        trigger_test="ferritin",
        recommended=["iron", "tibc", "transferrin", "hemoglobin", "mcv", "rdw"],
        description="Low ferritin may indicate iron deficiency",
    ),
    ConditionalRule(
        trigger_test="hba1c",
        recommended=["glucose", "creatinine", "egfr", "albumin"],
        description="Elevated HbA1c suggests monitoring metabolic/kidney function",
    ),
    ConditionalRule(
        trigger_test="tsh",
        recommended=["free_t3", "free_t4"],
        description="Abnormal TSH warrants full thyroid evaluation",
    ),
    ConditionalRule(
        trigger_test="alt",
        recommended=["ast", "alkaline_phosphatase", "bilirubin", "albumin"],
        description="Elevated ALT suggests checking full liver panel",
    ),
    ConditionalRule(
        trigger_test="creatinine",
        recommended=["bun", "egfr", "potassium", "calcium"],
        description="Elevated creatinine suggests monitoring kidney function",
    ),
    ConditionalRule(
        trigger_test="hemoglobin",
        recommended=["iron", "ferritin", "vitamin_b12", "folate", "mcv", "rdw"],
        description="Low hemoglobin warrants anemia workup",
    ),
    ConditionalRule(
        trigger_test="vitamin_d",
        recommended=["calcium", "phosphorus", "magnesium"],
        description="Low vitamin D may affect mineral metabolism",
    ),
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class PanelGap:
    """A partially-completed lab panel with missing tests."""

    panel_name: str
    present: list[str]
    missing: list[str]


@dataclass
class ConditionalGap:
    """A companion-test recommendation triggered by an abnormal result."""

    rule: ConditionalRule
    trigger_value: str
    trigger_flag: str
    missing_tests: list[str]


@dataclass
class GapReport:
    """Combined output of panel gap detection."""

    panel_gaps: list[PanelGap] = field(default_factory=list)
    conditional_gaps: list[ConditionalGap] = field(default_factory=list)

    @property
    def has_gaps(self) -> bool:
        return bool(self.panel_gaps or self.conditional_gaps)


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class PanelGapDetector:
    """Detect incomplete lab panels and conditional companion-test needs."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def _get_existing_tests(self, user_id: int | None = None) -> set[str]:
        """Query distinct canonical_name from lab results in the DB."""
        sql = (
            "SELECT DISTINCT canonical_name FROM observations "
            "WHERE record_type = 'lab_result' AND canonical_name != ''"
        )
        params: list = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        rows = self._db.conn.execute(sql, params).fetchall()
        return {
            r["canonical_name"] if isinstance(r, sqlite3.Row) else r[0]
            for r in rows
        }

    def detect_panel_gaps(self, existing: set[str]) -> list[PanelGap]:
        """Find panels where some -- but not all -- tests have been done."""
        gaps: list[PanelGap] = []
        for panel_name, tests in PANELS.items():
            present = [t for t in tests if t in existing]
            missing = [t for t in tests if t not in existing]
            # Only flag partial panels (have some but not all)
            if present and missing:
                gaps.append(PanelGap(
                    panel_name=panel_name,
                    present=present,
                    missing=missing,
                ))
        return gaps

    def detect_conditional_gaps(
        self, existing: set[str], user_id: int | None = None,
    ) -> list[ConditionalGap]:
        """Check conditional rules for abnormal triggers with missing companions."""
        conditional: list[ConditionalGap] = []

        for rule in CONDITIONAL_RULES:
            # Get latest result for the trigger test
            rows = self._db.query_observations(
                record_type="lab_result",
                canonical_name=rule.trigger_test,
                limit=1,
                user_id=user_id,
            )
            if not rows:
                continue

            latest = rows[0]
            meta = latest.get("_meta", {})
            triage_level = meta.get("triage_level", "normal")
            flag = latest.get("flag", "")

            # Check if flagged (abnormal triage or H/L flag)
            is_flagged = (
                triage_level in _FLAGGED_TRIAGE
                or any(c in (flag or "").upper() for c in ("H", "L"))
            )
            if not is_flagged:
                continue

            # Which recommended tests are missing?
            missing = [t for t in rule.recommended if t not in existing]
            if not missing:
                continue

            value_str = str(latest.get("value", ""))
            unit = latest.get("unit", "")
            trigger_display = f"{value_str} {unit}".strip()

            conditional.append(ConditionalGap(
                rule=rule,
                trigger_value=trigger_display,
                trigger_flag=flag or triage_level,
                missing_tests=missing,
            ))

        return conditional

    def detect(self, user_id: int | None = None) -> GapReport:
        """Run full gap detection and return a report."""
        existing = self._get_existing_tests(user_id=user_id)
        return GapReport(
            panel_gaps=self.detect_panel_gaps(existing),
            conditional_gaps=self.detect_conditional_gaps(
                existing, user_id=user_id,
            ),
        )

    def format_gaps(self, report: GapReport) -> str:
        """Format a GapReport for display with 'consider discussing' phrasing."""
        if not report.has_gaps:
            return (
                "No lab panel gaps detected. "
                "Your panels appear complete based on available records."
            )

        lines: list[str] = ["LAB PANEL GAP ANALYSIS", "=" * 30]

        if report.panel_gaps:
            lines.append("")
            lines.append("INCOMPLETE PANELS")
            lines.append("-" * 20)
            for gap in report.panel_gaps:
                panel_label = gap.panel_name.replace("_", " ").title()
                lines.append(f"\n{panel_label}:")
                lines.append(f"  Have: {', '.join(gap.present)}")
                lines.append(f"  Missing: {', '.join(gap.missing)}")
                lines.append(
                    "  -> Consider discussing these missing tests "
                    "with your provider."
                )

        if report.conditional_gaps:
            lines.append("")
            lines.append("COMPANION TEST RECOMMENDATIONS")
            lines.append("-" * 20)
            for cg in report.conditional_gaps:
                lines.append(
                    f"\n{cg.rule.trigger_test.upper()} "
                    f"({cg.trigger_value}, flag: {cg.trigger_flag}):"
                )
                lines.append(f"  {cg.rule.description}")
                lines.append(
                    f"  Consider discussing: {', '.join(cg.missing_tests)}"
                )

        return "\n".join(lines)
