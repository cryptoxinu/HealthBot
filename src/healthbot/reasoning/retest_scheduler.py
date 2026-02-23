"""Retest reminder scheduler for abnormal lab results.

When a lab comes back abnormal, clinical practice dictates retesting
within a specific window. This module tracks which abnormal results
lack a follow-up and computes retest urgency.

All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class RetestRule:
    """When to retest an abnormal lab value."""

    canonical_name: str
    condition: str          # "high", "low", or "any"
    retest_weeks_min: int
    retest_weeks_max: int
    reason: str
    priority: str           # "urgent", "standard"
    citation: str


RETEST_RULES: tuple[RetestRule, ...] = (
    # Thyroid
    RetestRule(
        "tsh", "high", 6, 8,
        "Confirm subclinical/overt hypothyroidism",
        "standard",
        "Garber JR et al. Thyroid. 2012;22(12):1200-1235.",
    ),
    RetestRule(
        "tsh", "low", 4, 6,
        "Evaluate for hyperthyroidism",
        "standard",
        "Ross DS et al. Thyroid. 2016;26(10):1343-1421.",
    ),
    # Liver
    RetestRule(
        "alt", "high", 4, 8,
        "Confirm hepatic injury vs transient elevation",
        "standard",
        "Kwo PY et al. Am J Gastroenterol. 2017;112(1):18-35.",
    ),
    RetestRule(
        "ast", "high", 4, 8,
        "Evaluate persistent transaminase elevation",
        "standard",
        "Kwo PY et al. Am J Gastroenterol. 2017;112(1):18-35.",
    ),
    # Glycemic
    RetestRule(
        "hba1c", "high", 12, 12,
        "Monitor glycemic control",
        "standard",
        "ADA Standards of Care. Diabetes Care. 2024;47(Suppl 1).",
    ),
    RetestRule(
        "glucose", "high", 4, 8,
        "Confirm fasting glucose elevation",
        "standard",
        "ADA Standards of Care. Diabetes Care. 2024;47(Suppl 1).",
    ),
    # Electrolytes — urgent
    RetestRule(
        "potassium", "high", 1, 2,
        "Hyperkalemia — risk of arrhythmia",
        "urgent",
        "Palmer BF. N Engl J Med. 2004;351(6):585-592.",
    ),
    RetestRule(
        "potassium", "low", 1, 2,
        "Hypokalemia — risk of arrhythmia",
        "urgent",
        "Kardalas E et al. Electrolyte Blood Press. 2018;16(1):5-20.",
    ),
    RetestRule(
        "sodium", "low", 1, 2,
        "Hyponatremia — evaluate etiology",
        "urgent",
        "Spasovski G et al. Eur J Endocrinol. 2014;170(3):G1-G47.",
    ),
    RetestRule(
        "sodium", "high", 1, 2,
        "Hypernatremia — assess hydration and renal function",
        "urgent",
        "Adrogué HJ, Madias NE. N Engl J Med. 2000;342(20):1493-1499.",
    ),
    # Deficiencies
    RetestRule(
        "vitamin_d", "low", 8, 12,
        "Recheck after supplementation period",
        "standard",
        "Holick MF et al. J Clin Endocrinol Metab. 2011;96(7):1911-1930.",
    ),
    RetestRule(
        "ferritin", "low", 8, 12,
        "Recheck after iron repletion therapy",
        "standard",
        "Stoffel NU et al. Blood. 2017;130(11):1336-1344.",
    ),
    RetestRule(
        "vitamin_b12", "low", 8, 12,
        "Recheck after B12 supplementation",
        "standard",
        "Devalia V et al. Br J Haematol. 2014;166(2):241-249.",
    ),
    RetestRule(
        "iron", "low", 8, 12,
        "Recheck iron stores after repletion",
        "standard",
        "Camaschella C. N Engl J Med. 2015;372(19):1832-1843.",
    ),
    # Kidney
    RetestRule(
        "creatinine", "high", 4, 8,
        "Assess for acute vs chronic kidney injury",
        "standard",
        "KDIGO CKD Work Group. Kidney Int Suppl. 2013;3(1):1-150.",
    ),
    RetestRule(
        "egfr", "low", 4, 12,
        "Confirm reduced eGFR and stage CKD",
        "standard",
        "KDIGO CKD Work Group. Kidney Int Suppl. 2013;3(1):1-150.",
    ),
    # Lipids
    RetestRule(
        "ldl", "high", 8, 12,
        "Recheck after lifestyle or statin intervention",
        "standard",
        "Grundy SM et al. J Am Coll Cardiol. 2019;73(24):e285-e350.",
    ),
    RetestRule(
        "triglycerides", "high", 8, 12,
        "Recheck after dietary/lifestyle intervention",
        "standard",
        "Grundy SM et al. J Am Coll Cardiol. 2019;73(24):e285-e350.",
    ),
    # Uric acid
    RetestRule(
        "uric_acid", "high", 4, 8,
        "Monitor for gout risk and urate-lowering therapy",
        "standard",
        "FitzGerald JD et al. Arthritis Care Res. 2020;72(6):744-760.",
    ),
    # Hemoglobin
    RetestRule(
        "hemoglobin", "low", 4, 8,
        "Confirm anemia and evaluate response to treatment",
        "standard",
        "Camaschella C. N Engl J Med. 2015;372(19):1832-1843.",
    ),
)

# Index by (canonical_name, condition) for fast lookup
_RULE_INDEX: dict[tuple[str, str], RetestRule] = {}
for _r in RETEST_RULES:
    _RULE_INDEX[(_r.canonical_name, _r.condition)] = _r


@dataclass
class PendingRetest:
    """An abnormal result that needs a follow-up test."""

    canonical_name: str
    display_name: str
    abnormal_value: str
    abnormal_flag: str       # "H", "HH", "L", "LL"
    abnormal_date: str       # ISO date
    retest_window: str       # e.g. "4-8 weeks"
    retest_due_date: str     # ISO date (earliest)
    retest_overdue_date: str  # ISO date (latest)
    days_until_due: int      # negative = overdue
    reason: str
    priority: str            # "urgent", "standard"
    citation: str
    status: str              # "due_soon", "overdue", "urgent_overdue"


class RetestScheduler:
    """Identify abnormal results needing follow-up tests."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def get_pending_retests(self, user_id: int) -> list[PendingRetest]:
        """Find abnormal results that lack a follow-up test."""
        now = datetime.now(UTC).date()
        pending: list[PendingRetest] = []

        for rule in RETEST_RULES:
            retest = self._check_rule(rule, user_id, now)
            if retest:
                pending.append(retest)

        # Sort: urgent first, then by days_until_due (most overdue first)
        pending.sort(key=lambda p: (
            0 if p.priority == "urgent" else 1,
            p.days_until_due,
        ))
        return pending

    def _check_rule(
        self, rule: RetestRule, user_id: int, now,
    ) -> PendingRetest | None:
        """Check if a rule applies: abnormal result exists + no follow-up."""
        # Get recent results for this marker (last 12 months)
        cutoff = (now - timedelta(days=365)).isoformat()
        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=rule.canonical_name,
            start_date=cutoff,
            limit=20,
            user_id=user_id,
        )
        if not rows:
            return None

        # Sort by date descending
        rows.sort(
            key=lambda r: r.get("date_collected", ""), reverse=True,
        )

        # Find the most recent abnormal result matching the rule direction
        abnormal_row = None
        abnormal_idx = -1
        for i, row in enumerate(rows):
            flag = (row.get("flag") or "").upper()
            if not flag:
                continue
            direction = self._flag_direction(flag)
            if direction == rule.condition or rule.condition == "any":
                abnormal_row = row
                abnormal_idx = i
                break

        if not abnormal_row:
            return None

        # Check if there's a follow-up AFTER the abnormal result
        abnormal_date_str = abnormal_row.get("date_collected", "")
        if not abnormal_date_str:
            return None

        try:
            abnormal_date = datetime.fromisoformat(abnormal_date_str).date()
        except ValueError:
            return None

        # Look for any result for this marker AFTER the abnormal date
        for row in rows[:abnormal_idx]:
            followup_date_str = row.get("date_collected", "")
            if not followup_date_str:
                continue
            try:
                followup_date = datetime.fromisoformat(followup_date_str).date()
            except ValueError:
                continue
            if followup_date > abnormal_date:
                # Follow-up exists — no reminder needed
                return None

        # No follow-up found — compute retest window
        due_date = abnormal_date + timedelta(weeks=rule.retest_weeks_min)
        overdue_date = abnormal_date + timedelta(weeks=rule.retest_weeks_max)
        days_until = (due_date - now).days

        # Only alert if within 2 weeks of window or past it
        if days_until > 14:
            return None

        # Determine status
        if days_until > 0:
            status = "due_soon"
        elif (now - overdue_date).days > 0:
            status = "urgent_overdue" if rule.priority == "urgent" else "overdue"
        else:
            status = "due_soon"

        display = rule.canonical_name.replace("_", " ").title()
        value = abnormal_row.get("value", "")
        flag = abnormal_row.get("flag", "")
        unit = abnormal_row.get("unit", "")
        value_str = f"{value}" if not unit else f"{value} {unit}"

        return PendingRetest(
            canonical_name=rule.canonical_name,
            display_name=display,
            abnormal_value=value_str,
            abnormal_flag=flag,
            abnormal_date=abnormal_date_str[:10],
            retest_window=f"{rule.retest_weeks_min}-{rule.retest_weeks_max} weeks",
            retest_due_date=due_date.isoformat(),
            retest_overdue_date=overdue_date.isoformat(),
            days_until_due=days_until,
            reason=rule.reason,
            priority=rule.priority,
            citation=rule.citation,
            status=status,
        )

    @staticmethod
    def _flag_direction(flag: str) -> str:
        """Map flag to direction: 'high' or 'low'."""
        flag = flag.upper().strip()
        if flag in ("H", "HH", "HIGH", "CRITICAL_HIGH"):
            return "high"
        if flag in ("L", "LL", "LOW", "CRITICAL_LOW"):
            return "low"
        return ""


def format_retests(retests: list[PendingRetest]) -> str:
    """Format pending retests for display."""
    if not retests:
        return "No pending retests. All abnormal results have follow-ups."

    lines = ["PENDING RETESTS", "-" * 30]

    urgent = [r for r in retests if r.priority == "urgent"]
    standard = [r for r in retests if r.priority == "standard"]

    if urgent:
        lines.append("\nURGENT:")
        for r in urgent:
            flag_text = "HIGH" if r.abnormal_flag.upper() in ("H", "HH") else "LOW"
            overdue_text = ""
            if r.days_until_due < 0:
                overdue_text = f" ({abs(r.days_until_due)} days overdue)"
            lines.append(
                f"  ! {r.display_name}: {r.abnormal_value} ({flag_text}) "
                f"on {r.abnormal_date}"
            )
            lines.append(
                f"    Retest: {r.retest_window}{overdue_text}"
            )
            lines.append(f"    Reason: {r.reason}")

    if standard:
        lines.append("\nStandard:")
        for r in standard:
            flag_text = "HIGH" if r.abnormal_flag.upper() in ("H", "HH") else "LOW"
            if r.days_until_due < 0:
                status = f"{abs(r.days_until_due)} days overdue"
            elif r.days_until_due == 0:
                status = "due today"
            else:
                status = f"due in {r.days_until_due} days"
            lines.append(
                f"  * {r.display_name}: {r.abnormal_value} ({flag_text}) "
                f"on {r.abnormal_date}"
            )
            lines.append(f"    Retest: {r.retest_window} — {status}")
            lines.append(f"    Reason: {r.reason}")

    return "\n".join(lines)
