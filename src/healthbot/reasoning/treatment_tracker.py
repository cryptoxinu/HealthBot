"""Treatment effectiveness tracker.

Monitors whether a medication is achieving its expected biomarker effect.
Deterministic. No LLM. Uses a static knowledge base mapping drugs to
target biomarkers and expected changes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

from healthbot.data.db import HealthDB
from healthbot.reasoning.interaction_kb import SUBSTANCE_ALIASES

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class DrugBiomarkerLink:
    """Maps a drug class to the biomarker it's expected to affect."""

    drug_key: str               # KB key (e.g., "statin")
    target_biomarker: str       # canonical lab name (e.g., "ldl")
    expected_direction: str     # "decrease" or "increase"
    typical_change_pct: float   # e.g., -30.0 for statins on LDL
    typical_weeks: int          # time to see effect
    citation: str


DRUG_BIOMARKER_LINKS: tuple[DrugBiomarkerLink, ...] = (
    # Statins
    DrugBiomarkerLink(
        "statin", "ldl", "decrease", -30.0, 6,
        "Weng TC et al. J Clin Pharm Ther. 2010;35(2):139-151.",
    ),
    DrugBiomarkerLink(
        "statin", "cholesterol_total", "decrease", -20.0, 6,
        "Weng TC et al. J Clin Pharm Ther. 2010;35(2):139-151.",
    ),
    DrugBiomarkerLink(
        "statin", "triglycerides", "decrease", -15.0, 6,
        "Weng TC et al. J Clin Pharm Ther. 2010;35(2):139-151.",
    ),
    # Metformin
    DrugBiomarkerLink(
        "metformin", "hba1c", "decrease", -15.0, 12,
        "Hirst JA et al. Diabet Med. 2012;29(11):1366-1374.",
    ),
    DrugBiomarkerLink(
        "metformin", "glucose", "decrease", -20.0, 8,
        "Hirst JA et al. Diabet Med. 2012;29(11):1366-1374.",
    ),
    # Thyroid
    DrugBiomarkerLink(
        "levothyroxine", "tsh", "decrease", -50.0, 6,
        "Jonklaas J et al. Thyroid. 2014;24(12):1670-1751.",
    ),
    # ACE inhibitors / ARBs (expected mild creatinine rise — not alarming)
    DrugBiomarkerLink(
        "ace_inhibitor", "creatinine", "increase", 10.0, 2,
        "Bakris GL, Weir MR. Am J Med. 2000;109(2):164-167.",
    ),
    DrugBiomarkerLink(
        "arb", "creatinine", "increase", 10.0, 2,
        "Bakris GL, Weir MR. Am J Med. 2000;109(2):164-167.",
    ),
    # Supplements
    DrugBiomarkerLink(
        "vitamin_d", "vitamin_d", "increase", 50.0, 8,
        "Tripkovic L et al. Am J Clin Nutr. 2012;95(6):1357-1364.",
    ),
    DrugBiomarkerLink(
        "iron", "ferritin", "increase", 100.0, 12,
        "Stoffel NU et al. Lancet Haematol. 2017;4(11):e524-e533.",
    ),
    DrugBiomarkerLink(
        "iron", "hemoglobin", "increase", 10.0, 8,
        "Stoffel NU et al. Lancet Haematol. 2017;4(11):e524-e533.",
    ),
    DrugBiomarkerLink(
        "vitamin_b12", "vitamin_b12", "increase", 80.0, 8,
        "Devalia V et al. Br J Haematol. 2014;166(4):496-513.",
    ),
    DrugBiomarkerLink(
        "calcium", "calcium", "increase", 5.0, 8,
        "Reid IR et al. BMJ. 2015;351:h4580.",
    ),
    # Antihypertensives — blood pressure not a lab, but RHR tracked
    DrugBiomarkerLink(
        "beta_blocker", "rhr", "decrease", -15.0, 2,
        "Frishman WH. N Engl J Med. 1998;339(24):1759-1765.",
    ),
)


@dataclass
class EffectivenessReport:
    """Assessment of a drug's effectiveness on a specific biomarker."""

    med_name: str               # user's actual medication name
    drug_key: str               # KB key
    biomarker: str              # canonical lab name
    start_date: str             # medication start date (ISO)
    baseline_value: float       # lab value near start
    baseline_date: str          # date of baseline measurement
    current_value: float        # most recent lab value
    current_date: str           # date of current measurement
    pct_change: float           # actual percent change
    expected_pct: float         # expected percent change
    weeks_elapsed: int          # weeks since medication start
    typical_weeks: int          # typical weeks to see effect
    verdict: str                # "effective", "very_effective", "insufficient",
    #                             "too_early", "worsening", "no_data"
    citation: str


class TreatmentTracker:
    """Track whether medications are achieving their expected effects."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def assess_all(self, user_id: int) -> list[EffectivenessReport]:
        """Assess effectiveness of all active medications."""
        meds = self._db.get_active_medications(user_id=user_id)
        reports: list[EffectivenessReport] = []
        for med in meds:
            reports.extend(self._assess_single_med(med, user_id))
        return reports

    def _assess_single_med(
        self, med: dict, user_id: int,
    ) -> list[EffectivenessReport]:
        """Assess a single medication against all linked biomarkers."""
        med_name = med.get("name", "")
        start_date_raw = med.get("start_date", "")
        if not med_name or not start_date_raw:
            return []

        drug_key = self._resolve_drug_key(med_name)
        if not drug_key:
            return []

        try:
            start_date = date.fromisoformat(str(start_date_raw))
        except (ValueError, TypeError):
            return []

        links = [lnk for lnk in DRUG_BIOMARKER_LINKS if lnk.drug_key == drug_key]
        if not links:
            return []

        reports: list[EffectivenessReport] = []
        for link in links:
            report = self._evaluate_link(
                med_name, drug_key, start_date, link, user_id,
            )
            if report:
                reports.append(report)
        return reports

    def _evaluate_link(
        self,
        med_name: str,
        drug_key: str,
        start_date: date,
        link: DrugBiomarkerLink,
        user_id: int,
    ) -> EffectivenessReport | None:
        """Evaluate one drug-biomarker link."""
        baseline = self._find_baseline(
            link.target_biomarker, start_date, user_id,
        )
        current = self._find_current(link.target_biomarker, user_id)
        if not baseline or not current:
            return None
        if baseline["date"] == current["date"]:
            return None  # same measurement, can't compare

        baseline_val = baseline["value"]
        current_val = current["value"]
        if baseline_val == 0:
            return None  # avoid division by zero

        pct_change = ((current_val - baseline_val) / abs(baseline_val)) * 100
        weeks_elapsed = max(1, (date.today() - start_date).days // 7)

        verdict = self._compute_verdict(
            pct_change, link.expected_direction,
            link.typical_change_pct, weeks_elapsed, link.typical_weeks,
        )

        return EffectivenessReport(
            med_name=med_name,
            drug_key=drug_key,
            biomarker=link.target_biomarker,
            start_date=start_date.isoformat(),
            baseline_value=baseline_val,
            baseline_date=baseline["date"],
            current_value=current_val,
            current_date=current["date"],
            pct_change=round(pct_change, 1),
            expected_pct=link.typical_change_pct,
            weeks_elapsed=weeks_elapsed,
            typical_weeks=link.typical_weeks,
            verdict=verdict,
            citation=link.citation,
        )

    def _find_baseline(
        self, canonical_name: str, start_date: date, user_id: int,
    ) -> dict | None:
        """Find lab value closest to (before or at) medication start date.

        Looks within a 90-day window before start_date, plus 7 days after
        (labs might be drawn just after starting).
        """
        window_start = (start_date - timedelta(days=90)).isoformat()
        window_end = (start_date + timedelta(days=7)).isoformat()
        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=canonical_name,
            start_date=window_start,
            end_date=window_end,
            limit=10,
            user_id=user_id,
        )
        return self._best_numeric(rows, target_date=start_date)

    def _find_current(
        self, canonical_name: str, user_id: int,
    ) -> dict | None:
        """Find the most recent lab value for a biomarker."""
        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=canonical_name,
            limit=1,
            user_id=user_id,
        )
        return self._best_numeric(rows)

    def _best_numeric(
        self, rows: list[dict], target_date: date | None = None,
    ) -> dict | None:
        """Extract best numeric value from query results.

        If target_date given, pick the row closest to that date.
        Otherwise, pick the first (most recent).
        """
        candidates: list[dict] = []
        for row in rows:
            val = row.get("value")
            if val is None:
                continue
            try:
                numeric = float(val)
            except (ValueError, TypeError):
                continue
            row_date = row.get("date_collected", row.get("_date_effective", ""))
            candidates.append({"value": numeric, "date": str(row_date)})

        if not candidates:
            return None

        if target_date is not None:
            # Pick closest to target_date
            def distance(c: dict) -> int:
                try:
                    d = date.fromisoformat(c["date"])
                    return abs((d - target_date).days)
                except (ValueError, TypeError):
                    return 9999
            candidates.sort(key=distance)

        return candidates[0]

    @staticmethod
    def _resolve_drug_key(med_name: str) -> str:
        """Resolve a medication name to its KB drug key."""
        name_lower = med_name.lower().strip()
        # Try full name first
        if name_lower in SUBSTANCE_ALIASES:
            return SUBSTANCE_ALIASES[name_lower]
        parts = name_lower.split()
        # Try multi-word combinations (e.g., "vitamin d", "fish oil")
        for i in range(len(parts)):
            for j in range(i + 1, min(i + 4, len(parts) + 1)):
                phrase = " ".join(parts[i:j]).rstrip(",;.")
                if phrase in SUBSTANCE_ALIASES:
                    return SUBSTANCE_ALIASES[phrase]
        # Try single tokens (e.g., "atorvastatin" from "atorvastatin 40mg")
        for token in parts:
            clean = token.rstrip(",;.")
            if clean in SUBSTANCE_ALIASES:
                return SUBSTANCE_ALIASES[clean]
        return ""

    @staticmethod
    def _compute_verdict(
        pct_change: float,
        expected_direction: str,
        expected_pct: float,
        weeks_elapsed: int,
        typical_weeks: int,
    ) -> str:
        """Determine treatment verdict."""
        if weeks_elapsed < typical_weeks:
            # Check if already showing strong results
            if expected_direction == "decrease" and pct_change <= expected_pct * 0.7:
                return "effective"
            if expected_direction == "increase" and pct_change >= expected_pct * 0.7:
                return "effective"
            return "too_early"

        # After typical timeframe: evaluate
        if expected_direction == "decrease":
            if pct_change > 5:
                return "worsening"
            if pct_change <= expected_pct * 1.3:  # exceeded expectation
                return "very_effective"
            if pct_change <= expected_pct * 0.7:  # at least 70% of expected
                return "effective"
            return "insufficient"
        else:  # increase
            if pct_change < -5:
                return "worsening"
            if pct_change >= expected_pct * 1.3:
                return "very_effective"
            if pct_change >= expected_pct * 0.7:
                return "effective"
            return "insufficient"


def format_effectiveness(reports: list[EffectivenessReport]) -> str:
    """Format effectiveness reports for Telegram display."""
    if not reports:
        return (
            "TREATMENT EFFECTIVENESS\n"
            "=" * 25 + "\n\n"
            "No trackable medications found. To track effectiveness, "
            "ensure medications have a start_date and matching lab data."
        )

    lines = ["TREATMENT EFFECTIVENESS", "=" * 25]

    verdict_icons = {
        "very_effective": "+++ ",
        "effective": "++ ",
        "insufficient": "- ",
        "too_early": "~ ",
        "worsening": "!! ",
    }
    verdict_labels = {
        "very_effective": "Exceeding expectations",
        "effective": "Working as expected",
        "insufficient": "Below expected improvement",
        "too_early": "Too early to assess",
        "worsening": "Moving in wrong direction",
    }

    for r in reports:
        icon = verdict_icons.get(r.verdict, "")
        label = verdict_labels.get(r.verdict, r.verdict)
        biomarker = r.biomarker.replace("_", " ").title()

        lines.append(f"\n{icon}{r.med_name} -> {biomarker}: {label}")
        lines.append(f"  Baseline: {r.baseline_value} ({r.baseline_date})")
        lines.append(f"  Current:  {r.current_value} ({r.current_date})")
        lines.append(f"  Change:   {r.pct_change:+.1f}% (expected ~{r.expected_pct:+.1f}%)")
        lines.append(f"  Duration: {r.weeks_elapsed} weeks (typical: {r.typical_weeks})")
        lines.append(f"  Source:   {r.citation}")

    return "\n".join(lines)
