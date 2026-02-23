"""Medication side effect monitoring.

Deterministic. No LLM. Maps active medications to known side effects
and checks if any lab markers or logged symptoms suggest a side effect
is occurring. Also provides a "watch list" of what the system monitors.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from healthbot.data.db import HealthDB
from healthbot.reasoning.interaction_kb import SUBSTANCE_ALIASES

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class SideEffectProfile:
    """A known side effect of a medication class."""

    drug_key: str           # KB key (e.g., "statin")
    effect: str             # "muscle pain/myalgia"
    frequency: str          # "common" (>10%), "occasional" (1-10%), "rare"
    onset_weeks: int        # typical time to onset
    lab_marker: str         # canonical lab name to monitor, or ""
    lab_direction: str      # "increase" or "decrease" or ""
    monitoring_note: str    # what to watch for
    citation: str


SIDE_EFFECT_PROFILES: tuple[SideEffectProfile, ...] = (
    # Statins
    SideEffectProfile(
        "statin", "muscle pain/myalgia", "common", 4,
        "ck", "increase",
        "Check CK if muscle symptoms. >5x ULN: stop statin.",
        "Thompson PD et al. JACC. 2016;67(24):2395-2410.",
    ),
    SideEffectProfile(
        "statin", "liver enzyme elevation", "occasional", 12,
        "alt", "increase",
        "Check ALT. >3x ULN: consider stopping.",
        "Bjornsson E et al. Hepatology. 2012;56(1):17-27.",
    ),
    # ACE inhibitors
    SideEffectProfile(
        "ace_inhibitor", "dry cough", "common", 4,
        "", "",
        "Persistent dry cough in 10-15%. Switch to ARB if bothersome.",
        "Dicpinigaitis PV. Chest. 2006;129(1 Suppl):169S-173S.",
    ),
    SideEffectProfile(
        "ace_inhibitor", "hyperkalemia", "occasional", 2,
        "potassium", "increase",
        "Check potassium 1-2 weeks after start, then every 6-12 mo.",
        "Palmer BF. N Engl J Med. 2004;351(6):585-592.",
    ),
    SideEffectProfile(
        "ace_inhibitor", "acute kidney injury", "occasional", 2,
        "creatinine", "increase",
        "Check creatinine 1-2 weeks after start. >30% rise: stop.",
        "Bakris GL, Weir MR. Am J Med. 2000;109(2):164-167.",
    ),
    # ARBs (similar to ACE but no cough)
    SideEffectProfile(
        "arb", "hyperkalemia", "occasional", 2,
        "potassium", "increase",
        "Check potassium 1-2 weeks after start, then periodically.",
        "Palmer BF. N Engl J Med. 2004;351(6):585-592.",
    ),
    # Metformin
    SideEffectProfile(
        "metformin", "B12 deficiency", "occasional", 52,
        "vitamin_b12", "decrease",
        "Check B12 annually. Supplement if <300 pg/mL.",
        "Aroda VR et al. J Clin Endocrinol Metab. 2016;101(4):1754.",
    ),
    SideEffectProfile(
        "metformin", "lactic acidosis (rare)", "rare", 4,
        "creatinine", "increase",
        "Contraindicated if eGFR <30. Monitor renal function.",
        "DeFronzo R et al. Diabetes Care. 2016;39(7):1104-1110.",
    ),
    # PPIs
    SideEffectProfile(
        "ppi", "magnesium depletion", "occasional", 52,
        "magnesium", "decrease",
        "Check magnesium annually on chronic PPI use.",
        "Hess MW et al. Aliment Pharmacol Ther. 2012;36(5):405.",
    ),
    SideEffectProfile(
        "ppi", "B12 malabsorption", "occasional", 104,
        "vitamin_b12", "decrease",
        "Check B12 if on PPI >2 years.",
        "Lam JR et al. JAMA. 2013;310(22):2435-2442.",
    ),
    # SSRIs
    SideEffectProfile(
        "ssri", "hyponatremia (SIADH)", "occasional", 4,
        "sodium", "decrease",
        "Check sodium 2-4 weeks after starting, especially elderly.",
        "Jacob S, Bhatt RR. Am J Med. 2006;119(11):893-901.",
    ),
    SideEffectProfile(
        "ssri", "bleeding risk", "rare", 4,
        "platelets", "decrease",
        "Caution with concurrent anticoagulants/NSAIDs.",
        "Andrade C et al. J Clin Psychiatry. 2010;71(12):1565.",
    ),
    # Thiazides
    SideEffectProfile(
        "thiazide", "hyponatremia", "occasional", 2,
        "sodium", "decrease",
        "Check sodium 1-2 weeks after start and periodically.",
        "Liamis G et al. Am J Kidney Dis. 2008;52(1):144-153.",
    ),
    SideEffectProfile(
        "thiazide", "hypokalemia", "common", 2,
        "potassium", "decrease",
        "Check potassium regularly. Supplement if low.",
        "Ellison DH, Loffing J. Hypertension. 2009;54(3):313.",
    ),
    # Corticosteroids
    SideEffectProfile(
        "corticosteroid", "hyperglycemia", "common", 1,
        "glucose", "increase",
        "Monitor glucose. Diabetics need dose adjustment.",
        "Hwang JL, Weiss RE. Endocrinol Metab Clin. 2014;43(1):75.",
    ),
    SideEffectProfile(
        "corticosteroid", "osteoporosis risk", "common", 12,
        "calcium", "decrease",
        "Supplement calcium + vitamin D. DEXA if >3 months.",
        "Van Staa TP et al. J Bone Miner Res. 2000;15(6):993.",
    ),
    # Lithium
    SideEffectProfile(
        "lithium", "hypothyroidism", "common", 26,
        "tsh", "increase",
        "Check TSH every 6-12 months on lithium.",
        "Bocchetta A et al. J Endocrinol Invest. 2001;24(5):334.",
    ),
    SideEffectProfile(
        "lithium", "nephrotoxicity", "occasional", 52,
        "creatinine", "increase",
        "Check creatinine/eGFR every 6 months.",
        "Gitlin M. Int J Bipolar Disord. 2016;4(1):27.",
    ),
    # Beta blockers
    SideEffectProfile(
        "beta_blocker", "hyperglycemia masking", "occasional", 8,
        "glucose", "increase",
        "Beta-blockers can mask hypoglycemia symptoms in diabetics.",
        "Dungan K et al. Endocrinol Metab Clin. 2009;38(4):687.",
    ),
)


@dataclass
class SideEffectWatch:
    """A side effect the system is monitoring for."""

    med_name: str           # user's actual med name
    drug_key: str
    effect: str
    frequency: str
    lab_marker: str
    monitoring_note: str
    last_checked: str       # date of most recent lab, or "never"
    months_since: int       # months since last lab check, or -1


@dataclass
class SideEffectAlert:
    """A detected potential side effect signal."""

    med_name: str
    drug_key: str
    effect: str
    lab_marker: str
    lab_value: str
    lab_flag: str           # "H" or "L"
    monitoring_note: str
    citation: str
    severity: str           # "watch" or "urgent"


class SideEffectMonitor:
    """Monitor active medications for known side effects."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def get_watch_list(self, user_id: int) -> list[SideEffectWatch]:
        """Get the full list of side effects being monitored."""
        meds = self._db.get_active_medications(user_id=user_id)
        watches: list[SideEffectWatch] = []

        for med in meds:
            med_name = med.get("name", "")
            drug_key = self._resolve_drug_key(med_name)
            if not drug_key:
                continue

            profiles = [
                p for p in SIDE_EFFECT_PROFILES
                if p.drug_key == drug_key
            ]
            for prof in profiles:
                last_checked = "never"
                months_since = -1
                if prof.lab_marker:
                    lc, ms = self._last_lab_check(
                        prof.lab_marker, user_id,
                    )
                    last_checked = lc
                    months_since = ms

                watches.append(SideEffectWatch(
                    med_name=med_name,
                    drug_key=drug_key,
                    effect=prof.effect,
                    frequency=prof.frequency,
                    lab_marker=prof.lab_marker,
                    monitoring_note=prof.monitoring_note,
                    last_checked=last_checked,
                    months_since=months_since,
                ))

        return watches

    def check_active_concerns(
        self, user_id: int,
    ) -> list[SideEffectAlert]:
        """Check if any side effects are currently manifesting."""
        meds = self._db.get_active_medications(user_id=user_id)
        alerts: list[SideEffectAlert] = []

        for med in meds:
            med_name = med.get("name", "")
            drug_key = self._resolve_drug_key(med_name)
            if not drug_key:
                continue

            profiles = [
                p for p in SIDE_EFFECT_PROFILES
                if p.drug_key == drug_key and p.lab_marker
            ]
            for prof in profiles:
                alert = self._check_lab_marker(
                    med_name, drug_key, prof, user_id,
                )
                if alert:
                    alerts.append(alert)

        return alerts

    def _check_lab_marker(
        self,
        med_name: str,
        drug_key: str,
        prof: SideEffectProfile,
        user_id: int,
    ) -> SideEffectAlert | None:
        """Check if a specific lab marker suggests a side effect."""
        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=prof.lab_marker,
            limit=1,
            user_id=user_id,
        )
        if not rows:
            return None

        row = rows[0]
        flag = row.get("flag", "")
        value = row.get("value", "")

        # Check if flag matches expected direction
        if prof.lab_direction == "increase" and flag in ("H", "HH"):
            severity = "urgent" if flag == "HH" else "watch"
            return SideEffectAlert(
                med_name=med_name,
                drug_key=drug_key,
                effect=prof.effect,
                lab_marker=prof.lab_marker,
                lab_value=str(value),
                lab_flag=flag,
                monitoring_note=prof.monitoring_note,
                citation=prof.citation,
                severity=severity,
            )
        if prof.lab_direction == "decrease" and flag in ("L", "LL"):
            severity = "urgent" if flag == "LL" else "watch"
            return SideEffectAlert(
                med_name=med_name,
                drug_key=drug_key,
                effect=prof.effect,
                lab_marker=prof.lab_marker,
                lab_value=str(value),
                lab_flag=flag,
                monitoring_note=prof.monitoring_note,
                citation=prof.citation,
                severity=severity,
            )
        return None

    def _last_lab_check(
        self, canonical_name: str, user_id: int,
    ) -> tuple[str, int]:
        """Find when the lab marker was last checked.

        Returns (date_str, months_since) or ("never", -1).
        """
        from datetime import date

        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=canonical_name,
            limit=1,
            user_id=user_id,
        )
        if not rows:
            return "never", -1

        date_str = rows[0].get(
            "date_collected",
            rows[0].get("_date_effective", ""),
        )
        try:
            lab_date = date.fromisoformat(str(date_str))
            months = (date.today() - lab_date).days // 30
            return str(date_str), months
        except (ValueError, TypeError):
            return str(date_str), -1

    @staticmethod
    def _resolve_drug_key(med_name: str) -> str:
        """Resolve a medication name to its KB drug key."""
        name_lower = med_name.lower().strip()
        if name_lower in SUBSTANCE_ALIASES:
            return SUBSTANCE_ALIASES[name_lower]
        parts = name_lower.split()
        for i in range(len(parts)):
            for j in range(i + 1, min(i + 4, len(parts) + 1)):
                phrase = " ".join(parts[i:j]).rstrip(",;.")
                if phrase in SUBSTANCE_ALIASES:
                    return SUBSTANCE_ALIASES[phrase]
        for token in parts:
            clean = token.rstrip(",;.")
            if clean in SUBSTANCE_ALIASES:
                return SUBSTANCE_ALIASES[clean]
        return ""


def format_watch_list(watches: list[SideEffectWatch]) -> str:
    """Format the side effect watch list for Telegram."""
    if not watches:
        return (
            "SIDE EFFECT MONITORING\n"
            "=" * 25 + "\n\n"
            "No medications with known side effect profiles found."
        )

    lines = ["SIDE EFFECT MONITORING", "=" * 25]

    freq_icon = {
        "common": "[common]",
        "occasional": "[occasional]",
        "rare": "[rare]",
    }

    current_med = ""
    for w in watches:
        if w.med_name != current_med:
            current_med = w.med_name
            lines.append(f"\n{w.med_name}:")

        icon = freq_icon.get(w.frequency, "")
        lab_info = ""
        if w.lab_marker:
            marker = w.lab_marker.replace("_", " ").title()
            if w.last_checked == "never":
                lab_info = f" | {marker}: never checked"
            elif w.months_since > 12:
                lab_info = f" | {marker}: {w.months_since}mo ago (overdue)"
            else:
                lab_info = f" | {marker}: last {w.last_checked}"

        lines.append(f"  {icon} {w.effect}{lab_info}")
        lines.append(f"    {w.monitoring_note}")

    return "\n".join(lines)


def format_alerts(alerts: list[SideEffectAlert]) -> str:
    """Format side effect alerts for Telegram."""
    if not alerts:
        return ""

    lines = ["\nPOTENTIAL SIDE EFFECT SIGNALS:", "-" * 30]

    for a in alerts:
        icon = "!!" if a.severity == "urgent" else "!"
        marker = a.lab_marker.replace("_", " ").title()
        flag_text = "HIGH" if a.lab_flag in ("H", "HH") else "LOW"
        lines.append(
            f"{icon} {a.med_name}: {a.effect}"
        )
        lines.append(
            f"   {marker} is {flag_text} ({a.lab_value})"
        )
        lines.append(f"   {a.monitoring_note}")
        lines.append(f"   Source: {a.citation}")

    return "\n".join(lines)
