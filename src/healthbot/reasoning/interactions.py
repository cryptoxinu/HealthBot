"""Medication and supplement interaction checker.

Deterministic. No LLM. Checks active medications against a curated
knowledge base of known drug-drug, drug-supplement, drug-lab, and
drug-condition interactions.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from healthbot.data.db import HealthDB
from healthbot.reasoning.interaction_kb import (
    CONDITION_ALIASES,
    DRUG_CONDITION_INTERACTIONS,
    DRUG_LAB_INTERACTIONS,
    INTERACTIONS,
    SUBSTANCE_ALIASES,
    TIMING_RULES,
    DrugConditionInteraction,
    DrugLabInteraction,
    Interaction,
)


@dataclass(frozen=True)
class InteractionResult:
    """A detected interaction between two of the user's medications."""

    med_a_name: str
    med_b_name: str
    interaction: Interaction


@dataclass(frozen=True)
class DrugLabResult:
    """A detected drug-lab interaction for a user's medication."""

    med_name: str
    lab_name: str
    lab_value: str       # Current lab value (if available), else ""
    lab_flag: str        # "H", "L", or ""
    interaction: DrugLabInteraction


@dataclass(frozen=True)
class DrugConditionResult:
    """A detected interaction between a medication and a known condition."""

    med_name: str
    condition_name: str
    interaction: DrugConditionInteraction


@dataclass(frozen=True)
class TherapeuticCorrelation:
    """A temporal correlation between a medication start and a lab change."""

    med_name: str
    test_name: str
    before_value: float
    after_value: float
    change_pct: float
    days_after_start: int


# Pre-computed set of all substance keys from INTERACTIONS (avoid rebuilding per call)
_ALL_SUBSTANCE_KEYS: frozenset[str] = frozenset(
    key
    for ix in INTERACTIONS
    for key in (ix.substance_a, ix.substance_b)
)

# Severity ordering for sorting (highest risk first)
_SEVERITY_ORDER: dict[str, int] = {
    "contraindicated": 0,
    "major": 1,
    "moderate": 2,
    "minor": 3,
}

# Icons for formatted output
_SEVERITY_ICONS: dict[str, str] = {
    "contraindicated": "!!!! CONTRAINDICATED",
    "major": "!!! MAJOR",
    "moderate": "!! MODERATE",
    "minor": "! Minor",
}


class InteractionChecker:
    """Check active medications for known interactions.

    Uses the static knowledge base in interaction_kb.py.
    All logic is deterministic -- no LLM calls.
    """

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def check_all(self, user_id: int | None = None) -> list[InteractionResult]:
        """Check all active medications for interactions with each other.

        Returns a list of InteractionResult sorted by severity (major first).
        """
        meds = self._db.get_active_medications(user_id=user_id)
        if len(meds) < 2:
            return []

        # Build list of (original_name, kb_key) pairs
        med_entries: list[tuple[str, str]] = []
        for med in meds:
            name = med.get("name", "")
            if not name:
                continue
            kb_key = self._normalize_to_kb(name)
            if kb_key is not None:
                med_entries.append((name, kb_key))

        # Check all unique pairs
        results: list[InteractionResult] = []
        seen_pairs: set[tuple[str, str]] = set()

        for i, (name_a, key_a) in enumerate(med_entries):
            for name_b, key_b in med_entries[i + 1:]:
                if key_a == key_b:
                    continue  # Same substance class, skip
                pair = tuple(sorted((key_a, key_b)))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                interaction = self._find_interaction(key_a, key_b)
                if interaction is not None:
                    results.append(InteractionResult(
                        med_a_name=name_a,
                        med_b_name=name_b,
                        interaction=interaction,
                    ))

        # Sort by severity (most dangerous first)
        results.sort(key=lambda r: _SEVERITY_ORDER.get(r.interaction.severity, 99))
        return results

    def check_against(
        self, new_med_name: str, user_id: int | None = None,
    ) -> list[InteractionResult]:
        """Check a proposed new medication against all active medications.

        Useful for answering "Would X interact with anything I'm taking?"

        Args:
            new_med_name: Name of the medication being considered.

        Returns:
            List of InteractionResult sorted by severity.
        """
        new_key = self._normalize_to_kb(new_med_name)
        if new_key is None:
            return []

        meds = self._db.get_active_medications(user_id=user_id)
        results: list[InteractionResult] = []
        seen_keys: set[str] = set()

        for med in meds:
            name = med.get("name", "")
            if not name:
                continue
            existing_key = self._normalize_to_kb(name)
            if existing_key is None or existing_key == new_key:
                continue
            if existing_key in seen_keys:
                continue
            seen_keys.add(existing_key)

            interaction = self._find_interaction(new_key, existing_key)
            if interaction is not None:
                results.append(InteractionResult(
                    med_a_name=new_med_name,
                    med_b_name=name,
                    interaction=interaction,
                ))

        results.sort(key=lambda r: _SEVERITY_ORDER.get(r.interaction.severity, 99))
        return results

    def _normalize_to_kb(self, name: str) -> str | None:
        """Map a medication name to its canonical KB key.

        Checks SUBSTANCE_ALIASES first (case-insensitive), then checks
        if the lowered name is itself a substance key used in INTERACTIONS.

        Returns:
            The canonical KB key, or None if not found.
        """
        lower = name.lower().strip()

        # 1. Direct alias lookup
        if lower in SUBSTANCE_ALIASES:
            return SUBSTANCE_ALIASES[lower]

        # 2. Check if it's already a KB key (appears in INTERACTIONS)
        if lower in _ALL_SUBSTANCE_KEYS:
            return lower

        # 3. Word-boundary match: check if name contains a known alias as a
        #    whole word (handles "atorvastatin 40mg" -> "atorvastatin" -> "statin"
        #    but avoids false positives from short aliases like "ca" matching "calcium")
        for alias, key in SUBSTANCE_ALIASES.items():
            if re.search(rf"\b{re.escape(alias)}\b", lower):
                return key

        return None

    def _find_interaction(
        self, key_a: str, key_b: str
    ) -> Interaction | None:
        """Look up a known interaction between two KB keys.

        Checks both orderings (a,b) and (b,a) since interactions
        are stored in one direction only.
        """
        for ix in INTERACTIONS:
            if (ix.substance_a == key_a and ix.substance_b == key_b) or \
               (ix.substance_a == key_b and ix.substance_b == key_a):
                return ix
        return None

    def get_timing_advice(
        self, user_id: int | None = None,
    ) -> list[str]:
        """Get timing advice for all active medications/supplements.

        Returns standalone timing rules + interaction-specific timing.
        """
        meds = self._db.get_active_medications(user_id=user_id)
        advice: list[str] = []
        seen_keys: set[str] = set()

        for med in meds:
            name = med.get("name", "")
            if not name:
                continue
            kb_key = self._normalize_to_kb(name)
            if kb_key is None or kb_key in seen_keys:
                continue
            seen_keys.add(kb_key)

            for rule in TIMING_RULES:
                if rule.substance == kb_key:
                    advice.append(
                        f"{name}: {rule.advice} ({rule.reason})"
                    )

        return advice

    def check_drug_lab(
        self, user_id: int | None = None,
    ) -> list[DrugLabResult]:
        """Check active medications against recent lab results.

        Returns drug-lab interactions sorted by severity. Includes both:
        - Active findings: medication + abnormal lab value matching the interaction
        - Monitoring gaps: medication in KB but lab not tested recently
        """
        meds = self._db.get_active_medications(user_id=user_id)
        if not meds:
            return []

        # Build unique (med_name, kb_key) pairs
        med_entries: list[tuple[str, str]] = []
        seen_keys: set[str] = set()
        for med in meds:
            name = med.get("name", "")
            if not name:
                continue
            kb_key = self._normalize_to_kb(name)
            if kb_key is None or kb_key in seen_keys:
                continue
            seen_keys.add(kb_key)
            med_entries.append((name, kb_key))

        if not med_entries:
            return []

        # Get recent labs (last 50 results for broad coverage)
        labs = self._db.query_observations(
            record_type="lab_result", limit=50, user_id=user_id,
        )
        # Build map: canonical_name -> latest lab dict
        lab_map: dict[str, dict] = {}
        for lab in labs:
            cn = lab.get("canonical_name", "")
            if cn and cn not in lab_map:
                lab_map[cn] = lab

        results: list[DrugLabResult] = []
        for med_name, kb_key in med_entries:
            for dlx in DRUG_LAB_INTERACTIONS:
                if dlx.drug != kb_key:
                    continue

                lab_data = lab_map.get(dlx.lab, {})
                lab_value = str(lab_data.get("value", "")) if lab_data else ""
                lab_flag = str(lab_data.get("flag", "")) if lab_data else ""

                # Include if: lab is abnormal, or lab has never been tested
                # (monitoring gap), or always for "alter" effects (INR, etc.)
                is_relevant = (
                    lab_flag in ("H", "L")
                    or not lab_data
                    or dlx.effect == "alter"
                )
                if is_relevant:
                    results.append(DrugLabResult(
                        med_name=med_name,
                        lab_name=dlx.lab,
                        lab_value=lab_value,
                        lab_flag=lab_flag,
                        interaction=dlx,
                    ))

        results.sort(key=lambda r: _SEVERITY_ORDER.get(r.interaction.severity, 99))
        return results

    @staticmethod
    def format_drug_lab_results(results: list[DrugLabResult]) -> str:
        """Format drug-lab interaction results for display."""
        if not results:
            return ""

        # Split into active findings vs monitoring gaps
        findings: list[DrugLabResult] = []
        gaps: list[DrugLabResult] = []
        for r in results:
            if r.lab_value:
                findings.append(r)
            else:
                gaps.append(r)

        lines: list[str] = []

        if findings:
            lines.append(f"Drug-Lab Interactions ({len(findings)} finding(s)):\n")
            for r in findings:
                icon = _SEVERITY_ICONS.get(r.interaction.severity, "?")
                flag_label = ""
                if r.lab_flag == "H":
                    flag_label = " (HIGH)"
                elif r.lab_flag == "L":
                    flag_label = " (LOW)"
                lab_display = r.lab_name.replace("_", " ").title()
                lines.append(
                    f"[{icon}] {r.med_name} affects {lab_display}: "
                    f"{r.lab_value}{flag_label}"
                )
                lines.append(f"  {r.interaction.mechanism}")
                lines.append(f"  Action: {r.interaction.monitor}")
                if r.interaction.citation:
                    lines.append(f"  Source: {r.interaction.citation}")
                lines.append("")

        if gaps:
            lines.append(f"Monitoring Gaps ({len(gaps)} lab(s) not recently tested):\n")
            for r in gaps:
                lab_display = r.lab_name.replace("_", " ").title()
                lines.append(
                    f"  {r.med_name} -> monitor {lab_display}: "
                    f"{r.interaction.monitor}"
                )
            lines.append("")

        return "\n".join(lines)

    def check_drug_condition(
        self, user_id: int | None = None,
    ) -> list[DrugConditionResult]:
        """Check active medications against known medical conditions in LTM.

        Cross-references active medications against stored conditions
        (from onboarding, MyChart import, or conversation) using the
        drug-condition interaction knowledge base.

        Returns:
            List of DrugConditionResult sorted by severity.
        """
        meds = self._db.get_active_medications(user_id=user_id)
        if not meds:
            return []

        # Get conditions from LTM
        conditions = self._db.get_ltm_by_category(
            user_id or 0, "condition",
        )
        if not conditions:
            return []

        # Build med KB keys
        med_entries: list[tuple[str, str]] = []
        seen_keys: set[str] = set()
        for med in meds:
            name = med.get("name", "")
            if not name:
                continue
            kb_key = self._normalize_to_kb(name)
            if kb_key and kb_key not in seen_keys:
                seen_keys.add(kb_key)
                med_entries.append((name, kb_key))

        if not med_entries:
            return []

        # Parse condition names from LTM facts
        cond_entries: list[tuple[str, str]] = []
        seen_conds: set[str] = set()
        for cond in conditions:
            fact_text = cond.get("fact", "")
            cond_name = self._extract_condition_name(fact_text)
            if not cond_name:
                continue
            cond_key = self._normalize_condition(cond_name)
            if cond_key and cond_key not in seen_conds:
                seen_conds.add(cond_key)
                cond_entries.append((cond_name, cond_key))

        if not cond_entries:
            return []

        # Cross-reference
        results: list[DrugConditionResult] = []
        for med_name, med_key in med_entries:
            for cond_name, cond_key in cond_entries:
                ix = self._find_drug_condition(med_key, cond_key)
                if ix is not None:
                    results.append(DrugConditionResult(
                        med_name=med_name,
                        condition_name=cond_name,
                        interaction=ix,
                    ))

        results.sort(
            key=lambda r: _SEVERITY_ORDER.get(r.interaction.severity, 99),
        )
        return results

    def check_therapeutic_response(
        self, user_id: int | None = None,
    ) -> list[TherapeuticCorrelation]:
        """Find temporal correlations between medication starts and lab changes.

        For each medication with a start_date, compares the most recent lab
        value to the closest lab value before the medication was started.
        Only reports changes >15%.

        Returns:
            List of TherapeuticCorrelation sorted by absolute change.
        """
        from datetime import datetime

        meds = self._db.get_active_medications(user_id=user_id)
        if not meds:
            return []

        labs = self._db.query_observations(
            record_type="lab_result", limit=500, user_id=user_id,
        )
        if not labs:
            return []

        # Build lab timeline: {canonical_name: [(date, value), ...]}
        lab_timeline: dict[str, list[tuple[str, float]]] = {}
        for lab in labs:
            cn = lab.get("canonical_name", "")
            if not cn:
                continue
            date_str = lab.get("_meta", {}).get("date_effective", "")
            if not date_str:
                continue
            try:
                val = float(lab.get("value", ""))
            except (ValueError, TypeError):
                continue
            lab_timeline.setdefault(cn, []).append((date_str, val))

        # Sort each timeline by date
        for cn in lab_timeline:
            lab_timeline[cn].sort(key=lambda x: x[0])

        correlations: list[TherapeuticCorrelation] = []
        for med in meds:
            start_date = med.get("start_date", "")
            if not start_date:
                continue
            med_name = med.get("name", "")
            if not med_name:
                continue

            # Check known affected labs for this medication
            kb_key = self._normalize_to_kb(med_name)
            if kb_key is None:
                continue

            affected_labs = set()
            for dlx in DRUG_LAB_INTERACTIONS:
                if dlx.drug == kb_key:
                    affected_labs.add(dlx.lab)

            for lab_name in affected_labs:
                timeline = lab_timeline.get(lab_name)
                if not timeline:
                    continue

                # Find closest value before start_date and most recent after
                before_val = None
                after_val = None
                after_date = None
                for date_str, val in timeline:
                    if date_str < start_date:
                        before_val = val
                    elif date_str >= start_date:
                        after_val = val
                        after_date = date_str

                if before_val is None or after_val is None or before_val == 0:
                    continue

                change_pct = ((after_val - before_val) / before_val) * 100
                if abs(change_pct) < 15:
                    continue

                try:
                    d1 = datetime.strptime(start_date, "%Y-%m-%d")
                    d2 = datetime.strptime(after_date, "%Y-%m-%d")
                    days = (d2 - d1).days
                except (ValueError, TypeError):
                    days = 0

                correlations.append(TherapeuticCorrelation(
                    med_name=med_name,
                    test_name=lab_name,
                    before_value=before_val,
                    after_value=after_val,
                    change_pct=round(change_pct, 1),
                    days_after_start=days,
                ))

        # Sort by absolute change (largest first)
        correlations.sort(key=lambda c: abs(c.change_pct), reverse=True)
        return correlations

    @staticmethod
    def format_therapeutic_response(
        results: list[TherapeuticCorrelation],
    ) -> str:
        """Format therapeutic response results for display."""
        if not results:
            return ""

        lines: list[str] = [
            f"Medication-Lab Correlations ({len(results)} found):\n",
        ]
        for r in results:
            direction = "improved" if r.change_pct < 0 else "increased"
            lab_display = r.test_name.replace("_", " ").title()
            lines.append(
                f"  {r.med_name} -> {lab_display}: "
                f"{r.before_value} -> {r.after_value} "
                f"({r.change_pct:+.1f}%, {direction}) "
                f"after {r.days_after_start} days"
            )

        return "\n".join(lines)

    @staticmethod
    def _extract_condition_name(fact_text: str) -> str:
        """Extract condition name from LTM fact text.

        Handles patterns like:
          "Known condition: Type 2 Diabetes"
          "Known condition: Hypertension (status: active)"
          "Known allergy: Penicillin"  (skipped — allergies are not conditions)
        """
        if "allergy" in fact_text.lower():
            return ""
        m = re.match(r"Known condition:\s*(.+?)(?:\s*\(|$)", fact_text)
        if m:
            return m.group(1).strip()
        # Direct condition name (e.g. from older imports)
        if fact_text and "allergy" not in fact_text.lower():
            return fact_text.strip()
        return ""

    @staticmethod
    def _normalize_condition(name: str) -> str | None:
        """Map a condition name to its canonical KB key."""
        lower = name.lower().strip()
        if lower in CONDITION_ALIASES:
            return CONDITION_ALIASES[lower]
        # Partial match: check if name contains a known alias
        for alias, key in CONDITION_ALIASES.items():
            if alias in lower:
                return key
        return None

    @staticmethod
    def _find_drug_condition(
        med_key: str, cond_key: str,
    ) -> DrugConditionInteraction | None:
        """Look up a known drug-condition interaction."""
        for ix in DRUG_CONDITION_INTERACTIONS:
            if ix.drug == med_key and ix.condition == cond_key:
                return ix
        return None

    @staticmethod
    def format_drug_condition_results(
        results: list[DrugConditionResult],
    ) -> str:
        """Format drug-condition interaction results for display."""
        if not results:
            return ""

        lines: list[str] = [
            f"Drug-Condition Interactions ({len(results)} found):\n",
        ]
        for r in results:
            icon = _SEVERITY_ICONS.get(r.interaction.severity, "?")
            lines.append(f"[{icon}] {r.med_name} + {r.condition_name}")
            lines.append(f"  {r.interaction.mechanism}")
            lines.append(f"  Recommendation: {r.interaction.recommendation}")
            if r.interaction.citation:
                lines.append(f"  Source: {r.interaction.citation}")
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def format_results(results: list[InteractionResult]) -> str:
        """Format interaction results for display.

        Uses severity icons:
            !!!! CONTRAINDICATED
            !!!  MAJOR
            !!   MODERATE
            !    Minor

        Args:
            results: List of InteractionResult to format.

        Returns:
            Formatted string for display in chat.
        """
        if not results:
            return "No known interactions detected among your active medications."

        lines: list[str] = []
        lines.append(f"Found {len(results)} interaction(s):\n")

        for r in results:
            icon = _SEVERITY_ICONS.get(r.interaction.severity, "?")
            lines.append(f"[{icon}] {r.med_a_name} + {r.med_b_name}")
            lines.append(f"  Mechanism: {r.interaction.mechanism}")
            lines.append(f"  Recommendation: {r.interaction.recommendation}")
            if r.interaction.timing_advice:
                lines.append(f"  Timing: {r.interaction.timing_advice}")
            lines.append(f"  Evidence: {r.interaction.evidence}")
            if r.interaction.citations:
                lines.append(f"  Source: {r.interaction.citations[0]}")
            lines.append("")

        return "\n".join(lines)
