"""Extended conversation context: substance knowledge + medication timelines.

Appends structured substance profiles and temporal medication data
to the Claude CLI prompt. Split from conversation_context.py.
"""
from __future__ import annotations

import json
import logging
import re

from healthbot.reasoning.interaction_kb import SUBSTANCE_ALIASES

logger = logging.getLogger("healthbot")

# Minimum alias length to avoid false positives during scan
_MIN_ALIAS_LEN = 4

# Common English words that happen to be substance aliases — only match these
# when the message has explicit medical/substance context.
_STOPWORDS: set[str] = {"same", "iron", "ace", "amp", "milk", "soy", "oral"}

# Context words that signal the user is discussing substances/medications.
_CONTEXT_WORDS = re.compile(
    r"\b(?:taking|supplement|started|dose|dosage|mg|mcg|iu|interaction|"
    r"medication|stack|cycle|taper|titrat|prescri|started|stopped|"
    r"discontinue|combine|mixing|adding)\b",
    re.IGNORECASE,
)


def append_substance_knowledge(
    mgr, parts: list[str], user_text: str,
) -> None:
    """Scan user text for substance names and append matching profiles.

    Pulls profiles from clean_substance_knowledge. Includes mechanism,
    CYP profile, pathway effects, and known interactions. Keeps concise.
    """
    from healthbot.llm.conversation_routing import get_clean_db

    # Detect mentioned substances
    mentioned = _detect_substances(user_text)
    if not mentioned:
        return

    clean_db = get_clean_db(mgr)
    if not clean_db:
        return

    try:
        profiles_added = 0
        for substance in mentioned[:3]:  # Limit to avoid bloating prompt
            canonical = SUBSTANCE_ALIASES.get(substance.lower(), substance.lower())
            profile = clean_db.get_substance_knowledge(canonical)
            if not profile:
                continue

            if profiles_added == 0:
                parts.append("## SUBSTANCE PROFILES\n")

            parts.append(f"### {canonical.replace('_', ' ').title()}")
            if profile.get("mechanism"):
                parts.append(f"  Mechanism: {profile['mechanism']}")
            if profile.get("half_life"):
                parts.append(f"  Half-life: {profile['half_life']}")
            if profile.get("cyp_interactions"):
                try:
                    cyp = json.loads(profile["cyp_interactions"])
                    if cyp:
                        cyp_str = ", ".join(
                            f"{e}: {r}" for e, r in cyp.items()
                        )
                        parts.append(f"  CYP-450: {cyp_str}")
                except (json.JSONDecodeError, TypeError):
                    pass
            if profile.get("pathway_effects"):
                try:
                    pw = json.loads(profile["pathway_effects"])
                    if pw:
                        pw_str = ", ".join(
                            f"{p}: {e}" for p, e in pw.items()
                        )
                        parts.append(f"  Pathways: {pw_str}")
                except (json.JSONDecodeError, TypeError):
                    pass
            if profile.get("clinical_summary"):
                summary = profile["clinical_summary"]
                if len(summary) > 300:
                    summary = summary[:297] + "..."
                parts.append(f"  Evidence: {summary}")
            profiles_added += 1

        if profiles_added:
            parts.append("")
    except Exception as exc:
        logger.debug("append_substance_knowledge failed: %s", exc)
    finally:
        clean_db.close()


def append_active_interactions_summary(
    mgr, parts: list[str],
) -> None:
    """Pre-compute interaction summary for all active meds.

    Includes CYP conflicts and pathway stacking. Added once per
    conversation context build (not per message).
    """
    try:
        from healthbot.llm.conversation_routing import get_clean_db
        from healthbot.reasoning.interaction_checker_ext import (
            CypInteractionChecker,
            PathwayInteractionChecker,
        )

        clean_db = get_clean_db(mgr)
        if not clean_db:
            return
        try:
            meds = clean_db.get_active_medications()
            med_names = [m.get("name", "") for m in meds if m.get("name")]
        finally:
            clean_db.close()

        if len(med_names) < 2:
            return

        all_cyp: list[str] = []
        all_pathway: list[str] = []

        for i, med in enumerate(med_names):
            others = med_names[:i] + med_names[i + 1:]
            cyp_conflicts = CypInteractionChecker.check_substance_cyp(
                med, others,
            )
            for c in cyp_conflicts:
                desc = f"{c.substance_a} vs {c.substance_b} ({c.enzyme}: {c.role_a}/{c.role_b})"
                if desc not in all_cyp:
                    all_cyp.append(desc)

            stacks = PathwayInteractionChecker.check_substance_pathways(
                med, others,
            )
            for s in stacks:
                desc = f"{s.pathway}: {', '.join(s.substances)}"
                if desc not in all_pathway:
                    all_pathway.append(desc)

        if not all_cyp and not all_pathway:
            return

        parts.append("## ACTIVE MEDICATION INTERACTIONS\n")
        if all_cyp:
            parts.append("CYP-450 conflicts:")
            for c in all_cyp[:10]:
                parts.append(f"  - {c}")
        if all_pathway:
            parts.append("Pathway stacking:")
            for p in all_pathway[:10]:
                parts.append(f"  - {p}")
        parts.append("")

    except Exception as exc:
        logger.debug("append_active_interactions_summary failed: %s", exc)


def append_medication_timelines(
    mgr, parts: list[str],
) -> None:
    """Append active medication timelines with week numbers.

    Shows active medications with start dates, week numbers since start,
    and any linked metrics.
    """
    try:
        from healthbot.reasoning.medication_timeline import MedicationTimeline

        timeline = MedicationTimeline(mgr)
        summaries = timeline.get_all_active_timelines(mgr._user_id)
        if not summaries:
            return

        parts.append("## MEDICATION TIMELINES\n")
        for s in summaries:
            line = f"- {s['name']}"
            if s.get("dose"):
                line += f" {s['dose']}"
            if s.get("week_number"):
                line += f" — Week {s['week_number']}"
            if s.get("start_date"):
                line += f" (started {s['start_date']})"
            parts.append(line)
            if s.get("linked_metrics"):
                for metric in s["linked_metrics"]:
                    parts.append(
                        f"    {metric['metric']}: {metric.get('start_value', '?')} → "
                        f"{metric.get('current_value', '?')} "
                        f"({metric.get('change', '')} {metric.get('unit', '')})"
                    )
        parts.append("")

    except Exception as exc:
        logger.debug("append_medication_timelines failed: %s", exc)


def _detect_substances(text: str) -> list[str]:
    """Detect substance names mentioned in text."""
    text_lower = text.lower()
    has_context = bool(_CONTEXT_WORDS.search(text_lower))
    detected: set[str] = set()

    for alias in sorted(SUBSTANCE_ALIASES.keys(), key=len, reverse=True):
        if len(alias) < _MIN_ALIAS_LEN:
            continue
        # Stopword aliases require explicit medical context
        if alias in _STOPWORDS and not has_context:
            continue
        # Short aliases (< 6 chars) that aren't stopwords still need context
        if len(alias) < 6 and not has_context:
            continue
        if alias in text_lower:
            if re.search(rf"\b{re.escape(alias)}\b", text_lower):
                canonical = SUBSTANCE_ALIASES[alias]
                if canonical not in detected:
                    detected.add(canonical)

    return list(detected)
