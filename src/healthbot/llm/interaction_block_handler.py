"""CHECK_INTERACTION block handler + fallback substance scanner.

Routes CHECK_INTERACTION blocks to the interaction checker stack
(drug-drug, CYP-450, pathway) and formats results for inline display.
Also provides a fallback scanner for when Claude doesn't emit the block.
"""
from __future__ import annotations

import logging
import re

from healthbot.reasoning.interaction_kb import SUBSTANCE_ALIASES

logger = logging.getLogger("healthbot")

# Substances the user has active — populated from medications context
_ACTIVE_MED_CACHE: dict[int, list[str]] = {}


def handle_check_interaction(mgr, block: dict) -> list[str]:
    """Handle a CHECK_INTERACTION block.

    Runs the full interaction check stack:
    1. Drug-drug interactions (existing InteractionChecker)
    2. CYP-450 enzyme conflicts
    3. Pathway stacking

    Returns list of formatted interaction result strings.
    """
    substance = block.get("substance", "").strip()
    if not substance:
        return []

    results: list[str] = []

    # Get active medications
    active_meds = get_active_medications(mgr)
    if not active_meds:
        return []

    # 1. Existing drug-drug interactions
    dd_results = _check_drug_drug(substance, active_meds)
    results.extend(dd_results)

    # 2. CYP-450 enzyme conflicts
    cyp_results = _check_cyp(substance, active_meds)
    results.extend(cyp_results)

    # 3. Pathway stacking
    pathway_results = _check_pathways(substance, active_meds)
    results.extend(pathway_results)

    # 4. Check substance knowledge profile
    profile_note = _check_substance_profile(mgr, substance)
    if profile_note:
        results.append(profile_note)

    if not results:
        results.append(
            f"No known interactions found between {substance} and your "
            f"active medications ({', '.join(active_meds[:5])})."
        )

    return results


def get_active_medications(mgr) -> list[str]:
    """Get list of active medication names from CleanDB.

    Tries three sources in order:
    1. clean_medications table (temporal medication tracking)
    2. clean_user_memory with category medication/supplement
    3. clean_health_records_ext with data_type medication/supplement
    """
    user_id = getattr(mgr, "_user_id", 0)
    if user_id in _ACTIVE_MED_CACHE:
        return _ACTIVE_MED_CACHE[user_id]

    try:
        from healthbot.llm.conversation_routing import get_clean_db
        clean_db = get_clean_db(mgr)
        if not clean_db:
            return []
        try:
            # Normalize helper — lowercase + strip for dedup
            seen: set[str] = set()
            names: list[str] = []

            def _add(name: str) -> None:
                norm = name.lower().strip()
                if norm and norm not in seen:
                    seen.add(norm)
                    names.append(norm)

            # Source 1: clean_medications table
            meds = clean_db.get_medications()
            for m in meds:
                _add(m.get("name", ""))

            # Source 2: user_memory with medication/supplement category
            if not names:
                mem_meds = clean_db.get_user_memory(category="medication")
                mem_supps = clean_db.get_user_memory(category="supplement")
                for mem in mem_meds + mem_supps:
                    # The key IS the substance name (e.g. "bromantane")
                    _add(mem.get("key", ""))

            # Source 3: health_records_ext (condition/supplement records)
            if not names:
                try:
                    records = clean_db.get_health_records_ext(
                        data_type="medication",
                    )
                    records += clean_db.get_health_records_ext(
                        data_type="supplement",
                    )
                    for rec in records:
                        _add(rec.get("label", ""))
                except Exception:
                    pass  # Table may not exist

            _ACTIVE_MED_CACHE[user_id] = names
            return names
        finally:
            clean_db.close()
    except Exception as e:
        logger.warning("Failed to get active medications: %s", e)
        return []


def invalidate_med_cache(user_id: int = 0) -> None:
    """Clear cached active medication list (call after med changes)."""
    _ACTIVE_MED_CACHE.pop(user_id, None)


def _check_drug_drug(substance: str, active_meds: list[str]) -> list[str]:
    """Check existing drug-drug interactions."""
    try:
        from healthbot.reasoning.interaction_kb import (
            INTERACTIONS,
            SUBSTANCE_ALIASES,
        )

        sub_key = SUBSTANCE_ALIASES.get(substance.lower(), substance.lower())
        results: list[str] = []

        for med in active_meds:
            med_key = SUBSTANCE_ALIASES.get(med.lower(), med.lower())
            for ix in INTERACTIONS:
                if (
                    (ix.substance_a == sub_key and ix.substance_b == med_key)
                    or (ix.substance_a == med_key and ix.substance_b == sub_key)
                ):
                    severity_icon = {
                        "major": "!!",
                        "moderate": "!",
                        "minor": "~",
                        "contraindicated": "!!!",
                    }.get(ix.severity, "")
                    results.append(
                        f"{severity_icon} [{ix.severity.upper()}] "
                        f"{substance} + {med}: {ix.mechanism} "
                        f"— {ix.recommendation}"
                    )
        return results
    except Exception as e:
        logger.warning("Drug-drug check failed: %s", e)
        return []


def _check_cyp(substance: str, active_meds: list[str]) -> list[str]:
    """Check CYP-450 enzyme conflicts."""
    try:
        from healthbot.reasoning.interaction_checker_ext import (
            CypInteractionChecker,
        )

        conflicts = CypInteractionChecker.check_substance_cyp(
            substance, active_meds,
        )
        results: list[str] = []
        for c in conflicts:
            icon = "!!" if c.severity == "major" else "!"
            results.append(
                f"{icon} [CYP {c.enzyme}] {c.substance_a} ({c.role_a}) vs "
                f"{c.substance_b} ({c.role_b}): {c.mechanism} "
                f"— {c.recommendation}"
            )
        return results
    except Exception as e:
        logger.warning("CYP check failed: %s", e)
        return []


def _check_pathways(substance: str, active_meds: list[str]) -> list[str]:
    """Check pathway stacking."""
    try:
        from healthbot.reasoning.interaction_checker_ext import (
            PathwayInteractionChecker,
        )

        stacks = PathwayInteractionChecker.check_substance_pathways(
            substance, active_meds,
        )
        results: list[str] = []
        for s in stacks:
            icon = "!!" if s.severity == "major" else "!"
            results.append(
                f"{icon} [Pathway: {s.pathway}] {s.mechanism} "
                f"— {s.recommendation}"
            )
        return results
    except Exception as e:
        logger.warning("Pathway check failed: %s", e)
        return []


def _check_substance_profile(mgr, substance: str) -> str | None:
    """Check if we have a deep research profile for this substance."""
    try:
        from healthbot.llm.conversation_routing import get_clean_db
        clean_db = get_clean_db(mgr)
        if not clean_db:
            return None
        try:
            profile = clean_db.get_substance_knowledge(substance.lower())
            if profile and profile.get("quality_score", 0) > 0.5:
                return None  # Profile exists, no note needed
            return (
                f"No deep profile found for {substance}. "
                f"Run /deep {substance} for comprehensive research."
            )
        finally:
            clean_db.close()
    except Exception:
        return None


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


def _has_substance_context(text: str) -> bool:
    """Return True if text contains words indicating substance discussion."""
    return bool(_CONTEXT_WORDS.search(text))


def scan_for_substance_mentions(
    user_text: str,
    active_med_names: list[str] | None = None,
) -> list[str]:
    """Scan user text for substance mentions not in active meds.

    Used as a fallback when Claude doesn't emit CHECK_INTERACTION.
    Returns list of substance names detected.
    """
    text_lower = user_text.lower()
    active_set = {m.lower() for m in (active_med_names or [])}
    has_context = _has_substance_context(text_lower)

    detected: list[str] = []
    # Check against all known substance aliases
    for alias, canonical in SUBSTANCE_ALIASES.items():
        if len(alias) < _MIN_ALIAS_LEN:
            continue  # Skip short aliases to avoid false positives
        # Stopword aliases require explicit medical context
        if alias in _STOPWORDS and not has_context:
            continue
        # Short aliases (< 6 chars) that aren't stopwords still need context
        if len(alias) < 6 and not has_context:
            continue
        if alias in text_lower and canonical not in active_set:
            # Verify it's a word boundary match
            if re.search(rf"\b{re.escape(alias)}\b", text_lower):
                if canonical not in [
                    SUBSTANCE_ALIASES.get(d, d) for d in detected
                ]:
                    detected.append(alias)

    return detected
