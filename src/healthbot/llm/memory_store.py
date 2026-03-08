"""Two-tier encrypted memory system.

STM (Short-Term Memory): Recent conversation messages per user.
LTM (Long-Term Memory): Consolidated medical profile facts.

Consolidation: medically relevant STM messages are archived to the
permanent medical journal. LTM facts are managed via Claude's
structured INSIGHT/CONDITION blocks routed through the Knowledge Base.
"""
from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher

from healthbot.data.db import HealthDB
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


class MemoryStore:
    """Manage STM and LTM for conversational memory."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db
        self._firewall = PhiFirewall()

    def get_stm_context(self, user_id: int, max_messages: int = 20) -> list[dict]:
        """Get recent STM messages."""
        return self._db.get_recent_stm(user_id, limit=max_messages)

    def get_ltm_profile(self, user_id: int) -> str:
        """Get formatted LTM profile for system prompt."""
        facts = self._db.get_ltm_by_user(user_id)
        if not facts:
            return ""

        by_category: dict[str, list[str]] = {}
        for fact in facts:
            cat = fact.get("category", fact.get("_category", "other"))
            text = fact.get("fact", "")
            if text:
                by_category.setdefault(cat, []).append(text)

        lines = []
        for cat in sorted(by_category.keys()):
            lines.append(f"**{cat.title()}**:")
            for f in by_category[cat]:
                lines.append(f"  - {f}")
        return "\n".join(lines)

    def store_ltm_fact(self, user_id: int, category: str, fact: str,
                       source: str = "conversation") -> str:
        """Store a single LTM fact."""
        return self._db.insert_ltm(user_id, category, fact, source)

    def consolidate(self, user_id: int) -> int:
        """Archive medically relevant STM messages to permanent journal.

        Called on vault lock/timeout. LTM fact extraction is now handled
        by Claude's structured INSIGHT/CONDITION blocks routed through
        the Knowledge Base — no Ollama needed.

        Returns number of archived messages.
        """
        stm_rows = self._db.get_recent_stm(user_id, limit=50)
        if not stm_rows:
            return 0

        # Archive medically relevant messages to permanent journal
        self._archive_medical_messages(user_id, stm_rows)

        # Mark STM as consolidated
        stm_ids = [row.get("_id", "") for row in stm_rows]
        valid_ids = [i for i in stm_ids if i]
        if valid_ids:
            self._db.mark_stm_consolidated(valid_ids)

        logger.info("Archived %d STM messages for user %d", len(stm_rows), user_id)
        return len(stm_rows)

    def _validate_facts(self, facts: list[dict], user_id: int) -> list[dict]:
        """Validate extracted facts: dedup/merge, PII check, quality filter.

        When a new fact is similar (≥85%) to an existing one but longer
        (more detail), the existing fact is updated in-place and the new
        fact is excluded from the insert list.
        """
        if not facts:
            return facts

        existing_facts = self._db.get_ltm_by_user(user_id)

        validated: list[dict] = []
        for fact_obj in facts:
            fact_text = fact_obj.get("fact", "")
            if not fact_text or len(fact_text.strip()) < 5:
                continue

            # 1. Dedup/merge against existing LTM
            is_dup, update_id = self._find_updatable_duplicate(
                fact_text, existing_facts,
            )
            if is_dup:
                if update_id:
                    # More detail — update existing fact
                    cat = fact_obj.get("category", "other")
                    try:
                        self._db.update_ltm(update_id, fact_text, cat)
                        logger.info(
                            "Merged LTM fact: %s", fact_text[:50],
                        )
                    except Exception as e:
                        logger.warning("LTM merge failed: %s", e)
                else:
                    logger.debug(
                        "Skipping duplicate LTM fact: %s",
                        fact_text[:50],
                    )
                continue

            # 2. PII check: block facts containing PHI patterns
            if self._contains_phi(fact_text):
                logger.warning(
                    "Blocked LTM fact with PHI: %s", fact_text[:30],
                )
                continue

            # 3. Dedup within current batch
            batch_texts = [f.get("fact", "") for f in validated]
            if self._is_duplicate(fact_text, batch_texts):
                continue

            validated.append(fact_obj)

        return validated

    @staticmethod
    def _find_updatable_duplicate(
        new_fact: str,
        existing_facts: list[dict],
        threshold: float = 0.85,
    ) -> tuple[bool, str]:
        """Check if new_fact matches an existing fact.

        Returns (is_duplicate, fact_id_to_update).
        - Similar and new is longer → (True, existing_id) for update
        - Similar and new is same/shorter → (True, "") for skip
        - No match → (False, "")
        """
        new_lower = new_fact.lower().strip()
        for ex in existing_facts:
            ex_text = ex.get("fact", "")
            if not ex_text:
                continue
            ex_lower = ex_text.lower().strip()
            if new_lower == ex_lower:
                return (True, "")
            ratio = SequenceMatcher(
                None, new_lower, ex_lower,
            ).ratio()
            if ratio >= threshold:
                if len(new_fact) > len(ex_text):
                    return (True, ex.get("_id", ""))
                return (True, "")
        return (False, "")

    @staticmethod
    def _is_duplicate(
        new_fact: str, existing: list[str], threshold: float = 0.85
    ) -> bool:
        """Check if new_fact is too similar to any existing fact."""
        new_lower = new_fact.lower().strip()
        for ex in existing:
            ex_lower = ex.lower().strip()
            if new_lower == ex_lower:
                return True
            ratio = SequenceMatcher(None, new_lower, ex_lower).ratio()
            if ratio >= threshold:
                return True
        return False

    def _contains_phi(self, text: str) -> bool:
        """Check for PHI patterns in a fact text."""
        try:
            return self._firewall.contains_phi(text)
        except Exception:
            return True

    def _archive_medical_messages(
        self, user_id: int, stm_rows: list[dict],
    ) -> None:
        """Archive medically relevant STM messages to permanent journal.

        Called before consolidation marks STM as processed. Only messages
        matching medical relevance patterns are archived — greetings,
        meta-conversation, and system messages are skipped.
        """
        try:
            from healthbot.nlu.medical_classifier import (
                classify_medical_category,
                is_medically_relevant,
            )
        except ImportError:
            return

        for row in stm_rows:
            content = row.get("content", "")
            role = row.get("role", "user")
            if not content or not is_medically_relevant(content):
                continue

            category = classify_medical_category(content)
            try:
                self._db.insert_journal_entry(
                    user_id=user_id,
                    speaker=role,
                    content=content,
                    category=category,
                    source="conversation",
                )
            except Exception as e:
                logger.debug("Journal archive failed: %s", e)

            # Extract key medical facts for LTM storage
            self._extract_ltm_from_message(user_id, content, category)

    _FACT_PATTERNS = [
        re.compile(r"(?:diagnosed|diagnosis|condition)[:\s]+(.{5,80})", re.I),
        re.compile(r"(?:allerg(?:y|ic) to|allergies)[:\s]+(.{3,60})", re.I),
        re.compile(r"(?:taking|started|prescribed|medication)[:\s]+(.{3,60})", re.I),
        re.compile(r"(?:A1c|HbA1c|hemoglobin a1c)\s*(?:is|was|=|:)\s*([\d.]+)", re.I),
        re.compile(r"(?:blood pressure|BP)\s*(?:is|was|=|:)\s*(\d{2,3}/\d{2,3})", re.I),
    ]

    def _extract_ltm_from_message(
        self, user_id: int, content: str, category: str,
    ) -> None:
        """Extract key medical facts from a message and store as LTM."""
        for pattern in self._FACT_PATTERNS:
            for match in pattern.finditer(content):
                fact = match.group(0).strip()
                if len(fact) < 5:
                    continue
                try:
                    facts = [{"fact": fact, "category": category}]
                    validated = self._validate_facts(facts, user_id)
                    for f in validated:
                        self._db.insert_ltm(
                            user_id, f["category"],
                            f["fact"], "stm_extraction",
                        )
                except Exception as e:
                    logger.debug("LTM extraction failed: %s", e)

    def cleanup(self, days: int = 7) -> int:
        """Clean up old consolidated STM entries."""
        return self._db.clear_old_stm(days=days)
