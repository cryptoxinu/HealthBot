"""Fact storage and data persistence.

Handles storing extracted clinical facts into LTM with PHI checking,
fuzzy dedup/merge logic, and anonymization.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("healthbot")


class StorageMixin:
    """Mixin providing clinical fact storage capabilities."""

    def _store_clinical_facts(
        self, facts: list[dict], user_id: int, filename: str,
    ) -> tuple[int, int]:
        """Validate and store extracted clinical facts in LTM.

        Returns (stored_count, pii_blocked_count).

        Dedup/merge logic:
        - PHI firewall check (with PII alert recording)
        - >=85% similarity -> skip or merge (if new is longer)
        - New facts inserted with source="document"
        """
        from difflib import SequenceMatcher

        try:
            existing = self._db.get_ltm_by_user(user_id)
        except Exception:
            existing = []

        count = 0
        blocked = 0
        for fact_obj in facts:
            category = fact_obj.get("category", "")
            fact_text = fact_obj.get("fact", "")
            if not fact_text or len(fact_text.strip()) < 5:
                continue

            # Anonymize fact text through full pipeline (NER + regex + Ollama)
            # before storing. This redacts names/cities instead of blocking
            # the entire fact.
            try:
                from healthbot.llm.anonymizer import AnonymizationError, Anonymizer
                from healthbot.security.phi_firewall import PhiFirewall

                fw = self._fw or PhiFirewall()
                ollama_layer = self._get_ollama_layer()
                anon = Anonymizer(
                    phi_firewall=fw, use_ner=True,
                    ollama_layer=ollama_layer,
                )
                cleaned, had_phi = anon.anonymize(fact_text)
                try:
                    anon.assert_safe(cleaned)
                except AnonymizationError:
                    # Retry once
                    cleaned, _ = anon.anonymize(cleaned)
                    try:
                        anon.assert_safe(cleaned)
                    except AnonymizationError:
                        blocked += 1
                        logger.warning(
                            "Blocked clinical fact with residual PHI "
                            "(category: %s)", category,
                        )
                        try:
                            from healthbot.security.pii_alert import PiiAlertService
                            PiiAlertService.get_instance().record(
                                category="PHI_in_clinical_fact",
                                destination="ltm",
                            )
                        except Exception:
                            pass
                        continue
                fact_text = cleaned
            except Exception:
                blocked += 1
                logger.warning("PHI check failed — blocking fact for safety")
                continue

            # Fuzzy dedup against existing LTM (inline, no MemoryStore dep)
            is_dup = False
            update_id = ""
            new_lower = fact_text.lower().strip()
            for ex in existing:
                ex_text = ex.get("fact", "")
                if not ex_text:
                    continue
                ex_lower = ex_text.lower().strip()
                if new_lower == ex_lower:
                    is_dup = True
                    break
                ratio = SequenceMatcher(None, new_lower, ex_lower).ratio()
                if ratio >= 0.85:
                    is_dup = True
                    if len(fact_text) > len(ex_text):
                        update_id = ex.get("_id", "")
                    break

            if is_dup:
                if update_id:
                    try:
                        self._db.update_ltm(update_id, fact_text, category)
                        logger.info(
                            "Updated LTM (document): %s", fact_text[:50],
                        )
                        count += 1
                    except Exception as e:
                        logger.warning("LTM update failed: %s", e)
                continue

            try:
                self._db.insert_ltm(
                    user_id, category, fact_text, source="document",
                )
                logger.info(
                    "New LTM (document): [%s] %s", category, fact_text[:50],
                )
                count += 1
            except Exception as e:
                logger.warning("LTM insert failed: %s", e)

        return count, blocked
