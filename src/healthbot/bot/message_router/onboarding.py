"""Auto-onboard + identity profile loading methods."""
from __future__ import annotations

import logging

from telegram import Update

logger = logging.getLogger("healthbot")


class OnboardingMixin:
    """Mixin providing auto-onboarding and identity profile loading."""

    def _load_identity_profile(self, user_id: int, db: object) -> None:
        """Load identity profile and enhance PII detection on unlock.

        Compiles identity data into regex patterns for PhiFirewall and
        known names for NER boosting. Only the compiled patterns are used —
        no PII leaves the raw vault.
        """
        try:
            from healthbot.security.identity_profile import IdentityProfile

            profile = IdentityProfile(db=db)
            fields = profile.get_all_fields(user_id)
            if not fields:
                return

            extra_patterns = profile.compile_phi_patterns(user_id)
            known_names = profile.compile_ner_known_names(user_id)

            if extra_patterns:
                # Mutate in-place so all components sharing this fw reference
                # (log scrubber, clean sync, anonymizer) see identity patterns
                self._fw.add_patterns(extra_patterns)
                logger.info(
                    "Identity profile loaded: %d extra patterns",
                    len(extra_patterns),
                )

            if known_names:
                # Try to set known names on NER layer via the anonymizer
                claude = self._get_claude() if self._get_claude else None
                if claude and hasattr(claude, "_get_anonymizer"):
                    anon = claude._get_anonymizer()
                    if anon and anon._ner:
                        anon._ner.set_known_names(known_names)
                    # Also update the anonymizer's firewall if we rebuilt it
                    if extra_patterns and anon:
                        anon._fw = self._fw
        except Exception as e:
            logger.debug("Identity profile load skipped: %s", e)

    async def _auto_onboard_if_empty(self, update: Update, user_id: int) -> None:
        """Auto-start onboarding if the user's profile is empty."""
        if not self._onboard_handlers:
            return
        try:
            db = self._get_db()
            ltm_facts = db.get_ltm_by_user(user_id)
            if not ltm_facts:
                engine = self._onboard_handlers._get_engine()
                first_q = engine.start(user_id)
                await update.effective_chat.send_message(
                    "Welcome to HealthBot! Let's get to know you.\n"
                    "I'll ask a few quick questions to build your "
                    "health profile.\n\n" + first_q
                )
        except Exception as e:
            logger.debug("Auto-onboard check skipped: %s", e)
