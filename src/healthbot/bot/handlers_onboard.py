"""Telegram handler for /onboard command -- health profile intake interview."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.config import Config
from healthbot.nlu.onboarding import OnboardingEngine
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")


class OnboardHandlers:
    """Handler for /onboard with multi-turn session management."""

    def __init__(
        self,
        config: Config,
        key_manager: KeyManager,
        get_db: callable,
        check_auth: callable,
    ) -> None:
        self._config = config
        self._km = key_manager
        self._get_db = get_db
        self._check_auth = check_auth
        self._engine: OnboardingEngine | None = None
        self._on_complete: callable | None = None

    def set_on_complete(self, callback: callable) -> None:
        """Set callback fired after onboarding completes.

        Callback signature: async def(update, user_id) -> None
        """
        self._on_complete = callback

    def _get_engine(self) -> OnboardingEngine:
        """Lazy-init engine (needs open DB)."""
        if self._engine is None:
            self._engine = OnboardingEngine(self._get_db())
        return self._engine

    def is_active(self, user_id: int) -> bool:
        """Check if user has an active onboarding session."""
        return self._engine is not None and self._engine.is_active(user_id)

    async def handle_answer(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> bool:
        """Handle text response during active onboarding.

        Returns True if consumed (caller should stop routing).
        """
        user_id = update.effective_user.id if update.effective_user else 0
        if not self.is_active(user_id):
            return False
        text = update.message.text or ""
        response = self._get_engine().process_answer(user_id, text)
        await update.message.reply_text(response)

        # Fire completion callback if session just ended
        if not self.is_active(user_id) and self._on_complete:
            try:
                await self._on_complete(update, user_id)
            except Exception as e:
                logger.debug("Post-onboard callback failed: %s", e)

        return True

    async def onboard(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /onboard command -- start health profile interview."""
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        user_id = update.effective_user.id
        engine = self._get_engine()
        first_question = engine.start(user_id)
        await update.message.reply_text(
            "Let's build your health profile.\n"
            "I'll ask a few questions -- answer each one, or type 'skip'.\n\n"
            + first_question
        )

    async def handle_onboard_callback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle inline keyboard callbacks for onboarding prompt."""
        if not self._check_auth(update):
            query = update.callback_query
            await query.answer()
            await query.edit_message_text("Unauthorized.")
            return
        query = update.callback_query
        await query.answer()
        data = query.data

        if data == "onboard:start":
            if not self._km.is_unlocked:
                await query.edit_message_text(
                    "Vault is locked. Send /unlock first, then /onboard."
                )
                return
            await query.edit_message_text("Starting health profile setup...")
            # Cannot call self.onboard() — it uses update.message which is
            # None for CallbackQuery updates.  Start the engine directly.
            user_id = update.effective_user.id
            engine = self._get_engine()
            first_question = engine.start(user_id)
            await update.effective_chat.send_message(
                "Let's build your health profile.\n"
                "I'll ask a few questions -- answer each one, or type 'skip'.\n\n"
                + first_question
            )

        elif data == "onboard:snooze":
            await query.edit_message_text(
                "No problem, I'll ask next time you unlock."
            )

        elif data == "onboard:dismiss":
            if not self._km.is_unlocked:
                await query.edit_message_text(
                    "Vault is locked. Send /unlock first to save preference."
                )
                return
            # Store permanent dismissal in user_identity
            try:
                user_id = update.effective_user.id
                db = self._get_db()
                db.upsert_identity_field(
                    user_id=user_id,
                    field_key="onboard_dismissed",
                    value="true",
                    field_type="preference",
                )
            except Exception as e:
                logger.debug("Failed to store onboard dismissal: %s", e)
            await query.edit_message_text(
                "Got it. Run /onboard anytime if you change your mind."
            )

    def on_vault_lock(self) -> None:
        """Clear onboarding sessions on vault lock."""
        if self._engine:
            self._engine.on_vault_lock()
            self._engine = None
