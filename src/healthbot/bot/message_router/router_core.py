"""Core MessageRouter class: __init__, handle_message dispatch, state, properties.

This module contains the central router class that composes all mixin
behaviours. State (passphrase-awaiting, pending dates, etc.) and
property accessors live here.
"""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.config import Config
from healthbot.security.key_manager import KeyManager

from .document_handler import DocumentMixin
from .free_text_handler import FreeTextMixin
from .health_data_handler import HealthDataMixin
from .intent_interceptor import IntentInterceptorMixin
from .onboarding import OnboardingMixin
from .unlock_handler import UnlockMixin

logger = logging.getLogger("healthbot")


class MessageRouter(
    UnlockMixin,
    DocumentMixin,
    FreeTextMixin,
    IntentInterceptorMixin,
    HealthDataMixin,
    OnboardingMixin,
):
    """Routes non-command messages to the correct handler."""

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
        self._awaiting_passphrase: set[int] = set()
        self._awaiting_onboard_consent: set[int] = set()
        self._on_unlock_callback = None  # Optional scheduler callback
        self._reset_handlers = None  # Set by app.py
        self._onboard_handlers = None  # Set by app.py
        self._identity_handlers = None  # Set by app.py
        self._data_handlers = None  # Set by app.py
        self._session_handlers = None  # Set by app.py
        self._get_errors: callable | None = None  # Set by set_error_source()
        self._fw = None  # PhiFirewall, set by set_error_source()
        self.ingestion_mode: bool = False  # Skip post-ingest analysis
        self._ingestion_count_cb: callable | None = None  # Increment counter
        self.upload_mode: bool = False  # Secure upload — block free text
        self._upload_count_cb: callable | None = None  # Increment counter
        self._get_claude: callable | None = None  # Returns ClaudeConversationManager
        self._last_logged_obs: dict[int, str] = {}  # user_id -> obs_id for /undo
        self._exchange_cb: callable | None = None  # (user_text, response) tracker
        self._track_msg_cb: callable | None = None  # (chat_id, msg_id) for wipe tracking
        self._last_user_input: str = ""
        self._last_bot_response: str = ""
        self._post_ingest_cb: callable | None = None  # targeted analysis
        self._post_ingest_sync_cb: callable | None = None  # clean sync
        # Pending date reply: user_id -> blob_id of undated results
        self._pending_date: dict[int, str] = {}
        # Connected sources callback for unlock message
        self._connected_sources_cb: callable | None = None

    @property
    def awaiting_passphrase(self) -> set[int]:
        """Expose passphrase-awaiting set for /unlock handler."""
        return self._awaiting_passphrase

    def set_on_unlock(self, callback: callable) -> None:
        """Register an async callback to run after successful vault unlock."""
        self._on_unlock_callback = callback

    def set_reset_handlers(self, handlers: object) -> None:
        """Register ResetHandlers for confirmation interception."""
        self._reset_handlers = handlers

    def set_onboard_handlers(self, handlers: object) -> None:
        """Register OnboardHandlers for onboarding answer interception."""
        self._onboard_handlers = handlers

    def set_identity_handlers(self, handlers: object) -> None:
        """Register IdentityHandlers for identity survey interception."""
        self._identity_handlers = handlers

    def set_data_handlers(self, handlers: object) -> None:
        """Register DataHandlers for natural-language wearable auth."""
        self._data_handlers = handlers

    def set_session_handlers(self, handlers: object) -> None:
        """Register SessionHandlers for natural-language session commands."""
        self._session_handlers = handlers

    def set_claude_getter(self, callback: callable) -> None:
        """Register Claude conversation manager getter."""
        self._get_claude = callback

    def set_error_source(
        self,
        get_errors: callable,
        phi_firewall: object,
    ) -> None:
        """Register error buffer + firewall for troubleshoot routing."""
        self._get_errors = get_errors
        self._fw = phi_firewall

    def _fallback_clean_sync(self) -> None:
        """Run clean sync directly when _post_ingest_sync_cb isn't registered."""
        try:
            from healthbot.data.clean_db import CleanDB
            from healthbot.data.clean_sync import CleanSyncEngine
            from healthbot.llm.anonymizer import Anonymizer

            db = self._get_db()
            clean = CleanDB(self._config.clean_db_path, phi_firewall=self._fw)
            try:
                clean.open(clean_key=self._km.get_clean_key())
                anon = Anonymizer(phi_firewall=self._fw)
                engine = CleanSyncEngine(
                    raw_db=db, clean_db=clean, anonymizer=anon,
                    phi_firewall=self._fw,
                )
                user_id = (
                    self._config.allowed_user_ids[0]
                    if hasattr(self._config, "allowed_user_ids") and self._config.allowed_user_ids
                    else 0
                )
                report = engine.sync_all(user_id)
                if report:
                    logger.info(
                        "Fallback post-ingestion sync: %d obs, %d errors",
                        report.observations_synced, len(report.errors),
                    )
            finally:
                clean.close()
        except Exception as e:
            logger.debug("Fallback clean sync failed: %s", e)

    async def handle_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle non-command messages (passphrase, PDFs, free text)."""
        if update.message is None:
            return
        if not self._check_auth(update):
            return

        user_id = update.effective_user.id if update.effective_user else 0

        # Any inbound message refreshes the session timeout
        self._km.touch()

        # Passphrase handling (two-step flow: /unlock then passphrase as next message)
        if user_id in self._awaiting_passphrase and update.message.text:
            passphrase = update.message.text
            try:
                await update.message.delete()
            except Exception:
                pass
            await self.try_unlock(passphrase, update, context)
            return

        # Onboard consent interception (after new vault: "yes" starts onboarding)
        if (
            user_id in self._awaiting_onboard_consent
            and update.message.text
        ):
            self._awaiting_onboard_consent.discard(user_id)
            reply = update.message.text.strip().lower()
            if reply in ("yes", "y", "sure", "ok", "yeah", "yep"):
                if self._onboard_handlers and self._km.is_unlocked:
                    engine = self._onboard_handlers._get_engine()
                    first_q = engine.start(user_id)
                    await update.message.reply_text(
                        "Let's build your health profile.\n"
                        "Answer each question, or type 'skip'.\n\n"
                        + first_q
                    )
                    return
            # Any other reply = opted out, continue normal routing

        # Wearable credential setup interception (client_id / client_secret input)
        if (
            self._data_handlers
            and update.message.text
            and self._data_handlers.is_awaiting_setup(user_id)
        ):
            handled = await self._data_handlers.handle_setup_input(update, context)
            if handled:
                return

        # Claude CLI auth key input interception
        if (
            self._session_handlers
            and update.message.text
            and self._session_handlers.is_awaiting_claude_auth(user_id)
        ):
            handled = await self._session_handlers.handle_claude_auth_input(
                update, context,
            )
            if handled:
                return

        # Rekey passphrase input interception
        if (
            self._session_handlers
            and update.message.text
            and self._session_handlers.is_awaiting_rekey(user_id)
        ):
            handled = await self._session_handlers.handle_rekey_input(
                update, context,
            )
            if handled:
                return

        # Reset/delete confirmation interception
        if (
            self._reset_handlers
            and update.message.text
            and self._reset_handlers.is_awaiting_confirm(user_id)
        ):
            handled = await self._reset_handlers.handle_confirm(update, context)
            if handled:
                return

        # Onboarding answer interception
        if (
            self._onboard_handlers
            and update.message.text
            and self._onboard_handlers.is_active(user_id)
        ):
            handled = await self._onboard_handlers.handle_answer(update, context)
            if handled:
                return

        # Identity survey answer interception
        if (
            self._identity_handlers
            and update.message.text
            and self._identity_handlers.is_active(user_id)
        ):
            handled = await self._identity_handlers.handle_answer(update, context)
            if handled:
                return

        # Pending date reply: user provides collection date for undated labs
        if (
            update.message.text
            and user_id in self._pending_date
        ):
            handled = await self._handle_date_reply(update, user_id)
            if handled:
                return

        # Photo handling (vision analysis)
        if update.message.photo:
            await self._handle_photo(update, context)
            return

        # Document handling (PDFs)
        if update.message.document:
            await self._handle_document(update, context)
            return

        # Natural language intent interception (deterministic patterns)
        intercepted = await self._intercept_intents(update, context, user_id)
        if intercepted:
            return

        # Upload mode: block free text, only allow document uploads
        if update.message.text and self._km.is_unlocked and self.upload_mode:
            await update.message.reply_text(
                "Secure upload mode active. Send documents or /finish when done."
            )
            return

        # Free-text conversation (Claude CLI)
        if update.message.text and self._km.is_unlocked:
            await self._handle_free_text(update, user_id)
            return

        # Vault locked
        if update.message.text and not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")

    def on_vault_lock(self) -> None:
        """Clear state on vault lock (security invariant)."""
        self._awaiting_passphrase.clear()
        self._awaiting_onboard_consent.clear()
        self.upload_mode = False
        self._upload_count_cb = None
        if self._onboard_handlers:
            self._onboard_handlers.on_vault_lock()
        if self._identity_handlers:
            self._identity_handlers.on_vault_lock()
        if self._data_handlers:
            self._data_handlers._setup_state.clear()
        if self._session_handlers:
            self._session_handlers._claude_auth_awaiting.clear()
            self._session_handlers._rekey_awaiting.clear()
