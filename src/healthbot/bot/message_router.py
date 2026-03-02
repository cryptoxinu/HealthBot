"""Message routing for non-command Telegram messages.

Routes passphrase entry, document uploads, and free-text conversation
to appropriate handlers. Manages passphrase-awaiting state.
"""
from __future__ import annotations

import asyncio
import logging
import re
import sqlite3

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate, strip_markdown
from healthbot.bot.typing_helper import TypingIndicator
from healthbot.config import Config
from healthbot.reasoning.triage import TriageEngine
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")

# Natural-language phrases that should trigger /delete_labs
_DELETE_LABS_PATTERN = re.compile(
    r"\b(?:delete|remove|clear|wipe|erase|nuke|purge|drop|trash)"
    r"\s+(?:all\s+)?(?:(?:my|the)\s+)?"
    r"(?:lab\s*(?:results?|records?|data|values?|tests?|work)?"
    r"|blood\s*(?:work|labs?|tests?|results?|records?|panels?)"
    r"|(?:pdf|uploaded)\s+(?:lab|blood)\s*\w*"
    r"|test\s+results?)\b",
    re.IGNORECASE,
)

# Natural-language phrases that should trigger /reset instead of going to Ollama
_RESET_PATTERN = re.compile(
    r"^(?:let['\u2018\u2019]?s?\s+)?(?:reset|start\s+(?:over|fresh)|wipe\s+(?:everything|all|data)"
    r"|delete\s+(?:everything|all|my\s+data)|clear\s+(?:everything|all|data))"
    r"[.!?]?\s*$",
    re.IGNORECASE,
)

# Natural-language phrases that should trigger wearable auth
_WHOOP_AUTH_PATTERN = re.compile(
    r"\b(?:(?:link|connect|authorize|auth|set\s*up|hook\s*up|pair|enable)"
    r"\s+(?:my\s+)?whoop"
    r"|whoop\s+(?:link|connect|authorize|auth|set\s*up|hook\s*up|pair|enable))\b",
    re.IGNORECASE,
)

_OURA_AUTH_PATTERN = re.compile(
    r"\b(?:(?:link|connect|authorize|auth|set\s*up|hook\s*up|pair|enable)"
    r"\s+(?:my\s+)?oura"
    r"|oura\s+(?:link|connect|authorize|auth|set\s*up|hook\s*up|pair|enable))\b",
    re.IGNORECASE,
)

# Natural-language phrases asking about wearable status (deterministic interception).
# Must be checked BEFORE auth patterns — "is whoop set up?" is a status query,
# not an auth request. Imperative "set up my whoop" still falls to auth.
_WEARABLE_STATUS_PATTERN = re.compile(
    r"\b(?:is\s+(?:my\s+)?(?:whoop|oura)\s+(?:work|connect|link|sync|broken|active"
    r"|set\s*up|configured|ready|enabled))"
    r"|(?:(?:whoop|oura)\s+(?:status|working|connected|broken|down|issue|problem))"
    r"|(?:(?:check|test|verify)\s+(?:my\s+)?(?:whoop|oura))"
    r"|(?:(?:why\s+)?(?:isn['\u2019]?t|is\s+not|isnt)\s+(?:my\s+)?(?:whoop|oura)\s+work)"
    r"|(?:(?:have\s+(?:you|we)\s+synced|when\s+did\s+(?:whoop|oura)\s+(?:last\s+)?sync))",
    re.IGNORECASE,
)

# Natural-language phrases that should trigger /onboard
_ONBOARD_PATTERN = re.compile(
    r"^(?:let['\u2018\u2019]?s?\s+)?(?:(?:start|begin|do|run)\s+)?"
    r"(?:onboard(?:ing)?|health\s+(?:profile|survey)|profile\s+setup)"
    r"[.!?]?\s*$",
    re.IGNORECASE,
)

# Technical troubleshooting — routes to Claude CLI for web-assisted debugging.
# Must NOT match health questions like "why is my iron low".
_TROUBLESHOOT_PATTERN = re.compile(
    r"\b(?:"
    # Direct triggers
    r"debug|troubleshoot"
    r"|help\s+(?:me\s+)?(?:fix|debug|troubleshoot|figure\s+out)"
    # Service + problem
    r"|(?:whoop|oura|sync|oauth|api|ollama)\s+"
    r"(?:error|fail(?:ed|ing|ure|s)?|broken|issue|problem|not\s+working|down)"
    r"|(?:error|fail(?:ed|ing|ure)?|broken|issue|problem)\s+"
    r"(?:with\s+)?(?:whoop|oura|sync|oauth|api|ollama)"
    # HTTP errors
    r"|https?\s*(?:status)?\s*error"
    r"|status\s*(?:code\s*)?\d{3}"
    r"|(?:4\d{2}|5\d{2})\s+(?:error|response|status)"
    # Connection / auth failures
    r"|(?:can'?t|cannot|won'?t|unable\s+to|couldn'?t)\s+"
    r"(?:connect|sync|authenticate|authorize|link|reach)"
    r"(?:\s+(?:to\s+)?(?:whoop|oura|api))?"
    # Why-questions about technical failures
    r"|why\s+(?:is|did|does|isn'?t|won'?t|can'?t)\s+"
    r"(?:the\s+)?(?:sync|connection|auth|whoop|oura|api|import)\s*"
    r"(?:fail|error|broken|not\s+work|failing|down)?"
    # Explicit error mentions
    r"|(?:timeout|timed?\s*out|connection\s+refused|auth(?:entication)?\s+fail)"
    r")\b",
    re.IGNORECASE,
)

# Natural-language phrases to pause overdue notifications
_PAUSE_OVERDUE_PATTERN = re.compile(
    r"\b(?:pause|snooze|mute|silence|quiet|stop|disable|turn\s+off)"
    r"\s+(?:the\s+)?(?:overdue\s+|screening\s+|lab\s+)?"
    r"(?:notification|alert|reminder|nag)s?"
    r"(?:\s+for\s+(.+))?"
    r"|\b(?:pause|snooze|mute)\s+(?:them|those)\s+for\s+(.+)",
    re.IGNORECASE,
)

# Natural-language phrases to unpause/resume overdue notifications
_UNPAUSE_OVERDUE_PATTERN = re.compile(
    r"(?:unpause|unmute|resume|enable|turn\s+on|re-?enable)"
    r"\s+(?:the\s+)?(?:overdue\s+|screening\s+|lab\s+)?"
    r"(?:notification|alert|reminder)s?"
    r"|(?:unpause|unmute|resume)\s+(?:them|those|it)",
    re.IGNORECASE,
)

# Natural-language health status check → lightweight summary
_STATUS_CHECK_PATTERN = re.compile(
    r"^(?:how(?:'?s|\s+(?:am|is|are))\s+(?:my\s+)?(?:health|status|body|numbers|labs?|blood\s*work)"
    r"|how\s+(?:am\s+)?I\s+(?:doing|looking)"
    r"|(?:give\s+me|show\s+me|what'?s)\s+(?:a\s+)?(?:summary|status|update|overview|snapshot)"
    r"|quick\s+(?:update|summary|status|check)"
    r"|any\s+(?:concerns?|issues?|problems?|alerts?)"
    r"|what\s+should\s+I\s+(?:know|worry\s+about)"
    r"|health\s+(?:check|status|update|summary))"
    r"[?.!]?\s*$",
    re.IGNORECASE,
)

# Natural-language visual health request → health card / chart
_VISUAL_HEALTH_PATTERN = re.compile(
    r"(?:show\s+(?:me\s+)?(?:my\s+)?(?:health|data|labs?)(?:\s+(?:visually|graphically|chart|graph)))"
    r"|visual(?:ize)?\s+(?:my\s+)?(?:health|data)"
    r"|health\s+(?:snapshot|card|visual)"
    r"|(?:give|send)\s+(?:me\s+)?(?:a\s+)?(?:health\s+)?(?:snapshot|card)"
    r"|shareable\s+(?:health\s+)?(?:summary|snapshot|card)",
    re.IGNORECASE,
)

# Natural-language phrases that should trigger /restart
_RESTART_PATTERN = re.compile(
    r"^(?:please\s+)?(?:restart|reboot|bounce)"
    r"(?:\s+(?:the\s+)?bot)?(?:\s+(?:now|please))?"
    r"[.!]?\s*$",
    re.IGNORECASE,
)


class MessageRouter:
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
        self._post_ingest_cb: callable | None = None  # (lab_results, user_id) targeted analysis
        self._post_ingest_sync_cb: callable | None = None  # () -> None: clean sync + context refresh
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

    async def try_unlock(
        self,
        passphrase: str,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Attempt vault unlock with given passphrase. Used by both inline and two-step flows."""
        user_id = update.effective_user.id
        self._awaiting_passphrase.discard(user_id)

        is_new_vault = not self._config.manifest_path.exists()

        # Immediate feedback — Argon2id derivation takes a few seconds
        status_msg = await update.effective_chat.send_message(
            "Deriving key..." if not is_new_vault else "Creating vault..."
        )

        if is_new_vault:
            await asyncio.to_thread(self._km.setup, passphrase)
        else:
            ok = await asyncio.to_thread(self._km.unlock, passphrase)
            if not ok:
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                await update.effective_chat.send_message(
                    "Invalid passphrase. Try again with /unlock."
                )
                return

        # Success path (both new vault and existing vault)
        db = self._get_db()

        # Load identity profile → compile patterns → enhance PII detection
        self._load_identity_profile(user_id, db)

        try:
            await status_msg.delete()
        except Exception:
            pass

        if is_new_vault:
            await update.effective_chat.send_message(
                "Vault created. Session active for 30 minutes.\n\n"
                "How it works:\n"
                "  - All data encrypted on your machine (AES-256-GCM)\n"
                "  - Intelligence computed locally (no cloud processing)\n"
                "  - Only anonymized, de-identified data reaches the AI\n"
                "  - Your passphrase is never stored anywhere\n\n"
                "Want to build your health profile? It helps me give\n"
                "better, personalized analysis. (Type 'yes' or /onboard)\n\n"
                "Or jump right in:\n"
                "  Upload a lab PDF · /help · Ask a health question"
            )
            self._awaiting_onboard_consent.add(user_id)
        else:
            connected_line = ""
            if self._connected_sources_cb:
                try:
                    connected_line = self._connected_sources_cb()
                except Exception:
                    pass

            from healthbot._version import __version__
            version_line = f"v{__version__}"
            try:
                import os
                import subprocess
                repo_dir = os.path.dirname(os.path.dirname(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                ))
                result = subprocess.run(
                    ["git", "rev-parse", "--short", "HEAD"],
                    capture_output=True, text=True, timeout=3,
                    cwd=repo_dir,
                )
                if result.returncode == 0:
                    version_line += f" ({result.stdout.strip()})"
            except Exception:
                pass

            if connected_line:
                await update.effective_chat.send_message(
                    f"Vault unlocked. Session active for 30 minutes.\n"
                    f"{connected_line}\n"
                    f"Running: {version_line}\n\n"
                    "Quick actions:\n"
                    "  /insights — Health dashboard\n"
                    "  /upload — Upload a lab PDF\n"
                    "  /sync — Sync connected wearables\n"
                    "  Or just type a health question.\n\n"
                    "Type /help for all commands."
                )
            else:
                await update.effective_chat.send_message(
                    f"Vault unlocked. Session active for 30 minutes.\n"
                    f"Running: {version_line}\n\n"
                    "Quick actions:\n"
                    "  /insights — Health dashboard\n"
                    "  /upload — Upload a lab PDF\n"
                    "  /connectors — Set up data sources\n"
                    "  Or just type a health question.\n\n"
                    "Type /help for all commands."
                )
        if self._on_unlock_callback:
            await self._on_unlock_callback(context.bot, update.effective_chat.id)

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

        # Natural language lab deletion (before reset check — more specific)
        if (
            update.message.text
            and self._km.is_unlocked
            and self._reset_handlers
            and _DELETE_LABS_PATTERN.search(update.message.text.strip())
        ):
            await self._reset_handlers.delete_labs(update, context)
            return

        # Natural language reset detection (before LLM)
        if (
            update.message.text
            and self._km.is_unlocked
            and self._reset_handlers
            and _RESET_PATTERN.match(update.message.text.strip())
        ):
            await self._reset_handlers.reset(update, context)
            return

        # Natural language onboarding detection (before LLM)
        if (
            update.message.text
            and self._km.is_unlocked
            and self._onboard_handlers
            and _ONBOARD_PATTERN.match(update.message.text.strip())
        ):
            await self._onboard_handlers.onboard(update, context)
            return

        # Natural language wearable status check (deterministic, before auth/LLM).
        # Must run BEFORE auth patterns — "is whoop set up?" is status, not auth.
        if (
            update.message.text
            and self._km.is_unlocked
            and self._data_handlers
            and _WEARABLE_STATUS_PATTERN.search(update.message.text)
        ):
            await self._handle_wearable_status_query(update, user_id)
            return

        # Natural language wearable auth detection (before LLM)
        if update.message.text and self._km.is_unlocked and self._data_handlers:
            if _WHOOP_AUTH_PATTERN.search(update.message.text):
                await self._data_handlers.whoop_auth(update, context)
                return
            if _OURA_AUTH_PATTERN.search(update.message.text):
                await self._data_handlers.oura_auth(update, context)
                return

        # Natural language restart detection (no unlock required, auth only)
        if (
            update.message.text
            and self._session_handlers
            and _RESTART_PATTERN.match(update.message.text.strip())
        ):
            await self._session_handlers.restart(update, context)
            return

        # Technical troubleshoot detection → Claude CLI (before LLM)
        if (
            update.message.text
            and self._km.is_unlocked
            and _TROUBLESHOOT_PATTERN.search(update.message.text)
        ):
            await self._handle_troubleshoot(update, update.message.text)
            return

        # Natural language pause/unpause overdue notifications (before LLM)
        if (
            update.message.text
            and self._km.is_unlocked
            and self._session_handlers
        ):
            pause_match = _PAUSE_OVERDUE_PATTERN.search(update.message.text)
            if pause_match:
                duration_text = pause_match.group(1) or pause_match.group(2)
                await self._session_handlers.pause_overdue(
                    update, context, duration_text,
                )
                return
            if _UNPAUSE_OVERDUE_PATTERN.search(update.message.text):
                await self._session_handlers.unpause_overdue(update, context)
                return

        # Natural language health status check → quick summary
        if (
            update.message.text
            and self._km.is_unlocked
            and _STATUS_CHECK_PATTERN.match(update.message.text.strip())
        ):
            await self._handle_status_check(update)
            return

        # Visual health request → health card chart
        if (
            update.message.text
            and self._km.is_unlocked
            and _VISUAL_HEALTH_PATTERN.search(update.message.text)
        ):
            await self._handle_visual_health(update)
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

    async def _handle_status_check(self, update: Update) -> None:
        """Handle NL health status check with a quick summary."""
        try:
            from healthbot.reasoning.digest import build_quick_summary

            db = self._get_db()
            uid = update.effective_user.id if update.effective_user else 0
            summary = build_quick_summary(db, uid)
            if summary:
                await update.message.reply_text(summary)
            else:
                await update.message.reply_text(
                    "No health data yet. Upload a lab PDF or use /sync to get started."
                )
        except Exception as e:
            logger.error("Status check failed: %s", e)
            await update.message.reply_text(
                "Couldn't build summary. Try /insights for details."
            )

    async def _handle_visual_health(self, update: Update) -> None:
        """Handle visual health request — send a health card chart."""
        try:
            import io

            user_id = update.effective_user.id if update.effective_user else 0
            from healthbot.export.chart_dispatch import dispatch as chart_dispatch

            chart_bytes = chart_dispatch(
                {"type": "health_card"}, self._get_db(), user_id,
            )
            if chart_bytes:
                img = io.BytesIO(chart_bytes)
                img.name = "health_card.png"
                await update.message.reply_photo(
                    photo=img, caption="Your Health Snapshot",
                )
            else:
                await update.message.reply_text(
                    "Not enough data for a visual snapshot yet.\n"
                    "Upload lab PDFs or sync wearable data to get started."
                )
        except Exception as e:
            logger.error("Visual health card failed: %s", e)
            await update.message.reply_text(
                "Couldn't generate health snapshot. Try /insights for details."
            )

    async def _handle_wearable_status_query(
        self, update: Update, user_id: int,
    ) -> None:
        """Handle NL wearable status questions with a direct, deterministic answer."""
        from healthbot.security.keychain import Keychain

        keychain = Keychain()
        db = self._get_db()
        lines: list[str] = []

        for name, cred_key, provider, sync_cmd, auth_cmd in [
            ("WHOOP", "whoop_client_id", "whoop", "/sync", "/whoop_auth"),
            ("Oura Ring", "oura_client_id", "oura", "/oura", "/oura_auth"),
        ]:
            stored = keychain.retrieve(cred_key)
            if not stored:
                # Don't nag about unconfigured wearables — skip silently
                continue

            # Validate credential format
            if (
                self._data_handlers
                and not self._data_handlers._is_valid_credential(stored)
            ):
                lines.append(
                    f"{name}: Credentials are corrupted. "
                    f"Run {auth_cmd} reset to fix it."
                )
                continue

            # Check for synced data
            try:
                rows = db.query_wearable_daily(
                    provider=provider, limit=1, user_id=user_id,
                )
                if rows:
                    w = rows[0]
                    date = w.get("_date", w.get("date", ""))
                    bits = []
                    if w.get("hrv"):
                        bits.append(f"HRV {w['hrv']}ms")
                    if w.get("rhr"):
                        bits.append(f"RHR {w['rhr']}bpm")
                    if w.get("recovery_score"):
                        bits.append(f"Recovery {w['recovery_score']}")
                    if w.get("sleep_score"):
                        bits.append(f"Sleep {w['sleep_score']}")
                    if w.get("strain"):
                        bits.append(f"Strain {w['strain']}")
                    metrics = f" {', '.join(bits)}." if bits else ""
                    line = f"{name}: Working. Last sync: {date}.{metrics}"
                    # Add total record count + date range
                    try:
                        stats = db.conn.execute(
                            "SELECT COUNT(*) as cnt, MIN(date) as first, "
                            "MAX(date) as last FROM wearable_daily "
                            "WHERE provider = ?",
                            (provider,),
                        ).fetchone()
                        if stats and stats["cnt"]:
                            line += (
                                f"\n  Total: {stats['cnt']} daily records"
                                f" ({stats['first']} to {stats['last']})."
                            )
                    except Exception:
                        pass
                    lines.append(line)
                else:
                    lines.append(
                        f"{name}: Connected but no data synced yet. "
                        f"Run {sync_cmd} to pull your data."
                    )
            except Exception:
                lines.append(f"{name}: Connected (couldn't check data).")

        await update.message.reply_text("\n\n".join(lines))

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

    async def _handle_free_text(self, update: Update, user_id: int) -> None:
        """Route all free-text conversation through Claude CLI."""
        # Emergency triage (deterministic — always runs first)
        text = update.message.text
        triage = TriageEngine()
        level, msg = triage.check_emergency_keywords(text)
        if level:
            await update.message.reply_text(f"EMERGENCY: {msg}")
            return

        claude = self._get_claude() if self._get_claude else None
        if claude is None:
            await update.message.reply_text(
                "Claude CLI not available. Install it:\n"
                "  brew install claude-code\n"
                "Use /commands for health analysis."
            )
            return

        # Send a visible placeholder so the user knows it's working
        thinking_msg = None
        try:
            thinking_msg = await update.message.reply_text("Thinking...")
            async with TypingIndicator(update.effective_chat):
                response, pii_warnings = await asyncio.to_thread(
                    claude.handle_message, text, user_id,
                )

            # Check for error sentinel responses
            from healthbot.llm.claude_client import (
                _CLI_ERROR_RESPONSE,
                _TIMEOUT_RESPONSE,
            )
            if response == _TIMEOUT_RESPONSE:
                await update.message.reply_text(
                    "Claude took too long to respond.\n"
                    "Try a shorter question, or use a /command."
                )
                return
            if _CLI_ERROR_RESPONSE in response or response.startswith(
                "Claude CLI error:"
            ):
                await update.message.reply_text(
                    "Claude CLI returned an error.\n"
                    "Run /claude_auth check to diagnose.\n"
                    "Use /commands for health analysis in the meantime."
                )
                return

            if self._exchange_cb:
                self._exchange_cb(text, response)
            response = strip_markdown(response)
            for page in paginate(response):
                await update.message.reply_text(page)

            # Generate charts requested by Claude via CHART blocks
            for chart_req in getattr(claude, "_pending_charts", [])[:3]:
                try:
                    import io

                    if not isinstance(chart_req, dict):
                        continue
                    from healthbot.export.chart_dispatch import dispatch as chart_dispatch

                    chart_bytes = chart_dispatch(chart_req, self._get_db(), user_id)
                    if chart_bytes:
                        chart_type = chart_req.get("type", "trend")
                        label = chart_req.get("metric", chart_type)
                        img = io.BytesIO(chart_bytes)
                        img.name = f"{chart_type}_{label}.png"
                        await update.message.reply_photo(photo=img)
                except Exception as exc:
                    logger.debug("CHART block skipped: %s", exc)
        except Exception as e:
            from healthbot.llm.claude_client import CLIAuthError

            if isinstance(e, CLIAuthError):
                await update.message.reply_text(
                    "Claude CLI is not authenticated.\n"
                    "Run 'claude login' in your terminal,\n"
                    "or /claude_auth setup to use an API key."
                )
                return
            logger.error("Claude conversation error: %s", e)
            await update.message.reply_text(
                "Error talking to Claude. Try again or use a /command.\n"
                "Run /claude_auth check if this keeps happening."
            )
        finally:
            if thinking_msg:
                try:
                    await thinking_msg.delete()
                except Exception:
                    pass

    async def _post_ingestion_analysis(
        self, update: Update, user_id: int, lab_results: list,
    ) -> None:
        """Trigger Claude deep analysis after new lab data arrives."""
        claude = self._get_claude() if self._get_claude else None
        if claude is None:
            return

        # Build a summary of new results (max 20)
        def _val(obj, key, default=""):
            """Get value from LabResult object or dict."""
            if isinstance(obj, dict):
                return obj.get(key, default)
            return getattr(obj, key, default)

        lines = []
        collection_date = None
        for lab in lab_results[:20]:
            name = _val(lab, "test_name", "?")
            val = _val(lab, "value", "?")
            unit = _val(lab, "unit", "")
            flag = _val(lab, "flag", "")
            cd = _val(lab, "date_collected", None)
            line = f"- {name}: {val} {unit}"
            if flag:
                line += f" ({flag})"
            lines.append(line)
            if not collection_date and cd:
                collection_date = cd
        lab_summary = "\n".join(lines)

        # Determine patient age at time of collection
        age_context = ""
        try:
            from healthbot.security.identity_profile import IdentityProfile
            db = self._get_db()
            profile = IdentityProfile(db=db)
            fields = profile.get_all_fields(user_id)
            dob_str = None
            for f in fields:
                if f.get("field_type") == "dob":
                    dob_str = f.get("value")
                    break
            if dob_str:
                from datetime import date as _date
                # Parse YYYY-MM-DD or MM/DD/YYYY
                if "-" in dob_str and len(dob_str.split("-")[0]) == 4:
                    dob = _date.fromisoformat(dob_str)
                elif "/" in dob_str:
                    parts = dob_str.split("/")
                    dob = _date(int(parts[2]), int(parts[0]), int(parts[1]))
                else:
                    dob = None
                if dob:
                    ref_date = collection_date or _date.today()
                    if isinstance(ref_date, str):
                        ref_date = _date.fromisoformat(ref_date)
                    age = (
                        ref_date.year - dob.year
                        - ((ref_date.month, ref_date.day) < (dob.month, dob.day))
                    )
                    age_context = f"Patient age: {age} at time of collection. "
        except Exception:
            pass

        date_context = ""
        if collection_date:
            date_context = f"Collection date: {collection_date}. "

        prompt = (
            f"New lab results just arrived:\n{lab_summary}\n\n"
            f"{date_context}{age_context}"
            "Give me a brief analysis — plain text only, no markdown "
            "formatting (no **, no ###, no tables). Keep it short:\n"
            "1. What stands out or changed from prior results\n"
            "2. Anything concerning (be direct, account for age)\n"
            "3. What to test next (if anything)\n"
            "Skip normal results. Only mention what matters."
        )

        try:
            async with TypingIndicator(update.effective_chat):
                response, _ = await asyncio.to_thread(
                    claude.handle_message, prompt, user_id,
                )
            if response:
                response = strip_markdown(response)
                for page in paginate(response):
                    await update.message.reply_text(page)
        except Exception as e:
            logger.warning("Post-ingestion analysis failed: %s", e)

    async def _handle_troubleshoot(self, update: Update, user_text: str) -> None:
        """Route technical questions to Claude CLI for debugging.

        Pulls recent errors from the error buffer, builds a debug prompt
        (no PHI), and calls Claude CLI with full tool access (can read/edit
        source code, run tests, and restart the bot to fix issues).
        """
        await update.message.reply_text("Looking into it...")

        # Build error context from recent buffer
        error_lines: list[str] = []
        if self._get_errors:
            for rec in self._get_errors():
                line = f"- [{rec.timestamp}] {rec.error_type}: {rec.message}"
                if rec.provider:
                    line += f" (provider: {rec.provider})"
                if rec.hint:
                    line += f"\n  Hint: {rec.hint}"
                error_lines.append(line)

        error_context = "\n".join(error_lines) if error_lines else ""

        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.research.claude_cli_client import ClaudeCLIResearchClient
                from healthbot.security.phi_firewall import PhiFirewall

                fw = self._fw or PhiFirewall()
                client = ClaudeCLIResearchClient(self._config, fw)
                result = await asyncio.to_thread(
                    client.debug, user_text, error_context,
                )

            result = strip_markdown(result)
            for page in paginate(result):
                await update.message.reply_text(page)
        except Exception as e:
            logger.error("Troubleshoot error: %s", e)
            await update.message.reply_text(
                f"Couldn't reach Claude CLI for debugging: {type(e).__name__}\n"
                "Make sure Claude CLI is installed: brew install claude-code"
            )

    async def _handle_genetic_upload(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
        user_id: int,
    ) -> None:
        """Handle uploaded TXT/CSV genetic data files."""
        doc = update.message.document

        try:
            file = await context.bot.get_file(doc.file_id)
            raw_bytes = bytes(await file.download_as_bytearray())
        except Exception as e:
            logger.error("Genetic file download error: %s", e)
            await update.message.reply_text("Failed to download file.")
            return

        # Check if it looks like genetic data (first 50 lines)
        try:
            text = raw_bytes.decode("utf-8", errors="replace")
            header_lines = text.splitlines()[:50]
            has_rsid = any("rsid" in line.lower() for line in header_lines)
            has_snp_data = any(
                line.strip().startswith("rs") for line in header_lines
                if line.strip() and not line.startswith("#")
            )
            if not has_rsid and not has_snp_data:
                await update.message.reply_text(
                    "This doesn't look like a genetic data file.\n"
                    "Expected TellMeGen, 23andMe, or AncestryDNA raw data "
                    "(with rsID column)."
                )
                return
        except Exception:
            await update.message.reply_text("Could not read file as text.")
            return

        await update.message.reply_text("Parsing genetic data...")

        try:
            async with TypingIndicator(update.effective_chat):
                from healthbot.ingest.genetic_parser import GeneticParser

                parser = GeneticParser()
                result = await asyncio.to_thread(parser.parse, raw_bytes)

                if not result.variants:
                    await update.message.reply_text(
                        "No valid genetic variants found in file.\n"
                        f"Lines processed: {result.total_lines}, "
                        f"skipped: {result.skipped_lines}"
                    )
                    return

                # Store variants in encrypted DB
                db = self._get_db()
                stored = 0
                for var in result.variants:
                    db.insert_genetic_variant(
                        user_id=user_id,
                        rsid=var.rsid,
                        chromosome=var.chromosome,
                        position=var.position,
                        variant_data={
                            "genotype": var.genotype,
                            "source": var.source,
                        },
                    )
                    stored += 1

            msg = (
                f"Genetic data imported: {stored:,} variants from "
                f"{result.source}.\n"
                f"Encrypted and stored in vault."
            )
            if result.skipped_lines:
                msg += f"\n({result.skipped_lines} malformed lines skipped)"
            if result.warnings:
                msg += "\n" + "\n".join(result.warnings[:3])

            # Quick risk scan
            try:
                from healthbot.reasoning.genetic_risk import GeneticRiskEngine

                engine = GeneticRiskEngine(db)
                findings = engine.scan_variants(user_id)
                if findings:
                    msg += f"\n\nFound {len(findings)} risk variant(s):"
                    msg += "\n" + engine.format_summary(findings)
                else:
                    msg += "\n\nNo clinically significant risk variants detected."
            except Exception as e:
                logger.warning("Post-import risk scan failed: %s", e)

            for page in paginate(msg):
                await update.message.reply_text(page)

            # Post-ingestion Claude analysis for genetic data
            if not self.upload_mode:
                await self._post_ingestion_genetic_analysis(
                    update, user_id, stored, result.source,
                )
        except Exception as e:
            logger.error("Genetic import error: %s", e)
            await update.message.reply_text(
                f"Error processing genetic data: {type(e).__name__}"
            )

    async def _handle_date_reply(
        self, update: Update, user_id: int,
    ) -> bool:
        """Handle user reply with collection date for undated lab results.

        Parses common date formats, updates all undated observations for
        the pending blob_id, and confirms to the user.
        Returns True if the message was consumed.
        """
        from datetime import datetime

        text = (update.message.text or "").strip()
        blob_id = self._pending_date.get(user_id)
        if not blob_id:
            return False

        # Try common date formats
        parsed = None
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y",
                     "%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y"):
            try:
                parsed = datetime.strptime(text, fmt).date()
                break
            except ValueError:
                continue

        if not parsed:
            # Don't consume — let it route to Claude as normal text
            return False

        # Validate: not future, not absurdly old
        from datetime import date as date_type
        today = date_type.today()
        if parsed > today or (today - parsed).days > 365 * 20:
            await update.message.reply_text(
                "That date doesn't look right. Please send the "
                "collection date (e.g. 01/27/2014)."
            )
            return True

        # Update undated observations in the database
        self._pending_date.pop(user_id, None)
        try:
            db = self._get_db()
            cursor = db.conn.execute(
                "UPDATE observations SET date_effective = ? "
                "WHERE source_doc_id = ? AND "
                "(date_effective IS NULL OR date_effective = '')",
                (parsed.isoformat(), blob_id),
            )
            db.conn.commit()
            updated = cursor.rowcount
            logger.info(
                "Stamped collection date %s on %d observations (blob %s)",
                parsed.isoformat(), updated, blob_id,
            )
            await update.message.reply_text(
                f"Got it — {parsed.strftime('%B %d, %Y')}. "
                f"Updated {updated} lab results."
            )
            # Sync so Claude sees the date immediately
            if self._post_ingest_sync_cb:
                asyncio.create_task(
                    asyncio.to_thread(self._post_ingest_sync_cb)
                )
        except Exception as e:
            logger.error("Failed to stamp date on observations: %s", e)
            await update.message.reply_text(
                "Saved, but couldn't update the database. "
                "Try /delete_labs and re-upload."
            )
        return True

    async def _handle_photo(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Analyze an inbound photo using two-stage vision pipeline."""
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        try:
            from healthbot.llm.vision_client import VisionClient

            vision = VisionClient(
                base_url=self._config.ollama_url,
                timeout=self._config.ollama_timeout,
            )
            if not vision.is_available():
                await update.message.reply_text(
                    "Vision model not available. Pull it with:\n"
                    "ollama pull gemma3:27b"
                )
                return

            await update.message.reply_text("Analyzing photo...")

            async with TypingIndicator(update.effective_chat):
                # Download the highest-resolution version
                photo = update.message.photo[-1]
                file = await context.bot.get_file(photo.file_id)
                image_bytes = bytes(await file.download_as_bytearray())

                # Build user context from recent health data
                user_context = ""

                result = await asyncio.to_thread(
                    vision.analyze_photo, image_bytes, user_context
                )

            for page in paginate(strip_markdown(result)):
                await update.message.reply_text(page)

            # Auto-extract structured data from the vision description
            try:
                from healthbot.reasoning.photo_extractor import (
                    classify_photo,
                    format_extraction_summary,
                )

                # Extract the description part (before interpretation)
                marker = "**Health context:**"
                desc_text = result.split(marker)[0] if marker in result else result
                classification = classify_photo(desc_text)
                summary = format_extraction_summary(classification)
                if summary:
                    await update.message.reply_text(summary)
            except Exception as e:
                logger.debug("Photo extraction skipped: %s", e)

        except Exception as e:
            logger.error("Photo analysis error: %s", e)
            await update.message.reply_text(
                "Error analyzing photo. Please try again."
            )

    async def _handle_document(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle uploaded documents."""
        if not self._km.is_unlocked:
            await update.message.reply_text("Vault is locked. Send /unlock first.")
            return

        user_id = update.effective_user.id if update.effective_user else 0
        doc = update.message.document

        # ZIP files: detect contents and route
        if doc.file_name and doc.file_name.lower().endswith(".zip"):
            await self._handle_zip_upload(update, context)
            return

        # Genetic data files (TXT/CSV from TellMeGen, 23andMe, AncestryDNA)
        if doc.file_name and doc.file_name.lower().endswith((".txt", ".csv")):
            await self._handle_genetic_upload(update, context, user_id)
            return

        if not doc.file_name or not doc.file_name.lower().endswith(".pdf"):
            await update.message.reply_text(
                "Supported files: PDF, ZIP, TXT/CSV (genetic data)."
            )
            return

        # Live status: show the user what's happening at each pipeline stage
        import queue as _queue

        status_msg = await update.message.reply_text(
            "Downloading PDF locally (never sent to AI)..."
        )
        progress_q: _queue.Queue[str] = _queue.Queue()

        def _on_progress(msg: str) -> None:
            """Called from worker thread to report pipeline stage."""
            progress_q.put(msg)

        async def _poll_progress() -> None:
            """Async task that updates the Telegram status message."""
            last_text = ""
            while True:
                await asyncio.sleep(0.5)
                msg = None
                # Drain queue — only keep the latest message
                while not progress_q.empty():
                    try:
                        msg = progress_q.get_nowait()
                    except _queue.Empty:
                        break
                if msg and msg != last_text:
                    try:
                        await status_msg.edit_text(msg)
                        last_text = msg
                    except Exception:
                        pass

        db = None
        vault = None
        pdf_bytes = None
        try:
            async with TypingIndicator(update.effective_chat):
                file = await context.bot.get_file(doc.file_id)
                pdf_bytes = await file.download_as_bytearray()

                from healthbot.ingest.lab_pdf_parser import LabPdfParser
                from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
                from healthbot.security.pdf_safety import PdfSafety
                from healthbot.security.vault import Vault

                db = self._get_db()
                vault = Vault(self._config.blobs_dir, self._km)
                safety = PdfSafety(self._config)
                parser = LabPdfParser(safety, config=self._config)
                triage = TriageEngine()

                ingest = TelegramPdfIngest(
                    vault, db, parser, safety, triage,
                    config=self._config, on_progress=_on_progress,
                    phi_firewall=self._fw,
                )

                # Run ingestion + progress poller concurrently
                progress_task = asyncio.create_task(_poll_progress())
                try:
                    result = await asyncio.wait_for(
                        asyncio.to_thread(
                            ingest.ingest,
                            bytes(pdf_bytes),
                            filename=doc.file_name,
                            user_id=user_id,
                        ),
                        timeout=300,  # 5 minutes max per PDF
                    )
                finally:
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass

                if result.is_rescan and result.success:
                    n_new = result.rescan_new
                    n_exist = result.rescan_existing
                    if n_new:
                        s = "s" if n_new != 1 else ""
                        msg = (
                            f"Rescanned: found {n_new} new result{s} "
                            f"({n_exist} already stored)."
                        )
                    elif result.clinical_facts_count:
                        doc_label = (
                            result.doc_type.replace("_", " ")
                            if result.doc_type else "document"
                        )
                        msg = (
                            f"Rescanned {doc_label}: extracted "
                            f"{result.clinical_facts_count} new medical "
                            f"fact{'s' if result.clinical_facts_count != 1 else ''}."
                        )
                        if result.clinical_summary:
                            msg += f"\n\n{result.clinical_summary}"
                    else:
                        msg = (
                            f"Rescanned: no new results "
                            f"({n_exist} already stored)."
                        )
                elif result.success:
                    n = len(result.lab_results)
                    if n:
                        # Include collection date in confirmation
                        _cd = next(
                            (l.date_collected for l in result.lab_results
                             if l.date_collected),
                            None,
                        )
                        if _cd:
                            msg = (
                                f"Saved {n} lab result{'s' if n != 1 else ''} "
                                f"(collected {_cd.strftime('%m/%d/%Y')})."
                            )
                        else:
                            msg = f"Saved {n} lab result{'s' if n != 1 else ''}."

                        # Concise triage: show flagged items, counts only
                        flagged = triage.get_triage_flagged(result.lab_results)
                        urgent_items = flagged["critical"] + flagged["urgent"]
                        watch_count = len(flagged["watch"])
                        normal_count = len(flagged["normal"])

                        if urgent_items:
                            msg += "\n\nFlagged:"
                            for lab in urgent_items:
                                ref = ""
                                if lab.reference_low is not None and lab.reference_high is not None:
                                    ref = f" (ref {lab.reference_low}-{lab.reference_high})"
                                elif lab.reference_high is not None:
                                    ref = f" (ref <{lab.reference_high})"
                                elif lab.reference_low is not None:
                                    ref = f" (ref >{lab.reference_low})"
                                msg += f"\n- {lab.test_name}: {lab.value} {lab.unit}{ref}"

                        parts = []
                        if watch_count:
                            parts.append(f"{watch_count} worth watching")
                        if normal_count:
                            parts.append(f"{normal_count} normal")
                        if parts:
                            msg += f"\n\n{', '.join(parts)}."

                        msg += "\nAsk me to break it down if you want details."

                        if result.missing_date:
                            msg += (
                                "\n\nI couldn't find the collection date "
                                "in the PDF. When were these labs drawn? "
                                "(e.g. 01/27/2014)"
                            )
                            self._pending_date[user_id] = result.blob_id

                        if result.cross_doc_dupes:
                            msg += (
                                f"\n\nSkipped {result.cross_doc_dupes} "
                                f"duplicate{'s' if result.cross_doc_dupes != 1 else ''} "
                                f"already in your records."
                            )

                    elif result.clinical_facts_count:
                        doc_label = (
                            result.doc_type.replace("_", " ")
                            if result.doc_type else "document"
                        )
                        msg = (
                            f"Saved {result.clinical_facts_count} "
                            f"medical fact{'s' if result.clinical_facts_count != 1 else ''} "
                            f"from {doc_label}."
                        )
                        if result.clinical_summary:
                            msg += f"\n\n{result.clinical_summary}"
                        msg += "\nAsk me anything about it."
                    elif result.cross_doc_dupes:
                        msg = (
                            f"All {result.cross_doc_dupes} lab results "
                            f"in this PDF are already in your records. "
                            f"No new data to store."
                        )
                    else:
                        msg = "PDF stored. No lab results or medical facts found."
                else:
                    msg = f"Ingestion failed: {'; '.join(result.warnings)}"

                # Post-processing for success cases (logging + triggers)
                if result.success:
                    if result.warnings:
                        logger.info("Ingestion warnings: %s", "; ".join(result.warnings))

                    # Track ingestion/upload count
                    if self.ingestion_mode and self._ingestion_count_cb:
                        self._ingestion_count_cb()
                    if self.upload_mode and self._upload_count_cb:
                        self._upload_count_cb()

                    # Trigger post-ingestion Claude analysis
                    if result.lab_results and not self.ingestion_mode and not self.upload_mode:
                        await self._post_ingestion_analysis(
                            update, user_id, result.lab_results,
                        )

                    # Review medication reminders against new lab data
                    if result.lab_results and not self.ingestion_mode:
                        try:
                            from healthbot.reasoning.med_reminders import (
                                review_reminders_after_ingestion,
                            )

                            canonical_names = {
                                lr.canonical_name
                                for lr in result.lab_results
                                if lr.canonical_name
                            }
                            reminder_msgs = review_reminders_after_ingestion(
                                db, user_id, canonical_names,
                            )
                            for rmsg in reminder_msgs:
                                await update.message.reply_text(rmsg)
                        except Exception as e:
                            logger.debug("Post-ingestion reminder review: %s", e)

                    # Trigger targeted deterministic analysis (non-blocking)
                    if result.lab_results and self._post_ingest_cb:
                        asyncio.create_task(
                            asyncio.to_thread(
                                self._post_ingest_cb,
                                result.lab_results,
                                user_id,
                            )
                        )

                    # Sync clean DB + refresh Claude context so new labs
                    # (including dates) are immediately available in chat
                    if result.lab_results:
                        if self._post_ingest_sync_cb:
                            asyncio.create_task(
                                asyncio.to_thread(self._post_ingest_sync_cb)
                            )
                        else:
                            # Fallback: run clean sync directly when no
                            # scheduler callback is registered (no job_queue)
                            asyncio.create_task(
                                asyncio.to_thread(self._fallback_clean_sync)
                            )

                    # Send redacted PDF back if enabled
                    if (
                        self._config.send_redacted_pdf
                        and result.redacted_blob_id
                        and vault is not None
                    ):
                        try:
                            import io

                            redacted_bytes = vault.retrieve_blob(
                                result.redacted_blob_id,
                            )
                            buf = io.BytesIO(redacted_bytes)
                            buf.name = "redacted.pdf"
                            await update.message.reply_document(
                                document=buf,
                                caption="Redacted copy (all PII removed)",
                            )
                        except Exception:
                            logger.warning("Failed to send redacted PDF back")

            # Delete status message, show final result
            try:
                await status_msg.delete()
            except Exception:
                pass

            for page in paginate(msg):
                await update.message.reply_text(page)

        except TimeoutError:
            logger.error("PDF ingestion timed out (300s): %s", doc.file_name)
            self._cleanup_failed_ingestion(db, vault, pdf_bytes)
            try:
                await status_msg.delete()
            except Exception:
                pass
            await update.message.reply_text(
                "PDF processing timed out after 5 minutes. "
                "Try uploading a smaller document or fewer pages."
            )
        except Exception as e:
            from healthbot.security.key_manager import LockedError

            self._cleanup_failed_ingestion(db, vault, pdf_bytes)
            try:
                await status_msg.delete()
            except Exception:
                pass
            if isinstance(e, (LockedError, sqlite3.ProgrammingError)):
                logger.warning("PDF processing interrupted by vault lock: %s", e)
                await update.message.reply_text(
                    "Vault locked during processing. Please unlock and try again."
                )
            else:
                logger.error("PDF ingestion error: %s", str(e))
                await update.message.reply_text(
                    f"Error processing PDF: {type(e).__name__}"
                )

    @staticmethod
    def _cleanup_failed_ingestion(db, vault, pdf_bytes) -> None:
        """Remove orphaned blob + doc record after a failed/timed-out ingestion."""
        if not pdf_bytes or not db:
            return
        try:
            import hashlib
            sha = hashlib.sha256(bytes(pdf_bytes)).hexdigest()
            row = db.document_exists_by_sha256(sha)
            if row:
                blob_id = row.get("enc_blob_path", "")
                try:
                    db.delete_document(row["doc_id"])
                except Exception:
                    pass
                if blob_id and vault:
                    try:
                        vault.delete_blob(blob_id)
                    except Exception:
                        pass
                logger.info("Cleaned up failed ingestion: doc=%s blob=%s",
                            row.get("doc_id", "?"), blob_id)
        except Exception:
            pass

    async def _handle_zip_upload(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle ZIP: detect contents and route accordingly."""
        import io
        import zipfile

        max_zip = 500 * 1024 * 1024  # 500 MB memory guard
        doc = update.message.document
        if doc.file_size and doc.file_size > max_zip:
            mb = doc.file_size // (1024 * 1024)
            await update.message.reply_text(
                f"ZIP too large ({mb} MB). Max is 500 MB."
            )
            return

        try:
            file = await context.bot.get_file(doc.file_id)
            zip_bytes = bytes(await file.download_as_bytearray())
        except Exception as e:
            logger.error("ZIP download error: %s", e)
            if "too big" in str(e).lower():
                incoming = self._config.incoming_dir
                await update.message.reply_text(
                    "That file is too large for Telegram.\n\n"
                    "Transfer the ZIP to your Mac and drop it in:\n"
                    f"  {incoming}\n\n"
                    "Then send /import and I'll process it from there."
                )
            else:
                await update.message.reply_text("Failed to download ZIP file.")
            return

        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                has_export_xml = any(n.endswith("export.xml") for n in names)
                pdf_names = [
                    n for n in names
                    if n.lower().endswith(".pdf") and not n.startswith("__MACOSX")
                ]
                xml_names = [
                    n for n in names
                    if n.lower().endswith(".xml")
                    and not n.endswith("export.xml")
                    and not n.startswith("__MACOSX")
                ]
                json_names = [
                    n for n in names
                    if n.lower().endswith(".json") and not n.startswith("__MACOSX")
                ]
        except zipfile.BadZipFile:
            await update.message.reply_text("Invalid ZIP file.")
            return

        # Apple Health takes priority
        if has_export_xml:
            await self._handle_apple_health_bytes(update, zip_bytes)
            return

        processable = pdf_names + xml_names + json_names
        if not processable:
            await update.message.reply_text(
                "No PDFs, XML, or Apple Health data found in this ZIP."
            )
            return

        n_files = len(processable)
        s = "s" if n_files != 1 else ""
        await update.message.reply_text(
            f"Processing ZIP: {n_files} file{s} found..."
        )

        user_id = update.effective_user.id if update.effective_user else 0
        total_labs = 0
        total_clinical = 0
        rescanned = 0
        rescan_new_labs = 0
        errors = 0

        from telegram.constants import ChatAction
        await update.effective_chat.send_action(ChatAction.TYPING)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for i, name in enumerate(processable, 1):
                if len(processable) > 3 and i % 3 == 0:
                    await update.message.reply_text(
                        f"Processing file {i}/{len(processable)}..."
                    )

                try:
                    entry_bytes = zf.read(name)
                    basename = name.rsplit("/", 1)[-1] if "/" in name else name

                    if name.lower().endswith(".pdf"):
                        labs, clinical, was_rescan = await self._ingest_pdf_from_zip(
                            entry_bytes, basename, user_id,
                        )
                        total_labs += labs
                        total_clinical += clinical
                        if was_rescan:
                            rescanned += 1
                            rescan_new_labs += labs

                    elif name.lower().endswith(".xml"):
                        self._ingest_xml_from_zip(entry_bytes)

                    elif name.lower().endswith(".json"):
                        self._ingest_json_from_zip(entry_bytes)

                except Exception as e:
                    logger.warning("ZIP entry %s failed: %s", name, e)
                    errors += 1

        # Build summary
        parts = []
        if total_labs:
            parts.append(f"{total_labs} lab result{'s' if total_labs != 1 else ''}")
        if total_clinical:
            parts.append(f"{total_clinical} medical fact{'s' if total_clinical != 1 else ''}")
        if rescanned:
            detail = f", {rescan_new_labs} new" if rescan_new_labs else ", no new results"
            parts.append(f"{rescanned} rescanned{detail}")
        if errors:
            parts.append(f"{errors} file{'s' if errors != 1 else ''} failed")

        if parts:
            msg = f"ZIP processed: {', '.join(parts)}. Encrypted and stored in vault."
        else:
            msg = "ZIP processed but no medical data was found in the files."

        # Track ingestion/upload count
        if self.ingestion_mode and self._ingestion_count_cb:
            self._ingestion_count_cb()
        if self.upload_mode and self._upload_count_cb:
            self._upload_count_cb()

        for page in paginate(msg):
            await update.message.reply_text(page)

    async def _ingest_pdf_from_zip(
        self, pdf_bytes: bytes, filename: str, user_id: int,
    ) -> tuple[int, int, bool]:
        """Ingest a single PDF from a ZIP. Returns (lab_count, clinical_count, was_rescan)."""
        from healthbot.ingest.lab_pdf_parser import LabPdfParser
        from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
        from healthbot.security.pdf_safety import PdfSafety
        from healthbot.security.vault import Vault

        db = self._get_db()
        vault = Vault(self._config.blobs_dir, self._km)
        safety = PdfSafety(self._config)
        parser = LabPdfParser(safety, config=self._config)
        triage = TriageEngine()

        ingest = TelegramPdfIngest(
            vault, db, parser, safety, triage, config=self._config,
            phi_firewall=self._fw,
        )
        result = await asyncio.wait_for(
            asyncio.to_thread(
                ingest.ingest, pdf_bytes, filename=filename, user_id=user_id,
            ),
            timeout=300,  # 5 minutes max per PDF in ZIP
        )

        return len(result.lab_results), result.clinical_facts_count, result.is_rescan

    def _ingest_xml_from_zip(self, xml_bytes: bytes) -> None:
        """Try MyChart CCDA import for an XML file from ZIP."""
        try:
            from healthbot.ingest.mychart_import import MyChartImporter
            from healthbot.security.vault import Vault

            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)
            importer = MyChartImporter(db, vault, phi_firewall=self._fw)
            importer.import_ccda_bytes(xml_bytes)
        except Exception as e:
            logger.debug("XML import skipped (not CCDA): %s", e)

    def _ingest_json_from_zip(self, json_bytes: bytes) -> None:
        """Try FHIR bundle import for a JSON file from ZIP."""
        try:
            from healthbot.ingest.mychart_import import MyChartImporter
            from healthbot.security.vault import Vault

            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)
            importer = MyChartImporter(db, vault, phi_firewall=self._fw)
            importer.import_fhir_bundle(json_bytes)
        except Exception as e:
            logger.debug("JSON import skipped (not FHIR): %s", e)

    async def _handle_apple_health_bytes(
        self, update: Update, zip_bytes: bytes,
    ) -> None:
        """Handle Apple Health ZIP from pre-downloaded bytes.

        Parses XML first, then inserts in batches with progress updates.
        All DB work runs on the event loop thread (no asyncio.to_thread)
        to avoid SQLite cross-thread deadlocks.
        """
        from healthbot.ingest.apple_health_import import (
            CATEGORY_TYPES,
            SUPPORTED_TYPES,
            AppleHealthImporter,
            AppleHealthImportResult,
        )

        await update.message.reply_text("Processing Apple Health export...")
        try:
            db = self._get_db()
            importer = AppleHealthImporter(db)
            user_id = update.effective_user.id if update.effective_user else 0
            privacy_mode = self._config.privacy_mode

            # Phase 1: parse (CPU-bound but fast relative to DB inserts)
            vitals, workouts, xml_bytes = importer.parse_zip_bytes(
                zip_bytes, privacy_mode,
            )
            total = len(vitals) + len(workouts)
            if total == 0:
                await update.message.reply_text(
                    "No supported health records found in the ZIP file.",
                )
                return

            await update.message.reply_text(
                f"Found {len(vitals)} vitals, {len(workouts)} workouts. Importing...",
            )

            # Phase 2: insert vitals in batches with progress
            result = AppleHealthImportResult()
            canonical_names = list(SUPPORTED_TYPES.values()) + [
                c["canonical"] for c in CATEGORY_TYPES.values()
            ]
            existing_keys = db.get_existing_observation_keys(
                record_type="vital_sign",
                canonical_names=canonical_names,
            )

            batch_size = 5000
            last_pct = -1
            for i in range(0, len(vitals), batch_size):
                batch = vitals[i : i + batch_size]
                importer.insert_vitals_batch(
                    batch, existing_keys, user_id, result,
                )
                pct = int((i + len(batch)) / total * 100)
                if pct >= last_pct + 10:
                    await update.message.reply_text(f"Importing... {pct}%")
                    last_pct = pct
                await asyncio.sleep(0)  # yield to event loop

            # Phase 3: insert workouts (usually small)
            existing_wo_keys = db.get_existing_workout_keys(user_id=user_id)
            importer.insert_workouts_batch(
                workouts, existing_wo_keys, user_id, result,
            )

            # Phase 4: clinical records
            if xml_bytes:
                importer._parse_clinical_records(xml_bytes, user_id, result)

            # Final summary
            if result.records_imported > 0 or result.clinical_records > 0:
                parts = []
                if result.records_imported:
                    type_summary = ", ".join(
                        f"{t}: {c}" for t, c in sorted(result.types_found.items())
                    )
                    parts.append(f"{result.records_imported} vitals ({type_summary})")
                if result.workouts_imported:
                    parts.append(f"{result.workouts_imported} workouts")
                if result.clinical_records:
                    clin_parts = ", ".join(
                        f"{c} {t}" for t, c in result.clinical_breakdown.items()
                    )
                    parts.append(f"{result.clinical_records} clinical records ({clin_parts})")
                msg = "Apple Health import complete: " + ", ".join(parts)
                if result.records_skipped:
                    msg += f"\n({result.records_skipped} duplicates skipped)"
                if privacy_mode == "strict" and not result.clinical_records:
                    msg += "\n(Clinical records skipped — /privacy relaxed to enable)"
            elif result.records_skipped > 0:
                msg = (
                    f"All {result.records_skipped} records already imported — nothing new."
                )
            else:
                msg = "No supported health records found in the ZIP file."

            for page in paginate(msg):
                await update.message.reply_text(page)

            # Trigger clean sync so Claude/MCP see the new data
            if result.records_imported > 0 or result.clinical_records > 0:
                if self._post_ingest_sync_cb:
                    asyncio.create_task(
                        asyncio.to_thread(self._post_ingest_sync_cb)
                    )
                else:
                    asyncio.create_task(
                        asyncio.to_thread(self._fallback_clean_sync)
                    )

            # Post-ingestion Claude analysis for Apple Health
            if result.records_imported > 0 and not self.upload_mode:
                await self._post_ingestion_health_analysis(
                    update, user_id, result.records_imported, result.types_found,
                )
        except Exception as e:
            logger.error("Apple Health import error: %s", e)
            await update.message.reply_text(f"Import failed: {type(e).__name__}")

    async def _post_ingestion_health_analysis(
        self,
        update: Update,
        user_id: int,
        records_imported: int,
        types_found: dict,
    ) -> None:
        """Trigger Claude deep analysis after Apple Health import."""
        claude = self._get_claude() if self._get_claude else None
        if claude is None:
            return

        type_summary = ", ".join(
            f"{t}: {c}" for t, c in sorted(types_found.items())
        )
        prompt = (
            f"New Apple Health data just imported: {records_imported} records.\n"
            f"Types: {type_summary}\n\n"
            "Analyze this in context of my full health history. "
            "Are there any new patterns, concerns, or changes worth noting? "
            "Update hypotheses and create action items as needed. "
            "Reply in plain text only — no markdown formatting."
        )

        try:
            async with TypingIndicator(update.effective_chat):
                response, _ = await asyncio.to_thread(
                    claude.handle_message, prompt, user_id,
                )
            if response:
                response = strip_markdown(response)
                for page in paginate(response):
                    await update.message.reply_text(page)
        except Exception as e:
            logger.warning("Post-Apple Health analysis failed: %s", e)

    async def _post_ingestion_genetic_analysis(
        self,
        update: Update,
        user_id: int,
        variant_count: int,
        source: str,
    ) -> None:
        """Trigger Claude deep analysis after genetic data upload."""
        claude = self._get_claude() if self._get_claude else None
        if claude is None:
            return

        prompt = (
            f"New genetic data just imported: {variant_count:,} variants "
            f"from {source}.\n\n"
            "Analyze the genetic risk findings in context of my full "
            "health history. Cross-reference with lab results. "
            "Are there any new hypotheses? What tests should I prioritize? "
            "Update hypotheses and create action items as needed. "
            "Reply in plain text only — no markdown formatting."
        )

        try:
            async with TypingIndicator(update.effective_chat):
                response, _ = await asyncio.to_thread(
                    claude.handle_message, prompt, user_id,
                )
            if response:
                response = strip_markdown(response)
                for page in paginate(response):
                    await update.message.reply_text(page)
        except Exception as e:
            logger.warning("Post-genetic analysis failed: %s", e)
