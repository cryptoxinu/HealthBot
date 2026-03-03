"""Natural language pattern routing (save/unsave, status, visual health, etc.)."""
from __future__ import annotations

import logging
import re

from telegram import Update
from telegram.ext import ContextTypes

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

# "Save this" message interception → local saved messages
_SAVE_MESSAGE_PATTERN = re.compile(
    r"^(?:save\s+(?:this|that|message|it|msg)"
    r"|bookmark\s+(?:this|that|it|message)"
    r"|keep\s+(?:this|that|message))"
    r"(?:\s*[.!]?\s*$|:\s*(.+))",
    re.IGNORECASE | re.DOTALL,
)

# "Unsave this" message interception → delete from saved messages
_UNSAVE_MESSAGE_PATTERN = re.compile(
    r"^(?:unsave\s+(?:this|that|it|message)"
    r"|unbookmark\s+(?:this|that|it|message)"
    r"|(?:remove|delete)\s+(?:this|that|it)\s+from\s+saved"
    r"|delete\s+(?:this\s+)?saved\s+(?:message|msg)"
    r"|forget\s+(?:this|that))"
    r"[.!?]?\s*$",
    re.IGNORECASE,
)


class IntentInterceptorMixin:
    """Mixin providing natural language intent interception methods."""

    async def _intercept_intents(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
        user_id: int,
    ) -> bool:
        """Run all NL pattern checks. Returns True if a pattern matched."""
        if not update.message.text:
            return False

        text = update.message.text
        text_stripped = text.strip()

        # Natural language lab deletion (before reset check — more specific)
        if (
            self._km.is_unlocked
            and self._reset_handlers
            and _DELETE_LABS_PATTERN.search(text_stripped)
        ):
            await self._reset_handlers.delete_labs(update, context)
            return True

        # Natural language reset detection (before LLM)
        if (
            self._km.is_unlocked
            and self._reset_handlers
            and _RESET_PATTERN.match(text_stripped)
        ):
            await self._reset_handlers.reset(update, context)
            return True

        # Natural language onboarding detection (before LLM)
        if (
            self._km.is_unlocked
            and self._onboard_handlers
            and _ONBOARD_PATTERN.match(text_stripped)
        ):
            await self._onboard_handlers.onboard(update, context)
            return True

        # "Save this" message interception → local saved messages
        if (
            self._km.is_unlocked
            and _SAVE_MESSAGE_PATTERN.match(text_stripped)
        ):
            await self._handle_save_message(update)
            return True

        # "Unsave this" message interception → delete from saved messages
        if (
            self._km.is_unlocked
            and _UNSAVE_MESSAGE_PATTERN.match(text_stripped)
        ):
            await self._handle_unsave_message(update)
            return True

        # Natural language wearable status check (deterministic, before auth/LLM).
        # Must run BEFORE auth patterns — "is whoop set up?" is status, not auth.
        if (
            self._km.is_unlocked
            and self._data_handlers
            and _WEARABLE_STATUS_PATTERN.search(text)
        ):
            await self._handle_wearable_status_query(update, user_id)
            return True

        # Natural language wearable auth detection (before LLM)
        if self._km.is_unlocked and self._data_handlers:
            if _WHOOP_AUTH_PATTERN.search(text):
                await self._data_handlers.whoop_auth(update, context)
                return True
            if _OURA_AUTH_PATTERN.search(text):
                await self._data_handlers.oura_auth(update, context)
                return True

        # Natural language restart detection (no unlock required, auth only)
        if (
            self._session_handlers
            and _RESTART_PATTERN.match(text_stripped)
        ):
            await self._session_handlers.restart(update, context)
            return True

        # Technical troubleshoot detection → Claude CLI (before LLM)
        if (
            self._km.is_unlocked
            and _TROUBLESHOOT_PATTERN.search(text)
        ):
            await self._handle_troubleshoot(update, text)
            return True

        # Natural language pause/unpause overdue notifications (before LLM)
        if (
            self._km.is_unlocked
            and self._session_handlers
        ):
            pause_match = _PAUSE_OVERDUE_PATTERN.search(text)
            if pause_match:
                duration_text = pause_match.group(1) or pause_match.group(2)
                await self._session_handlers.pause_overdue(
                    update, context, duration_text,
                )
                return True
            if _UNPAUSE_OVERDUE_PATTERN.search(text):
                await self._session_handlers.unpause_overdue(update, context)
                return True

        # Natural language health status check → quick summary
        if (
            self._km.is_unlocked
            and _STATUS_CHECK_PATTERN.match(text_stripped)
        ):
            await self._handle_status_check(update)
            return True

        # Visual health request → health card chart
        if (
            self._km.is_unlocked
            and _VISUAL_HEALTH_PATTERN.search(text)
        ):
            await self._handle_visual_health(update)
            return True

        return False

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
                        stats = db.query_wearable_stats(provider)
                        if stats:
                            line += (
                                f"\n  Total: {stats['count']} daily records"
                                f" ({stats['first_date']} to {stats['last_date']})."
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

    async def _handle_save_message(self, update) -> None:
        """Handle 'save this' / 'bookmark this' natural language pattern."""
        user_id = update.effective_user.id
        text = (update.message.text or "").strip()
        match = _SAVE_MESSAGE_PATTERN.match(text)
        if not match:
            return

        colon_text = match.group(1)  # Text after "save this: ..."

        save_text = None
        context_text = None

        if update.message.reply_to_message and update.message.reply_to_message.text:
            # Replying to a message — save the replied-to message
            save_text = update.message.reply_to_message.text
            # Try to get user's original question for context
            replied = update.message.reply_to_message
            if replied.reply_to_message and replied.reply_to_message.text:
                context_text = replied.reply_to_message.text
            elif self._last_user_input:
                context_text = self._last_user_input
        elif colon_text:
            # "save this: <pasted text>"
            save_text = colon_text.strip()
        else:
            # Fallback: save last bot response
            if self._last_bot_response:
                save_text = self._last_bot_response
                context_text = self._last_user_input or None

        if not save_text:
            await update.message.reply_text(
                "Reply to a message with 'save this', or type "
                "'save this: <text>' to save custom text."
            )
            return

        try:
            db = self._get_db()
            db.save_message(user_id, save_text, context=context_text)
            await update.message.reply_text(
                "Saved. /savedmessages to browse."
            )
        except Exception as e:
            logger.warning("Failed to save message: %s", e)
            await update.message.reply_text("Failed to save message.")

    async def _handle_unsave_message(self, update) -> None:
        """Handle 'unsave this' / 'forget this' natural language pattern."""
        user_id = update.effective_user.id
        db = self._get_db()
        all_saved = db.get_saved_messages(user_id)

        if not all_saved:
            await update.message.reply_text("No saved messages to delete.")
            return

        if update.message.reply_to_message and update.message.reply_to_message.text:
            # Reply mode: match replied-to text against saved messages
            target_text = update.message.reply_to_message.text
            match = None
            for msg in all_saved:
                if msg["text"] == target_text:
                    match = msg
                    break
            # Fallback: partial match (bot may have paginated/truncated)
            if not match:
                for msg in all_saved:
                    if target_text in msg["text"] or msg["text"] in target_text:
                        match = msg
                        break
            if match:
                db.delete_saved_message(user_id, match["id"])
                await update.message.reply_text("Unsaved.")
            else:
                await update.message.reply_text(
                    "Message not found in saved messages."
                )
        else:
            # No reply: delete most recent saved message
            newest = all_saved[0]  # already sorted by saved_at DESC
            db.delete_saved_message(user_id, newest["id"])
            preview = newest.get("preview", "")
            await update.message.reply_text(f'Unsaved: "{preview}"')
