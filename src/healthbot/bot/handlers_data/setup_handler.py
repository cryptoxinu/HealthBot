"""Wearable credential setup flow mixin."""
from __future__ import annotations

import re

from telegram import Update
from telegram.ext import ContextTypes


class SetupHandlerMixin:
    """Handles interactive wearable credential setup via Telegram chat."""

    def is_awaiting_setup(self, user_id: int) -> bool:
        """Check if user is in the middle of wearable credential setup."""
        return user_id in self._setup_state

    @staticmethod
    def _extract_credential(text: str) -> str:
        """Extract a credential value from natural language input.

        Users often type 'This is the client ID abc-123' or 'my key is xyz'.
        Extract just the credential (UUID, hex, or long alphanumeric token).
        """
        # Try UUID pattern first (most common for OAuth client IDs)
        uuid_match = re.search(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            text, re.IGNORECASE,
        )
        if uuid_match:
            return uuid_match.group(0)

        # Try long hex or alphanumeric token (32+ chars, typical for secrets)
        token_match = re.search(r"[A-Za-z0-9_\-]{32,}", text)
        if token_match:
            return token_match.group(0)

        # Fall back to last whitespace-delimited token (the value is usually last)
        parts = text.split()
        if len(parts) > 1:
            # If last token looks like a credential (has digits+letters or dashes)
            last = parts[-1]
            if re.search(r"[0-9]", last) and len(last) >= 8:
                return last

        # Return as-is if we can't parse it
        return text

    async def handle_setup_input(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> bool:
        """Process credential input during wearable setup. Returns True if consumed."""
        user_id = update.effective_user.id if update.effective_user else 0
        state = self._setup_state.get(user_id)
        if not state:
            return False
        if not update.message.text or not update.message.text.strip():
            await update.message.reply_text(
                f"Please send your {state['name']} "
                f"{'Client ID' if state['step'] == 'client_id' else 'Client Secret'}."
            )
            return True

        from healthbot.security.keychain import Keychain

        keychain = Keychain()
        text = update.message.text.strip()

        # Handle /cancel
        if text.lower() == "/cancel":
            self._setup_state.pop(user_id, None)
            await update.message.reply_text("Setup cancelled.")
            return True

        if state["step"] == "client_id":
            # Delete the message (contains credential)
            try:
                await update.message.delete()
            except Exception:
                pass
            # Extract UUID/credential from natural language
            extracted = self._extract_credential(text)
            state["client_id"] = extracted
            state["step"] = "client_secret"
            await update.effective_chat.send_message(
                f"Got it. Now send me your {state['name']} Client Secret.\n"
                "(Your message will be deleted immediately for security.)"
            )
            return True

        if state["step"] == "client_secret":
            # Delete the message containing the secret immediately
            try:
                await update.message.delete()
            except Exception:
                pass

            secret = self._extract_credential(text)
            keychain.store(state["keychain_id_key"], state["client_id"])
            keychain.store(state["keychain_secret_key"], secret)
            name = state["name"]
            auth_cmd = state["auth_cmd"]
            del self._setup_state[user_id]

            await update.effective_chat.send_message(
                f"{name} credentials stored in Keychain.\n"
                f"Starting authorization... (same as {auth_cmd})"
            )

            # Auto-trigger the auth flow now that credentials exist
            if "whoop" in auth_cmd.lower():
                await self.whoop_auth(update, context)
            else:
                await self.oura_auth(update, context)
            return True

        return False

    @staticmethod
    def _is_valid_credential(value: str) -> bool:
        """Check if a stored credential looks like a valid UUID or token."""
        # UUID format (most OAuth client IDs)
        if re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            value, re.IGNORECASE,
        ):
            return True
        # Long alphanumeric token (32+ chars, no spaces)
        if len(value) >= 32 and " " not in value:
            return True
        return False
