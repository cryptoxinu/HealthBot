"""Passphrase unlock + vault lock handling methods."""
from __future__ import annotations

import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger("healthbot")


class UnlockMixin:
    """Mixin providing passphrase unlock and vault lock handling."""

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
