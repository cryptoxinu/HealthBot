"""Apple Health sync handler mixin."""
from __future__ import annotations

import logging
from pathlib import Path

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.middleware import rate_limited, require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class AppleSyncMixin:
    """Apple Health sync handler."""

    @rate_limited(max_per_minute=5)
    @require_unlocked
    async def apple_sync(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /apple_sync -- import from Health Auto Export (iCloud Drive)."""
        from healthbot.importers.apple_health_auto import (
            AppleHealthAutoImporter,
        )

        export_path = self._core._config.apple_health_export_path
        if not export_path:
            await update.message.reply_text(
                "Apple Health sync not configured.\n"
                "Set apple_health_export_path in app.json to your "
                "Health Auto Export iCloud Drive folder."
            )
            return

        path = Path(export_path).expanduser()
        if not path.exists():
            await update.message.reply_text(
                f"Export folder not found: {path}\n"
                "Make sure Health Auto Export is saving to iCloud Drive."
            )
            return

        json_files = sorted(path.glob("*.json"))
        if not json_files:
            await update.message.reply_text(
                "No JSON files found in Apple Health export folder.\n"
                "Check that Health Auto Export is configured to save as JSON."
            )
            return

        await update.message.reply_text(
            f"Found {len(json_files)} file(s). Importing..."
        )
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            uid = update.effective_user.id if update.effective_user else 0
            importer = AppleHealthAutoImporter()
            total = 0
            processed_dir = path / "processed"
            processed_dir.mkdir(exist_ok=True)

            for json_path in json_files:
                if json_path.name.startswith("."):
                    continue
                try:
                    data = json_path.read_bytes()
                    result = importer.import_from_json(
                        data, db, user_id=uid,
                    )
                    total += result.imported
                    json_path.rename(processed_dir / json_path.name)
                except Exception as e:
                    logger.warning(
                        "Apple Health file %s failed: %s",
                        json_path.name, e,
                    )

        if total:
            await update.message.reply_text(
                f"Apple Health sync complete: {total} records imported."
            )
        else:
            await update.message.reply_text(
                "No new records to import (all duplicates or empty)."
            )
