"""Daily backup and auto AI export jobs."""
from __future__ import annotations

import logging

from telegram.ext import ContextTypes

logger = logging.getLogger("healthbot")


class BackupJobsMixin:
    """Mixin for backup and AI export jobs."""

    async def _daily_backup(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Create a daily backup and prune old ones. Silently skips if locked."""
        if not self._km.is_unlocked:
            return
        try:
            from healthbot.vault_ops.backup import VaultBackup

            vb = VaultBackup(self._config, self._km)
            path = vb.create_backup()
            pruned = vb.cleanup_old_backups()
            logger.info("Daily backup: %s (pruned %d old)", path.name, pruned)
        except Exception as e:
            logger.warning("Daily backup failed: %s", e)
            # Notify user via Telegram so backup failures are not silently lost
            try:
                await self._tracked_send(
                    context.bot,
                    f"Daily backup failed: {e}. Check logs for details.",
                )
            except Exception:
                pass  # Notification is best-effort

    async def _auto_ai_export(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Auto-generate anonymized AI export. Skips if locked."""
        if not self._km.is_unlocked:
            return
        try:
            from healthbot.export.ai_export import AiExporter
            from healthbot.llm.anonymizer import Anonymizer
            from healthbot.llm.ollama_client import OllamaClient
            from healthbot.security.phi_firewall import PhiFirewall

            db = self._get_db()
            fw = PhiFirewall()
            anon = Anonymizer(phi_firewall=fw, use_ner=True)
            ollama = OllamaClient(
                model=self._config.ollama_model,
                base_url=self._config.ollama_url,
                timeout=self._config.ollama_timeout,
            )
            exporter = AiExporter(
                db=db, anonymizer=anon, phi_firewall=fw, ollama=ollama,
                key_manager=self._km,
            )
            uid = self._primary_user_id
            result = exporter.export_to_file(uid, self._config.exports_dir)
            logger.info("Auto AI export: %s", result.file_path)

            import io

            if result.file_path.suffix == ".enc":
                # Send the actual encrypted bytes, not plaintext
                doc = io.BytesIO(result.file_path.read_bytes())
                doc.name = result.file_path.name
                await context.bot.send_document(
                    chat_id=self._chat_id, document=doc,
                )
                await self._tracked_send(
                    context.bot,
                    f"Auto AI export complete.\n{result.validation.summary()}"
                    "\n\nEncrypted export attached."
                    " Decrypt with your vault passphrase"
                    " via /export decrypt or the CLI.",
                )
            else:
                doc = io.BytesIO(result.markdown.encode("utf-8"))
                doc.name = result.file_path.name
                await context.bot.send_document(
                    chat_id=self._chat_id, document=doc,
                )
                await self._tracked_send(
                    context.bot,
                    f"Auto AI export complete.\n{result.validation.summary()}",
                )
            # Remove export file from disk after sending
            try:
                if result.file_path and result.file_path.exists():
                    result.file_path.unlink()
            except OSError as cleanup_err:
                logger.warning("Failed to remove export file: %s", cleanup_err)
        except Exception as e:
            logger.warning("Auto AI export failed: %s", e)
