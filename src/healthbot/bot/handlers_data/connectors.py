"""Connectors, debug, scrub_pii, and rescan handlers mixin."""
from __future__ import annotations

import logging
from pathlib import Path

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class ConnectorsMixin:
    """Handlers for /connectors, /debug, /scrub_pii, and /rescan commands."""

    @require_unlocked
    async def connectors(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /connectors — show available data sources and status."""
        from healthbot.security.keychain import Keychain

        keychain = Keychain()
        db = self._core._get_db()
        uid = update.effective_user.id if update.effective_user else 0

        lines = ["DATA CONNECTORS", "=" * 20]

        # WHOOP
        whoop_id = keychain.retrieve("whoop_client_id")
        if whoop_id and self._is_valid_credential(whoop_id):
            last = self._get_last_sync_date(db, "whoop", uid)
            status = f"Connected (last sync: {last})" if last else "Connected"
            lines.append(f"\n  WHOOP: {status}")
            lines.append("    Sync: /sync")
        else:
            lines.append("\n  WHOOP: Not configured")
            lines.append("    Set up: /whoop_auth")

        # Oura Ring
        oura_id = keychain.retrieve("oura_client_id")
        if oura_id and self._is_valid_credential(oura_id):
            last = self._get_last_sync_date(db, "oura", uid)
            status = f"Connected (last sync: {last})" if last else "Connected"
            lines.append(f"\n  Oura Ring: {status}")
            lines.append("    Sync: /sync")
        else:
            lines.append("\n  Oura Ring: Not configured")
            lines.append("    Set up: /oura_auth")

        # Apple Health
        export_path = getattr(self._core._config, "apple_health_export_path", "")
        if export_path:
            path = Path(export_path).expanduser()
            if path.exists():
                pending = len(list(path.glob("*.json")))
                if pending:
                    lines.append(f"\n  Apple Health: Configured ({pending} pending files)")
                else:
                    lines.append("\n  Apple Health: Configured")
                lines.append(f"    Path: {export_path}")
                lines.append("    Sync: /apple_sync or /sync")
            else:
                lines.append("\n  Apple Health: Path not found")
                lines.append(f"    Expected: {export_path}")
        else:
            lines.append("\n  Apple Health: Not configured")
            lines.append("    Set apple_health_export_path in app.json")

        # MyChart/FHIR
        incoming = self._core._config.incoming_dir
        lines.append("\n  MyChart/FHIR: Available (file-based import)")
        lines.append(f"    Drop CCDA/FHIR files into: {incoming}")
        lines.append("    Then: /mychart")

        lines.append("")
        lines.append("Use /sync to sync all connected sources at once.")

        await update.message.reply_text("\n".join(lines))

    @require_unlocked
    async def scrub_pii(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /scrub_pii command -- remove PII from existing vault data."""
        await update.message.reply_text(
            "Scrubbing PII from existing records...\n"
            "This strips: provider names, lab names, "
            "patient name, exact DOB."
        )
        async with TypingIndicator(update.effective_chat):
            from healthbot.vault_ops.scrub_pii import VaultPiiScrubber

            db = self._core._get_db()
            uid = update.effective_user.id if update.effective_user else 0
            scrubber = VaultPiiScrubber(db, self._core._fw)
            result = scrubber.scrub_all(user_id=uid)

            parts = []
            if result.observations_scrubbed:
                parts.append(f"{result.observations_scrubbed} lab records cleaned")
            if result.medications_scrubbed:
                parts.append(f"{result.medications_scrubbed} medications cleaned")
            if result.ltm_entries_removed:
                parts.append(f"{result.ltm_entries_removed} PII entries removed")
            if result.ltm_entries_redacted:
                parts.append(f"{result.ltm_entries_redacted} entries redacted")

            if parts:
                await update.message.reply_text(
                    "PII scrub complete:\n" + "\n".join(f"  - {p}" for p in parts)
                )
            else:
                await update.message.reply_text(
                    "No PII found to scrub. Records are already clean."
                )

            if result.errors:
                await update.message.reply_text(
                    f"{len(result.errors)} errors during scrub:\n"
                    + "\n".join(result.errors[:5])
                )

    @require_unlocked
    async def debug(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /debug command — troubleshoot technical issues via Claude CLI."""
        topic = " ".join(context.args) if context.args else ""
        if not topic:
            # Show recent errors if no question provided
            errors = self._core.get_recent_errors()
            if not errors:
                await update.message.reply_text(
                    "No recent errors recorded.\n"
                    "Describe your issue: /debug why is whoop sync failing"
                )
                return
            lines = ["Recent errors:"]
            for rec in errors:
                line = f"- [{rec.timestamp}] {rec.error_type}: {rec.message}"
                if rec.provider:
                    line += f" ({rec.provider})"
                lines.append(line)
            lines.append("\nAsk about any of these: /debug <your question>")
            await update.message.reply_text("\n".join(lines))
            return

        # Route to troubleshoot handler (same as natural language path)
        await self._core._router._handle_troubleshoot(update, topic)

    @require_unlocked
    async def rescan(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /rescan <n> — re-ingest document with current redaction pipeline.

        Safety: retrieves PDF bytes before deletion. If re-ingest fails,
        the original encrypted blob and document row are restored so no
        data is permanently lost.
        """
        import asyncio

        args = context.args or []
        if not args:
            await update.message.reply_text(
                "Usage: /rescan <n>\n"
                "Re-ingests document #n with the current (fixed) redaction pipeline.\n"
                "Use /docs to see document numbers."
            )
            return

        try:
            idx = int(args[0]) - 1
        except ValueError:
            await update.message.reply_text("Invalid number. Use /docs to see document list.")
            return

        db = self._core._get_db()
        user_id = update.effective_user.id
        docs = db.list_documents(user_id=user_id)

        if idx < 0 or idx >= len(docs):
            await update.message.reply_text(
                f"Invalid number. You have {len(docs)} document(s)."
            )
            return

        doc = docs[idx]
        doc_id = doc["doc_id"]
        filename = doc.get("filename") or "document.pdf"
        blob_path = doc.get("enc_blob_path", "")

        if not blob_path:
            await update.message.reply_text("Document has no stored file — cannot rescan.")
            return

        await update.message.reply_text(f"Rescanning '{filename}' with updated redaction...")

        # 1. Retrieve original PDF from vault (before any deletion)
        from healthbot.security.vault import Vault
        vault = Vault(self._core._config.blobs_dir, self._core._km)
        try:
            pdf_bytes = vault.retrieve_blob(blob_path)
        except Exception as e:
            await update.message.reply_text(f"Failed to retrieve PDF: {e}")
            return

        # 2. Snapshot document row for rollback on failure
        doc_row = db.conn.execute(
            "SELECT * FROM documents WHERE doc_id = ?", (doc_id,),
        ).fetchone()
        doc_snapshot = dict(doc_row) if doc_row else None

        # 3. Delete existing data for this document
        from healthbot.data.bulk_ops import BulkOps

        clean_db = None
        try:
            from healthbot.data.clean_db import CleanDB
            if self._core._config.clean_db_path.exists():
                clean_db = CleanDB(self._core._config.clean_db_path)
                clean_db.open(clean_key=self._core._km.get_clean_key())
        except Exception:
            pass

        try:
            ops = BulkOps(db, vault, clean_db=clean_db, config=self._core._config)
            ops.delete_document_cascade(doc_id)
        finally:
            if clean_db:
                try:
                    clean_db.close()
                except Exception:
                    pass

        # 4. Re-ingest with current pipeline
        from healthbot.ingest.lab_pdf_parser import LabPdfParser
        from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
        from healthbot.reasoning.triage import TriageEngine
        from healthbot.security.pdf_safety import PdfSafety

        safety = PdfSafety(self._core._config)
        parser = LabPdfParser(safety, config=self._core._config)
        triage = TriageEngine()

        ingest = TelegramPdfIngest(
            vault, db, parser, safety, triage,
            config=self._core._config,
            phi_firewall=self._core._fw,
        )

        try:
            result = await asyncio.to_thread(
                ingest.ingest,
                bytes(pdf_bytes),
                filename=filename,
                user_id=user_id,
            )
        except Exception as e:
            # Re-ingest failed — restore original blob and document row
            self._restore_document(vault, db, blob_path, pdf_bytes, doc_snapshot)
            await update.message.reply_text(
                f"Rescan failed: {e}\nOriginal document preserved."
            )
            return

        if result.success:
            n_labs = len(result.lab_results) if result.lab_results else 0
            await update.message.reply_text(
                f"Rescanned '{filename}': {n_labs} lab result(s) extracted "
                f"with updated redaction."
            )
        else:
            warnings = "; ".join(result.warnings) if result.warnings else "unknown error"
            await update.message.reply_text(f"Rescan completed with issues: {warnings}")

    @staticmethod
    def _restore_document(
        vault, db, blob_path: str, pdf_bytes: bytes,
        doc_snapshot: dict | None,
    ) -> None:
        """Restore an encrypted blob and document row after a failed rescan."""
        try:
            vault.store_blob(pdf_bytes, blob_id=blob_path)
        except Exception as e:
            logger.error("Failed to restore blob %s: %s", blob_path, e)

        if doc_snapshot:
            cols = [c for c in doc_snapshot if c != "doc_id"]
            placeholders = ", ".join("?" for _ in cols)
            col_names = ", ".join(["doc_id"] + cols)
            values = [doc_snapshot["doc_id"]] + [doc_snapshot[c] for c in cols]
            try:
                db.conn.execute(
                    f"INSERT OR IGNORE INTO documents ({col_names}) "  # noqa: S608
                    f"VALUES (?, {placeholders})",
                    values,
                )
                db.conn.commit()
            except Exception as e:
                logger.error("Failed to restore document row: %s", e)
