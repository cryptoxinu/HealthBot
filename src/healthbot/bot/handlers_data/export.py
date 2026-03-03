"""Export handlers mixin (FHIR, CSV, AI export, docs)."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.middleware import require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class ExportMixin:
    """Handlers for /export, /ai_export, and /docs commands."""

    @require_unlocked
    async def export_fhir(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /export command -- export health data as FHIR R4 JSON or CSV."""
        args = context.args
        fmt = args[0].lower() if args else "fhir"

        if fmt == "csv":
            await self._export_csv(update)
            return

        if fmt != "fhir":
            await update.message.reply_text(
                f"Unknown format: {fmt}. Supported: fhir, csv"
            )
            return
        await update.message.reply_text("Generating FHIR R4 export...")
        try:
            async with TypingIndicator(update.effective_chat):
                import io

                db = self._core._get_db()
                from healthbot.export.fhir_export import FhirExporter

                uid = update.effective_user.id
                exporter = FhirExporter(db, phi_firewall=self._core._fw)
                all_flag = "--all" in args or len(args) <= 1
                json_str = exporter.export_json(
                    include_labs=all_flag or "--labs" in args,
                    include_meds=all_flag or "--meds" in args,
                    include_vitals=all_flag or "--vitals" in args,
                    include_symptoms=all_flag or "--symptoms" in args,
                    include_wearables=all_flag or "--wearables" in args,
                    include_concerns=all_flag or "--concerns" in args,
                    user_id=uid,
                )
                doc = io.BytesIO(json_str.encode("utf-8"))
                doc.name = "health_export_fhir_r4.json"
                await update.message.reply_document(document=doc)
            await update.message.reply_text(
                "FHIR R4 Bundle exported. Import into EHR systems "
                "or share with your provider.\n\n"
                "Note: This export is unencrypted plaintext. "
                "Use /ai_export for an encrypted, anonymized version."
            )
        except Exception as e:
            logger.error("FHIR export error: %s", e)
            await update.message.reply_text(f"Export failed: {type(e).__name__}")

    async def _export_csv(self, update: Update) -> None:
        """Export lab results and medications as CSV files."""
        import io

        from healthbot.export.csv_exporter import export_labs_csv, export_medications_csv

        db = self._core._get_db()
        uid = update.effective_user.id
        try:
            labs_csv = export_labs_csv(db, uid, phi_firewall=self._core._fw)
            meds_csv = export_medications_csv(db, uid, phi_firewall=self._core._fw)

            if labs_csv.count("\n") > 1:
                doc = io.BytesIO(labs_csv.encode("utf-8"))
                doc.name = "lab_results.csv"
                await update.message.reply_document(document=doc)

            if meds_csv.count("\n") > 1:
                doc = io.BytesIO(meds_csv.encode("utf-8"))
                doc.name = "medications.csv"
                await update.message.reply_document(document=doc)

            if labs_csv.count("\n") <= 1 and meds_csv.count("\n") <= 1:
                await update.message.reply_text("No data to export.")
            else:
                await update.message.reply_text(
                    "CSV export complete. Open in Excel or Google Sheets."
                )
        except Exception as e:
            logger.error("CSV export error: %s", e)
            await update.message.reply_text(f"CSV export failed: {type(e).__name__}")

    @require_unlocked
    async def ai_export(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /ai_export — export anonymized health data for AI analysis."""
        await update.message.reply_text("Generating anonymized health data export...")
        try:
            async with TypingIndicator(update.effective_chat):
                import io

                from healthbot.export.ai_export import AiExporter
                from healthbot.llm.anonymizer import Anonymizer
                from healthbot.llm.ollama_client import OllamaClient
                from healthbot.security.phi_firewall import PhiFirewall

                db = self._core._get_db()
                fw = PhiFirewall()
                anon = Anonymizer(phi_firewall=fw, use_ner=True)
                ollama = OllamaClient(
                    model=self._core._config.ollama_model,
                    base_url=self._core._config.ollama_url,
                    timeout=self._core._config.ollama_timeout,
                )

                uid = update.effective_user.id
                exporter = AiExporter(
                    db=db, anonymizer=anon, phi_firewall=fw,
                    ollama=ollama, key_manager=self._core._km,
                )
                result = exporter.export_to_file(uid, self._core._config.exports_dir)

                if result.file_path.suffix == ".enc":
                    # Send the actual encrypted bytes, not plaintext
                    doc = io.BytesIO(result.file_path.read_bytes())
                    doc.name = result.file_path.name
                    await update.message.reply_document(document=doc)
                    await update.message.reply_text(
                        result.validation.summary()
                        + "\n\nEncrypted export attached."
                        " Decrypt with your vault passphrase"
                        " via /export decrypt or the CLI."
                    )
                else:
                    doc = io.BytesIO(result.markdown.encode("utf-8"))
                    doc.name = result.file_path.name
                    await update.message.reply_document(document=doc)
                    await update.message.reply_text(result.validation.summary())
        except Exception as e:
            logger.error("AI export error: %s", e)
            await update.message.reply_text(f"Export failed: {type(e).__name__}")

    # ── Knowledge export ───────────────────────────────────────────

    @require_unlocked
    async def export_knowledge(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /export_knowledge — export accumulated knowledge as JSON.

        Usage:
            /export_knowledge              → plain JSON, PII-stripped
            /export_knowledge password X   → encrypted with password X
        """
        args = context.args or []
        password = None
        mode = "plain"

        if len(args) >= 2 and args[0].lower() == "password":
            password = " ".join(args[1:])
            mode = "encrypted"

        await update.message.reply_text("Exporting knowledge stores...")
        try:
            async with TypingIndicator(update.effective_chat):
                import asyncio
                import io

                from healthbot.export.knowledge_export import KnowledgeExporter

                db = self._core._get_db()
                uid = update.effective_user.id
                exporter = KnowledgeExporter(
                    db=db,
                    config=self._core._config,
                    key_manager=self._core._km,
                    phi_firewall=self._core._fw,
                )
                file_bytes, counts = await asyncio.to_thread(
                    exporter.export_all, uid, mode, password,
                )

                # Build summary
                parts = []
                for store, count in counts.items():
                    if count:
                        label = store.replace("_", " ")
                        parts.append(f"{count} {label}")

                total = sum(counts.values())
                summary = ", ".join(parts) if parts else "no records"

                if mode == "encrypted":
                    fname = "knowledge_export.enc"
                else:
                    fname = "knowledge_export.json"

                doc = io.BytesIO(file_bytes)
                doc.name = fname
                await update.message.reply_document(document=doc)

                msg = f"Knowledge export complete ({total} records: {summary})."
                if mode == "plain":
                    msg += "\nPII has been redacted from all text fields."
                else:
                    msg += "\nEncrypted with your password. Keep it safe."
                await update.message.reply_text(msg)

        except Exception as e:
            logger.error("Knowledge export error: %s", e)
            await update.message.reply_text(
                f"Knowledge export failed: {type(e).__name__}"
            )

    # ── Document retrieval ──────────────────────────────────────────

    @require_unlocked
    async def docs(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /docs — list uploaded documents or send one back."""
        db = self._core._get_db()
        uid = update.effective_user.id
        docs = db.list_documents(user_id=uid)

        if not docs:
            await update.message.reply_text("No documents uploaded yet.")
            return

        args = context.args or []

        # /docs <number> [redacted] — send the file
        if args:
            try:
                idx = int(args[0]) - 1
            except ValueError:
                await update.message.reply_text(
                    "Usage: /docs [number] [redacted]"
                )
                return
            if idx < 0 or idx >= len(docs):
                await update.message.reply_text(
                    f"Invalid number. You have {len(docs)} document(s)."
                )
                return

            doc = docs[idx]
            want_redacted = len(args) > 1 and args[1].lower() == "redacted"

            from healthbot.security.vault import Vault
            vault = Vault(self._core._config.blobs_dir, self._core._km)

            if want_redacted:
                meta = db.get_document_meta(doc["doc_id"])
                redacted_blob_id = meta.get("redacted_blob_id")
                if not redacted_blob_id:
                    await update.message.reply_text(
                        "No redacted version available for this document."
                    )
                    return
                try:
                    import io
                    pdf_bytes = vault.retrieve_blob(redacted_blob_id)
                    fname = meta.get("redacted_filename", "redacted.pdf")
                    buf = io.BytesIO(pdf_bytes)
                    buf.name = fname
                    await update.message.reply_document(document=buf)
                except Exception as e:
                    logger.error("Redacted document retrieval error: %s", e)
                    await update.message.reply_text(
                        "Error retrieving redacted document."
                    )
                return

            # Send original
            blob_id = doc["enc_blob_path"]
            if not blob_id:
                await update.message.reply_text("Document has no stored file.")
                return
            try:
                import io
                pdf_bytes = vault.retrieve_blob(blob_id)
                fname = (
                    db.get_document_filename(doc["doc_id"])
                    or f"document_{doc['received_at'][:10]}.pdf"
                )
                buf = io.BytesIO(pdf_bytes)
                buf.name = fname
                await update.message.reply_document(document=buf)
            except Exception as e:
                logger.error("Document retrieval error: %s", e)
                await update.message.reply_text("Error retrieving document.")
            return

        # /docs — list all
        lines = ["Uploaded Documents:", ""]
        for i, doc in enumerate(docs, 1):
            fname = db.get_document_filename(doc["doc_id"]) or "untitled"
            size_kb = (doc.get("size_bytes") or 0) / 1024
            date = (doc["received_at"] or "")[:10]
            src = doc["source"].replace("_", " ")
            meta = db.get_document_meta(doc["doc_id"])
            tag = " [R]" if meta.get("redacted_blob_id") else ""
            lines.append(
                f"{i}. {fname} ({size_kb:.0f} KB) — {date} [{src}]{tag}"
            )
        lines.append("")
        lines.append("/docs <n> — download original")
        lines.append("/docs <n> redacted — download redacted version")
        lines.append("[R] = redacted version available")
        await update.message.reply_text("\n".join(lines))
