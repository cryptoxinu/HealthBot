"""PDF/ZIP ingestion routing methods."""
from __future__ import annotations

import asyncio
import logging
import sqlite3

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate, strip_markdown
from healthbot.bot.typing_helper import TypingIndicator
from healthbot.reasoning.triage import TriageEngine

logger = logging.getLogger("healthbot")


class DocumentMixin:
    """Mixin providing document (PDF/ZIP) handling methods."""

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

        # Knowledge import: .json or .enc files
        if doc.file_name and doc.file_name.lower().endswith((".json", ".enc")):
            await self._handle_knowledge_import(update, context, user_id)
            return

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

        # PDF file size validation (same limit concept as ZIP guard)
        max_pdf = 50 * 1024 * 1024  # 50 MB
        if doc.file_size and doc.file_size > max_pdf:
            mb = doc.file_size // (1024 * 1024)
            await update.message.reply_text(
                f"PDF too large ({mb} MB). Max is 50 MB.\n"
                "Split it into smaller files or drop it in "
                f"{self._config.incoming_dir} and send /import."
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
                            (lr.date_collected for lr in result.lab_results
                             if lr.date_collected),
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

    async def _handle_knowledge_import(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
        user_id: int,
    ) -> None:
        """Handle uploaded .json or .enc files for knowledge import."""
        import asyncio

        doc = update.message.document
        fname = (doc.file_name or "").lower()

        try:
            file = await context.bot.get_file(doc.file_id)
            file_bytes = bytes(await file.download_as_bytearray())
        except Exception as e:
            logger.error("Knowledge import download error: %s", e)
            await update.message.reply_text("Failed to download file.")
            return

        if fname.endswith(".enc"):
            # Encrypted: stash bytes and ask for password
            self._awaiting_import_password[user_id] = file_bytes
            await update.message.reply_text(
                "Encrypted knowledge export detected.\n"
                "Send the password to decrypt and import.\n"
                "(Your password message will be deleted for security.)"
            )
            return

        # .json: check if it's a knowledge export
        from healthbot.ingest.knowledge_import import is_knowledge_export

        if not is_knowledge_export(file_bytes):
            # Not a knowledge export — fall through to existing FHIR/JSON handling
            self._ingest_json_from_zip(file_bytes)
            await update.message.reply_text(
                "JSON file processed (tried FHIR import)."
            )
            return

        # Plain JSON knowledge import
        await update.message.reply_text("Knowledge export detected. Importing...")
        try:
            from healthbot.ingest.knowledge_import import KnowledgeImporter

            db = self._get_db()
            importer = KnowledgeImporter(
                db=db,
                config=self._config,
                key_manager=self._km,
                phi_firewall=self._fw,
            )
            report = await asyncio.to_thread(
                importer.import_bytes, file_bytes, user_id,
            )
            if report.errors and report.total_imported == 0:
                await update.message.reply_text(
                    "Import failed: " + "; ".join(report.errors)
                )
            else:
                await update.message.reply_text(report.summary())
        except Exception as e:
            logger.error("Knowledge import error: %s", e)
            await update.message.reply_text(
                f"Knowledge import failed: {type(e).__name__}"
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
            updated = db.stamp_collection_date(blob_id, parsed.isoformat())
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
