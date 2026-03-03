"""Apple Health + genetic data routing methods."""
from __future__ import annotations

import asyncio
import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate, strip_markdown
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class HealthDataMixin:
    """Mixin providing Apple Health and genetic data handling methods."""

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
