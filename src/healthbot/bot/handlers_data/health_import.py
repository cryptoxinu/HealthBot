"""Health data import handlers mixin (Apple Health ZIP, MyChart, Fasten)."""
from __future__ import annotations

import logging

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.middleware import rate_limited, require_unlocked
from healthbot.bot.typing_helper import TypingIndicator

logger = logging.getLogger("healthbot")


class HealthImportMixin:
    """Handlers for /import, /mychart, and /fasten commands."""

    @rate_limited(max_per_minute=5)
    @require_unlocked
    async def import_health(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /import command -- import Apple Health ZIP from incoming/.

        Parses XML first, then inserts in batches with progress updates.
        All DB work runs on the event loop thread (no asyncio.to_thread)
        to avoid SQLite cross-thread deadlocks.
        """
        import asyncio
        import zipfile

        incoming = self._core._config.incoming_dir
        zips = list(incoming.glob("*.zip"))
        if not zips:
            await update.message.reply_text(
                "No ZIP files found in incoming/.\n"
                "Drop your Apple Health export.zip into:\n"
                f"  {incoming}"
            )
            return

        await update.message.reply_text(f"Found {len(zips)} ZIP file(s). Importing...")
        total_imported = 0
        async with TypingIndicator(update.effective_chat):
            for zip_path in zips:
                try:
                    with zipfile.ZipFile(str(zip_path)) as zf:
                        has_export = any(
                            name.endswith("export.xml") for name in zf.namelist()
                        )
                    if not has_export:
                        await update.message.reply_text(
                            f"Skipping {zip_path.name}: not an Apple Health export."
                        )
                        continue

                    from healthbot.ingest.apple_health_import import (
                        SUPPORTED_TYPES,
                        AppleHealthImporter,
                        AppleHealthImportResult,
                    )

                    db = self._core._get_db()
                    importer = AppleHealthImporter(db)
                    privacy_mode = self._core._config.privacy_mode
                    uid = update.effective_user.id if update.effective_user else 0

                    # Phase 1: parse (no DB writes)
                    vitals, workouts, xml_bytes = importer.parse_zip_bytes(
                        zip_path.read_bytes(), privacy_mode,
                    )
                    total = len(vitals) + len(workouts)
                    if total == 0:
                        await update.message.reply_text(
                            f"{zip_path.name}: no supported records found."
                        )
                        continue

                    await update.message.reply_text(
                        f"{zip_path.name}: {len(vitals)} vitals, "
                        f"{len(workouts)} workouts. Importing...",
                    )

                    # Phase 2: insert vitals in batches with progress
                    result = AppleHealthImportResult()
                    canonical_names = list(SUPPORTED_TYPES.values())
                    existing_keys = db.get_existing_observation_keys(
                        record_type="vital_sign",
                        canonical_names=canonical_names,
                    )

                    batch_size = 5000
                    last_pct = -1
                    for i in range(0, len(vitals), batch_size):
                        batch = vitals[i : i + batch_size]
                        importer.insert_vitals_batch(
                            batch, existing_keys, uid, result,
                        )
                        pct = int((i + len(batch)) / total * 100)
                        if pct >= last_pct + 10:
                            await update.message.reply_text(f"Importing... {pct}%")
                            last_pct = pct
                        await asyncio.sleep(0)

                    # Phase 3: insert workouts
                    existing_wo_keys = db.get_existing_workout_keys(user_id=uid)
                    importer.insert_workouts_batch(
                        workouts, existing_wo_keys, uid, result,
                    )

                    # Phase 4: clinical records
                    if xml_bytes:
                        importer._parse_clinical_records(xml_bytes, uid, result)

                    # Move to processed/
                    processed = incoming / "processed"
                    processed.mkdir(exist_ok=True)
                    zip_path.rename(processed / zip_path.name)

                    total_imported += result.records_imported
                    lines = [
                        f"{zip_path.name}: {result.records_imported} vitals, "
                        f"{result.workouts_imported} workouts",
                    ]
                    if result.types_found:
                        type_summary = ", ".join(
                            f"{t}: {c}" for t, c in result.types_found.items()
                        )
                        lines.append(f"  ({type_summary})")
                    if result.clinical_records:
                        clin_parts = ", ".join(
                            f"{c} {t}" for t, c
                            in result.clinical_breakdown.items()
                        )
                        lines.append(
                            f"  {result.clinical_records} clinical records"
                            f" ({clin_parts})"
                        )
                    elif privacy_mode == "strict":
                        lines.append(
                            "  (Clinical records skipped — "
                            "/privacy relaxed to enable)"
                        )
                    await update.message.reply_text("\n".join(lines))
                except Exception as e:
                    logger.error("Import error for %s: %s", zip_path.name, e)
                    await update.message.reply_text(
                        f"Error importing {zip_path.name}: {type(e).__name__}"
                    )

        if total_imported:
            self._rebuild_search_index()
            await update.message.reply_text(
                f"Import complete: {total_imported} total records imported."
            )

    @rate_limited(max_per_minute=5)
    @require_unlocked
    async def import_mychart(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Handle /mychart command -- import MyChart CCDA/FHIR files from incoming/."""
        incoming = self._core._config.incoming_dir
        ccda_files = list(incoming.glob("*.xml")) + list(incoming.glob("*.json"))
        if not ccda_files:
            await update.message.reply_text(
                "No MyChart files found in incoming/.\n"
                "Drop your CCDA (.xml) or FHIR (.json) export into:\n"
                f"  {incoming}"
            )
            return

        await update.message.reply_text(f"Found {len(ccda_files)} file(s). Importing...")
        async with TypingIndicator(update.effective_chat):
            db = self._core._get_db()
            from healthbot.ingest.mychart_import import MyChartImporter
            from healthbot.security.vault import Vault

            vault = Vault(self._core._config.blobs_dir, self._core._km)
            importer = MyChartImporter(db, vault, phi_firewall=self._core._fw)

            total_labs = 0
            total_meds = 0
            for fpath in ccda_files:
                try:
                    raw = fpath.read_bytes()
                    if fpath.suffix == ".json":
                        result = importer.import_fhir_bundle(raw)
                    else:
                        result = importer.import_ccda_bytes(raw)

                    total_labs += result.get("labs", 0)
                    total_meds += result.get("meds", 0)

                    # Move to processed/
                    processed = incoming / "processed"
                    processed.mkdir(exist_ok=True)
                    fpath.rename(processed / fpath.name)

                    await update.message.reply_text(
                        f"{fpath.name}: {result.get('labs', 0)} labs, "
                        f"{result.get('meds', 0)} medications"
                    )
                except Exception as e:
                    logger.error("MyChart import error for %s: %s", fpath.name, e)
                    await update.message.reply_text(
                        f"Error importing {fpath.name}: {type(e).__name__}"
                    )

        if total_labs or total_meds:
            self._rebuild_search_index()
            await update.message.reply_text(
                f"MyChart import complete: {total_labs} labs, {total_meds} medications."
            )

    @rate_limited(max_per_minute=5)
    @require_unlocked
    async def import_fasten(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Handle /fasten command -- import Fasten Health FHIR data from incoming/."""
        incoming = self._core._config.incoming_dir
        fhir_files = (
            list(incoming.glob("*.ndjson"))
            + list(incoming.glob("*.fhir.json"))
            + [
                f for f in incoming.glob("*.json")
                if not f.name.endswith(".fhir.json")
                and "fhir" in f.name.lower()
            ]
        )
        if not fhir_files:
            await update.message.reply_text(
                "No Fasten FHIR files found in incoming/.\n"
                "Export your data from Fasten Health and drop "
                "the .ndjson or .json file into:\n"
                f"  {incoming}\n\n"
                "All PII will be stripped before import."
            )
            return

        await update.message.reply_text(
            f"Found {len(fhir_files)} FHIR file(s). "
            "De-identifying and importing..."
        )
        async with TypingIndicator(update.effective_chat):
            from healthbot.ingest.fasten_import import FastenImporter
            from healthbot.security.vault import Vault

            db = self._core._get_db()
            vault = Vault(self._core._config.blobs_dir, self._core._km)
            uid = update.effective_user.id if update.effective_user else 0
            importer = FastenImporter(db, vault, self._core._fw)

            for fpath in fhir_files:
                try:
                    raw = fpath.read_bytes()
                    if fpath.suffix == ".ndjson":
                        result = importer.import_ndjson(raw, user_id=uid)
                    else:
                        result = importer.import_bundle(raw, user_id=uid)

                    # Move to processed/
                    processed = incoming / "processed"
                    processed.mkdir(exist_ok=True)
                    fpath.rename(processed / fpath.name)

                    parts = []
                    if result.labs:
                        parts.append(f"{result.labs} labs")
                    if result.medications:
                        parts.append(f"{result.medications} medications")
                    if result.vitals:
                        parts.append(f"{result.vitals} vitals")
                    if result.conditions:
                        parts.append(f"{result.conditions} conditions")
                    if result.allergies:
                        parts.append(f"{result.allergies} allergies")
                    if result.immunizations:
                        parts.append(f"{result.immunizations} immunizations")

                    summary = ", ".join(parts) if parts else "no records"
                    demo = ""
                    if result.demographics:
                        d = result.demographics
                        demo_parts = []
                        if d.get("age"):
                            demo_parts.append(f"age {d['age']}")
                        if d.get("sex"):
                            demo_parts.append(d["sex"])
                        demo = f"\nDemographics: {', '.join(demo_parts)}" if demo_parts else ""

                    await update.message.reply_text(
                        f"{fpath.name}: {summary}{demo}\n"
                        f"All PII stripped. {result.skipped} resources filtered."
                    )
                    if result.errors:
                        await update.message.reply_text(
                            f"Warnings: {len(result.errors)} errors\n"
                            + "\n".join(result.errors[:5])
                        )
                except Exception as e:
                    logger.error("Fasten import error for %s: %s", fpath.name, e)
                    await update.message.reply_text(
                        f"Error importing {fpath.name}: {type(e).__name__}"
                    )
