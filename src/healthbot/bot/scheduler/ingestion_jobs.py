"""Incoming folder polling and PDF/XML/JSON ingestion jobs."""
from __future__ import annotations

import logging
from pathlib import Path

from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.reasoning.watcher import HealthWatcher

logger = logging.getLogger("healthbot")


class IngestionJobsMixin:
    """Mixin for incoming folder polling and document ingestion."""

    async def _poll_apple_health(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Poll iCloud Drive for new Health Auto Export JSON files."""
        if not self._km.is_unlocked:
            return
        export_path = getattr(self._config, "apple_health_export_path", "")
        if not export_path:
            return
        path = Path(export_path).expanduser()
        if not path.exists():
            return

        json_files = sorted(path.glob("*.json"))
        if not json_files:
            return

        from healthbot.importers.apple_health_auto import (
            AppleHealthAutoImporter,
        )

        db = self._get_db()
        importer = AppleHealthAutoImporter()
        processed_dir = path / "processed"
        processed_dir.mkdir(exist_ok=True)
        total = 0

        for json_path in json_files:
            if json_path.name.startswith("."):
                continue
            try:
                data = json_path.read_bytes()
                result = importer.import_from_json(
                    data, db, user_id=self._primary_user_id,
                )
                total += result.imported
                json_path.rename(processed_dir / json_path.name)
            except Exception as e:
                logger.warning(
                    "Apple Health file %s failed: %s", json_path.name, e,
                )

        if total:
            await self._tracked_send(
                context.bot,
                f"Apple Health synced: {total} records imported.",
            )

    async def _poll_incoming(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Poll incoming/ folder for new PDFs and Apple Health ZIPs."""
        if not self._km.is_unlocked:
            return
        incoming = self._config.incoming_dir
        if not incoming.exists():
            return
        for pdf_path in sorted(incoming.glob("*.pdf")):
            try:
                await self._ingest_incoming(pdf_path, context.bot)
            except Exception as e:
                logger.warning("Incoming file %s failed: %s", pdf_path.name, e)
        # ZIP files (Apple Health, PDF archives, CCDA/FHIR)
        for zip_path in sorted(incoming.glob("*.zip")):
            try:
                await self._ingest_zip(zip_path, context.bot)
            except Exception as e:
                logger.warning("Incoming ZIP %s failed: %s", zip_path.name, e)

    async def _ingest_incoming(self, path: Path, bot: object) -> None:
        """Ingest a PDF from incoming/, move to processed/ when done."""
        import asyncio

        from healthbot.ingest.lab_pdf_parser import LabPdfParser
        from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
        from healthbot.reasoning.triage import TriageEngine
        from healthbot.security.pdf_safety import PdfSafety
        from healthbot.security.vault import Vault

        db = self._get_db()
        vault = Vault(self._config.blobs_dir, self._km)
        safety = PdfSafety(self._config)
        parser = LabPdfParser(safety, config=self._config)
        triage = TriageEngine()
        ingest = TelegramPdfIngest(
            vault, db, parser, safety, triage,
            config=self._config, phi_firewall=self._fw,
        )

        pdf_bytes = path.read_bytes()
        result = await asyncio.wait_for(
            asyncio.to_thread(
                ingest.ingest, pdf_bytes,
                filename=path.name, user_id=self._primary_user_id,
            ),
            timeout=300,  # 5 minutes max per PDF
        )

        # Move to processed/
        processed = self._config.incoming_dir / "processed"
        processed.mkdir(exist_ok=True)
        path.rename(processed / path.name)

        if result.is_duplicate:
            reason = result.warnings[0] if result.warnings else "already uploaded"
            msg = f"Skipped {path.name}: {reason}"
            await self._tracked_send(bot, msg)
            return

        if result.success:
            n = len(result.lab_results)
            if n:
                s = "s" if n != 1 else ""
                msg = (
                    f"Auto-ingested {path.name}: {n} lab result{s}. "
                    f"Encrypted and stored in vault."
                )
            elif result.clinical_facts_count:
                doc_label = result.doc_type.replace("_", " ") if result.doc_type else "document"
                fc = result.clinical_facts_count
                fs = "s" if fc != 1 else ""
                msg = (
                    f"Auto-ingested {path.name} ({doc_label}): "
                    f"{fc} medical fact{fs} "
                    f"extracted into health profile."
                )
            else:
                msg = f"Auto-ingested {path.name}: PDF stored. No medical data found."
            if result.triage_summary:
                msg += f"\n\n{result.triage_summary}"
            for page in paginate(msg):
                await self._tracked_send(bot, page)

            # Skip alerts, insights, and index rebuild in ingestion mode
            if not self.ingestion_mode:
                # Run alerts on new data
                watcher = HealthWatcher(db, user_id=self._primary_user_id)
                alerts = watcher.check_all()
                await self._send_alerts(alerts, bot)

                # Lab insights now delivered via enriched AI export
                # when user asks Claude — no proactive Ollama call needed

                # Rebuild search index with new data
                self._rebuild_search_index()

    async def _ingest_zip(self, path: Path, bot: object) -> None:
        """Detect ZIP contents and route: Apple Health, PDFs, CCDA/FHIR."""
        import asyncio
        import io
        import zipfile

        # Read bytes once and move to processed/ immediately
        # (eliminates TOCTOU and prevents re-processing on next poll)
        max_zip = 500 * 1024 * 1024  # 500 MB memory guard
        if path.stat().st_size > max_zip:
            logger.warning("ZIP %s too large (%d MB), skipping",
                           path.name, path.stat().st_size // (1024 * 1024))
            return
        zip_bytes = path.read_bytes()
        processed = self._config.incoming_dir / "processed"
        processed.mkdir(exist_ok=True)
        dest = processed / path.name
        path.rename(dest)

        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                has_export = any(n.endswith("export.xml") for n in names)
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
            logger.warning("Bad ZIP file: %s", path.name)
            return

        # Apple Health path — run synchronously on the event loop thread
        # to avoid SQLite cross-thread deadlocks (scheduler is background-safe)
        if has_export:
            from healthbot.ingest.apple_health_import import AppleHealthImporter

            db = self._get_db()
            importer = AppleHealthImporter(db)
            privacy_mode = self._config.privacy_mode
            result = importer.import_from_zip_bytes(
                zip_bytes,
                privacy_mode=privacy_mode,
            )

            if result.records_imported > 0 or result.clinical_records > 0:
                parts = []
                if result.records_imported:
                    type_summary = ", ".join(
                        f"{t}: {c}" for t, c in result.types_found.items()
                    )
                    parts.append(f"{result.records_imported} vitals ({type_summary})")
                if result.workouts_imported:
                    parts.append(f"{result.workouts_imported} workouts")
                if result.clinical_records:
                    clin_parts = ", ".join(
                        f"{c} {t}" for t, c in result.clinical_breakdown.items()
                    )
                    parts.append(
                        f"{result.clinical_records} clinical records ({clin_parts})"
                    )
                msg = "Apple Health import: " + ", ".join(parts)
                for page in paginate(msg):
                    await self._tracked_send(bot, page)
                self._rebuild_search_index()
            return

        # PDF / XML / JSON extraction
        processable = pdf_names + xml_names + json_names
        if not processable:
            logger.debug("ZIP %s has no processable files", path.name)
            return

        total_labs = 0
        total_clinical = 0
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in processable:
                try:
                    entry_bytes = zf.read(name)
                    basename = name.rsplit("/", 1)[-1] if "/" in name else name

                    if name.lower().endswith(".pdf"):
                        result = await asyncio.to_thread(
                            self._ingest_pdf_bytes, entry_bytes, basename,
                        )
                        if result and not result.is_duplicate:
                            total_labs += len(result.lab_results)
                            total_clinical += result.clinical_facts_count
                    elif name.lower().endswith(".xml"):
                        await asyncio.to_thread(
                            self._ingest_xml_bytes, entry_bytes,
                        )
                    elif name.lower().endswith(".json"):
                        await asyncio.to_thread(
                            self._ingest_json_bytes, entry_bytes,
                        )
                except Exception as e:
                    logger.warning("ZIP entry %s failed: %s", name, e)

        parts = []
        if total_labs:
            parts.append(f"{total_labs} lab result{'s' if total_labs != 1 else ''}")
        if total_clinical:
            parts.append(f"{total_clinical} medical fact{'s' if total_clinical != 1 else ''}")
        if parts:
            msg = f"Auto-ingested {path.name}: {', '.join(parts)}."
            for page in paginate(msg):
                await self._tracked_send(bot, page)
            self._rebuild_search_index()

    def _ingest_pdf_bytes(self, pdf_bytes: bytes, filename: str) -> object | None:
        """Ingest a single PDF from bytes. Returns IngestResult or None."""
        from healthbot.ingest.lab_pdf_parser import LabPdfParser
        from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
        from healthbot.reasoning.triage import TriageEngine
        from healthbot.security.pdf_safety import PdfSafety
        from healthbot.security.vault import Vault

        db = self._get_db()
        vault = Vault(self._config.blobs_dir, self._km)
        safety = PdfSafety(self._config)
        parser = LabPdfParser(safety, config=self._config)
        triage = TriageEngine()
        ingest = TelegramPdfIngest(
            vault, db, parser, safety, triage,
            config=self._config, phi_firewall=self._fw,
        )
        return ingest.ingest(
            pdf_bytes, filename=filename, user_id=self._primary_user_id,
        )

    def _ingest_xml_bytes(self, xml_bytes: bytes) -> None:
        """Try MyChart CCDA import for XML bytes."""
        try:
            from healthbot.ingest.mychart_import import MyChartImporter
            from healthbot.security.phi_firewall import PhiFirewall
            from healthbot.security.vault import Vault

            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)
            importer = MyChartImporter(db, vault, phi_firewall=self._fw or PhiFirewall())
            importer.import_ccda_bytes(xml_bytes)
        except Exception as e:
            logger.debug("XML import skipped (not CCDA): %s", e)

    def _ingest_json_bytes(self, json_bytes: bytes) -> None:
        """Try FHIR bundle import for JSON bytes."""
        try:
            from healthbot.ingest.mychart_import import MyChartImporter
            from healthbot.security.phi_firewall import PhiFirewall
            from healthbot.security.vault import Vault

            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)
            importer = MyChartImporter(db, vault, phi_firewall=self._fw or PhiFirewall())
            importer.import_fhir_bundle(json_bytes)
        except Exception as e:
            logger.debug("JSON import skipped (not FHIR): %s", e)
