"""Background job scheduling for proactive alerts.

Uses python-telegram-bot's JobQueue (APScheduler-backed).
Three triggers:
1. On vault unlock -- immediate check
2. Periodic (every 4 hours while unlocked)
3. Incoming folder poll (every 60 seconds while unlocked)
"""
from __future__ import annotations

import logging
import time
from pathlib import Path

from telegram.ext import ContextTypes

from healthbot.bot.formatters import paginate
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.reasoning.watcher import Alert, HealthWatcher
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")

PERIODIC_INTERVAL = 4 * 3600  # 4 hours

# Specialty-to-relevant-lab-metrics mapping for appointment prep
_SPECIALTY_LABS: dict[str, list[str]] = {
    "endocrinology": [
        "tsh", "free_t4", "free_t3", "glucose", "hba1c",
        "insulin", "testosterone", "cortisol", "vitamin_d",
    ],
    "cardiology": [
        "ldl", "hdl", "total_cholesterol", "triglycerides",
        "crp", "bnp", "troponin", "potassium", "magnesium",
    ],
    "nephrology": [
        "creatinine", "bun", "egfr", "potassium", "sodium",
        "calcium", "phosphorus", "albumin",
    ],
    "hepatology": [
        "alt", "ast", "alp", "ggt", "bilirubin",
        "albumin", "inr", "platelets",
    ],
    "gi": [
        "alt", "ast", "alp", "bilirubin", "albumin",
        "iron", "ferritin", "b12", "folate",
    ],
    "hematology": [
        "wbc", "rbc", "hemoglobin", "hematocrit", "platelets",
        "mcv", "iron", "ferritin", "b12", "folate",
    ],
    "rheumatology": [
        "crp", "esr", "ana", "rf", "uric_acid",
        "vitamin_d", "calcium",
    ],
    "general": [
        "glucose", "hba1c", "ldl", "hdl", "triglycerides",
        "tsh", "creatinine", "alt", "cbc",
    ],
}
INCOMING_POLL_INTERVAL = 60    # 1 minute
DEFAULT_CONSOLIDATION_INTERVAL = 7200  # 2 hours
TIMEOUT_CHECK_INTERVAL = 30    # 30 seconds
DAILY_BACKUP_INTERVAL = 24 * 3600  # 24 hours
RESEARCH_INTERVAL = 12 * 3600  # 12 hours
WEARABLE_GAP_CHECK_INTERVAL = 12 * 3600  # 12 hours
AUTH_HEALTH_CHECK_INTERVAL = 12 * 3600   # 12 hours
WEARABLE_GAP_THRESHOLD_DAYS = 7  # Alert if no data for 7+ days
APPLE_HEALTH_POLL_INTERVAL = 6 * 3600  # 6 hours
DAILY_WEARABLE_SYNC_INTERVAL = 24 * 3600  # 24 hours


class AlertScheduler:
    """Manages background jobs for proactive health alerts."""

    def __init__(
        self,
        config: Config,
        key_manager: KeyManager,
        chat_id: int,
        memory_store: object | None = None,
        phi_firewall: object | None = None,
    ) -> None:
        self._config = config
        self._km = key_manager
        self._chat_id = chat_id
        self._memory_store = memory_store
        self._fw = phi_firewall
        self._sent_keys: dict[str, float] = {}  # In-memory dedup: key -> timestamp
        self._sent_keys_max = 5000  # Max entries before forced eviction
        self._sent_keys_ttl = 24 * 3600  # 24-hour TTL for dedup keys
        self._db: HealthDB | None = None
        self._message_tracker: callable | None = None
        self._warned_5min: bool = False
        self._warned_1min: bool = False
        self.ingestion_mode: bool = False
        self.upload_mode: bool = False
        self._cached_conditions: list[str] = []  # Cached on lock for research
        self._timeout_wipe_cb: object | None = None  # async cb(bot) for chat wipe
        self._timeout_wiped: bool = False  # Prevents repeated wipe on timeout
        self._claude_getter: object | None = None  # callable → ClaudeConversationManager

    @property
    def _primary_user_id(self) -> int:
        """Get the primary user ID from config (first allowed user)."""
        if self._config.allowed_user_ids:
            return self._config.allowed_user_ids[0]
        return 0

    def set_message_tracker(self, tracker: callable) -> None:
        """Set callback to track outgoing messages for session chat wipe."""
        self._message_tracker = tracker

    def _record_sent_key(self, key: str) -> None:
        """Record a dedup key with timestamp, evicting stale entries if needed."""
        now = time.time()
        # Periodic eviction: remove expired entries when nearing max size
        if len(self._sent_keys) >= self._sent_keys_max:
            cutoff = now - self._sent_keys_ttl
            self._sent_keys = {
                k: ts for k, ts in self._sent_keys.items() if ts > cutoff
            }
        self._sent_keys[key] = now

    def _has_sent_key(self, key: str) -> bool:
        """Check if a dedup key exists and is still within TTL."""
        ts = self._sent_keys.get(key)
        if ts is None:
            return False
        if time.time() - ts > self._sent_keys_ttl:
            del self._sent_keys[key]
            return False
        return True


    def set_claude_getter(self, getter: object) -> None:
        """Set callable that returns a ClaudeConversationManager."""
        self._claude_getter = getter

    def set_memory_store(self, memory_store: object) -> None:
        """Set or update the memory store reference (lazy-init friendly)."""
        self._memory_store = memory_store

    def register_jobs(self, job_queue: object) -> None:
        """Register recurring jobs with the Application's job queue."""
        if job_queue is None:
            logger.warning("JobQueue not available; proactive alerts disabled.")
            return
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._periodic_check,
            interval=PERIODIC_INTERVAL,
            first=10,
            name="health_periodic",
        )
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._poll_incoming,
            interval=INCOMING_POLL_INTERVAL,
            first=5,
            name="incoming_poll",
        )
        consolidation_interval = getattr(
            self._config, "consolidation_interval_seconds", DEFAULT_CONSOLIDATION_INTERVAL,
        )
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._consolidate_stm,
            interval=consolidation_interval,
            first=consolidation_interval,
            name="stm_consolidation",
        )
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._check_timeout_warnings,
            interval=TIMEOUT_CHECK_INTERVAL,
            first=TIMEOUT_CHECK_INTERVAL,
            name="timeout_warnings",
        )
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._daily_backup,
            interval=DAILY_BACKUP_INTERVAL,
            first=DAILY_BACKUP_INTERVAL,
            name="daily_backup",
        )
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._research_conditions,
            interval=RESEARCH_INTERVAL,
            first=RESEARCH_INTERVAL,
            name="research_monitor",
        )
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._deep_analysis,
            interval=PERIODIC_INTERVAL,
            first=PERIODIC_INTERVAL,
            name="deep_analysis",
        )
        # Medication reminder check (every 60 seconds)
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._check_med_reminders,
            interval=60,
            first=30,
            name="med_reminders",
        )
        # Appointment prep auto-send (every 4 hours)
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._check_appointment_prep,
            interval=PERIODIC_INTERVAL,
            first=PERIODIC_INTERVAL,
            name="appointment_prep",
        )
        # Daily health digest
        digest_interval = getattr(self._config, "digest_interval", 86400)
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._send_daily_digest,
            interval=digest_interval,
            first=self._compute_digest_first_delay(),
            name="daily_digest",
        )
        # Weekly PDF report (opt-in via config)
        weekly_day = getattr(self._config, "weekly_report_day", "")
        if weekly_day:
            job_queue.run_repeating(  # type: ignore[union-attr]
                self._send_weekly_pdf_report,
                interval=86400,  # Check daily, only sends on the right day
                first=self._compute_weekly_first_delay(),
                name="weekly_pdf_report",
            )
        # Monthly PDF report (opt-in via config)
        monthly_day = getattr(self._config, "monthly_report_day", 0)
        if monthly_day:
            job_queue.run_repeating(  # type: ignore[union-attr]
                self._send_monthly_pdf_report,
                interval=86400,  # Check daily, only sends on the right day
                first=self._compute_monthly_first_delay(),
                name="monthly_pdf_report",
            )
        # Auto AI export (opt-in via config)
        if getattr(self._config, "auto_ai_export", False):
            export_interval = getattr(
                self._config, "auto_ai_export_interval", 86400,
            )
            job_queue.run_repeating(  # type: ignore[union-attr]
                self._auto_ai_export,
                interval=export_interval,
                first=export_interval,
                name="auto_ai_export",
            )
        # Wearable data gap detection (every 12 hours)
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._check_wearable_gaps,
            interval=WEARABLE_GAP_CHECK_INTERVAL,
            first=WEARABLE_GAP_CHECK_INTERVAL,
            name="wearable_gap_check",
        )
        # Integration auth health check (every 12 hours)
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._check_auth_health,
            interval=AUTH_HEALTH_CHECK_INTERVAL,
            first=3600,  # 1 hour after startup
            name="auth_health_check",
        )
        # Daily wearable sync — catch up on data every 24 hours
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._daily_wearable_sync,
            interval=DAILY_WEARABLE_SYNC_INTERVAL,
            first=DAILY_WEARABLE_SYNC_INTERVAL,
            name="daily_wearable_sync",
        )
        # Daily medication reminder resume check
        job_queue.run_repeating(  # type: ignore[union-attr]
            self._check_reminder_resumes,
            interval=DAILY_BACKUP_INTERVAL,  # 24 hours
            first=3600,  # 1 hour after startup
            name="reminder_resume_check",
        )
        # Apple Health auto-sync (opt-in via config)
        apple_path = getattr(self._config, "apple_health_export_path", "")
        if apple_path:
            job_queue.run_repeating(  # type: ignore[union-attr]
                self._poll_apple_health,
                interval=APPLE_HEALTH_POLL_INTERVAL,
                first=300,  # 5 min after startup
                name="apple_health_poll",
            )
        logger.info(
            "Alert scheduler registered (periodic=%ds, incoming=%ds, consolidation=%ds)",
            PERIODIC_INTERVAL,
            INCOMING_POLL_INTERVAL,
            consolidation_interval,
        )

    async def run_on_unlock(self, bot: object) -> None:
        """Build welcome briefing after vault unlock.

        Sends briefing immediately (all local DB), then syncs WHOOP after.
        Wearable alerts and deep analysis run on the periodic schedule.
        """
        import asyncio

        self._sent_keys.clear()
        if not self._km.is_unlocked:
            return

        # Build briefing in a thread — DB queries on large vaults can block
        briefing = await asyncio.to_thread(self._build_welcome_briefing)
        if briefing:
            for page in paginate(briefing):
                await self._tracked_send(bot, page)

        # Auto-sync WHOOP after briefing (network call, may take a while)
        try:
            from healthbot.importers.whoop_client import WhoopAuthError, WhoopClient
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            keychain = Keychain()
            if keychain.retrieve("whoop_client_id"):
                db = self._get_db()
                vault = Vault(self._config.blobs_dir, self._km)
                client = WhoopClient(self._config, keychain, vault)
                clean = self._get_clean_db()
                try:
                    count = await client.sync_daily(
                        db, days=7, clean_db=clean,
                        user_id=self._primary_user_id,
                    ) or 0
                finally:
                    if clean:
                        clean.close()
                if count:
                    await self._tracked_send(
                        bot, f"WHOOP synced ({count} records).",
                    )
        except (ImportError, WhoopAuthError):
            pass
        except Exception as e:
            logger.warning("WHOOP auto-sync failed: %s", e)

    async def _periodic_check(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Periodic check. Silently skips if vault locked."""
        if not self._km.is_unlocked:
            return
        try:
            db = self._get_db()
            watcher = HealthWatcher(db, user_id=self._primary_user_id)
            alerts = watcher.check_all()
            await self._send_alerts(alerts, context.bot)
        except Exception as e:
            logger.warning("Periodic alert check failed: %s", e)

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

    async def _check_timeout_warnings(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Check time remaining and warn at 5 min and 1 min before auto-lock.

        Also triggers the lock cascade if the session has expired — this is
        the primary proactive timeout detector (runs every 30 seconds).
        """
        if not self._km.is_unlocked:
            # is_unlocked triggers lock() cascade if timeout expired.
            # Use the scheduler's bot reference to wipe chat — more
            # reliable than the fire-and-forget task in _on_vault_lock.
            self._warned_5min = False
            self._warned_1min = False
            if self._timeout_wipe_cb and not self._timeout_wiped:
                self._timeout_wiped = True  # Prevent repeated wipe calls
                try:
                    await self._timeout_wipe_cb(context.bot)
                except Exception as e:
                    logger.warning("Timeout wipe callback failed: %s", e)
            return

        remaining = self._km.get_remaining_seconds()

        if remaining > 300:
            # Activity refreshed — reset warning flags
            self._warned_5min = False
            self._warned_1min = False
            self._timeout_wiped = False
        elif remaining <= 300 and not self._warned_5min:
            self._warned_5min = True
            await self._tracked_send(
                context.bot,
                "Session expires in ~5 minutes. Send any message to stay unlocked.",
            )
        if remaining <= 60 and not self._warned_1min:
            self._warned_1min = True
            await self._tracked_send(
                context.bot,
                "Session expires in ~1 minute. Send any message to stay unlocked.",
            )

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

            # Send via in-memory buffer (avoid leaving unencrypted file on disk)
            doc = io.BytesIO(result.markdown.encode("utf-8"))
            doc.name = result.file_path.name
            await context.bot.send_document(
                chat_id=self._chat_id, document=doc,
            )
            await self._tracked_send(
                context.bot,
                f"Auto AI export complete.\n{result.validation.summary()}",
            )
            # Remove unencrypted export file from disk immediately
            try:
                if result.file_path and result.file_path.exists():
                    result.file_path.unlink()
            except OSError as cleanup_err:
                logger.warning("Failed to remove export file: %s", cleanup_err)
        except Exception as e:
            logger.warning("Auto AI export failed: %s", e)

    async def _send_alerts(self, alerts: list[Alert], bot: object) -> None:
        """Send non-duplicate alerts. Respects overdue pause."""
        from healthbot.bot.overdue_pause import is_overdue_paused

        overdue_paused = is_overdue_paused(self._config)
        sent_overdue = False
        for alert in alerts:
            if self._has_sent_key(alert.dedup_key):
                continue
            if alert.alert_type == "overdue" and overdue_paused:
                continue
            self._record_sent_key(alert.dedup_key)
            icon = {"urgent": "!", "watch": "~", "info": ""}.get(alert.severity, "")
            msg = f"{icon} {alert.title}\n{alert.body}"
            for page in paginate(msg):
                await self._tracked_send(bot, page)
            if alert.alert_type == "overdue":
                sent_overdue = True
        # Single tip after all overdue alerts (not per-alert)
        if sent_overdue:
            await self._tracked_send(
                bot,
                "Tip: Say 'pause notifications for 2 weeks' to snooze overdue alerts.",
            )

    async def _tracked_send(self, bot: object, text: str) -> None:
        """Send a message and track it for session chat wipe."""
        if self.ingestion_mode or self.upload_mode:
            return  # All scheduler notifications muted during ingestion/upload
        sent = await bot.send_message(chat_id=self._chat_id, text=text)  # type: ignore[union-attr]
        if self._message_tracker and sent:
            self._message_tracker(self._chat_id, sent.message_id)


    async def _consolidate_stm(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Periodic STM consolidation + cleanup. Silently skips if locked."""
        if not self._km.is_unlocked or not self._memory_store:
            return
        for uid in self._config.allowed_user_ids:
            try:
                count = self._memory_store.consolidate(uid)
                if count:
                    logger.info("Periodic consolidation: %d facts for user %d", count, uid)
            except Exception as e:
                logger.warning("Periodic consolidation failed for user %d: %s", uid, e)
        # Clean up old STM entries
        try:
            cleanup_days = getattr(self._config, "stm_cleanup_days", 30)
            deleted = self._memory_store.cleanup(days=cleanup_days)
            if deleted:
                logger.info("STM cleanup: removed %d old entries", deleted)
        except Exception as e:
            logger.debug("STM cleanup skipped: %s", e)

    def on_lock(self) -> None:
        """Cache conditions for research, then clean up on vault lock."""
        # Cache conditions BEFORE closing DB (for locked-safe research)
        if self._db:
            try:
                from healthbot.reasoning.condition_extractor import extract_conditions

                self._cached_conditions = extract_conditions(
                    self._db, self._primary_user_id,
                )
                logger.info(
                    "Cached %d conditions for background research",
                    len(self._cached_conditions),
                )
            except Exception as e:
                logger.debug("Condition caching on lock failed: %s", e)

        # Unload Ollama model from GPU memory
        from healthbot.llm.ollama_client import OllamaClient
        OllamaClient.safe_unload_on_lock(self._config)

        self.ingestion_mode = False
        self.upload_mode = False
        self._sent_keys.clear()
        self._warned_5min = False
        self._warned_1min = False
        self._memory_store = None
        if self._db:
            self._db.close()
            self._db = None

    def _build_welcome_briefing(self) -> str:
        """Build intelligence briefing on unlock.

        Combines: pending research (from locked period) + fresh analysis.
        Returns empty string if nothing notable.
        """
        parts: list[str] = []

        try:
            db = self._get_db()
            user_id = self._primary_user_id

            # 1. Overdue screenings (respect pause state)
            try:
                from healthbot.bot.overdue_pause import get_pause_until, is_overdue_paused
                from healthbot.reasoning.overdue import OverdueDetector

                if is_overdue_paused(self._config):
                    deadline = get_pause_until(self._config)
                    if deadline:
                        local = deadline.astimezone()
                        parts.append(
                            f"Overdue notifications paused until "
                            f"{local.strftime('%b %d, %Y %H:%M %Z')}."
                        )
                else:
                    detector = OverdueDetector(db)
                    overdue = detector.check_overdue()
                    if overdue:
                        urgent = [o for o in overdue if o.days_overdue > 180]
                        if urgent:
                            lines = []
                            for o in urgent[:5]:
                                months = o.days_overdue // 30
                                lines.append(f"  {o.test_name} — {months} months overdue")
                            parts.append(
                                "Overdue labs:\n" + "\n".join(lines)
                                + "\n  Use /overdue for full list."
                                + "\n  Use /snooze 2w to pause these reminders."
                            )
            except Exception as e:
                logger.debug("Welcome briefing (overdue): %s", e)

            # 2. Hypothesis check
            try:
                from healthbot.reasoning.hypothesis_generator import (
                    HypothesisGenerator,
                )

                gen = HypothesisGenerator(db)
                demographics = db.get_user_demographics(user_id)
                new_hyps = gen.scan_all(
                    user_id,
                    sex=demographics.get("sex"),
                    age=demographics.get("age"),
                )
                for h in new_hyps[:2]:
                    evidence = ", ".join(h.evidence_for[:2])
                    parts.append(
                        f"Pattern: {h.title} "
                        f"({h.confidence:.0%} confidence, based on {evidence})."
                    )
            except Exception as e:
                logger.debug("Welcome briefing (hypotheses): %s", e)

            # 3. Trend alerts (worsening >15%)
            try:
                from healthbot.reasoning.trends import TrendAnalyzer

                analyzer = TrendAnalyzer(db)
                # Check key tests for trends
                for test_name in [
                    "glucose", "hba1c", "ldl", "alt", "tsh", "creatinine",
                ]:
                    trend = analyzer.analyze_test(test_name)
                    if trend and abs(trend.pct_change) > 15:
                        parts.append(
                            f"Trend: {trend.canonical_name} "
                            f"{trend.direction} {trend.pct_change:+.0f}% "
                            f"over {trend.data_points} results."
                        )
            except Exception as e:
                logger.debug("Welcome briefing (trends): %s", e)

            # 4. Research findings gathered while locked
            if self._cached_conditions:
                try:
                    from healthbot.research.external_evidence_store import (
                        ExternalEvidenceStore,
                    )

                    store = ExternalEvidenceStore(db)
                    evidence = store.list_evidence(limit=5)
                    recent = [
                        e for e in evidence
                        if e.get("source") == "pubmed_monitor"
                        and not e.get("expired")
                    ]
                    if recent:
                        parts.append(
                            f"{len(recent)} research article"
                            f"{'s' if len(recent) != 1 else ''} "
                            f"found for your conditions. Use /evidence to browse."
                        )
                except Exception as e:
                    logger.debug("Welcome briefing (research): %s", e)

        except Exception as e:
            logger.debug("Welcome briefing failed: %s", e)

        # 5. Wearable connection hints (with connection history awareness)
        try:
            from healthbot.security.keychain import Keychain

            kc = Keychain()
            hints: list[str] = []
            for name, cred_key, _sync_cmd, auth_cmd, _desc in [
                ("WHOOP", "whoop_client_id", "/sync", "/whoop_auth",
                 "sleep, recovery, strain"),
                ("Oura Ring", "oura_client_id", "/oura", "/oura_auth",
                 "sleep, readiness, activity"),
            ]:
                has_creds = bool(kc.retrieve(cred_key))
                config_name = name.lower().replace(" ", "_").replace("_ring", "")
                was_connected = self._config.was_wearable_ever_connected(config_name)

                if was_connected and not has_creds:
                    hints.append(
                        f"{name} was connected but credentials are missing. "
                        f"Run {auth_cmd} to reconnect."
                    )

            if hints:
                parts.append(
                    "Wearables:\n" + "\n".join(f"  {h}" for h in hints)
                )
        except Exception as e:
            logger.debug("Welcome briefing (wearables): %s", e)

        if not parts:
            return ""

        return "Welcome back.\n\n" + "\n".join(parts)

    async def _deep_analysis(self, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Periodic deep analysis — runs reasoning modules automatically.

        Runs every 4 hours while unlocked (same interval as periodic check).
        Auto-runs: correlations, delta, panel gaps, hypothesis generation.
        """
        if not self._km.is_unlocked:
            return

        try:
            t_start = time.monotonic()
            db = self._get_db()
            user_id = self._primary_user_id
            demographics = db.get_user_demographics(user_id)

            # 1. Auto-discover correlations (lab <-> wearable)
            try:
                from healthbot.reasoning.correlate import CorrelationEngine

                engine = CorrelationEngine(db)
                stored = engine.discover_and_store(
                    user_id=user_id, days=90,
                )
                if stored:
                    logger.info(
                        "Discovered %d significant correlations",
                        len(stored),
                    )
            except Exception as e:
                logger.debug("Deep analysis (correlate): %s", e)

            # 2. Auto-hypothesis generation
            try:
                from healthbot.reasoning.hypothesis_generator import (
                    HypothesisGenerator,
                )
                from healthbot.reasoning.hypothesis_tracker import (
                    HypothesisTracker,
                )

                gen = HypothesisGenerator(db)
                new_hyps = gen.scan_all(
                    user_id,
                    sex=demographics.get("sex"),
                    age=demographics.get("age"),
                )
                if new_hyps:
                    tracker = HypothesisTracker(db)
                    for h in new_hyps:
                        tracker.upsert_hypothesis(
                            user_id,
                            {
                                "title": h.title,
                                "confidence": h.confidence,
                                "evidence_for": h.evidence_for,
                                "evidence_against": h.evidence_against,
                                "missing_tests": h.missing_tests,
                                "pattern_id": h.pattern_id,
                            },
                        )
                    logger.info(
                        "Deep analysis: %d hypotheses generated", len(new_hyps),
                    )
            except Exception as e:
                logger.debug("Deep analysis (hypotheses): %s", e)

            # 3. Panel gap check
            try:
                from healthbot.reasoning.panel_gaps import PanelGapDetector

                detector = PanelGapDetector(db)
                detector.detect()  # Results cached for /gaps command
            except Exception as e:
                logger.debug("Deep analysis (panel gaps): %s", e)

            # 4. Intelligence audit (unfollowed flags, condition gaps, screening gaps)
            try:
                from healthbot.reasoning.intelligence_auditor import (
                    IntelligenceAuditor,
                )

                auditor = IntelligenceAuditor(db)
                gaps = auditor.audit(user_id, demographics)
                if gaps:
                    logger.info(
                        "Intelligence audit: %d gaps found (%s)",
                        len(gaps),
                        ", ".join(g.gap_type for g in gaps[:3]),
                    )
            except Exception as e:
                logger.debug("Deep analysis (intelligence audit): %s", e)

            # 5. Wearable trend analysis
            try:
                from healthbot.reasoning.wearable_trends import (
                    WearableTrendAnalyzer,
                )

                wt_analyzer = WearableTrendAnalyzer(db)
                wearable_trends = wt_analyzer.detect_all_trends(
                    days=14, user_id=user_id,
                )
                anomalies = wt_analyzer.detect_anomalies(
                    days=1, user_id=user_id,
                )
                if wearable_trends or anomalies:
                    logger.info(
                        "Deep analysis: %d wearable trends, %d anomalies",
                        len(wearable_trends), len(anomalies),
                    )
            except Exception as e:
                logger.debug("Deep analysis (wearable trends): %s", e)

            # 6. Recovery readiness check
            try:
                from healthbot.reasoning.recovery_readiness import (
                    RecoveryReadinessEngine,
                )

                readiness = RecoveryReadinessEngine(db).compute(
                    user_id=user_id,
                )
                if readiness and readiness.score < 40:
                    msg = (
                        f"Recovery alert: {readiness.score:.0f}/100 "
                        f"({readiness.grade}). "
                        f"{readiness.recommendation}"
                    )
                    await self._tracked_send(context.bot, msg)
            except Exception as e:
                logger.debug("Deep analysis (recovery readiness): %s", e)

            # 7. KB enrichment — store significant findings, cleanup stale
            try:
                from healthbot.reasoning.kb_enrichment import (
                    KBEnrichmentEngine,
                )

                enricher = KBEnrichmentEngine(db)
                enricher.cleanup_stale(max_age_days=90)
            except Exception as e:
                logger.debug("Deep analysis (kb enrichment): %s", e)

            # 8. Claude synthesis — reviews new data against full patient profile
            try:
                conv = self._claude_getter() if self._claude_getter else None
                if conv:
                    import asyncio as _asyncio

                    from healthbot.llm.background_analysis import (
                        BackgroundAnalysisEngine,
                    )

                    engine = BackgroundAnalysisEngine(db, self._config)
                    prompt = engine.build_health_synthesis_prompt(user_id)
                    if prompt:
                        response, _ = await _asyncio.to_thread(
                            conv.handle_message, prompt, user_id,
                        )
                        engine.commit_health_watermarks()
                        alert = engine.extract_alert(response)
                        if alert:
                            await self._tracked_send(context.bot, alert)
                        logger.info(
                            "Background synthesis: %d chars", len(response),
                        )
                    else:
                        logger.debug(
                            "Background synthesis: no new data, skipped",
                        )
            except Exception as e:
                logger.debug("Background synthesis skipped: %s", e)

            elapsed = time.monotonic() - t_start
            logger.info("Deep analysis completed in %.1fs", elapsed)

        except Exception as e:
            logger.warning("Deep analysis failed: %s", e)

    async def _research_conditions(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Periodic research monitoring for user's conditions.

        Runs even while vault is locked (uses cached condition list).
        When unlocked, refreshes conditions from DB first.
        PubMed queries are anonymized — no PHI leaves the machine.
        """
        conditions = self._cached_conditions

        # If unlocked, refresh from DB
        if self._km.is_unlocked:
            try:
                from healthbot.reasoning.condition_extractor import extract_conditions

                db = self._get_db()
                conditions = extract_conditions(db, self._primary_user_id)
                self._cached_conditions = conditions
            except Exception as e:
                logger.debug("Research monitor: condition refresh failed: %s", e)

        if not conditions:
            return

        try:
            from healthbot.research.pubmed_client import PubMedClient
            from healthbot.security.phi_firewall import PhiFirewall

            firewall = PhiFirewall()
            client = PubMedClient(self._config, firewall)

            total_found = 0
            for condition in conditions[:5]:  # Cap at 5 to avoid rate limits
                try:
                    results = await client.search(
                        f"{condition} recent advances",
                        max_results=3,
                    )
                    if results:
                        total_found += len(results)
                        # Store in evidence cache if vault is unlocked
                        if self._km.is_unlocked:
                            self._store_research_results(
                                condition, results,
                            )
                except Exception as e:
                    logger.debug(
                        "Research monitor: PubMed search failed for '%s': %s",
                        condition, e,
                    )

            if total_found:
                logger.info(
                    "Research monitor: found %d articles for %d conditions",
                    total_found, len(conditions),
                )
                # Notify user if unlocked and notable findings
                if self._km.is_unlocked and total_found > 0:
                    msg = (
                        f"Background research: found {total_found} new "
                        f"article{'s' if total_found != 1 else ''} relevant "
                        f"to your conditions. Use /evidence to browse."
                    )
                    await self._tracked_send(context.bot, msg)

            # Claude research synthesis — cross-references articles against patient
            if self._km.is_unlocked and total_found > 0:
                try:
                    import asyncio as _asyncio

                    conv = (
                        self._claude_getter()
                        if self._claude_getter
                        else None
                    )
                    if conv:
                        from healthbot.llm.background_analysis import (
                            BackgroundAnalysisEngine,
                        )

                        engine = BackgroundAnalysisEngine(
                            self._get_db(), self._config,
                        )
                        prompt = engine.build_research_synthesis_prompt(
                            self._primary_user_id,
                        )
                        if prompt:
                            response, _ = await _asyncio.to_thread(
                                conv.handle_message, prompt,
                                self._primary_user_id,
                            )
                            engine.commit_research_watermarks()
                            alert = engine.extract_alert(response)
                            if alert:
                                await self._tracked_send(context.bot, alert)
                except Exception as e:
                    logger.debug("Research synthesis skipped: %s", e)
        except Exception as e:
            logger.debug("Research monitor failed: %s", e)

    def _store_research_results(
        self, condition: str, results: list,
    ) -> None:
        """Store PubMed results in external evidence store."""
        try:
            from healthbot.research.external_evidence_store import (
                ExternalEvidenceStore,
            )

            db = self._get_db()
            store = ExternalEvidenceStore(db)
            for r in results:
                store.store(
                    source="pubmed_monitor",
                    query=condition,
                    result={
                        "pmid": r.pmid,
                        "title": r.title,
                        "journal": r.journal,
                        "year": r.year,
                        "authors": r.authors[:3],
                        "abstract": r.abstract[:500] if r.abstract else "",
                        "condition": condition,
                    },
                    ttl_days=90,
                    condition_related=True,  # Never expires
                )
        except Exception as e:
            logger.debug("Store research results failed: %s", e)

    async def _check_med_reminders(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Check and send medication reminders. Skips if locked."""
        if not self._km.is_unlocked:
            return

        try:
            from healthbot.reasoning.med_reminders import (
                format_reminder,
                get_due_reminders,
            )

            db = self._get_db()
            user_id = self._primary_user_id
            due = get_due_reminders(db, user_id)
            for reminder in due:
                # Dedup: only send each reminder once per minute
                dedup = f"med_reminder_{reminder.med_name}_{reminder.time}"
                if self._has_sent_key(dedup):
                    continue
                self._record_sent_key(dedup)
                msg = format_reminder(reminder)
                await self._tracked_send(context.bot, msg)
        except Exception as e:
            logger.debug("Med reminder check: %s", e)

    async def _check_reminder_resumes(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Resume paused reminders whose retest window has opened. Daily check."""
        if not self._km.is_unlocked:
            return

        try:
            from healthbot.reasoning.med_reminders import check_reminder_resumes

            db = self._get_db()
            user_id = self._primary_user_id
            messages = check_reminder_resumes(db, user_id)
            for msg in messages:
                await self._tracked_send(context.bot, msg)
        except Exception as e:
            logger.debug("Reminder resume check: %s", e)

    async def _check_wearable_gaps(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Check for stale wearable data and alert user. Skips if locked."""
        if not self._km.is_unlocked:
            return

        try:
            from datetime import date

            from healthbot.security.keychain import Keychain

            kc = Keychain()
            db = self._get_db()
            user_id = self._primary_user_id

            for provider, cred_key, auth_cmd in [
                ("whoop", "whoop_client_id", "/whoop_auth"),
                ("oura", "oura_client_id", "/oura_auth"),
            ]:
                if not kc.retrieve(cred_key):
                    continue  # Not connected — skip

                # Check last data date for this provider
                try:
                    rows = db.conn.execute(
                        "SELECT MAX(date) as last_date FROM wearable_daily "
                        "WHERE provider = ? AND user_id = ?",
                        (provider, user_id),
                    ).fetchall()
                    last_date_str = rows[0]["last_date"] if rows else None
                except Exception:
                    continue

                if not last_date_str:
                    continue

                try:
                    last_date = date.fromisoformat(last_date_str)
                except ValueError:
                    continue

                gap_days = (date.today() - last_date).days
                if gap_days >= WEARABLE_GAP_THRESHOLD_DAYS:
                    dedup = f"wearable_gap_{provider}_{last_date_str}"
                    if self._has_sent_key(dedup):
                        continue
                    self._record_sent_key(dedup)

                    name = provider.upper() if provider == "whoop" else provider.title()
                    await self._tracked_send(
                        context.bot,
                        f"{name} data is {gap_days} days stale "
                        f"(last sync: {last_date_str}). "
                        f"Try {auth_cmd} to reconnect or sync.",
                    )
        except Exception as e:
            logger.debug("Wearable gap check: %s", e)

    async def _daily_wearable_sync(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Daily background sync — pulls last 2 days from connected wearables."""
        if not self._km.is_unlocked:
            return
        try:
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            kc = Keychain()
            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)

            for provider, cred_key, client_cls in [
                ("whoop", "whoop_client_id", "WhoopClient"),
                ("oura", "oura_client_id", "OuraClient"),
            ]:
                if not kc.retrieve(cred_key):
                    continue
                try:
                    if client_cls == "WhoopClient":
                        from healthbot.importers.whoop_client import WhoopClient
                        client = WhoopClient(self._config, kc, vault)
                    else:
                        from healthbot.importers.oura_client import OuraClient
                        client = OuraClient(self._config, kc, vault)
                    clean = self._get_clean_db()
                    try:
                        count = await client.sync_daily(
                            db, days=2, clean_db=clean,
                            user_id=self._primary_user_id,
                        ) or 0
                    finally:
                        if clean:
                            clean.close()
                    if count:
                        logger.info(
                            "Daily %s sync: %d records", provider, count,
                        )
                except Exception as e:
                    logger.warning("Daily %s sync failed: %s", provider, e)
        except Exception as e:
            logger.debug("Daily wearable sync: %s", e)

    async def _check_auth_health(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Validate integration tokens and alert user on failures."""
        if not self._km.is_unlocked:
            return

        try:
            from healthbot.security.keychain import Keychain
            from healthbot.security.vault import Vault

            kc = Keychain()
            vault = Vault(self._config.blobs_dir, self._km)

            checks = [
                ("WHOOP", "whoop_client_id", "whoop_client_secret",
                 "whoop_refresh_token", "/whoop_auth"),
                ("Oura", "oura_client_id", "oura_client_secret",
                 "oura_refresh_token", "/oura_auth"),
            ]

            for name, cid_key, secret_key, token_blob, auth_cmd in checks:
                client_id = kc.retrieve(cid_key)
                if not client_id:
                    continue  # Not configured — skip

                dedup = f"auth_health_{name}"
                if self._has_sent_key(dedup):
                    continue

                # Validate credential format (catch corrupted values)
                if " " in client_id or len(client_id) < 8:
                    self._record_sent_key(dedup)
                    await self._tracked_send(
                        context.bot,
                        f"{name} client ID looks corrupted. "
                        f"Run {auth_cmd} reset to fix it.",
                    )
                    continue

                # Check credentials exist
                client_secret = kc.retrieve(secret_key)
                if not client_secret:
                    self._record_sent_key(dedup)
                    await self._tracked_send(
                        context.bot,
                        f"{name} credentials incomplete — client secret "
                        f"missing. Run {auth_cmd} to reconnect.",
                    )
                    continue

                # Check refresh token exists
                try:
                    token = vault.retrieve_blob(token_blob)
                    if not token:
                        raise ValueError("empty")
                except Exception:
                    self._record_sent_key(dedup)
                    await self._tracked_send(
                        context.bot,
                        f"{name} auth token expired or missing. "
                        f"Run {auth_cmd} to reconnect.",
                    )
                    continue

                # Check token validity without refreshing
                try:
                    # Token exists and credentials are present — check
                    # if the token looks valid (non-empty, decodable).
                    # Only flag if the token is clearly broken. Do NOT
                    # call the refresh endpoint — that consumes the
                    # token and should only happen during actual sync.
                    token_str = (
                        token.decode() if isinstance(token, bytes)
                        else token
                    )
                    if not token_str or len(token_str) < 10:
                        self._record_sent_key(dedup)
                        await self._tracked_send(
                            context.bot,
                            f"{name} refresh token looks corrupted "
                            f"(too short). Run {auth_cmd} to "
                            f"re-authorize.",
                        )
                    else:
                        logger.info("%s auth health check: OK", name)
                except Exception as e:
                    logger.warning(
                        "%s auth health check failed: %s", name, e,
                    )
        except Exception as e:
            logger.debug("Auth health check: %s", e)

    async def _check_appointment_prep(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Auto-send prep packet for tomorrow's appointments."""
        if not self._km.is_unlocked:
            return

        try:
            db = self._get_db()
            user_id = self._primary_user_id
            upcoming = db.get_upcoming_appointments(user_id, within_days=1)

            for appt in upcoming:
                if appt.get("_prep_sent"):
                    continue

                specialty = appt.get("specialty", "")
                provider_name = appt.get("provider_name", "Unknown")
                appt_date = appt.get("date", appt.get("_appt_date", ""))
                reason = appt.get("reason", "")

                # Build prep text
                prep_text = self._build_appointment_prep(
                    db, user_id, specialty, provider_name, appt_date, reason,
                )

                if prep_text:
                    for page in paginate(prep_text):
                        await self._tracked_send(context.bot, page)
                    db.mark_appointment_prep_sent(appt["_id"])
                    logger.info(
                        "Appointment prep sent for %s on %s",
                        provider_name, appt_date,
                    )
        except Exception as e:
            logger.debug("Appointment prep check: %s", e)

    def _build_appointment_prep(
        self, db: HealthDB, user_id: int,
        specialty: str, provider_name: str,
        appt_date: str, reason: str,
    ) -> str:
        """Build a specialty-aware appointment prep packet."""
        lines = [
            f"APPOINTMENT PREP: {provider_name}",
            f"Date: {appt_date}",
        ]
        if reason:
            lines.append(f"Reason: {reason}")
        lines.append("-" * 30)

        # Specialty-relevant lab metrics
        specialty_labs = _SPECIALTY_LABS.get(
            specialty.lower(), _SPECIALTY_LABS.get("general", []),
        )

        # Pull relevant labs
        relevant_labs = []
        for canonical in specialty_labs:
            rows = db.query_observations(
                record_type="lab_result",
                canonical_name=canonical,
                limit=3,
                user_id=user_id,
            )
            for row in rows:
                name = row.get("test_name", canonical)
                val = row.get("value", "")
                unit = row.get("unit", "")
                dt = row.get("date_collected", "")
                flag = row.get("flag", "")
                flag_str = f" [{flag}]" if flag else ""
                relevant_labs.append(f"  {name}: {val} {unit}{flag_str} ({dt})")

        if relevant_labs:
            lines.append(f"\nRelevant Labs ({specialty or 'general'}):")
            lines.extend(relevant_labs[:15])

        # Active medications
        meds = db.get_active_medications(user_id=user_id)
        if meds:
            lines.append("\nActive Medications:")
            for med in meds[:10]:
                name = med.get("name", "")
                dose = med.get("dose", "")
                unit = med.get("unit", "")
                freq = med.get("frequency", "")
                lines.append(f"  {name} {dose} {unit} {freq}".strip())

        # Active hypotheses
        try:
            hyps = db.get_active_hypotheses(user_id)
            if hyps:
                lines.append("\nActive Hypotheses:")
                for h in hyps[:5]:
                    conf = h.get("confidence", 0)
                    lines.append(
                        f"  {h.get('title', '?')} ({float(conf) * 100:.0f}%)",
                    )
        except Exception:
            pass

        # Questions to ask
        lines.append("\nSuggested Discussion Points:")
        if relevant_labs:
            flagged = [lab for lab in relevant_labs if "[" in lab]
            if flagged:
                lines.append("  - Review flagged lab results above")
        lines.append("  - Any changes to medications?")
        lines.append("  - Next follow-up schedule?")
        if reason:
            lines.append(f"  - Follow up on: {reason}")

        return "\n".join(lines)

    def _compute_digest_first_delay(self) -> float:
        """Compute seconds until the configured digest time.

        If the configured time has already passed today, schedules for tomorrow.
        Returns interval (24h) if digest is disabled (empty digest_time).
        """
        digest_time = getattr(self._config, "digest_time", "")
        if not digest_time:
            return float(getattr(self._config, "digest_interval", 86400))
        try:
            from datetime import datetime, timedelta
            now = datetime.now()
            hour, minute = int(digest_time.split(":")[0]), int(digest_time.split(":")[1])
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            return (target - now).total_seconds()
        except (ValueError, IndexError):
            return float(getattr(self._config, "digest_interval", 86400))

    async def _send_daily_digest(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Send daily health digest. Skips if vault is locked."""
        if not self._km.is_unlocked:
            return

        digest_time = getattr(self._config, "digest_time", "")
        if not digest_time:
            return  # Digest disabled

        try:
            from healthbot.reasoning.digest import build_daily_digest, format_digest

            db = self._get_db()
            user_id = self._primary_user_id
            report = build_daily_digest(db, user_id)
            text = format_digest(report)
            for page in paginate(text):
                await self._tracked_send(context.bot, page)
            logger.info("Daily digest sent")
        except Exception as e:
            logger.warning("Daily digest failed: %s", e)

    def _rebuild_search_index(self) -> None:
        """Rebuild the search index after data ingestion."""
        try:
            from healthbot.retrieval.search import SearchEngine
            from healthbot.security.vault import Vault

            db = self._get_db()
            vault = Vault(self._config.blobs_dir, self._km)
            engine = SearchEngine(self._config, db, vault)
            count = engine.build_index()
            logger.info("Search index rebuilt: %d documents", count)
        except Exception as e:
            logger.debug("Search index rebuild skipped: %s", e)

    def _compute_weekly_first_delay(self) -> float:
        """Compute seconds until next configured weekly report time."""
        weekly_day = getattr(self._config, "weekly_report_day", "")
        weekly_time = getattr(self._config, "weekly_report_time", "20:00")
        if not weekly_day:
            return 86400.0

        try:
            from datetime import datetime, timedelta

            day_map = {
                "monday": 0, "tuesday": 1, "wednesday": 2,
                "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
            }
            target_day = day_map.get(weekly_day.lower(), 6)
            hour, minute = int(weekly_time.split(":")[0]), int(weekly_time.split(":")[1])

            now = datetime.now()
            days_ahead = (target_day - now.weekday()) % 7
            if days_ahead == 0:
                # Today is the day — check if time has passed
                target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if target <= now:
                    days_ahead = 7
            target = now.replace(
                hour=hour, minute=minute, second=0, microsecond=0,
            ) + timedelta(days=days_ahead)
            return max(60.0, (target - now).total_seconds())
        except (ValueError, IndexError):
            return 86400.0

    def _compute_monthly_first_delay(self) -> float:
        """Compute seconds until next configured monthly report time."""
        monthly_day = getattr(self._config, "monthly_report_day", 0)
        monthly_time = getattr(self._config, "monthly_report_time", "20:00")
        if not monthly_day:
            return 86400.0

        try:
            from datetime import datetime

            hour, minute = int(monthly_time.split(":")[0]), int(monthly_time.split(":")[1])
            now = datetime.now()

            target = now.replace(
                day=min(monthly_day, 28),
                hour=hour, minute=minute, second=0, microsecond=0,
            )
            if target <= now:
                # Next month
                if now.month == 12:
                    target = target.replace(year=now.year + 1, month=1)
                else:
                    target = target.replace(month=now.month + 1)
            return max(60.0, (target - now).total_seconds())
        except (ValueError, IndexError):
            return 86400.0

    async def _send_weekly_pdf_report(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Send weekly PDF health report. Skips if locked or wrong day."""
        if not self._km.is_unlocked:
            return

        weekly_day = getattr(self._config, "weekly_report_day", "")
        if not weekly_day:
            return

        from datetime import datetime
        day_map = {
            "monday": 0, "tuesday": 1, "wednesday": 2,
            "thursday": 3, "friday": 4, "saturday": 5, "sunday": 6,
        }
        target_day = day_map.get(weekly_day.lower(), -1)
        if datetime.now().weekday() != target_day:
            return

        try:
            import io

            from healthbot.export.weekly_pdf_report import WeeklyPdfReportGenerator

            db = self._get_db()
            gen = WeeklyPdfReportGenerator(db)
            pdf_bytes = gen.generate_weekly(self._primary_user_id)

            doc = io.BytesIO(pdf_bytes)
            doc.name = f"weekly_report_{datetime.now().strftime('%Y%m%d')}.pdf"
            await context.bot.send_document(
                chat_id=self._chat_id, document=doc,
            )
            await self._tracked_send(
                context.bot, "Weekly health report attached.",
            )
            logger.info("Weekly PDF report sent")
        except Exception as e:
            logger.warning("Weekly PDF report failed: %s", e)

    async def _send_monthly_pdf_report(
        self, context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        """Send monthly PDF health report. Skips if locked or wrong day."""
        if not self._km.is_unlocked:
            return

        monthly_day = getattr(self._config, "monthly_report_day", 0)
        if not monthly_day:
            return

        from datetime import datetime
        if datetime.now().day != min(monthly_day, 28):
            return

        try:
            import io

            from healthbot.export.weekly_pdf_report import WeeklyPdfReportGenerator

            db = self._get_db()
            gen = WeeklyPdfReportGenerator(db)
            pdf_bytes = gen.generate_monthly(self._primary_user_id)

            doc = io.BytesIO(pdf_bytes)
            doc.name = f"monthly_report_{datetime.now().strftime('%Y%m')}.pdf"
            await context.bot.send_document(
                chat_id=self._chat_id, document=doc,
            )
            await self._tracked_send(
                context.bot, "Monthly health report attached.",
            )
            logger.info("Monthly PDF report sent")
        except Exception as e:
            logger.warning("Monthly PDF report failed: %s", e)

    def trigger_post_ingestion(
        self, lab_results: list, user_id: int,
    ) -> None:
        """Run targeted analysis immediately after PDF ingestion.

        Only analyzes engines relevant to the ingested test names.
        Called from message_router after successful ingestion.
        """
        if not self._km.is_unlocked:
            return
        try:
            from healthbot.reasoning.targeted_analysis import (
                TargetedAnalyzer,
            )

            db = self._get_db()
            analyzer = TargetedAnalyzer(db)
            result = analyzer.analyze_new_labs(lab_results, user_id)
            logger.info(
                "Post-ingestion analysis complete: %s",
                {
                    "trends": len(result.trends_found),
                    "interactions": len(result.interactions_found),
                    "hypotheses_new": result.hypotheses_created,
                    "fulfilled": len(result.fulfilled_tests),
                    "reminders": len(result.reminder_updates),
                },
            )
            # Store reminder updates for async delivery
            if result.reminder_updates:
                self._pending_reminder_updates = result.reminder_updates
        except Exception as e:
            logger.debug("Post-ingestion analysis failed: %s", e)

    def _get_db(self) -> HealthDB:
        if self._db is None or self._db._conn is None:
            self._db = HealthDB(self._config, self._km)
            self._db.open()
            self._db.run_migrations()
        return self._db

    def _get_clean_db(self):
        """Get CleanDB for direct wearable writes. Returns None if unavailable."""
        try:
            from healthbot.data.clean_db import CleanDB

            path = self._config.clean_db_path
            if not path.exists():
                return None
            clean = CleanDB(path)
            clean.open(clean_key=self._km.get_clean_key())
            return clean
        except Exception:
            return None
