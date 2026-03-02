"""Core handler infrastructure — shared state and helpers.

All sub-handler groups hold a reference to HandlerCore for accessing
config, key_manager, database, conversation, and router state.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from dataclasses import dataclass
from datetime import UTC, datetime

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.message_router import MessageRouter
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.reasoning.triage import TriageEngine
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")

_ERROR_BUFFER_MAX = 20


@dataclass
class ErrorRecord:
    """A captured technical error for debug context (no PHI)."""

    timestamp: str
    error_type: str
    message: str
    provider: str = ""
    hint: str = ""


class HandlerCore:
    """Shared state container for all handler groups."""

    def __init__(
        self,
        config: Config,
        key_manager: KeyManager,
        phi_firewall: PhiFirewall,
    ) -> None:
        self._config = config
        self._km = key_manager
        self._fw = phi_firewall
        self._db: HealthDB | None = None
        self._triage = TriageEngine()
        self._scheduler = None  # AlertScheduler, set by wire_scheduler()
        self._claude_conversation = None  # Claude CLI conversation

        # Ingestion mode: skip post-ingest analysis until user says "done"
        self._ingestion_mode: bool = False
        self._ingestion_count: int = 0  # docs ingested during this mode

        # Upload mode: secure upload — Claude CLI paused, nothing leaves machine
        self._upload_mode: bool = False
        self._upload_count: int = 0

        # Last conversation exchange for /feedback capture
        self._last_user_input: str = ""
        self._last_bot_response: str = ""

        # Technical error buffer for debug/troubleshoot (no PHI)
        self._error_buffer: list[ErrorRecord] = []

        # Last clean sync report (for PII block notifications)
        self._last_sync_report = None

        # Session chat wipe tracking
        self._session_msg_ids: list[int] = []
        self._msg_ids_lock = threading.Lock()
        self._session_chat_id: int | None = None
        self._pending_wipe: bool = False
        self._bot = None  # Telegram bot reference for proactive wipe
        self._session_chat_id_for_notify: int | None = None

        # Active query tracking for safe vault lock (M9)
        self._active_queries: int = 0
        self._query_lock = threading.Lock()

        self._router = MessageRouter(
            config=config,
            key_manager=key_manager,
            get_db=self._get_db,
            check_auth=self._check_auth,
        )
        self._router.set_error_source(self.get_recent_errors, phi_firewall)
        self._router.set_claude_getter(self._get_claude_conversation)
        self._router._exchange_cb = self._track_exchange
        self._router._track_msg_cb = self.track_message

    def _track_exchange(self, user_input: str, bot_response: str) -> None:
        """Track last conversation exchange for /feedback capture."""
        self._last_user_input = user_input
        self._last_bot_response = bot_response

    def track_message(self, chat_id: int, msg_id: int) -> None:
        """Track an incoming message ID for session chat wipe on lock."""
        self._session_chat_id = chat_id
        with self._msg_ids_lock:
            self._session_msg_ids.append(msg_id)

    def log_capability_manifest(self) -> str:
        """Log startup capability matrix. Returns formatted string."""
        lines = ["[STARTUP] Capability manifest:"]

        # Claude CLI
        try:
            from healthbot.llm.claude_client import ClaudeClient
            client = ClaudeClient()
            if client.is_available():
                lines.append("  Claude CLI:  OK")
            else:
                lines.append("  Claude CLI:  NOT FOUND")
        except Exception:
            lines.append("  Claude CLI:  NOT FOUND")

        # Ollama (check availability — started on-demand at unlock)
        try:
            from healthbot.llm.ollama_client import OllamaClient
            ollama = OllamaClient(
                model=getattr(self._config, "ollama_model", ""),
                base_url=getattr(self._config, "ollama_url", ""),
                timeout=getattr(self._config, "ollama_timeout", 30),
            )
            if ollama.is_available():
                lines.append(f"  Ollama:      OK ({ollama._model})")
            else:
                lines.append("  Ollama:      NOT RUNNING (starts on unlock)")
        except Exception:
            lines.append("  Ollama:      NOT AVAILABLE")

        # GLiNER NER
        try:
            from healthbot.security.ner_layer import NerLayer
            if NerLayer.is_available():
                lines.append("  GLiNER NER:  OK")
            else:
                lines.append("  GLiNER NER:  NOT INSTALLED")
        except Exception:
            lines.append("  GLiNER NER:  NOT INSTALLED")

        # Wearables (check Keychain for credentials)
        try:
            from healthbot.security.keychain import Keychain
            kc = Keychain()
            for name, key in [("WHOOP", "whoop_client_id"), ("Oura", "oura_client_id")]:
                if kc.retrieve(key):
                    lines.append(f"  {name:12s} OK (credentials present)")
                else:
                    lines.append(f"  {name:12s} NOT CONNECTED")
        except Exception:
            lines.append("  Wearables:   CHECK FAILED")

        # PII layers count
        pii_count = 1  # regex always active
        try:
            from healthbot.security.ner_layer import NerLayer
            if NerLayer.is_available():
                pii_count += 1
        except Exception:
            pass
        try:
            from healthbot.llm.ollama_client import OllamaClient
            o = OllamaClient(
                model=getattr(self._config, "ollama_model", ""),
                base_url=getattr(self._config, "ollama_url", ""),
                timeout=getattr(self._config, "ollama_timeout", 30),
            )
            if o.is_available():
                pii_count += 1
        except Exception:
            pass
        lines.append(f"  PII layers:  {pii_count}/3 active")

        manifest = "\n".join(lines)
        logger.info(manifest)
        return manifest

    def record_error(
        self, error_type: str, message: str,
        provider: str = "", hint: str = "",
    ) -> None:
        """Record a technical error for troubleshoot context (no PHI).

        All fields are scrubbed through PhiFirewall before storage.
        """
        scrub = self._fw.redact
        record = ErrorRecord(
            timestamp=datetime.now(UTC).isoformat(timespec="seconds"),
            error_type=scrub(error_type),
            message=scrub(message),
            provider=provider,
            hint=scrub(hint),
        )
        self._error_buffer.append(record)
        if len(self._error_buffer) > _ERROR_BUFFER_MAX:
            self._error_buffer = self._error_buffer[-_ERROR_BUFFER_MAX:]

    def get_recent_errors(self, count: int = 5) -> list[ErrorRecord]:
        """Return the most recent error records."""
        return list(self._error_buffer[-count:])

    async def _wipe_and_notify(self) -> None:
        """Wipe session chat and send lock notification. Fired from lock callback."""
        bot = self._bot
        if not bot:
            logger.warning("Cannot wipe chat: no bot reference available")
            return
        self._pending_wipe = False
        await self.wipe_session_chat(bot)
        if self._session_chat_id_for_notify:
            try:
                await bot.send_message(
                    chat_id=self._session_chat_id_for_notify,
                    text=(
                        "Session expired. Vault locked. Chat cleared.\n"
                        "Send /unlock to start a new session."
                    ),
                )
            except Exception as e:
                logger.warning("Failed to send lock notification: %s", e)
            self._session_chat_id_for_notify = None

    async def wipe_session_chat(self, bot) -> None:
        """Delete all session messages from the Telegram chat."""
        with self._msg_ids_lock:
            if not self._session_chat_id or not self._session_msg_ids:
                self._session_msg_ids.clear()
                return

            chat_id = self._session_chat_id
            all_ids = sorted(set(self._session_msg_ids))

            # Clear tracking state
            self._session_msg_ids.clear()
            self._session_chat_id = None

        # Delete only tracked message IDs (user messages + bot replies
        # that were explicitly tracked via track_message()).

        # Batch delete (Telegram allows up to 100 per call)
        for i in range(0, len(all_ids), 100):
            batch = all_ids[i:i + 100]
            try:
                await bot.delete_messages(chat_id=chat_id, message_ids=batch)
            except Exception as e:
                logger.debug("Batch delete failed for chat %s: %s", chat_id, e)
                for mid in batch:
                    try:
                        await bot.delete_message(chat_id=chat_id, message_id=mid)
                    except Exception:
                        pass

    async def _timeout_wipe(self, bot) -> None:
        """Reliable wipe+notify triggered by scheduler on timeout lock.

        Unlike the fire-and-forget task in _on_vault_lock, this runs
        directly in the scheduler's async context with a guaranteed bot ref.
        """
        self._pending_wipe = False
        await self.wipe_session_chat(bot)
        chat_id = self._session_chat_id_for_notify
        if chat_id:
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "Session expired. Vault locked. Chat cleared.\n"
                        "Send /unlock to start a new session."
                    ),
                )
            except Exception as e:
                logger.warning("Failed to send lock notification: %s", e)
            self._session_chat_id_for_notify = None

    def wire_scheduler(self, job_queue: object) -> None:
        """Create AlertScheduler and register recurring jobs."""
        from healthbot.bot.scheduler import AlertScheduler

        self._scheduler = AlertScheduler(
            config=self._config, key_manager=self._km, chat_id=0,
            phi_firewall=self._fw,
        )
        self._scheduler.set_message_tracker(self.track_message)
        self._scheduler.set_claude_getter(self._get_claude_conversation)
        self._scheduler._timeout_wipe_cb = self._timeout_wipe
        self._scheduler.register_jobs(job_queue)
        self._router._post_ingest_cb = self._scheduler.trigger_post_ingestion
        self._router._post_ingest_sync_cb = self._sync_after_ingestion

    def _sync_after_ingestion(self) -> None:
        """Run clean sync + refresh Claude context after PDF ingestion.

        Called in a background thread from message_router so new lab data
        (including collection dates) is immediately visible to Claude.
        """
        try:
            report = self._trigger_clean_sync(full=False)
            if report:
                logger.info(
                    "Post-ingestion sync: %d obs, %d errors",
                    report.observations_synced, len(report.errors),
                )
            # Refresh Claude's health data context
            conv = self._claude_conversation
            if conv:
                from healthbot.data.clean_db import CleanDB
                clean = CleanDB(self._config.clean_db_path, phi_firewall=self._fw)
                try:
                    clean.open(clean_key=self._km.get_clean_key())
                    conv.refresh_data_from_clean_db(clean)
                    logger.info(
                        "Post-ingestion context refresh: %d chars",
                        len(conv._health_data),
                    )
                finally:
                    clean.close()
        except Exception as e:
            logger.debug("Post-ingestion sync/refresh failed: %s", e)

    def wire_unlock_callback(self) -> None:
        """Register the vault-unlock callback. Always called (no job queue dependency)."""

        async def _on_unlock(bot: object, chat_id: int) -> None:
            # Always register lock callback on unlock (not lazy)
            self._km.set_on_lock(self._on_vault_lock)
            # Store bot reference for proactive wipe
            self._bot = bot

            # Scheduler integration (only if available)
            if self._scheduler is not None:
                self._scheduler._chat_id = chat_id
                await self._scheduler.run_on_unlock(bot)

            # Wire MemoryStore for STM consolidation
            if self._scheduler is not None:
                try:
                    db = self._get_db()
                    if db and not self._scheduler._memory_store:
                        from healthbot.llm.memory_store import MemoryStore
                        self._scheduler.set_memory_store(MemoryStore(db))
                except Exception as e:
                    logger.debug("MemoryStore init skipped: %s", e)

            # Init Claude CLI conversation FIRST (fast — reads existing clean DB)
            try:
                conv = self._get_claude_conversation()
                if conv is not None:
                    # Ensure raw vault handle is available for LTM sync
                    conv._db = self._get_db()
                    uid = 0
                    if hasattr(self._config, "allowed_user_ids") and self._config.allowed_user_ids:
                        uid = self._config.allowed_user_ids[0]
                    conv._user_id = uid
                    # Prefer CleanDB (pre-anonymized, faster)
                    clean = self._get_clean_db()
                    if clean:
                        try:
                            conv.refresh_data_from_clean_db(clean)
                            conv._reconcile_demographics_to_ltm()
                            logger.info(
                                "Health data refreshed: %d chars, wearables: %s",
                                len(conv._health_data),
                                "Monthly" in conv._health_data,
                            )
                        finally:
                            clean.close()
                    else:
                        from healthbot.llm.anonymizer import Anonymizer

                        db = self._get_db()
                        fw = self._fw
                        anon = Anonymizer(phi_firewall=fw, use_ner=True)
                        conv.refresh_data(db, anon, fw)
                    # Register live integration status builder
                    conv._status_builder = self._build_integration_status
                    # Register system improvement notification callback
                    conv._on_system_improvement = (
                        lambda block, _bot=bot, _cid=chat_id:
                        self._notify_system_improvement(_bot, _cid, block)
                    )
                    logger.info("Claude CLI conversation initialized on unlock")
                else:
                    logger.info("Claude CLI unavailable; commands still work")
            except Exception as e:
                logger.warning("Claude CLI init failed: %s", e)

            # Clean sync in background thread (slow — Ollama anonymizes each record)
            # After sync completes, re-refresh context so Claude sees new data.
            import asyncio

            def _bg_clean_sync() -> None:
                try:
                    # Backfill source_lab from encrypted data (one-time)
                    try:
                        db = self._get_db()
                        n = db.backfill_source_lab()
                        if n:
                            logger.info("Backfilled source_lab for %d observations", n)
                    except Exception as e:
                        logger.debug("source_lab backfill: %s", e)

                    report = self._trigger_clean_sync(full=False)
                    if report:
                        logger.info(
                            "Clean sync done: %d obs, %d meds, %d wearables, %d errors",
                            report.observations_synced, report.medications_synced,
                            report.wearables_synced, len(report.errors),
                        )
                        # Re-refresh context with newly synced data
                        conv = self._claude_conversation
                        if conv is not None:
                            clean = self._get_clean_db()
                            if clean:
                                try:
                                    conv.refresh_data_from_clean_db(clean)
                                    logger.info(
                                        "Post-sync refresh: %d chars, wearables: %s",
                                        len(conv._health_data),
                                        "Monthly" in conv._health_data,
                                    )
                                finally:
                                    clean.close()
                except Exception as e:
                    logger.warning("Clean sync on unlock failed: %s", e)

            asyncio.get_event_loop().run_in_executor(None, _bg_clean_sync)

            # Check wearable connections and notify if disconnected
            try:
                wearable_notes = self._check_wearable_health()
                if wearable_notes:
                    await bot.send_message(chat_id=chat_id, text=wearable_notes)
            except Exception as e:
                logger.debug("Wearable health check skipped: %s", e)

        self._router.set_on_unlock(_on_unlock)
        self._router._connected_sources_cb = self._get_connected_sources_summary

    def _get_db(self) -> HealthDB:
        if self._db is None or self._db._conn is None:
            self._db = HealthDB(self._config, self._km)
            self._db.open()
            self._db.run_migrations()
        return self._db

    def _get_clean_db(self):
        """Lazy-init CleanDB. Returns None if unavailable."""
        try:
            from healthbot.data.clean_db import CleanDB

            path = self._config.clean_db_path
            if not path.exists():
                return None
            clean = CleanDB(path, phi_firewall=self._fw)
            clean.open(clean_key=self._km.get_clean_key())
            return clean
        except Exception as e:
            logger.warning("CleanDB unavailable: %s", e)
            return None

    def _get_connected_sources_summary(self) -> str:
        """One-line summary of connected data sources for unlock message."""
        from pathlib import Path

        from healthbot.bot.handlers_data import DataHandlers
        from healthbot.security.keychain import Keychain

        kc = Keychain()
        connected = []

        whoop_id = kc.retrieve("whoop_client_id")
        if whoop_id and DataHandlers._is_valid_credential(whoop_id):
            connected.append("WHOOP")

        oura_id = kc.retrieve("oura_client_id")
        if oura_id and DataHandlers._is_valid_credential(oura_id):
            connected.append("Oura Ring")

        apple_path = getattr(self._config, "apple_health_export_path", "")
        if apple_path and Path(apple_path).expanduser().exists():
            connected.append("Apple Health")

        if connected:
            return "Connected: " + ", ".join(connected)
        return ""

    def _build_integration_status(self) -> str:
        """Build integration status text for Claude conversation context."""
        from healthbot.security.keychain import Keychain

        kc = Keychain()
        user_id = (
            self._config.allowed_user_ids[0]
            if self._config.allowed_user_ids
            else 0
        )
        lines: list[str] = []

        for name, cred_key, provider, sync_cmd, auth_cmd in [
            ("WHOOP", "whoop_client_id", "whoop", "/sync", "/whoop_auth"),
            ("Oura Ring", "oura_client_id", "oura", "/oura", "/oura_auth"),
        ]:
            stored = kc.retrieve(cred_key)
            if not stored:
                lines.append(f"- {name}: Not connected ({auth_cmd} to set up)")
                continue
            if " " in stored or len(stored) < 8:
                lines.append(
                    f"- {name}: BROKEN — credentials corrupted. "
                    f"Run {auth_cmd} reset to fix."
                )
                continue
            try:
                db = self._get_db()
                rows = db.query_wearable_daily(
                    provider=provider, user_id=user_id, limit=1,
                )
                if rows:
                    date = rows[0].get("_date", rows[0].get("date", ""))
                    lines.append(
                        f"- {name}: Connected, last sync {date}"
                    )
                else:
                    lines.append(
                        f"- {name}: Connected but no data synced. "
                        f"Run {sync_cmd} to pull data."
                    )
            except Exception:
                lines.append(f"- {name}: Connected (status unknown)")

        if lines:
            return "\n".join(lines)
        return ""

    def _notify_system_improvement(
        self, bot, chat_id: int, block: dict,
    ) -> None:
        """Send Telegram push notification for a system improvement suggestion.

        Includes inline Approve/Reject buttons.
        """
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        imp_id = block.get("id", "")
        area = block.get("area", "general")
        suggestion = block.get("suggestion", "")
        priority = block.get("priority", "low")

        text = (
            f"SYSTEM IMPROVEMENT SUGGESTION\n"
            f"Area: {area} | Priority: {priority}\n\n"
            f"{suggestion}"
        )
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "Approve", callback_data=f"si:approve:{imp_id}",
                ),
                InlineKeyboardButton(
                    "Reject", callback_data=f"si:reject:{imp_id}",
                ),
            ],
        ])

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=keyboard,
                )
            )
        except RuntimeError:
            # No running event loop (sync context) — skip notification
            logger.debug("Cannot send SI notification: no event loop")

    def _check_wearable_health(self) -> str:
        """Check wearable connections and return notification if issues found."""
        from healthbot.security.keychain import Keychain

        kc = Keychain()
        notes: list[str] = []
        user_id = (
            self._config.allowed_user_ids[0]
            if self._config.allowed_user_ids
            else 0
        )

        for name, cred_key, provider, sync_cmd, auth_cmd in [
            ("WHOOP", "whoop_client_id", "whoop", "/sync", "/whoop_auth"),
            ("Oura Ring", "oura_client_id", "oura", "/oura", "/oura_auth"),
        ]:
            stored_id = kc.retrieve(cred_key)
            has_creds = bool(stored_id)
            config_name = name.lower().replace(" ", "_").replace("_ring", "")
            was_connected = self._config.was_wearable_ever_connected(config_name)

            # Detect corrupted credentials
            if stored_id and (" " in stored_id or len(stored_id) < 8):
                notes.append(
                    f"{name} credentials look corrupted. "
                    f"Run {auth_cmd} reset to fix them."
                )
            elif was_connected and not has_creds:
                notes.append(
                    f"{name} was connected but credentials are missing. "
                    f"Run {auth_cmd} to reconnect."
                )
            elif has_creds:
                try:
                    db = self._get_db()
                    rows = db.query_wearable_daily(
                        start_date=None, provider=provider,
                        user_id=user_id, limit=1,
                    )
                    if not rows:
                        notes.append(
                            f"{name} is connected but has no data yet. "
                            f"Try {sync_cmd} to pull your data."
                        )
                except Exception as e:
                    logger.debug("Wearable health check failed for %s: %s", name, e)

        if not notes:
            return ""
        return "Wearable status:\n" + "\n".join(f"  - {n}" for n in notes)

    def estimate_clean_sync(self):
        """Run a pre-sync estimate (counts records, estimates time).

        Returns SyncEstimate or None on failure.
        """
        from healthbot.data.clean_db import CleanDB
        from healthbot.data.clean_sync import CleanSyncEngine
        from healthbot.llm.anonymizer import Anonymizer

        self._km.touch()
        clean = None
        try:
            db = self._get_db()
            clean = CleanDB(self._config.clean_db_path, phi_firewall=self._fw)
            clean.open(clean_key=self._km.get_clean_key())
            user_id = (
                self._config.allowed_user_ids[0]
                if self._config.allowed_user_ids else 0
            )
            anon = Anonymizer(phi_firewall=self._fw, use_ner=False)
            engine = CleanSyncEngine(db, clean, anon, self._fw)
            return engine.estimate(user_id)
        except Exception as e:
            logger.warning("Clean sync estimate failed: %s", e)
            return None
        finally:
            if clean:
                clean.close()

    def _trigger_clean_sync(
        self, full: bool = False, rebuild: bool = False,
        on_progress=None, skip_ollama: bool = False,
        mode: str | None = None,
    ) -> object | None:
        """Run a clean sync (raw vault -> clean DB). Returns SyncReport.

        Args:
            full: If True, run full sync (reprocess everything, delete stale).
                  If False, run incremental sync (only records changed since
                  last sync). Falls back to full if no watermark exists.
            rebuild: If True, clear all clean DB tables first then full sync.
                     Use after anonymizer upgrades.
            on_progress: Optional callback ``(str) -> None`` invoked after
                each sync phase with a human-readable status line.
            skip_ollama: If True, run without Ollama Layer 3 (regex + NER only).
            mode: Sync mode — "fast", "full", or "hybrid". When "hybrid",
                Ollama layer is created but only used selectively on uncertain
                fields after the fast pass completes.
        """
        from healthbot.data.clean_db import CleanDB
        from healthbot.data.clean_sync import CleanSyncEngine
        from healthbot.llm.anonymizer import Anonymizer

        # Keep vault session alive during sync — touch before starting
        # and wrap progress callback to touch on each update.
        self._km.touch()

        def _touch_progress(msg: str) -> None:
            self._km.touch()
            if on_progress is not None:
                on_progress(msg)

        clean = None
        try:
            db = self._get_db()
            clean = CleanDB(self._config.clean_db_path, phi_firewall=self._fw)
            clean.open(clean_key=self._km.get_clean_key())
            # Wire Ollama Layer 3 for enhanced PII detection during sync
            # Hybrid mode needs Ollama available but doesn't use it on every field
            ollama_layer = None
            need_ollama = not skip_ollama or mode == "hybrid"
            if need_ollama:
                ollama = self._get_ollama_for_anonymization()
                if ollama:
                    from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer
                    ollama_layer = OllamaAnonymizationLayer(ollama)
            user_id = self._config.allowed_user_ids[0] if self._config.allowed_user_ids else 0
            anon = Anonymizer(
                phi_firewall=self._fw, use_ner=True, ollama_layer=ollama_layer,
            )
            # Propagate known names from identity profile to sync NER
            if anon.has_ner:
                try:
                    from healthbot.security.identity_profile import IdentityProfile
                    profile = IdentityProfile(db=db)
                    known = profile.compile_ner_known_names(user_id)
                    if known:
                        anon._ner.set_known_names(known)
                except Exception:
                    pass  # Identity profile may not be configured
            engine = CleanSyncEngine(
                db, clean, anon, self._fw, on_touch=self._km.touch,
                skip_ollama=skip_ollama,
                mode=mode or ("fast" if skip_ollama else "full"),
            )
            # Stash engine reference BEFORE sync starts so progress polling works
            self._last_sync_engine = engine

            if rebuild:
                report = engine.rebuild(user_id, on_progress=_touch_progress)
            elif full:
                report = engine.sync_all(user_id, on_progress=_touch_progress)
            else:
                report = engine.sync_incremental(
                    user_id, on_progress=_touch_progress,
                )

            logger.info("Clean sync: %s", report.summary())
            self._last_sync_report = report
            return report
        except Exception as e:
            logger.warning("Clean sync failed: %s", e)
            return None
        finally:
            if clean:
                clean.close()

    def _get_ollama_for_anonymization(self):
        """Get OllamaClient for PII anonymization (Layer 3).

        Auto-starts Ollama if installed but not running.
        """
        try:
            from healthbot.llm.ollama_client import OllamaClient

            ollama = OllamaClient(
                model=self._config.ollama_model,
                base_url=self._config.ollama_url,
                timeout=self._config.ollama_timeout,
            )
            if ollama.ensure_running():
                return ollama
            return None
        except Exception as e:
            logger.warning("Ollama for anonymization unavailable: %s", e)
            return None

    def _on_vault_lock(self) -> None:
        """Called BEFORE key is zeroed on any lock (explicit or passive timeout).

        Cleans up LLM state, closes DB, clears error buffer.
        Fires an async task to immediately wipe the chat and notify the user.
        """
        # Clear ingestion mode (session is over)
        self._ingestion_mode = False
        self._ingestion_count = 0
        self._router.ingestion_mode = False
        self._router._ingestion_count_cb = None
        # Clear upload mode
        self._upload_mode = False
        self._upload_count = 0
        self._router.upload_mode = False
        self._router._upload_count_cb = None
        # Wait for active queries to finish before closing DB
        import time
        deadline = time.monotonic() + 5.0  # 5-second timeout
        while self._active_queries > 0 and time.monotonic() < deadline:
            time.sleep(0.05)
        if self._active_queries > 0:
            logger.warning(
                "Vault lock: %d queries still active after timeout, closing DB anyway",
                self._active_queries,
            )
        # Close handler's DB connection (matches explicit /lock behavior)
        if self._db:
            self._db.close()
            self._db = None
        self._error_buffer.clear()
        # Save and clear Claude CLI conversation state
        if self._claude_conversation:
            self._claude_conversation.save_state()
            self._claude_conversation.clear()
            self._claude_conversation = None
        # Clear identity-specific patterns from shared PhiFirewall
        # (decrypted PII compiled into regex patterns at unlock)
        if hasattr(self, '_fw') and self._fw:
            self._fw.clear_identity_patterns()
        # Unload Ollama model from GPU (frees VRAM immediately)
        from healthbot.llm.ollama_client import OllamaClient
        OllamaClient.safe_unload_on_lock(self._config)
        self._router.on_vault_lock()
        if self._scheduler:
            self._scheduler.on_lock()

        # Immediately wipe chat and notify (async task in the running event loop)
        self._session_chat_id_for_notify = self._session_chat_id
        self._pending_wipe = True
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._wipe_and_notify())
        except RuntimeError:
            # No running loop (e.g. tests or explicit /lock from sync context)
            # _pending_wipe flag ensures wipe happens on next message as fallback
            pass

    def _check_auth(self, update: Update) -> bool:
        """Check user is allowed."""
        user_id = update.effective_user.id if update.effective_user else 0
        return not self._config.allowed_user_ids or user_id in self._config.allowed_user_ids

    def _get_claude_conversation(self):
        """Lazy-init ClaudeConversationManager."""
        if self._claude_conversation is not None:
            return self._claude_conversation
        try:
            from healthbot.llm.claude_client import ClaudeClient
            from healthbot.llm.claude_conversation import (
                ClaudeConversationManager,
            )
            from healthbot.security.keychain import Keychain

            # Load API key from Keychain (if user configured via /claude_auth)
            keychain = Keychain()
            api_key = keychain.retrieve("claude_api_key")

            claude = ClaudeClient(
                cli_path=self._config.claude_cli_path,
                timeout=self._config.claude_cli_timeout,
                api_key=api_key,
            )
            if not claude.is_available():
                return None

            self._claude_conversation = ClaudeConversationManager(
                config=self._config,
                claude_client=claude,
                phi_firewall=self._fw,
                key_manager=self._km,
            )
            self._claude_conversation.load()
            return self._claude_conversation
        except Exception as e:
            logger.error("Failed to init ClaudeConversationManager: %s", e)
            return None

    async def handle_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        """Delegate non-command messages to MessageRouter."""
        # Store bot reference for proactive wipe on lock
        self._bot = context.bot

        # Handle pending wipe from passive timeout
        if self._pending_wipe and update.effective_chat:
            self._pending_wipe = False
            await self.wipe_session_chat(context.bot)
            try:
                await update.message.delete()
            except Exception:
                pass
            await update.effective_chat.send_message(
                "Session expired. Vault locked. Chat cleared.\n"
                "Send /unlock to start a new session."
            )
            return

        await self._router.handle_message(update, context)
