"""Core AlertScheduler class: init, properties, job registration, lifecycle.

Uses python-telegram-bot's JobQueue (APScheduler-backed).
Three triggers:
1. On vault unlock -- immediate check
2. Periodic (every 4 hours while unlocked)
3. Incoming folder poll (every 60 seconds while unlocked)
"""
from __future__ import annotations

import logging
import time

from healthbot.config import Config
from healthbot.data.db import HealthDB
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


class AlertSchedulerCore:
    """Core state and lifecycle for the AlertScheduler."""

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

    async def _tracked_send(self, bot: object, text: str) -> None:
        """Send a message and track it for session chat wipe."""
        if self.ingestion_mode or self.upload_mode:
            return  # All scheduler notifications muted during ingestion/upload
        sent = await bot.send_message(chat_id=self._chat_id, text=text)  # type: ignore[union-attr]
        if self._message_tracker and sent:
            self._message_tracker(self._chat_id, sent.message_id)

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
