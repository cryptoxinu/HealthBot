"""Sync engine: raw vault -> clean DB.

Reads encrypted data from the raw vault, anonymizes it via the 3-layer
anonymizer, and writes PII-free copies to the clean DB (Tier 2).

Runs at:
  - Vault unlock (full sync on first run, incremental after)
  - After PDF ingestion (incremental)
  - Periodically via scheduler (every 2 hours)
  - On-demand via `python -m healthbot --clean-sync`
"""
from __future__ import annotations

import hashlib
import logging
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from healthbot.data.clean_db import CleanDB, PhiDetectedError
from healthbot.data.clean_sync_workers import (
    SyncReport,
    _normalize_lab_brand,
    sync_demographics,
    sync_health_context,
    sync_hypotheses,
    sync_medications,
    sync_observations,
    sync_wearables,
)
from healthbot.data.clean_sync_workers_ext import (
    sync_appointments,
    sync_genetic_variants,
    sync_health_goals,
    sync_health_records_ext,
    sync_med_reminders,
    sync_providers,
    sync_substance_knowledge,
    sync_workouts,
)
from healthbot.llm.anonymize_pipeline import AnonymizePipeline
from healthbot.llm.anonymizer import AnonymizationError, Anonymizer, PiiSpan
from healthbot.normalize.lab_normalizer import TEST_NAME_MAP
from healthbot.security.phi_firewall import PhiFirewall

# Re-export for backward compatibility
__all__ = [
    "CleanSyncEngine", "SyncEstimate", "SyncProgress",
    "SyncReport", "_normalize_lab_brand", "_sync_lock",
]

logger = logging.getLogger("healthbot")


# Text fields anonymized per record type (counted from sync workers)
_TEXT_FIELDS_PER_TYPE: dict[str, int] = {
    "obs": 4,       # test_name, ref_text, value, canonical_name
    "meds": 3,      # name, dose, frequency
    "hyps": 1,      # title
    "ctx": 1,       # fact
    "goals": 1,     # goal_text
    "reminders": 2, # med_name, notes
    "providers": 2, # specialty, notes
    "appts": 1,     # reason
    "ext": 4,       # label, value, source, details
}

# Estimated time per anonymize call (seconds)
_MS_FAST = 0.002        # regex + NER only
_MS_CACHE_HIT = 0.0005  # cache lookup
_S_OLLAMA = 3.0          # Ollama LLM call (single field)
_S_OLLAMA_BATCH = 1.5    # Ollama LLM call (batched, amortized per field)


@dataclass
class SyncEstimate:
    """Pre-sync estimate of work and time."""

    obs_count: int = 0
    meds_count: int = 0
    hyps_count: int = 0
    ctx_count: int = 0
    wearable_count: int = 0
    goals_count: int = 0
    reminders_count: int = 0
    providers_count: int = 0
    appointments_count: int = 0
    genetics_count: int = 0
    ext_count: int = 0
    workouts_count: int = 0
    total_text_fields: int = 0
    cache_size: int = 0
    estimated_safe_skip: int = 0
    estimated_fast_sec: int = 0
    estimated_full_sec: int = 0
    estimated_rebuild_sec: int = 0
    estimated_hybrid_sec: int = 0
    hybrid_ollama_fields: int = 0


@dataclass
class SyncProgress:
    """Live progress tracker for sync operations."""

    safe_skipped: int = 0
    cache_hits: int = 0
    ollama_calls: int = 0
    total_fields: int = 0
    processed_fields: int = 0
    current_phase: str = ""
    phase_done: int = 0
    phase_total: int = 0
    start_time: float = field(default_factory=time.monotonic)
    phases_completed: list[str] = field(default_factory=list)
    hybrid_queued: int = 0
    hybrid_reviewed: int = 0

# ── Smart pre-filter: skip anonymization for text that can't contain PII ──
# PhiFirewall in every upsert_* method is the belt-and-suspenders safety net.

_NUMERIC_RE = re.compile(r'^[<>≤≥±~]?\s*-?\d[\d,]*\.?\d*\s*[%]?\s*$')
_REF_RANGE_RE = re.compile(r'^\s*\d+\.?\d*\s*[-–]\s*\d+\.?\d*')

# Known-safe medical terms from lab normalizer (350+ terms)
_KNOWN_SAFE_TERMS: frozenset[str] = frozenset(
    {v.lower() for v in TEST_NAME_MAP.values()}
    | {k.lower() for k in TEST_NAME_MAP}
)


def _is_obviously_safe(text: str) -> bool:
    """Return True if text structurally cannot contain PII.

    Matched categories:
    - Pure numeric values: "5.7", "<0.1", "1,200", "95%"
    - Reference ranges: "4.0-5.6 %", "70–100 mmol/L"
    - Short strings (≤2 chars): units like "mg", "%"
    - Known medical terms: "glucose", "hba1c", "hemoglobin"
    """
    t = text.strip()
    if len(t) <= 2:
        return True
    if _NUMERIC_RE.match(t):
        return True
    if _REF_RANGE_RE.match(t):
        return True
    if t.lower() in _KNOWN_SAFE_TERMS:
        return True
    return False


# ── Hybrid mode: uncertainty detection for selective Ollama review ──

_NER_CERTAINTY_THRESHOLD = 0.7   # NER spans below this trigger Ollama review
_LONG_TEXT_THRESHOLD = 80         # Undetected text longer than this is uncertain


def _is_uncertain(text: str, spans: list[PiiSpan]) -> bool:
    """Determine if a fast-pass result needs Ollama review.

    Returns True if:
    (a) NER detected something with low confidence
    (b) NER found an entity not confirmed by regex (unknown name/location)
    (c) Long text with zero detections (potential hiding spot for PII)
    """
    # (a) Low-confidence NER detection
    if any(s.layer == "NER" and s.confidence < _NER_CERTAINTY_THRESHOLD for s in spans):
        return True
    # (b) NER-only detections not confirmed by regex/identity patterns
    ner_pos = {(s.start, s.end) for s in spans if s.layer == "NER"}
    regex_pos = {(s.start, s.end) for s in spans if s.layer == "regex"}
    if ner_pos - regex_pos:
        return True
    # (c) Long free-text with nothing detected
    if len(text) > _LONG_TEXT_THRESHOLD and not spans:
        return True
    return False


# Module-level lock prevents concurrent sync_all() calls
_sync_lock = threading.Lock()
_sync_lock_acquired_at: float | None = None
_SYNC_LOCK_TIMEOUT = 7200  # 2 hours max


_SYNC_LOCK_FORCE_THRESHOLD = 300  # 5 minutes — minimum age before force-release
_sync_meta_lock = threading.Lock()  # protects _sync_lock_acquired_at reads/writes


def _acquire_sync_lock() -> bool:
    """Try to acquire _sync_lock, force-releasing if stale (>2h).

    Uses a separate meta-lock to atomically check the timestamp and
    force-release, preventing a race where two threads both decide the
    lock is stale and both try to release/acquire simultaneously.
    """
    global _sync_lock_acquired_at
    if _sync_lock.acquire(blocking=False):
        with _sync_meta_lock:
            _sync_lock_acquired_at = time.monotonic()
        return True
    # Check for stale lock — must hold meta-lock to read timestamp safely
    with _sync_meta_lock:
        if (
            _sync_lock_acquired_at is not None
            and time.monotonic() - _sync_lock_acquired_at > _SYNC_LOCK_TIMEOUT
            and time.monotonic() - _sync_lock_acquired_at > _SYNC_LOCK_FORCE_THRESHOLD
        ):
            logger.warning("Force-releasing stale sync lock (held >2h)")
            try:
                _sync_lock.release()
            except RuntimeError:
                pass  # Already released
            _sync_lock.acquire()
            _sync_lock_acquired_at = time.monotonic()
            return True
    return False


def _release_sync_lock() -> None:
    """Release _sync_lock and clear timestamp."""
    global _sync_lock_acquired_at
    with _sync_meta_lock:
        _sync_lock_acquired_at = None
    _sync_lock.release()

# Type alias for optional progress callback
ProgressCallback = Callable[[str], None] | None


def _progress(on_progress: ProgressCallback, msg: str) -> None:
    """Invoke progress callback if provided. Never raises."""
    if on_progress is None:
        return
    try:
        on_progress(msg)
    except Exception:
        pass


class CleanSyncEngine:
    """Sync raw vault data to the clean (anonymized) DB."""

    def __init__(
        self,
        raw_db: object,
        clean_db: CleanDB,
        anonymizer: Anonymizer,
        phi_firewall: PhiFirewall,
        on_touch: Callable[[], None] | None = None,
        skip_ollama: bool = False,
        mode: str = "full",
    ) -> None:
        self._raw = raw_db
        self._clean = clean_db
        self._anon = anonymizer
        self._fw = phi_firewall
        self._on_touch = on_touch
        self._skip_ollama = skip_ollama
        self._mode = mode
        self.progress = SyncProgress()

        # Hybrid mode: keep Ollama layer for selective pass 2
        # Fast mode: disable Ollama entirely
        if skip_ollama and mode != "hybrid" and hasattr(anonymizer, "_ollama_layer"):
            anonymizer._ollama_layer = None

        # Queue for uncertain fields during hybrid mode pass 1
        self._uncertain_queue: list[tuple[str, str, str, list]] = []

        self._pipeline = AnonymizePipeline(
            anonymizer, max_passes=2, fallback="block",
        )

    def estimate(self, user_id: int) -> SyncEstimate:
        """Count records and estimate sync time without running the sync."""
        est = SyncEstimate()

        # Count records per type from raw vault
        obs_lab = self._fetch("query_observations",
                              record_type="lab_result", user_id=user_id, limit=10000)
        obs_vital = self._fetch("query_observations",
                                record_type="vital_sign", user_id=user_id, limit=10000)
        est.obs_count = len(obs_lab or []) + len(obs_vital or [])

        meds = self._fetch("get_active_medications", user_id=user_id)
        est.meds_count = len(meds or [])

        hyps = self._fetch("get_active_hypotheses", user_id)
        est.hyps_count = len(hyps or [])

        ctx = self._fetch("get_ltm_by_user", user_id)
        est.ctx_count = len(ctx or [])

        goals = self._fetch("get_health_goals", user_id)
        est.goals_count = len(goals or [])

        reminders = self._fetch("get_med_reminders", user_id)
        est.reminders_count = len(reminders or [])

        providers = self._fetch("get_providers", user_id)
        est.providers_count = len(providers or [])

        appts = self._fetch("get_appointments", user_id)
        est.appointments_count = len(appts or [])

        try:
            wearable_rows = self._raw.query_wearable_daily(user_id=user_id, limit=10000)
            est.wearable_count = len(wearable_rows or [])
        except Exception:
            est.wearable_count = 0

        try:
            ext = self._raw.get_health_records_ext(user_id)
            est.ext_count = len(ext or [])
        except Exception:
            est.ext_count = 0

        # Total text fields that need anonymization
        est.total_text_fields = (
            est.obs_count * _TEXT_FIELDS_PER_TYPE["obs"]
            + est.meds_count * _TEXT_FIELDS_PER_TYPE["meds"]
            + est.hyps_count * _TEXT_FIELDS_PER_TYPE["hyps"]
            + est.ctx_count * _TEXT_FIELDS_PER_TYPE["ctx"]
            + est.goals_count * _TEXT_FIELDS_PER_TYPE["goals"]
            + est.reminders_count * _TEXT_FIELDS_PER_TYPE["reminders"]
            + est.providers_count * _TEXT_FIELDS_PER_TYPE["providers"]
            + est.appointments_count * _TEXT_FIELDS_PER_TYPE["appts"]
            + est.ext_count * _TEXT_FIELDS_PER_TYPE["ext"]
        )

        # Cache hit estimate
        try:
            est.cache_size = self._clean.count_rows("clean_anon_cache")
        except Exception:
            est.cache_size = 0

        # Estimate safe-skip ratio by sampling existing text
        est.estimated_safe_skip = self._estimate_safe_ratio(est.total_text_fields)

        # Time estimates
        cache_pct = min(est.cache_size / max(est.total_text_fields, 1), 1.0)
        uncached = max(0, est.total_text_fields - est.estimated_safe_skip
                       - int(est.total_text_fields * cache_pct))

        est.estimated_fast_sec = max(1, int(est.total_text_fields * _MS_FAST))
        est.estimated_full_sec = max(1, int(
            est.estimated_safe_skip * 0.0001
            + int(est.total_text_fields * cache_pct) * _MS_CACHE_HIT
            + uncached * _S_OLLAMA
        ))
        est.estimated_rebuild_sec = max(1, int(
            est.estimated_safe_skip * 0.0001
            + (est.total_text_fields - est.estimated_safe_skip) * _S_OLLAMA
        ))

        # Hybrid: fast pass on all, Ollama only on uncertain (~15%)
        uncertain_pct = 0.15
        est.hybrid_ollama_fields = int(uncached * uncertain_pct)
        est.estimated_hybrid_sec = max(1, int(
            est.estimated_safe_skip * 0.0001
            + int(est.total_text_fields * cache_pct) * _MS_CACHE_HIT
            + uncached * _MS_FAST
            + est.hybrid_ollama_fields * _S_OLLAMA_BATCH
        ))

        return est

    def _estimate_safe_ratio(self, total_fields: int) -> int:
        """Sample some texts to estimate how many are obviously safe."""
        if total_fields == 0:
            return 0
        # Use a rough ratio based on typical medical data:
        # numeric values, units, ref ranges, known lab terms ~ 60-70% of fields
        # This avoids fetching all text just for an estimate.
        return int(total_fields * 0.65)

    def _verify_anonymizer(self) -> bool:
        """Verify the PII detection pipeline is functional via canary token."""
        canary_text = "SSN: 123-45-6789 phone: 555-123-4567"
        if not self._fw.contains_phi(canary_text):
            logger.error(
                "Clean sync canary FAILED — PhiFirewall regex not detecting "
                "known PII patterns. Sync aborted to protect clean DB."
            )
            return False
        return True

    def sync_all(
        self, user_id: int, on_progress: ProgressCallback = None,
    ) -> SyncReport:
        """Full sync: read all raw data, anonymize, write to clean DB.

        Uses a module-level lock to prevent concurrent syncs.
        Non-blocking: returns immediately if already running.
        """
        if not _acquire_sync_lock():
            logger.info("Clean sync already in progress, skipping")
            return SyncReport()

        try:
            return self._sync_all_locked(user_id, on_progress=on_progress)
        finally:
            _release_sync_lock()

    def _sync_all_locked(
        self, user_id: int, on_progress: ProgressCallback = None,
    ) -> SyncReport:
        """Full sync implementation. Caller must hold _sync_lock."""
        report = SyncReport()
        self.progress = SyncProgress()
        self._uncertain_queue.clear()

        if not self._verify_anonymizer():
            report.errors.append("Anonymizer canary check failed — sync aborted")
            return report

        self._clean.begin_transaction()
        try:
            # Pre-fetch data from raw vault (None = query failed)
            obs_lab = self._fetch("query_observations",
                                  record_type="lab_result", user_id=user_id, limit=10000)
            obs_vital = self._fetch("query_observations",
                                    record_type="vital_sign", user_id=user_id, limit=10000)
            # Merge both types; None means query failed
            if obs_lab is not None and obs_vital is not None:
                obs_records = obs_lab + obs_vital
            elif obs_lab is not None:
                obs_records = obs_lab
            elif obs_vital is not None:
                obs_records = obs_vital
            else:
                obs_records = None
            med_records = self._fetch("get_active_medications", user_id=user_id)
            hyp_records = self._fetch("get_active_hypotheses", user_id)
            ctx_records = self._fetch("get_ltm_by_user", user_id)

            # Run workers — pass [] if fetch succeeded, skip if None
            self._set_phase("Observations", len(obs_records or []))
            obs_ids = (sync_observations(
                obs_records, self._anonymize_text, self._clean, report,
                on_progress=on_progress,
            ) if obs_records is not None else None)
            self.progress.phases_completed.append("Observations")
            _progress(on_progress, f"Observations: {report.observations_synced} synced")

            self._set_phase("Medications", len(med_records or []))
            med_ids = (sync_medications(
                med_records, self._anonymize_text, self._clean, report,
            ) if med_records is not None else None)
            self.progress.phases_completed.append("Medications")
            _progress(on_progress, f"Medications: {report.medications_synced} synced")

            self._set_phase("Wearables", 0)
            wearable_ids = sync_wearables(
                self._raw, self._clean, report, user_id,
            )
            self.progress.phases_completed.append("Wearables")
            _progress(on_progress, f"Wearables: {report.wearables_synced} synced")

            self._set_phase("Demographics", 0)
            sync_demographics(self._raw, self._clean, report, user_id)
            self.progress.phases_completed.append("Demographics")
            _progress(on_progress, "Demographics: done")

            self._set_phase("Hypotheses", len(hyp_records or []))
            hyp_ids = (sync_hypotheses(
                hyp_records, self._anonymize_text, self._clean, report,
            ) if hyp_records is not None else None)
            self.progress.phases_completed.append("Hypotheses")
            _progress(on_progress, f"Hypotheses: {report.hypotheses_synced} synced")

            self._set_phase("Health context", len(ctx_records or []))
            ctx_ids = (sync_health_context(
                ctx_records, self._anonymize_text, self._clean, report,
            ) if ctx_records is not None else None)
            self.progress.phases_completed.append("Health context")
            _progress(on_progress, f"Health context: {report.health_context_synced} synced")

            # Extended data types
            self._set_phase("Workouts", 0)
            workout_ids = sync_workouts(
                self._raw, self._clean, report, user_id,
            )
            self.progress.phases_completed.append("Workouts")
            _progress(on_progress, f"Workouts: {report.workouts_synced} synced")

            self._set_phase("Genetics", 0)
            genetic_ids = sync_genetic_variants(
                self._raw, self._clean, report, user_id,
            )
            self.progress.phases_completed.append("Genetics")
            _progress(on_progress, f"Genetics: {report.genetic_variants_synced} synced")

            goal_records = self._fetch("get_health_goals", user_id)
            self._set_phase("Goals", len(goal_records or []))
            goal_ids = (sync_health_goals(
                goal_records, self._anonymize_text, self._clean, report,
            ) if goal_records is not None else None)
            self.progress.phases_completed.append("Goals")
            _progress(on_progress, f"Goals: {report.health_goals_synced} synced")

            reminder_records = self._fetch("get_med_reminders", user_id)
            self._set_phase("Reminders", len(reminder_records or []))
            reminder_ids = (sync_med_reminders(
                reminder_records, self._anonymize_text, self._clean, report,
            ) if reminder_records is not None else None)
            self.progress.phases_completed.append("Reminders")
            _progress(on_progress, f"Reminders: {report.med_reminders_synced} synced")

            provider_records = self._fetch("get_providers", user_id)
            self._set_phase("Providers", len(provider_records or []))
            provider_ids = (sync_providers(
                provider_records, self._anonymize_text, self._clean, report,
            ) if provider_records is not None else None)
            self.progress.phases_completed.append("Providers")
            _progress(on_progress, f"Providers: {report.providers_synced} synced")

            appt_records = self._fetch("get_appointments", user_id)
            self._set_phase("Appointments", len(appt_records or []))
            appt_ids = (sync_appointments(
                appt_records, self._anonymize_text, self._clean, report,
            ) if appt_records is not None else None)
            self.progress.phases_completed.append("Appointments")
            _progress(on_progress, f"Appointments: {report.appointments_synced} synced")

            self._set_phase("Extended records", 0)
            ext_ids = sync_health_records_ext(
                self._raw, self._anonymize_text, self._clean, report, user_id,
            )
            self.progress.phases_completed.append("Extended records")
            _progress(on_progress, f"Extended records: {report.health_records_ext_synced} synced")

            self._set_phase("Substance knowledge", 0)
            sync_substance_knowledge(
                self._raw, self._anonymize_text, self._clean, report, user_id,
            )
            self.progress.phases_completed.append("Substance knowledge")
            _progress(
                on_progress,
                f"Substance knowledge: {getattr(report, 'substance_knowledge_synced', 0)} synced",
            )

            # Hybrid pass 2: Ollama review of uncertain fields
            # Wrapped in try/except — Ollama failure must not roll back pass 1 data
            if self._mode == "hybrid" and self._uncertain_queue:
                self._set_phase("Ollama review", len(self._uncertain_queue))
                try:
                    self._run_ollama_review(on_progress)
                except Exception as e:
                    logger.warning("Hybrid Ollama review failed (non-fatal): %s", e)
                    _progress(on_progress, "Ollama review failed, using fast-pass results")
                self.progress.phases_completed.append("Ollama review")
                _progress(
                    on_progress,
                    f"Ollama reviewed {self.progress.hybrid_reviewed} uncertain fields",
                )

            _progress(on_progress, "Cleaning stale records...")

            # Delete stale records (skip if fetch failed → ids is None)
            if obs_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_observations", "obs_id", obs_ids,
                )
            if med_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_medications", "med_id", med_ids,
                )
            if wearable_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_wearable_daily", "id", wearable_ids,
                )
            if hyp_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_hypotheses", "id", hyp_ids,
                )
            if ctx_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_health_context", "id", ctx_ids,
                )
            if workout_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_workouts", "id", workout_ids,
                )
            if genetic_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_genetic_variants", "id", genetic_ids,
                )
            if goal_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_health_goals", "id", goal_ids,
                )
            if reminder_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_med_reminders", "id", reminder_ids,
                )
            if provider_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_providers", "id", provider_ids,
                )
            if appt_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_appointments", "id", appt_ids,
                )
            if ext_ids is not None:
                report.stale_deleted += self._clean.delete_stale(
                    "clean_health_records_ext", "id", ext_ids,
                )

            self._clean.set_meta("last_sync_at", self._clean._now())
            self._clean.commit()
        except Exception:
            self._clean.rollback()
            raise

        self._check_drift(user_id, report)
        logger.info("Clean sync complete: %s", report.summary())
        return report

    def sync_incremental(
        self, user_id: int, on_progress: ProgressCallback = None,
    ) -> SyncReport:
        """Incremental sync: only records changed since last sync."""
        if not _acquire_sync_lock():
            logger.info("Clean sync already in progress, skipping")
            return SyncReport(incremental=True)

        report = SyncReport(incremental=True)
        try:
            if not self._verify_anonymizer():
                report.errors.append("Anonymizer canary check failed — sync aborted")
                return report

            since = self._clean.get_meta("last_sync_at")
            if not since:
                return self._sync_all_locked(user_id, on_progress=on_progress)

            watermark = self._clean._now()

            self._clean.begin_transaction()
            try:
                obs_lab = self._fetch("query_observations",
                                      record_type="lab_result", user_id=user_id,
                                      limit=10000, since=since)
                obs_vital = self._fetch("query_observations",
                                        record_type="vital_sign", user_id=user_id,
                                        limit=10000, since=since)
                if obs_lab is not None and obs_vital is not None:
                    obs_records = obs_lab + obs_vital
                elif obs_lab is not None:
                    obs_records = obs_lab
                elif obs_vital is not None:
                    obs_records = obs_vital
                else:
                    obs_records = None
                med_records = self._fetch("get_active_medications",
                                          user_id=user_id, since=since)
                hyp_records = self._fetch("get_active_hypotheses", user_id, since=since)
                ctx_records = self._fetch("get_ltm_by_user", user_id, since=since)

                sync_observations(
                    obs_records or [], self._anonymize_text, self._clean, report,
                    incremental=True, on_progress=on_progress,
                )
                _progress(on_progress, f"Observations: {report.observations_synced} synced")

                sync_medications(
                    med_records or [], self._anonymize_text, self._clean, report,
                    incremental=True,
                )
                _progress(on_progress, f"Medications: {report.medications_synced} synced")

                sync_wearables(
                    self._raw, self._clean, report, user_id, since=since,
                )
                _progress(on_progress, f"Wearables: {report.wearables_synced} synced")

                sync_demographics(self._raw, self._clean, report, user_id)
                _progress(on_progress, "Demographics: done")

                sync_hypotheses(
                    hyp_records or [], self._anonymize_text, self._clean, report,
                    incremental=True,
                )
                _progress(on_progress, f"Hypotheses: {report.hypotheses_synced} synced")

                sync_health_context(
                    ctx_records or [], self._anonymize_text, self._clean, report,
                    incremental=True,
                )
                _progress(on_progress, f"Health context: {report.health_context_synced} synced")

                # Extended data types
                sync_workouts(
                    self._raw, self._clean, report, user_id, since=since,
                )
                _progress(on_progress, f"Workouts: {report.workouts_synced} synced")

                # Genetics: always full sync (raw vault has no since param)
                sync_genetic_variants(
                    self._raw, self._clean, report, user_id,
                )
                _progress(on_progress, f"Genetics: {report.genetic_variants_synced} synced")

                goal_records = self._fetch("get_health_goals", user_id)
                sync_health_goals(
                    goal_records or [], self._anonymize_text, self._clean, report,
                    incremental=True,
                )
                _progress(on_progress, f"Goals: {report.health_goals_synced} synced")

                reminder_records = self._fetch("get_med_reminders", user_id)
                sync_med_reminders(
                    reminder_records or [], self._anonymize_text, self._clean, report,
                    incremental=True,
                )
                _progress(on_progress, f"Reminders: {report.med_reminders_synced} synced")

                provider_records = self._fetch("get_providers", user_id)
                sync_providers(
                    provider_records or [], self._anonymize_text, self._clean, report,
                    incremental=True,
                )
                _progress(on_progress, f"Providers: {report.providers_synced} synced")

                appt_records = self._fetch("get_appointments", user_id)
                sync_appointments(
                    appt_records or [], self._anonymize_text, self._clean, report,
                    incremental=True,
                )
                _progress(on_progress, f"Appointments: {report.appointments_synced} synced")

                sync_health_records_ext(
                    self._raw, self._anonymize_text, self._clean, report,
                    user_id, since=since,
                )
                _progress(
                    on_progress,
                    f"Extended records: {report.health_records_ext_synced} synced",
                )

                sync_substance_knowledge(
                    self._raw, self._anonymize_text, self._clean, report,
                    user_id,
                )
                _progress(
                    on_progress,
                    f"Substance knowledge: "
                    f"{getattr(report, 'substance_knowledge_synced', 0)} synced",
                )

                # Delete orphaned records that no longer exist in the source
                self._delete_incremental_orphans(user_id, report)

                self._clean.set_meta("last_sync_at", watermark)
                self._clean.commit()
            except Exception:
                self._clean.rollback()
                raise
        finally:
            _release_sync_lock()

        logger.info("Incremental sync complete: %s", report.summary())
        return report

    def _delete_incremental_orphans(
        self, user_id: int, report: SyncReport,
    ) -> None:
        """Compare IDs between source and target, delete orphans.

        Called during incremental sync to remove records from the clean DB
        that no longer exist in the raw vault.
        """
        # Observations
        obs_ids = self._fetch_source_ids("query_observations",
                                         record_type="lab_result", user_id=user_id, limit=100000)
        vital_ids = self._fetch_source_ids("query_observations",
                                           record_type="vital_sign", user_id=user_id, limit=100000)
        if obs_ids is not None and vital_ids is not None:
            all_obs_ids = obs_ids | vital_ids
        elif obs_ids is not None:
            all_obs_ids = obs_ids
        elif vital_ids is not None:
            all_obs_ids = vital_ids
        else:
            all_obs_ids = None
        if all_obs_ids is not None:
            report.stale_deleted += self._clean.delete_stale(
                "clean_observations", "id", all_obs_ids,
            )

        # Medications
        med_ids = self._fetch_source_ids("get_active_medications", user_id=user_id)
        if med_ids is not None:
            report.stale_deleted += self._clean.delete_stale(
                "clean_medications", "med_id", med_ids,
            )

        # Hypotheses
        hyp_ids = self._fetch_source_ids("get_active_hypotheses", user_id)
        if hyp_ids is not None:
            report.stale_deleted += self._clean.delete_stale(
                "clean_hypotheses", "id", hyp_ids,
            )

        if report.stale_deleted:
            logger.info(
                "Incremental sync deleted %d orphaned records",
                report.stale_deleted,
            )

    def _fetch_source_ids(
        self, method: str, *args: Any, **kwargs: Any,
    ) -> set[str] | None:
        """Fetch all record IDs from raw DB, returning None on failure."""
        try:
            records = getattr(self._raw, method)(*args, **kwargs)
            if records is None:
                return None
            ids: set[str] = set()
            for r in records:
                rid = r.get("_id") or r.get("obs_id") or r.get("id", "")
                if rid:
                    ids.add(rid)
            return ids
        except Exception as e:
            logger.debug("Failed to fetch source IDs for %s: %s", method, e)
            return None

    def rebuild(
        self, user_id: int, on_progress: ProgressCallback = None,
    ) -> SyncReport:
        """Drop all clean DB data and rebuild from scratch.

        Use after anonymizer upgrades to re-process all records
        with the improved pipeline. Uses a single transaction for the
        clear phase.
        """
        if not _acquire_sync_lock():
            logger.info("Clean sync already in progress, skipping rebuild")
            return SyncReport()
        try:
            self._clean.begin_transaction()
            try:
                for table in [
                    "clean_observations", "clean_medications",
                    "clean_wearable_daily", "clean_hypotheses",
                    "clean_health_context",
                    "clean_workouts", "clean_genetic_variants",
                    "clean_health_goals", "clean_med_reminders",
                    "clean_providers", "clean_appointments",
                    "clean_health_records_ext",
                ]:
                    self._clean.conn.execute(f'DELETE FROM "{table}"')
                # Clear anonymization cache too (rebuild = re-anonymize all)
                self._clean.conn.execute("DELETE FROM clean_anon_cache")
                self._clean.set_meta("last_sync_at", "")
                self._clean.commit()
            except Exception:
                self._clean.rollback()
                raise
            logger.info("Clean DB cleared for rebuild")
            _progress(on_progress, "Cleared clean DB, starting full rebuild...")
            return self._sync_all_locked(user_id, on_progress=on_progress)
        finally:
            _release_sync_lock()

    def _fetch(
        self, method: str, *args, report: SyncReport | None = None, **kwargs,
    ) -> list[dict] | None:
        """Fetch records from raw DB, returning None on failure."""
        try:
            return getattr(self._raw, method)(*args, **kwargs)
        except Exception as e:
            logger.warning("Raw DB fetch %s failed: %s", method, e)
            return None

    def _check_drift(self, user_id: int, report: SyncReport) -> None:
        """Compare row counts and token density between raw and clean DB."""
        try:
            raw_obs = len(
                self._raw.query_observations(
                    record_type="lab_result", user_id=user_id, limit=10000,
                )
            ) + len(
                self._raw.query_observations(
                    record_type="vital_sign", user_id=user_id, limit=10000,
                )
            )
            clean_obs = self._clean.count_rows("clean_observations")
            if raw_obs != clean_obs:
                diff = raw_obs - clean_obs
                logger.warning(
                    "Sync drift: raw=%d clean=%d observations (diff=%d, "
                    "%d PII-blocked)",
                    raw_obs, clean_obs, diff, report.pii_blocked,
                )
        except Exception as e:
            logger.debug("Drift detection failed: %s", e)

        self._check_over_redaction(user_id)

    def _check_over_redaction(self, user_id: int) -> None:
        """Sample raw vs clean records. Warn if clean text is <30% of raw."""
        try:
            raw_facts = self._raw.get_ltm_by_user(user_id)
            if not raw_facts:
                return
            clean_facts = self._clean.get_health_context()
            if not clean_facts:
                return

            sample_size = min(5, len(raw_facts), len(clean_facts))
            raw_sample = raw_facts[:sample_size]
            clean_sample = clean_facts[:sample_size]

            raw_len = sum(len(f.get("fact", "")) for f in raw_sample)
            clean_len = sum(len(f.get("fact", "")) for f in clean_sample)

            if raw_len > 0 and clean_len / raw_len < 0.3:
                logger.warning(
                    "Over-redaction drift: clean text is %.0f%% of raw "
                    "text length (sample=%d). Anonymizer may be too aggressive.",
                    clean_len / raw_len * 100, sample_size,
                )
        except Exception as e:
            logger.debug("Over-redaction check failed: %s", e)

    def _set_phase(self, phase: str, total: int) -> None:
        """Update progress tracker with current sync phase."""
        self.progress.current_phase = phase
        self.progress.phase_done = 0
        self.progress.phase_total = total

    def _anonymize_text(self, text: str) -> str:
        """Run anonymizer on text via pipeline with smart fast-paths.

        Fast-path 1: structurally safe text (numbers, medical terms) → skip
        Fast-path 2: persistent cache hit → return cached result
        Slow path: full 3-layer pipeline (NER + regex + Ollama)

        PhiFirewall in every upsert_* method is the belt-and-suspenders gate.
        """
        if not text:
            return text

        self.progress.processed_fields += 1

        # Fast path: text that structurally cannot contain PII
        if _is_obviously_safe(text):
            self.progress.safe_skipped += 1
            return text

        # Persistent cache: check if we've anonymized this exact text before
        text_hash = hashlib.sha256(text.encode()).hexdigest()
        cached = self._clean.get_anon_cache(text_hash)
        if cached is not None:
            self.progress.cache_hits += 1
            return cached

        # Hybrid mode: fast pass (NER + regex only), queue uncertain for Ollama
        if self._mode == "hybrid":
            if self._on_touch is not None:
                self._on_touch()
            cleaned, merged, raw_spans = self._anon.anonymize_fast_only(text)
            if _is_uncertain(text, raw_spans):
                self._uncertain_queue.append((text, text_hash, cleaned, merged))
                self.progress.hybrid_queued += 1
            self._clean.put_anon_cache(text_hash, cleaned)
            return cleaned

        # Full 3-layer pipeline (NER + regex + Ollama)
        # Touch vault session to prevent auto-lock during slow Ollama calls
        if self._on_touch is not None:
            self._on_touch()
        self.progress.ollama_calls += 1
        try:
            result = self._pipeline.process(text)
            if result.audit_trail:
                for event in result.audit_trail:
                    logger.debug(
                        "Redaction: layer=%s cat=%s hash=%s confidence=%.2f",
                        event.layer, event.category,
                        event.original_hash, event.confidence,
                    )
            # Cache the result for future syncs
            self._clean.put_anon_cache(text_hash, result.text)
            return result.text
        except AnonymizationError as e:
            raise PhiDetectedError(str(e)) from e

    def _run_ollama_review(
        self, on_progress: ProgressCallback = None,
    ) -> None:
        """Pass 2: run Ollama on uncertain fields, update cache."""
        if not self._uncertain_queue:
            return
        ollama_layer = getattr(self._anon, "_ollama_layer", None)
        if not ollama_layer:
            logger.warning("Hybrid pass 2 skipped: Ollama layer not available")
            return

        total = len(self._uncertain_queue)
        _progress(on_progress, f"Reviewing {total} uncertain fields with Ollama...")

        originals = [entry[0] for entry in self._uncertain_queue]
        batch_results = ollama_layer.scan_batch(originals)

        for i, (original, text_hash, _fast_cleaned, fast_spans) in enumerate(
            self._uncertain_queue,
        ):
            ollama_spans = batch_results[i] if i < len(batch_results) else []
            if ollama_spans:
                # Merge fast spans + Ollama spans on original text, re-redact
                all_spans = list(fast_spans)
                for start, end, tag in ollama_spans:
                    span_hash = hashlib.sha256(
                        original[start:end].encode(),
                    ).hexdigest()[:12]
                    all_spans.append(PiiSpan(
                        start=start, end=end, tag=tag,
                        layer="LLM", confidence=0.8, text_hash=span_hash,
                    ))
                merged = Anonymizer._merge_pii_spans(all_spans)
                result = original
                for span in reversed(merged):
                    result = (
                        result[:span.start]
                        + f"[REDACTED-{span.tag}]"
                        + result[span.end:]
                    )
                self._clean.put_anon_cache(text_hash, result)
                self.progress.ollama_calls += 1
            self.progress.hybrid_reviewed += 1
            self.progress.phase_done = self.progress.hybrid_reviewed
            if self._on_touch is not None:
                self._on_touch()
