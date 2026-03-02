"""Tests for the clean sync engine (raw vault -> clean DB)."""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from healthbot.data.clean_db import CleanDB, PhiDetectedError
from healthbot.data.clean_sync import (
    CleanSyncEngine,
    SyncReport,
    _is_obviously_safe,
    _sync_lock,
)
from healthbot.llm.anonymizer import AnonymizationError, Anonymizer
from healthbot.security.phi_firewall import PhiFirewall

# 32-byte test key for clean DB encryption (M24: _encrypt now raises
# EncryptionError instead of falling back to plaintext when no key).
_TEST_CLEAN_KEY = os.urandom(32)

# ── Fixtures ──────────────────────────────────────────────


@pytest.fixture()
def phi_firewall():
    return PhiFirewall()


@pytest.fixture()
def clean_db(tmp_path, phi_firewall):
    db = CleanDB(tmp_path / "clean.db", phi_firewall=phi_firewall)
    db.open(clean_key=_TEST_CLEAN_KEY)
    yield db
    db.close()


@pytest.fixture()
def anonymizer(phi_firewall):
    anon = Anonymizer(phi_firewall=phi_firewall, use_ner=False)
    # The canary SSN (999-88-7777) is no longer matched by the tightened
    # PhiFirewall SSN regex (which excludes 9xx area numbers).  Mark canary
    # as pre-verified so the pipeline doesn't raise AnonymizationError.
    anon._canary_verified = True
    return anon


def _make_obs_side_effect(lab_results: list | None = None):
    """Build a side_effect for query_observations that routes by record_type.

    The sync engine queries once for record_type="lab_result" and once for
    "vital_sign". This helper returns lab_results for "lab_result" and []
    for "vital_sign" (unless the caller explicitly overrides).
    """
    labs = lab_results if lab_results is not None else []

    def _side_effect(*args, **kwargs):
        rtype = kwargs.get("record_type", "lab_result")
        if rtype == "vital_sign":
            return []
        return labs

    return _side_effect


@pytest.fixture()
def raw_db():
    """Mock raw vault DB."""
    mock = MagicMock()
    mock.query_observations.side_effect = _make_obs_side_effect([])
    mock.get_active_medications.return_value = []
    mock.query_wearable_daily.return_value = []
    mock.get_user_demographics.return_value = {}
    mock.get_active_hypotheses.return_value = []
    mock.get_ltm_by_user.return_value = []
    return mock


@pytest.fixture()
def engine(raw_db, clean_db, anonymizer, phi_firewall):
    return CleanSyncEngine(raw_db, clean_db, anonymizer, phi_firewall)


# ── canonical_name anonymization ─────────────────────────


class TestCanonicalNameAnonymized:
    def test_canonical_name_passes_through_anonymizer(self, engine, raw_db, clean_db):
        """canonical_name should be anonymized before writing to clean DB."""
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {
                "_meta": {"obs_id": "obs1", "record_type": "lab_result",
                          "date_effective": "2024-01-01"},
                "test_name": "Glucose",
                "canonical_name": "Glucose",
                "value": "95",
                "unit": "mg/dL",
                "flag": "",
            },
        ])
        report = engine.sync_all(user_id=1)
        assert report.observations_synced == 1

        labs = clean_db.get_lab_results()
        assert len(labs) == 1
        assert labs[0]["canonical_name"] == "Glucose"

    def test_canonical_name_with_pii_gets_redacted(self, raw_db, clean_db, phi_firewall):
        """canonical_name containing PII patterns should be redacted."""
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {
                "_meta": {"obs_id": "obs1", "record_type": "lab_result",
                          "date_effective": "2024-01-01"},
                "test_name": "Glucose",
                "canonical_name": "Test SSN 123-45-6789",
                "value": "95",
                "unit": "mg/dL",
                "flag": "",
            },
        ])
        anon = Anonymizer(phi_firewall=phi_firewall, use_ner=False)
        anon._canary_verified = True
        eng = CleanSyncEngine(raw_db, clean_db, anon, phi_firewall)
        report = eng.sync_all(user_id=1)

        # Anonymizer redacts the SSN, record still syncs with redacted text
        assert report.observations_synced == 1
        labs = clean_db.get_lab_results()
        assert len(labs) == 1
        assert "[REDACTED-" in labs[0]["canonical_name"]
        assert "123-45-6789" not in labs[0]["canonical_name"]


# ── assert_safe gate ─────────────────────────────────────


class TestAssertSafeGate:
    def test_anonymize_text_raises_phi_detected_on_residual_pii(
        self, raw_db, clean_db, phi_firewall,
    ):
        """If PII remains after anonymization, PhiDetectedError is raised."""
        # Create an anonymizer that doesn't redact (simulates a bypass)
        broken_anon = MagicMock(unsafe=True)
        broken_anon.anonymize.return_value = ("SSN: 123-45-6789", True)
        broken_anon.assert_safe.side_effect = AnonymizationError("PII detected")

        eng = CleanSyncEngine(raw_db, clean_db, broken_anon, phi_firewall)

        raw_db.get_ltm_by_user.return_value = [
            {"_id": "ctx1", "_category": "medical", "fact": "SSN: 123-45-6789"},
        ]
        report = eng.sync_all(user_id=1)
        # assert_safe raises AnonymizationError -> PhiDetectedError -> pii_blocked
        assert report.pii_blocked == 1
        assert report.health_context_synced == 0

    def test_clean_text_passes_assert_safe(self, engine, raw_db, clean_db):
        """Clean text should pass assert_safe and be written."""
        raw_db.get_ltm_by_user.return_value = [
            {"_id": "ctx1", "_category": "medical", "fact": "Glucose trending up"},
        ]
        report = engine.sync_all(user_id=1)
        assert report.health_context_synced == 1

        ctx = clean_db.get_health_context()
        assert len(ctx) == 1
        assert ctx[0]["fact"] == "Glucose trending up"


# ── Deletion propagation ─────────────────────────────────


class TestDeletionPropagation:
    def test_stale_observations_deleted(self, engine, raw_db, clean_db):
        """Observations no longer in raw vault should be removed from clean DB."""
        # First sync: two observations
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {"_meta": {"obs_id": "obs1", "record_type": "lab_result",
                       "date_effective": "2024-01-01"},
             "test_name": "Glucose", "canonical_name": "Glucose",
             "value": "95", "unit": "mg/dL", "flag": ""},
            {"_meta": {"obs_id": "obs2", "record_type": "lab_result",
                       "date_effective": "2024-01-02"},
             "test_name": "TSH", "canonical_name": "TSH",
             "value": "2.1", "unit": "mIU/L", "flag": ""},
        ])
        report1 = engine.sync_all(user_id=1)
        assert report1.observations_synced == 2
        assert len(clean_db.get_lab_results()) == 2

        # Second sync: only obs1 remains in raw vault
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {"_meta": {"obs_id": "obs1", "record_type": "lab_result",
                       "date_effective": "2024-01-01"},
             "test_name": "Glucose", "canonical_name": "Glucose",
             "value": "95", "unit": "mg/dL", "flag": ""},
        ])
        report2 = engine.sync_all(user_id=1)
        assert report2.stale_deleted == 1
        labs = clean_db.get_lab_results()
        assert len(labs) == 1
        assert labs[0]["obs_id"] == "obs1"

    def test_stale_medications_deleted(self, engine, raw_db, clean_db):
        """Medications no longer active should be removed from clean DB."""
        raw_db.get_active_medications.return_value = [
            {"id": "med1", "name": "Metformin", "dose": "500", "unit": "mg",
             "frequency": "daily", "status": "active"},
        ]
        engine.sync_all(user_id=1)
        assert len(clean_db.get_medications()) == 1

        # Medication discontinued (no longer returned by raw vault)
        raw_db.get_active_medications.return_value = []
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted >= 1
        assert len(clean_db.get_medications()) == 0

    def test_stale_hypotheses_deleted(self, engine, raw_db, clean_db):
        """Hypotheses removed from raw vault should be deleted from clean DB."""
        raw_db.get_active_hypotheses.return_value = [
            {"_id": "hyp1", "title": "Iron deficiency", "confidence": 0.8,
             "evidence_for": [], "evidence_against": [], "missing_tests": [],
             "status": "active"},
        ]
        engine.sync_all(user_id=1)
        assert len(clean_db.get_hypotheses()) == 1

        raw_db.get_active_hypotheses.return_value = []
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted >= 1
        assert len(clean_db.get_hypotheses()) == 0

    def test_no_deletion_when_query_fails(self, engine, raw_db, clean_db):
        """If the raw vault query fails, don't delete everything."""
        # First: sync some data
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {"_meta": {"obs_id": "obs1", "record_type": "lab_result",
                       "date_effective": "2024-01-01"},
             "test_name": "Glucose", "canonical_name": "Glucose",
             "value": "95", "unit": "mg/dL", "flag": ""},
        ])
        engine.sync_all(user_id=1)
        assert len(clean_db.get_lab_results()) == 1

        # Query fails — returns None, so no deletion happens
        raw_db.query_observations.side_effect = RuntimeError("DB locked")
        report = engine.sync_all(user_id=1)
        assert report.stale_deleted == 0
        assert len(clean_db.get_lab_results()) == 1


# ── Concurrency guard ────────────────────────────────────


class TestConcurrencyGuard:
    def test_concurrent_sync_skipped(self, engine, raw_db):
        """Second sync_all should skip when first is in progress."""
        # Hold the lock to simulate an in-progress sync
        _sync_lock.acquire()
        try:
            report = engine.sync_all(user_id=1)
            # Should return empty report (skipped)
            assert report.observations_synced == 0
            assert report.medications_synced == 0
        finally:
            _sync_lock.release()

    def test_sync_proceeds_when_lock_free(self, engine, raw_db, clean_db):
        """sync_all should work normally when no other sync is running."""
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {"_meta": {"obs_id": "obs1", "record_type": "lab_result",
                       "date_effective": "2024-01-01"},
             "test_name": "A1C", "canonical_name": "HbA1c",
             "value": "5.7", "unit": "%", "flag": ""},
        ])
        report = engine.sync_all(user_id=1)
        assert report.observations_synced == 1

    def test_lock_released_after_sync(self, engine, raw_db):
        """Lock must be released even if sync completes normally."""
        engine.sync_all(user_id=1)
        # Lock should be free
        assert _sync_lock.acquire(blocking=False)
        _sync_lock.release()

    def test_lock_released_on_exception(self, raw_db, clean_db, phi_firewall):
        """Lock must be released even if sync raises an exception."""
        broken_anon = MagicMock()
        broken_anon.anonymize.side_effect = RuntimeError("boom")

        eng = CleanSyncEngine(raw_db, clean_db, broken_anon, phi_firewall)
        raw_db.get_ltm_by_user.return_value = [
            {"_id": "ctx1", "_category": "medical", "fact": "some text"},
        ]
        eng.sync_all(user_id=1)
        # Lock should be free
        assert _sync_lock.acquire(blocking=False)
        _sync_lock.release()


# ── missing_tests PII validation ─────────────────────────


class TestMissingTestsPiiValidation:
    def test_missing_tests_pii_blocked(self, clean_db):
        """missing_tests with PII should raise PhiDetectedError."""
        with pytest.raises(PhiDetectedError):
            clean_db.upsert_hypothesis(
                hyp_id="hyp1",
                title="Test hypothesis",
                confidence=0.5,
                evidence_for="[]",
                evidence_against="[]",
                missing_tests='["Test by Dr. SSN 123-45-6789"]',
            )

    def test_missing_tests_clean_passes(self, clean_db):
        """missing_tests without PII should be accepted."""
        clean_db.upsert_hypothesis(
            hyp_id="hyp1",
            title="Test hypothesis",
            confidence=0.5,
            evidence_for="[]",
            evidence_against="[]",
            missing_tests='["ferritin", "TIBC"]',
        )
        hyps = clean_db.get_hypotheses()
        assert len(hyps) == 1
        assert hyps[0]["missing_tests"] == '["ferritin", "TIBC"]'


# ── delete_stale ──────────────────────────────────────────


class TestDeleteStale:
    def test_delete_stale_removes_orphans(self, clean_db):
        """delete_stale removes records not in valid_ids."""
        clean_db.upsert_observation(obs_id="obs1", test_name="A", value="1")
        clean_db.upsert_observation(obs_id="obs2", test_name="B", value="2")
        clean_db.upsert_observation(obs_id="obs3", test_name="C", value="3")

        deleted = clean_db.delete_stale(
            "clean_observations", "obs_id", {"obs1", "obs3"},
        )
        assert deleted == 1
        labs = clean_db.get_lab_results()
        ids = {lab["obs_id"] for lab in labs}
        assert ids == {"obs1", "obs3"}

    def test_delete_stale_no_orphans(self, clean_db):
        """delete_stale returns 0 when all records are valid."""
        clean_db.upsert_observation(obs_id="obs1", test_name="A", value="1")
        deleted = clean_db.delete_stale(
            "clean_observations", "obs_id", {"obs1"},
        )
        assert deleted == 0

    def test_delete_stale_none_skips_deletion(self, clean_db):
        """delete_stale with None (query failed) should not delete anything."""
        clean_db.upsert_observation(obs_id="obs1", test_name="A", value="1")
        deleted = clean_db.delete_stale(
            "clean_observations", "obs_id", None,
        )
        assert deleted == 0
        assert len(clean_db.get_lab_results()) == 1

    def test_delete_stale_empty_set_deletes_all(self, clean_db):
        """delete_stale with empty set (no records in vault) deletes all stale."""
        clean_db.upsert_observation(obs_id="obs1", test_name="A", value="1")
        clean_db.upsert_observation(obs_id="obs2", test_name="B", value="2")
        deleted = clean_db.delete_stale(
            "clean_observations", "obs_id", set(),
        )
        assert deleted == 2
        assert len(clean_db.get_lab_results()) == 0

    def test_delete_stale_empty_table(self, clean_db):
        """delete_stale on empty table returns 0."""
        deleted = clean_db.delete_stale(
            "clean_observations", "obs_id", {"obs1"},
        )
        assert deleted == 0


# ── SyncReport ────────────────────────────────────────────


class TestSyncReport:
    def test_summary_includes_stale_deleted(self):
        report = SyncReport(stale_deleted=5)
        assert "Stale deleted: 5" in report.summary()

    def test_summary_omits_stale_when_zero(self):
        report = SyncReport()
        assert "Stale" not in report.summary()


# ── Ollama Layer 3 wiring ────────────────────────────────


class TestOllamaLayer3Wiring:
    def test_trigger_clean_sync_wires_ollama_layer(self):
        """_trigger_clean_sync should create OllamaAnonymizationLayer if available."""
        from healthbot.bot.handler_core import HandlerCore

        config = MagicMock()
        config.allowed_user_ids = [123]
        # TODO: Use tmp_path / "clean.db" instead of MagicMock() to avoid
        # creating MagicMock-named SQLite files in the project root.
        config.clean_db_path = MagicMock()
        config.ollama_model = "qwen3:14b"
        config.ollama_url = "http://localhost:11434"
        config.ollama_timeout = 120

        km = MagicMock()
        km.is_unlocked = True
        fw = PhiFirewall()

        with (
            patch("healthbot.data.clean_db.CleanDB"),
            patch("healthbot.data.clean_sync.CleanSyncEngine") as mock_engine_cls,
            patch.object(HandlerCore, "_get_ollama_for_anonymization") as mock_ollama,
            patch("healthbot.llm.anonymizer.Anonymizer") as mock_anon_cls,
        ):
            mock_ollama.return_value = MagicMock()  # Ollama is available
            mock_engine_instance = MagicMock()
            mock_engine_cls.return_value = mock_engine_instance

            core = HandlerCore.__new__(HandlerCore)
            core._config = config
            core._km = km
            core._fw = fw
            core._db = MagicMock()

            core._trigger_clean_sync()

            # Verify Anonymizer was created with an ollama_layer
            mock_anon_cls.assert_called_once()
            call_kwargs = mock_anon_cls.call_args
            assert call_kwargs.kwargs.get("ollama_layer") is not None


# ── PII retry logic ─────────────────────────────────────


class TestAnonymizeTextRetry:
    def test_passes_first_attempt(self, engine, raw_db, clean_db):
        """Clean text passes assert_safe on first try — no retry needed."""
        raw_db.get_ltm_by_user.return_value = [
            {"_id": "ctx1", "_category": "medical", "fact": "Glucose is 95"},
        ]
        report = engine.sync_all(user_id=1)
        assert report.health_context_synced == 1
        assert report.pii_blocked == 0

    def test_retries_on_first_failure(self, raw_db, clean_db, phi_firewall):
        """If first assert_safe fails but retry succeeds, record is synced."""
        mock_anon = MagicMock(unsafe=True)
        # First call: anonymize returns text with residual PII marker
        # Second call: anonymize cleans it fully
        mock_anon.anonymize.side_effect = [
            ("[REDACTED-NER-person] has glucose 95", True),
            ("has glucose 95", True),
        ]
        # First assert_safe fails, second succeeds
        mock_anon.assert_safe.side_effect = [
            AnonymizationError("NER: person detected"),
            None,  # success
        ]

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.get_ltm_by_user.return_value = [
            {"_id": "ctx1", "_category": "medical", "fact": "John has glucose 95"},
        ]
        report = eng.sync_all(user_id=1)
        assert report.health_context_synced == 1
        assert report.pii_blocked == 0
        # Verify exactly 2 anonymize calls (first pass + retry)
        assert mock_anon.anonymize.call_count == 2
        # Verify exactly 2 assert_safe calls (first check + retry check)
        assert mock_anon.assert_safe.call_count == 2
        # Verify retry input was the output of the first anonymize
        second_call_args = mock_anon.anonymize.call_args_list[1]
        assert second_call_args[0][0] == "[REDACTED-NER-person] has glucose 95"

    def test_empty_text_returns_unchanged(self, engine):
        """Empty string bypasses anonymization entirely."""
        result = engine._anonymize_text("")
        assert result == ""

    def test_none_text_returns_unchanged(self, engine):
        """None bypasses anonymization entirely."""
        result = engine._anonymize_text(None)
        assert result is None

    def test_fails_both_attempts(self, raw_db, clean_db, phi_firewall):
        """If both attempts fail, record is blocked with details."""
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("SSN: 123-45-6789", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("PII detected")

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.get_ltm_by_user.return_value = [
            {"_id": "ctx1", "_category": "medical", "fact": "SSN: 123-45-6789"},
        ]
        report = eng.sync_all(user_id=1)
        assert report.pii_blocked == 1
        assert report.health_context_synced == 0
        assert len(report.pii_blocked_details) == 1
        assert "Health fact" in report.pii_blocked_details[0]


class TestPiiBlockedDetails:
    def test_observation_detail(self, raw_db, clean_db, phi_firewall):
        """Blocked observation includes test name and date in details."""
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("Dr. Smith Glucose", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("NER: person")

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {
                "_meta": {"obs_id": "obs1", "record_type": "lab_result",
                          "date_effective": "2024-03-15"},
                "test_name": "Custom Panel by Dr Smith",
                "canonical_name": "Custom Panel by Dr Smith",
                "value": "see attached report", "unit": "mg/dL", "flag": "",
            },
        ])
        report = eng.sync_all(user_id=1)
        assert report.pii_blocked == 1
        assert len(report.pii_blocked_details) == 1
        assert "Custom Panel by Dr Smith" in report.pii_blocked_details[0]
        assert "2024-03-15" in report.pii_blocked_details[0]

    def test_medication_detail(self, raw_db, clean_db, phi_firewall):
        """Blocked medication includes med name in details."""
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("Dr. Smith's Metformin", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("NER: person")

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.get_active_medications.return_value = [
            {"id": "med1", "name": "Metformin", "dose": "500",
             "unit": "mg", "frequency": "daily", "status": "active"},
        ]
        report = eng.sync_all(user_id=1)
        assert report.pii_blocked == 1
        assert "Metformin" in report.pii_blocked_details[0]

    def test_hypothesis_detail(self, raw_db, clean_db, phi_firewall):
        """Blocked hypothesis includes truncated title in details."""
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("John's iron deficiency", True)
        mock_anon.assert_safe.side_effect = AnonymizationError("NER: person")

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)
        raw_db.get_active_hypotheses.return_value = [
            {"_id": "hyp1", "title": "Iron deficiency in patient",
             "confidence": 0.7, "evidence_for": [], "evidence_against": [],
             "missing_tests": [], "status": "active"},
        ]
        report = eng.sync_all(user_id=1)
        assert report.pii_blocked == 1
        assert "Iron deficiency" in report.pii_blocked_details[0]


class TestIsObviouslySafe:
    """Tests for the _is_obviously_safe pre-filter."""

    def test_empty_string(self):
        assert _is_obviously_safe("")

    def test_short_strings(self):
        assert _is_obviously_safe("mg")
        assert _is_obviously_safe("%")
        assert _is_obviously_safe("L")

    def test_pure_numeric(self):
        assert _is_obviously_safe("5.7")
        assert _is_obviously_safe("95")
        assert _is_obviously_safe("1,200")
        assert _is_obviously_safe("<0.1")
        assert _is_obviously_safe(">100")
        assert _is_obviously_safe("95%")

    def test_reference_ranges(self):
        assert _is_obviously_safe("4.0-5.6 %")
        assert _is_obviously_safe("70-100")
        assert _is_obviously_safe("13.0–17.5 g/dL")

    def test_known_medical_terms(self):
        assert _is_obviously_safe("glucose")
        assert _is_obviously_safe("Hemoglobin")
        assert _is_obviously_safe("hba1c")
        assert _is_obviously_safe("TSH")

    def test_unsafe_free_text(self):
        assert not _is_obviously_safe("Dr. Smith ordered this test")
        assert not _is_obviously_safe("Patient John Doe")
        assert not _is_obviously_safe("Referred by Memorial Hospital")

    def test_unsafe_mixed_content(self):
        assert not _is_obviously_safe("Iron deficiency in patient")
        assert not _is_obviously_safe("Custom Panel by Dr Smith")

    def test_medication_names_not_in_safelist(self):
        # Medication names not in lab normalizer should NOT be safe
        assert not _is_obviously_safe("Metformin 500mg daily")


class TestPersistentCache:
    """Tests for the persistent anonymization cache in _anonymize_text."""

    def test_cache_hit_skips_pipeline(self, raw_db, clean_db, phi_firewall):
        """Cached text should be returned without calling the pipeline."""
        mock_anon = MagicMock(unsafe=True)
        mock_anon.anonymize.return_value = ("cleaned text", False)
        mock_anon.assert_safe.return_value = None

        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)

        # Pre-populate cache
        import hashlib
        text = "Some unusual free text"
        text_hash = hashlib.sha256(text.encode()).hexdigest()
        clean_db.put_anon_cache(text_hash, "cached result")

        result = eng._anonymize_text(text)
        assert result == "cached result"

    def test_safe_text_skips_cache_and_pipeline(
        self, raw_db, clean_db, phi_firewall,
    ):
        """Obviously safe text should return immediately (no cache lookup)."""
        mock_anon = MagicMock()
        eng = CleanSyncEngine(raw_db, clean_db, mock_anon, phi_firewall)

        result = eng._anonymize_text("95")
        assert result == "95"
        # Pipeline never called
        mock_anon.anonymize.assert_not_called()


# ── SyncEstimate ─────────────────────────────────────────


class TestSyncEstimate:
    def test_estimate_counts_records(self, raw_db, clean_db, anonymizer, phi_firewall):
        """estimate() should count records from each raw vault type."""
        raw_db.query_observations.side_effect = _make_obs_side_effect([
            {"_meta": {"obs_id": f"obs{i}", "record_type": "lab_result",
                       "date_effective": "2024-01-01"},
             "test_name": "Glucose", "canonical_name": "Glucose",
             "value": "95", "unit": "mg/dL", "flag": ""}
            for i in range(10)
        ])
        raw_db.get_active_medications.return_value = [
            {"id": f"med{i}", "name": "Met"} for i in range(3)
        ]
        raw_db.get_active_hypotheses.return_value = [
            {"_id": "hyp1", "title": "Test"}
        ]
        raw_db.get_ltm_by_user.return_value = [
            {"_id": f"ctx{i}", "_category": "medical", "fact": "text"}
            for i in range(5)
        ]
        raw_db.get_health_goals.return_value = []
        raw_db.get_med_reminders.return_value = []
        raw_db.get_providers.return_value = []
        raw_db.get_appointments.return_value = []
        raw_db.query_wearable_daily.return_value = [
            {"id": f"w{i}"} for i in range(20)
        ]
        raw_db.get_health_records_ext.return_value = []

        eng = CleanSyncEngine(raw_db, clean_db, anonymizer, phi_firewall)
        est = eng.estimate(user_id=1)

        assert est.obs_count == 10
        assert est.meds_count == 3
        assert est.hyps_count == 1
        assert est.ctx_count == 5
        assert est.wearable_count == 20
        # Total text fields: 10*4 + 3*3 + 1*1 + 5*1 = 40+9+1+5 = 55
        assert est.total_text_fields == 55
        assert est.estimated_fast_sec >= 1
        assert est.estimated_full_sec >= 1
        assert est.estimated_rebuild_sec >= 1

    def test_estimate_returns_zero_on_empty_vault(
        self, raw_db, clean_db, anonymizer, phi_firewall,
    ):
        """estimate() with empty vault returns all zeros."""
        raw_db.get_health_goals.return_value = []
        raw_db.get_med_reminders.return_value = []
        raw_db.get_providers.return_value = []
        raw_db.get_appointments.return_value = []
        raw_db.query_wearable_daily.return_value = []
        raw_db.get_health_records_ext.return_value = []

        eng = CleanSyncEngine(raw_db, clean_db, anonymizer, phi_firewall)
        est = eng.estimate(user_id=1)

        assert est.obs_count == 0
        assert est.total_text_fields == 0


# ── SyncProgress ─────────────────────────────────────────


class TestSyncProgress:
    def test_progress_counters_increment(self, raw_db, clean_db, phi_firewall):
        """_anonymize_text should increment SyncProgress counters."""
        anon = Anonymizer(phi_firewall=phi_firewall, use_ner=False)
        eng = CleanSyncEngine(raw_db, clean_db, anon, phi_firewall)

        # Safe text → safe_skipped
        eng._anonymize_text("95")
        assert eng.progress.safe_skipped == 1
        assert eng.progress.processed_fields == 1

        # Cache hit → cache_hits
        import hashlib
        text = "Some unusual free text"
        text_hash = hashlib.sha256(text.encode()).hexdigest()
        clean_db.put_anon_cache(text_hash, "cached")
        eng._anonymize_text(text)
        assert eng.progress.cache_hits == 1
        assert eng.progress.processed_fields == 2

    def test_progress_tracks_ollama_calls(self, raw_db, clean_db, phi_firewall):
        """Pipeline calls should increment ollama_calls counter."""
        anon = Anonymizer(phi_firewall=phi_firewall, use_ner=False)
        anon._canary_verified = True
        eng = CleanSyncEngine(raw_db, clean_db, anon, phi_firewall)

        # Non-safe, non-cached text → pipeline call → ollama_calls
        eng._anonymize_text("Glucose trending up for patient")
        assert eng.progress.ollama_calls == 1
        assert eng.progress.processed_fields == 1


# ── Skip Ollama ──────────────────────────────────────────


class TestSkipOllama:
    def test_skip_ollama_disables_layer3(self, raw_db, clean_db, phi_firewall):
        """skip_ollama=True should set anonymizer._ollama_layer to None."""
        from healthbot.llm.anonymizer import Anonymizer

        anon = Anonymizer(
            phi_firewall=phi_firewall, use_ner=False,
            ollama_layer=MagicMock(),
        )
        assert anon._ollama_layer is not None

        CleanSyncEngine(
            raw_db, clean_db, anon, phi_firewall, skip_ollama=True,
        )
        assert anon._ollama_layer is None

    def test_skip_ollama_false_preserves_layer3(self, raw_db, clean_db, phi_firewall):
        """skip_ollama=False (default) should preserve the ollama_layer."""
        mock_layer = MagicMock()
        anon = Anonymizer(
            phi_firewall=phi_firewall, use_ner=False,
            ollama_layer=mock_layer,
        )
        CleanSyncEngine(
            raw_db, clean_db, anon, phi_firewall, skip_ollama=False,
        )
        assert anon._ollama_layer is mock_layer


# ── Phase tracking ───────────────────────────────────────


class TestPhaseTracking:
    def test_sync_all_populates_phases(self, engine, raw_db, clean_db):
        """sync_all should track completed phases."""
        raw_db.get_health_goals.return_value = []
        raw_db.get_med_reminders.return_value = []
        raw_db.get_providers.return_value = []
        raw_db.get_appointments.return_value = []

        engine.sync_all(user_id=1)

        assert "Observations" in engine.progress.phases_completed
        assert "Medications" in engine.progress.phases_completed
        assert "Wearables" in engine.progress.phases_completed
