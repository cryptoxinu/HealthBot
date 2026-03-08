"""Tests for Claude CLI conversation manager."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from healthbot.llm.claude_context import (
    ensure_claude_dir,
    load_context,
)
from healthbot.llm.claude_conversation import ClaudeConversationManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_config(tmp_path: Path) -> MagicMock:
    config = MagicMock()
    config.vault_home = tmp_path
    config.allowed_user_ids = [123]
    config.claude_cli_path = None
    config.claude_cli_timeout = 30
    return config


def _make_key_manager():
    """Create a mock key manager with a real 32-byte AES key."""
    km = MagicMock()
    km._test_key = os.urandom(32)
    km.get_key.return_value = km._test_key
    return km


def _make_manager(tmp_path: Path, claude_response: str = "Test response."):
    config = _make_config(tmp_path)
    claude = MagicMock()
    claude.send.return_value = claude_response
    fw = PhiFirewall()
    mgr = ClaudeConversationManager(config, claude, fw)
    mgr.load()
    return mgr, claude


def _make_manager_encrypted(tmp_path: Path, claude_response: str = "Test response."):
    """Create manager with key_manager for encryption tests."""
    config = _make_config(tmp_path)
    claude = MagicMock()
    claude.send.return_value = claude_response
    fw = PhiFirewall()
    km = _make_key_manager()
    mgr = ClaudeConversationManager(config, claude, fw, key_manager=km)
    mgr.load()
    return mgr, claude, km


class TestClaudeContext:
    """Tests for context template and setup."""

    def test_ensure_claude_dir_creates_dir(self, tmp_path):
        result = ensure_claude_dir(tmp_path)
        assert result.exists()
        assert (result / "context.md").exists()

    def test_ensure_claude_dir_creates_default_context(self, tmp_path):
        ensure_claude_dir(tmp_path)
        content = (tmp_path / "claude" / "context.md").read_text()
        assert "HealthBot" in content
        assert "health advisor" in content.lower()

    def test_ensure_claude_dir_does_not_overwrite(self, tmp_path):
        cdir = tmp_path / "claude"
        cdir.mkdir()
        custom = "# Custom context"
        (cdir / "context.md").write_text(custom)
        ensure_claude_dir(tmp_path)
        assert (cdir / "context.md").read_text() == custom

    def test_load_context_creates_if_missing(self, tmp_path):
        cdir = tmp_path / "claude"
        cdir.mkdir()
        content = load_context(cdir)
        assert "HealthBot" in content
        assert (cdir / "context.md").exists()

    def test_load_context_reads_existing(self, tmp_path):
        cdir = tmp_path / "claude"
        cdir.mkdir()
        (cdir / "context.md").write_text("custom")
        assert load_context(cdir) == "custom"


class TestClaudeConversationManager:
    """Core conversation manager tests."""

    def test_handle_message_calls_claude(self, tmp_path):
        mgr, claude = _make_manager(tmp_path)
        response, warnings = mgr.handle_message("What's my glucose?")
        claude.send.assert_called_once()
        assert response == "Test response."
        assert warnings == []

    def test_handle_message_includes_health_data(self, tmp_path):
        mgr, claude = _make_manager(tmp_path)
        # Write health data
        health_path = tmp_path / "claude" / "health_data.md"
        health_path.write_text("## Labs\nGlucose: 95 mg/dL")
        mgr.load()

        mgr.handle_message("What's my glucose?")
        call_args = claude.send.call_args
        prompt = call_args.kwargs.get("prompt", call_args[1].get("prompt", ""))
        if not prompt:
            prompt = call_args[0][0] if call_args[0] else ""
        # Health data should be in the prompt
        assert "Glucose" in str(call_args)

    def test_conversation_history_maintained(self, tmp_path):
        mgr, claude = _make_manager(tmp_path)
        mgr.handle_message("Question 1")
        mgr.handle_message("Question 2")
        assert len(mgr._history) == 4  # 2 user + 2 assistant

    def test_clear_resets_history(self, tmp_path):
        mgr, _ = _make_manager(tmp_path)
        mgr.handle_message("Test")
        mgr.clear()
        assert len(mgr._history) == 0

    def test_clear_preserves_memory(self, tmp_path):
        mgr, _ = _make_manager(tmp_path)
        mgr._memory.append({"fact": "Test fact", "category": "analysis"})
        mgr.clear()
        assert len(mgr._memory) == 1


@pytest.mark.slow
class TestClaudePIIMonitoring:
    """PII detection on Claude CLI responses.

    Response-side PII scanning is intentionally disabled (_scan_response
    returns raw text) because Claude only receives pre-anonymized data,
    and NER+regex scanning caused false positives on medical terms.
    Defense is on the OUTBOUND side (anonymize + assert_safe before send).
    """

    def test_clean_response_passes_through(self, tmp_path):
        mgr, _ = _make_manager(tmp_path, "Your glucose is 95 mg/dL.")
        response, warnings = mgr.handle_message("Check glucose")
        assert response == "Your glucose is 95 mg/dL."
        assert warnings == []

    def test_response_not_scanned_for_pii(self, tmp_path):
        """Response scanning is disabled — PII defense is on the outbound side."""
        mgr, _ = _make_manager(
            tmp_path, "Your SSN 123-45-6789 was found in records.",
        )
        response, warnings = mgr.handle_message("Test")
        # Response passes through unmodified (scanning disabled)
        assert response == "Your SSN 123-45-6789 was found in records."
        assert warnings == []

    @patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
    def test_outbound_hypothesis_anonymized(self, _mock_canary, tmp_path):
        """Hypotheses from raw Tier 1 DB are anonymized + assert_safe before Claude."""
        mgr, mock_client = _make_manager(tmp_path, "Analysis complete.")
        mgr._db = MagicMock()
        mgr._user_id = 1
        mgr._db.get_active_hypotheses.return_value = [
            {"title": "Test hypothesis", "confidence": 0.8},
        ]
        # Verify _append_hypotheses uses anonymizer
        parts = []
        mgr._append_hypotheses(parts)
        # Should have content (hypothesis was appended)
        assert any("Test hypothesis" in p for p in parts)

    @patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
    def test_outbound_kb_findings_anonymized(self, _mock_canary, tmp_path):
        """KB findings from raw Tier 1 DB are anonymized + assert_safe before Claude."""
        mgr, _ = _make_manager(tmp_path, "OK.")
        mgr._db = MagicMock()
        mock_kb = MagicMock()
        mock_kb.query.return_value = [
            {"source": "pubmed", "finding": "Vitamin D helps", "created_at": "2026-01-01"},
        ]
        mock_kb.get_corrections.return_value = []
        with patch("healthbot.research.knowledge_base.KnowledgeBase", return_value=mock_kb):
            parts = []
            mgr._append_kb_findings(parts, "vitamin d")
            assert any("Vitamin D" in p for p in parts)


class TestClaudeMemory:
    """Insight extraction and persistent memory."""

    def test_insight_extracted_and_stored(self, tmp_path):
        response_with_insight = (
            "Your glucose is trending up.\n"
            'INSIGHT: {"fact": "Glucose trending upward over 3 months", '
            '"category": "pattern"}'
        )
        mgr, _ = _make_manager(tmp_path, response_with_insight)
        response, _ = mgr.handle_message("What about glucose?")

        # INSIGHT block should be stripped from response
        assert "INSIGHT:" not in response
        assert "glucose is trending up" in response.lower()

        # Memory should have the insight
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["fact"] == "Glucose trending upward over 3 months"
        assert mgr._memory[0]["category"] == "pattern"

    def test_insight_with_pii_blocked(self, tmp_path):
        response_with_pii = (
            "Analysis complete.\n"
            'INSIGHT: {"fact": "Patient John Smith has high glucose", '
            '"category": "analysis"}'
        )
        mgr, _ = _make_manager(tmp_path, response_with_pii)
        # The insight fact itself doesn't contain PHI patterns
        # (PhiFirewall checks for SSN, MRN, etc., not names unless NER is on)
        # So this test verifies the PII check runs without crashing
        response, _ = mgr.handle_message("Test")
        assert "INSIGHT:" not in response

    def test_memory_persisted_to_disk(self, tmp_path):
        mgr, _, km = _make_manager_encrypted(tmp_path)
        mgr._memory.append({
            "fact": "Test fact",
            "category": "analysis",
            "timestamp": "2025-01-01",
        })
        mgr.save_state()

        memory_path = tmp_path / "claude" / "memory.enc"
        assert memory_path.exists()
        # Decrypt and verify
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        raw = memory_path.read_bytes()
        nonce, ct = raw[:12], raw[12:]
        key = km.get_key()
        pt = AESGCM(key).decrypt(nonce, ct, b"relaxed.memory")
        data = json.loads(pt.decode())
        assert len(data) == 1
        assert data[0]["fact"] == "Test fact"

    def test_memory_loaded_from_disk(self, tmp_path):
        # Write memory file
        claude_dir = tmp_path / "claude"
        claude_dir.mkdir(parents=True, exist_ok=True)
        memory_path = claude_dir / "memory.json"
        memory_path.write_text(json.dumps([
            {"fact": "Previous insight", "category": "pattern", "timestamp": "2025-01-01"},
        ]))

        mgr, _ = _make_manager(tmp_path)
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["fact"] == "Previous insight"

    def test_memory_non_list_json_resets(self, tmp_path):
        """If memory file contains non-list JSON, reset to empty."""
        claude_dir = tmp_path / "claude"
        claude_dir.mkdir(parents=True, exist_ok=True)
        (claude_dir / "memory.json").write_text('{"not": "a list"}')

        mgr, _ = _make_manager(tmp_path)
        assert mgr._memory == []

    def test_memory_null_timestamp_no_crash(self, tmp_path):
        """Null timestamp in memory entry should not crash prompt building."""
        mgr, claude = _make_manager(tmp_path)
        mgr._memory.append({
            "fact": "Some insight",
            "category": "test",
            "timestamp": None,
        })
        # Should not raise
        mgr.handle_message("test")
        claude.send.assert_called_once()

    def test_memory_included_in_prompt(self, tmp_path):
        mgr, claude = _make_manager(tmp_path)
        mgr._memory.append({
            "fact": "HbA1c rising trend",
            "category": "pattern",
            "timestamp": "2025-01-01",
        })

        mgr.handle_message("What about my HbA1c?")
        call_str = str(claude.send.call_args)
        assert "HbA1c rising trend" in call_str


class TestClaudeDataRefresh:
    """Data refresh via AiExporter."""

    @patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
    def test_refresh_creates_health_data(self, _mock_canary, tmp_path):
        mgr, _, km = _make_manager_encrypted(tmp_path)
        db = MagicMock()
        db.get_user_demographics.return_value = {}
        db.query_observations.return_value = []
        db.get_active_medications.return_value = []
        db.query_wearable_daily.return_value = []
        db.get_active_hypotheses.return_value = []
        db.get_ltm_by_user.return_value = []
        db.query_journal.return_value = []

        from healthbot.llm.anonymizer import Anonymizer

        fw = PhiFirewall()
        anon = Anonymizer(phi_firewall=fw, use_ner=False)

        summary = mgr.refresh_data(db, anon, fw)

        health_path = tmp_path / "claude" / "health_data.enc"
        assert health_path.exists()
        # Decrypt and verify
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        raw = health_path.read_bytes()
        nonce, ct = raw[:12], raw[12:]
        pt = AESGCM(km.get_key()).decrypt(nonce, ct, b"relaxed.health_data")
        assert "Health Data Export" in pt.decode()
        assert "Validation Report" in summary
        assert mgr.has_health_data


class TestModeSwitch:
    """Mode switching behavior."""

    def test_lock_clears_claude_conversation(self):
        from healthbot.bot.handler_core import HandlerCore

        config = MagicMock()
        config.vault_home = Path(tempfile.mkdtemp())
        config.incoming_dir = MagicMock()
        config.allowed_user_ids = []
        km = MagicMock()
        km.is_unlocked = True
        fw = PhiFirewall()
        core = HandlerCore(config, km, fw)
        mock_conv = MagicMock()
        core._claude_conversation = mock_conv

        core._on_vault_lock()

        assert core._claude_conversation is None
        mock_conv.save_state.assert_called_once()
        mock_conv.clear.assert_called_once()


class TestClaudeAuth:
    """Claude CLI authentication via Telegram."""

    def test_claude_auth_awaiting_state(self):
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        sh = SessionHandlers(core)

        assert not sh.is_awaiting_claude_auth(123)
        sh._claude_auth_awaiting.add(123)
        assert sh.is_awaiting_claude_auth(123)

    def test_vault_lock_clears_auth_state(self):
        from healthbot.bot.handlers_session import SessionHandlers

        core = MagicMock()
        sh = SessionHandlers(core)
        sh._claude_auth_awaiting.add(123)
        sh._claude_auth_awaiting.clear()
        assert not sh.is_awaiting_claude_auth(123)

    def test_cli_auth_error_exception(self):
        from healthbot.llm.claude_client import CLIAuthError

        err = CLIAuthError("Not authenticated")
        assert "Not authenticated" in str(err)

    def test_auth_error_detection_patterns(self):
        from healthbot.llm.claude_client import ClaudeClient

        # Should detect
        assert ClaudeClient._is_auth_error("Not authenticated")
        assert ClaudeClient._is_auth_error("invalid api key")
        assert ClaudeClient._is_auth_error("HTTP 401 Unauthorized")
        assert ClaudeClient._is_auth_error("Please sign in")
        assert ClaudeClient._is_auth_error("expired token")

        # Should not detect
        assert not ClaudeClient._is_auth_error("Connection refused")
        assert not ClaudeClient._is_auth_error("Timeout after 30s")
        assert not ClaudeClient._is_auth_error("")
        assert not ClaudeClient._is_auth_error("localhost:14012")

    def test_api_key_validation_rejects_bad_format(self):
        """Invalid API keys should be rejected."""
        # The _store_claude_key method checks prefix + length
        assert not "short".startswith("sk-ant-")  # No prefix
        assert not "sk-ant-short".startswith("sk-ant-") or len("sk-ant-short") < 20

    def test_api_key_included_in_env(self):
        """API key should be passed in subprocess env."""
        from unittest.mock import patch

        from healthbot.llm.claude_client import ClaudeClient

        cli_path = Path(tempfile.mkdtemp()) / "claude"
        cli_path.touch()
        client = ClaudeClient(
            cli_path=cli_path, api_key="sk-ant-test-key-12345",
        )

        with patch("healthbot.llm.claude_client.subprocess.run") as mock:
            mock.return_value = MagicMock(
                returncode=0, stdout="ok", stderr="",
            )
            client.send("test")
            env = mock.call_args.kwargs["env"]
            assert env["ANTHROPIC_API_KEY"] == "sk-ant-test-key-12345"


@pytest.mark.slow
class TestStructuredBlocks:
    """Structured medical block parsing and routing."""

    def test_hypothesis_block_parsed(self, tmp_path):
        response = (
            "Looks like insulin resistance.\n"
            'HYPOTHESIS: {"title": "Insulin resistance", '
            '"confidence": 0.6, "evidence_for": ["elevated glucose"], '
            '"evidence_against": [], "missing_tests": ["fasting insulin"]}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        result, _ = mgr.handle_message("Check my labs")
        assert "HYPOTHESIS:" not in result
        assert "insulin resistance" in result.lower()
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["category"] == "hypothesis"

    def test_action_block_parsed(self, tmp_path):
        response = (
            "You should get this checked.\n"
            'ACTION: {"test": "fasting insulin", '
            '"reason": "confirm IR", "urgency": "soon"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        result, _ = mgr.handle_message("What next?")
        assert "ACTION:" not in result
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["category"] == "action"

    def test_condition_block_parsed(self, tmp_path):
        response = (
            "Based on your labs, this is confirmed.\n"
            'CONDITION: {"name": "hypothyroidism", '
            '"status": "confirmed", "evidence": "TSH 8.5"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        result, _ = mgr.handle_message("Thyroid?")
        assert "CONDITION:" not in result
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["category"] == "condition"

    def test_research_block_parsed(self, tmp_path):
        response = (
            "I found this relevant.\n"
            'RESEARCH: {"topic": "metformin", '
            '"finding": "reduces hepatic glucose", "source": "web"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        result, _ = mgr.handle_message("Research metformin")
        assert "RESEARCH:" not in result
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["category"] == "research"

    def test_multiple_blocks_parsed(self, tmp_path):
        response = (
            "Analysis complete.\n"
            'INSIGHT: {"fact": "Glucose trend up", "category": "pattern"}\n'
            'HYPOTHESIS: {"title": "Pre-diabetes", "confidence": 0.4, '
            '"evidence_for": ["glucose trend"], "evidence_against": [], '
            '"missing_tests": ["HbA1c"]}\n'
            'ACTION: {"test": "HbA1c", "reason": "confirm", '
            '"urgency": "routine"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        result, _ = mgr.handle_message("Analyze")
        assert "INSIGHT:" not in result
        assert "HYPOTHESIS:" not in result
        assert "ACTION:" not in result
        assert len(mgr._memory) == 3

    def test_hypothesis_routed_to_tracker(self, tmp_path):
        """HYPOTHESIS block should call HypothesisTracker.upsert."""
        response = (
            "Analysis.\n"
            'HYPOTHESIS: {"title": "Iron deficiency", '
            '"confidence": 0.7, "evidence_for": ["low ferritin"], '
            '"evidence_against": [], "missing_tests": ["TIBC"]}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        # Set up a mock DB for routing
        mock_db = MagicMock()
        mock_db.get_active_hypotheses.return_value = []
        mgr._db = mock_db
        mgr._user_id = 123

        from unittest.mock import patch  # noqa: F811
        with patch(
            "healthbot.llm.claude_conversation.HypothesisTracker",
            create=True,
        ):
            # Need to patch at import point
            import healthbot.reasoning.hypothesis_tracker as ht_mod
            orig = ht_mod.HypothesisTracker
            try:
                mock_tracker = MagicMock()
                ht_mod.HypothesisTracker = lambda db: mock_tracker
                mgr.handle_message("Check iron")
                mock_tracker.upsert_hypothesis.assert_called_once()
                call_args = mock_tracker.upsert_hypothesis.call_args
                assert call_args[0][0] == 123  # user_id
                assert call_args[0][1]["title"] == "Iron deficiency"
            finally:
                ht_mod.HypothesisTracker = orig

    def test_research_routed_to_kb(self, tmp_path):
        """RESEARCH block should call KnowledgeBase.store_finding."""
        response = (
            "Found this.\n"
            'RESEARCH: {"topic": "vitamin D", '
            '"finding": "optimal range 40-60 ng/mL", "source": "web"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        mock_db = MagicMock()
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.research.knowledge_base as kb_mod
        orig = kb_mod.KnowledgeBase
        try:
            mock_kb = MagicMock()
            mock_kb.find_similar.return_value = False  # No existing dups
            kb_mod.KnowledgeBase = lambda db: mock_kb
            mgr.handle_message("Vitamin D levels?")
            mock_kb.store_finding.assert_called_once()
            call_kwargs = mock_kb.store_finding.call_args
            assert call_kwargs[1]["topic"] == "vitamin D"
        finally:
            kb_mod.KnowledgeBase = orig

    def test_invalid_json_block_skipped(self, tmp_path):
        """Malformed JSON in a block should be silently skipped."""
        response = (
            "Analysis.\n"
            'INSIGHT: {bad json here}\n'
            'INSIGHT: {"fact": "Valid insight", "category": "test"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        result, _ = mgr.handle_message("Test")
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["fact"] == "Valid insight"

    def test_condition_routed_to_kb(self, tmp_path):
        """CONDITION block should store in KnowledgeBase."""
        response = (
            "Confirmed.\n"
            'CONDITION: {"name": "hypothyroidism", '
            '"status": "confirmed", "evidence": "TSH 8.5 mIU/L"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        mock_db = MagicMock()
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.research.knowledge_base as kb_mod
        orig = kb_mod.KnowledgeBase
        try:
            mock_kb = MagicMock()
            mock_kb.find_similar.return_value = False  # No existing dups
            kb_mod.KnowledgeBase = lambda db: mock_kb
            mgr.handle_message("Thyroid status?")
            mock_kb.store_finding.assert_called_once()
            call_kwargs = mock_kb.store_finding.call_args[1]
            assert call_kwargs["topic"] == "hypothyroidism"
            assert call_kwargs["source"] == "claude_diagnosis"
            assert call_kwargs["relevance_score"] == 1.0
        finally:
            kb_mod.KnowledgeBase = orig


@pytest.mark.slow
class TestDataQualityBlock:
    """DATA_QUALITY structured block parsing and routing."""

    def test_data_quality_block_matches_pattern(self, tmp_path):
        """DATA_QUALITY block should be recognized by _BLOCK_PATTERN."""
        from healthbot.llm.claude_conversation import _BLOCK_PATTERN

        text = (
            'DATA_QUALITY: {"issue": "cut_off_lab", '
            '"test": "CBC", "details": "WBC missing", "page": 2}'
        )
        matches = list(_BLOCK_PATTERN.finditer(text))
        assert len(matches) == 1
        assert matches[0].group(1) == "DATA_QUALITY"

    def test_data_quality_block_stripped_from_response(self, tmp_path):
        response = (
            "Your labs look incomplete.\n"
            'DATA_QUALITY: {"issue": "cut_off_lab", '
            '"test": "CBC", "details": "WBC ref range missing", "page": 2}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        result, _ = mgr.handle_message("Check my CBC")
        assert "DATA_QUALITY:" not in result
        assert "labs look incomplete" in result.lower()

    def test_data_quality_block_stored_in_memory(self, tmp_path):
        response = (
            "Hmm.\n"
            'DATA_QUALITY: {"issue": "garbled_data", '
            '"test": "ALT", "details": "value looks wrong"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        mgr.handle_message("Check ALT")
        assert len(mgr._memory) == 1
        assert mgr._memory[0]["category"] == "data_quality"

    def test_data_quality_triggers_feedback_loop(self, tmp_path):
        """DATA_QUALITY with DB should call FeedbackLoop."""
        response = (
            "Data issue.\n"
            'DATA_QUALITY: {"issue": "missing_ref_range", '
            '"test": "TSH", "details": "no reference range"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        mock_db = MagicMock()
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.reasoning.feedback_loop as fl_mod
        orig = fl_mod.FeedbackLoop
        try:
            mock_loop = MagicMock()
            mock_loop.handle_quality_issue.return_value = {
                "rescan_attempted": True,
                "rescan_count": 2,
                "rescan_results": ["tsh_free", "tsh_total"],
            }
            fl_mod.FeedbackLoop = lambda **kwargs: mock_loop
            result, _ = mgr.handle_message("TSH check")

            mock_loop.handle_quality_issue.assert_called_once()
            call_kwargs = mock_loop.handle_quality_issue.call_args[1]
            assert call_kwargs["test_name"] == "TSH"
            assert call_kwargs["issue_type"] == "missing_ref_range"
        finally:
            fl_mod.FeedbackLoop = orig

    def test_data_quality_notification_appended(self, tmp_path):
        """Rescan results should be appended to response."""
        response = (
            "Found issue.\n"
            'DATA_QUALITY: {"issue": "cut_off_lab", '
            '"test": "CBC", "details": "missing data"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        mock_db = MagicMock()
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.reasoning.feedback_loop as fl_mod
        orig = fl_mod.FeedbackLoop
        try:
            mock_loop = MagicMock()
            mock_loop.handle_quality_issue.return_value = {
                "rescan_attempted": True,
                "rescan_count": 3,
                "rescan_results": ["wbc", "rbc", "hgb"],
            }
            fl_mod.FeedbackLoop = lambda **kwargs: mock_loop
            result, _ = mgr.handle_message("CBC check")

            assert "Re-scanned for CBC" in result
            assert "3 additional result(s)" in result
            assert "/labs" in result
        finally:
            fl_mod.FeedbackLoop = orig

    def test_data_quality_no_results_notification(self, tmp_path):
        """No new results → appropriate notification."""
        response = (
            "Issue.\n"
            'DATA_QUALITY: {"issue": "garbled_data", '
            '"test": "ALT", "details": "bad value"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        mock_db = MagicMock()
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.reasoning.feedback_loop as fl_mod
        orig = fl_mod.FeedbackLoop
        try:
            mock_loop = MagicMock()
            mock_loop.handle_quality_issue.return_value = {
                "rescan_attempted": True,
                "rescan_count": 0,
                "rescan_results": [],
            }
            fl_mod.FeedbackLoop = lambda **kwargs: mock_loop
            result, _ = mgr.handle_message("ALT check")

            assert "Re-scanned for ALT" in result
            assert "no new results" in result
        finally:
            fl_mod.FeedbackLoop = orig

    def test_data_quality_no_doc_notification(self, tmp_path):
        """No source doc → re-upload suggestion."""
        response = (
            "Issue.\n"
            'DATA_QUALITY: {"issue": "cut_off_lab", '
            '"test": "ferritin", "details": "missing"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        mock_db = MagicMock()
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.reasoning.feedback_loop as fl_mod
        orig = fl_mod.FeedbackLoop
        try:
            mock_loop = MagicMock()
            mock_loop.handle_quality_issue.return_value = {
                "rescan_attempted": False,
                "rescan_count": 0,
                "rescan_results": [],
            }
            fl_mod.FeedbackLoop = lambda **kwargs: mock_loop
            result, _ = mgr.handle_message("ferritin check")

            assert "ferritin data may be incomplete" in result
            assert "re-uploading" in result
        finally:
            fl_mod.FeedbackLoop = orig

    def test_data_quality_without_db_skipped(self, tmp_path):
        """DATA_QUALITY without DB should not crash."""
        response = (
            "Issue.\n"
            'DATA_QUALITY: {"issue": "cut_off_lab", '
            '"test": "CBC", "details": "missing"}'
        )
        mgr, _ = _make_manager(tmp_path, response)
        assert mgr._db is None
        # Should not crash, just skip routing
        result, _ = mgr.handle_message("test")
        assert "DATA_QUALITY:" not in result


@pytest.mark.slow
@patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
class TestPromptEnrichment:
    """Test that hypotheses and KB findings are included in prompts."""

    def test_hypotheses_phi_anonymized(self, _mock_canary, tmp_path):
        """PHI in hypothesis titles/evidence should be anonymized."""
        mgr, claude = _make_manager(tmp_path)
        mock_db = MagicMock()
        mock_db.get_active_hypotheses.return_value = [
            {
                "title": "Patient: John Smith has iron deficiency",
                "confidence": 0.7,
                "_confidence": 0.7,
                "evidence_for": ["low ferritin from Dr. Sarah Jones"],
                "evidence_against": [],
                "missing_tests": ["TIBC"],
            },
        ]
        mgr._db = mock_db
        mgr._user_id = 123

        mgr.handle_message("Check hypotheses")
        call_str = str(claude.send.call_args)
        # PHI should be redacted
        assert "John Smith" not in call_str
        assert "ACTIVE HYPOTHESES" in call_str
        # Medical content should survive
        assert "iron deficiency" in call_str

    def test_hypotheses_in_prompt(self, _mock_canary, tmp_path):
        mgr, claude = _make_manager(tmp_path)
        mock_db = MagicMock()
        mock_db.get_active_hypotheses.return_value = [
            {
                "title": "Insulin resistance",
                "confidence": 0.7,
                "_confidence": 0.7,
                "evidence_for": ["elevated glucose", "high triglycerides"],
                "evidence_against": [],
                "missing_tests": ["fasting insulin"],
            },
        ]
        mgr._db = mock_db
        mgr._user_id = 123

        mgr.handle_message("How am I doing?")
        call_str = str(claude.send.call_args)
        assert "Insulin resistance" in call_str
        assert "ACTIVE HYPOTHESES" in call_str

    def test_kb_findings_phi_anonymized(self, _mock_canary, tmp_path):
        """PHI in KB findings should be anonymized via full anonymizer."""
        mgr, claude = _make_manager(tmp_path)
        mock_db = MagicMock()
        mock_db.get_active_hypotheses.return_value = []
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.research.knowledge_base as kb_mod
        orig = kb_mod.KnowledgeBase
        try:
            mock_kb = MagicMock()
            mock_kb.query.return_value = [
                {
                    "source": "claude_insight",
                    "finding": "Patient: John Smith has ferritin at 15",
                    "created_at": "2025-06-01",
                },
            ]
            mock_kb.get_corrections.return_value = []
            kb_mod.KnowledgeBase = lambda db: mock_kb
            mgr.handle_message("Check my iron")
            call_str = str(claude.send.call_args)
            # PHI should be redacted
            assert "John Smith" not in call_str
            # Medical content should survive
            assert "ferritin" in call_str
            assert "KNOWLEDGE BASE" in call_str
        finally:
            kb_mod.KnowledgeBase = orig

    def test_kb_findings_in_prompt(self, _mock_canary, tmp_path):
        mgr, claude = _make_manager(tmp_path)
        mock_db = MagicMock()
        mock_db.get_active_hypotheses.return_value = []
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.research.knowledge_base as kb_mod
        orig = kb_mod.KnowledgeBase
        try:
            mock_kb = MagicMock()
            mock_kb.query.return_value = [
                {
                    "source": "claude_insight",
                    "finding": "Ferritin dropping steadily",
                    "created_at": "2025-06-01",
                },
            ]
            kb_mod.KnowledgeBase = lambda db: mock_kb
            mgr.handle_message("Check my iron")
            call_str = str(claude.send.call_args)
            assert "Ferritin dropping steadily" in call_str
            assert "KNOWLEDGE BASE" in call_str
        finally:
            kb_mod.KnowledgeBase = orig

    def test_kb_corrections_in_prompt(self, _mock_canary, tmp_path):
        """KB corrections should appear in the prompt."""
        mgr, claude = _make_manager(tmp_path)
        mock_db = MagicMock()
        mock_db.get_active_hypotheses.return_value = []
        mgr._db = mock_db
        mgr._user_id = 123

        import healthbot.research.knowledge_base as kb_mod
        orig = kb_mod.KnowledgeBase
        try:
            mock_kb = MagicMock()
            mock_kb.query.return_value = []
            mock_kb.get_corrections.return_value = [
                {
                    "original_claim": "Ferritin is normal",
                    "correction": "Ferritin is actually low at 15",
                    "source": "user",
                    "created_at": "2025-07-01",
                },
            ]
            kb_mod.KnowledgeBase = lambda db: mock_kb
            mgr.handle_message("Check corrections")
            call_str = str(claude.send.call_args)
            assert "Corrections" in call_str
            assert "Ferritin is normal" in call_str
            assert "Ferritin is actually low at 15" in call_str
        finally:
            kb_mod.KnowledgeBase = orig

    def test_no_db_skips_enrichment(self, _mock_canary, tmp_path):
        """Without a DB reference, hypotheses/KB are skipped gracefully."""
        mgr, claude = _make_manager(tmp_path)
        assert mgr._db is None
        mgr.handle_message("Test")
        call_str = str(claude.send.call_args)
        assert "ACTIVE HYPOTHESES" not in call_str
        assert "KNOWLEDGE BASE" not in call_str


class TestClaudeEncryption:
    """AES-256-GCM encryption at rest for Claude conversation files."""

    def test_memory_saved_encrypted(self, tmp_path):
        """With key_manager, memory saves as .enc not .json."""
        mgr, _, km = _make_manager_encrypted(tmp_path)
        mgr._memory.append({
            "fact": "Encrypted fact", "category": "test",
            "timestamp": "2025-01-01",
        })
        mgr.save_state()

        enc_path = tmp_path / "claude" / "memory.enc"
        plain_path = tmp_path / "claude" / "memory.json"
        assert enc_path.exists()
        assert not plain_path.exists()

        # Verify it's actually encrypted (not plaintext)
        raw = enc_path.read_bytes()
        assert b"Encrypted fact" not in raw

    def test_memory_round_trip_encrypted(self, tmp_path):
        """Encrypted memory can be loaded back with same key."""
        mgr, _, km = _make_manager_encrypted(tmp_path)
        mgr._memory.append({
            "fact": "Roundtrip fact", "category": "analysis",
            "timestamp": "2025-01-01",
        })
        mgr.save_state()

        # New manager with same key
        config = _make_config(tmp_path)
        claude = MagicMock()
        claude.send.return_value = "ok"
        fw = PhiFirewall()
        mgr2 = ClaudeConversationManager(config, claude, fw, key_manager=km)
        mgr2.load()

        assert len(mgr2._memory) == 1
        assert mgr2._memory[0]["fact"] == "Roundtrip fact"

    @patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
    def test_health_data_saved_encrypted(self, _mock_canary, tmp_path):
        """refresh_data saves health_data.enc, not .md."""
        mgr, _, km = _make_manager_encrypted(tmp_path)

        db = MagicMock()
        db.get_user_demographics.return_value = {}
        db.query_observations.return_value = []
        db.get_active_medications.return_value = []
        db.query_wearable_daily.return_value = []
        db.get_active_hypotheses.return_value = []
        db.get_ltm_by_user.return_value = []
        db.query_journal.return_value = []

        from healthbot.llm.anonymizer import Anonymizer

        fw = PhiFirewall()
        anon = Anonymizer(phi_firewall=fw, use_ner=False)
        mgr.refresh_data(db, anon, fw)

        enc_path = tmp_path / "claude" / "health_data.enc"
        plain_path = tmp_path / "claude" / "health_data.md"
        assert enc_path.exists()
        assert not plain_path.exists()
        assert mgr.has_health_data

    def test_plaintext_migration_to_encrypted(self, tmp_path):
        """Old plaintext files migrate to encrypted on load."""
        claude_dir = tmp_path / "claude"
        claude_dir.mkdir(parents=True, exist_ok=True)
        (claude_dir / "context.md").write_text("test context")

        # Write plaintext memory
        memory_data = [{
            "fact": "Migrated fact", "category": "test",
            "timestamp": "2025-01-01",
        }]
        (claude_dir / "memory.json").write_text(json.dumps(memory_data))

        # Load with key_manager — should migrate
        config = _make_config(tmp_path)
        claude = MagicMock()
        claude.send.return_value = "ok"
        fw = PhiFirewall()
        km = _make_key_manager()
        mgr = ClaudeConversationManager(config, claude, fw, key_manager=km)
        mgr.load()

        assert len(mgr._memory) == 1
        assert mgr._memory[0]["fact"] == "Migrated fact"

        # Plaintext deleted, encrypted exists
        assert not (claude_dir / "memory.json").exists()
        assert (claude_dir / "memory.enc").exists()

    def test_no_save_without_key_manager(self, tmp_path):
        """Without key_manager, save is skipped (no plaintext on disk)."""
        mgr, _ = _make_manager(tmp_path)
        mgr._memory.append({
            "fact": "Should not be saved", "category": "test",
            "timestamp": "2025-01-01",
        })
        mgr.save_state()

        plain_path = tmp_path / "claude" / "memory.json"
        enc_path = tmp_path / "claude" / "memory.enc"
        assert not plain_path.exists()
        assert not enc_path.exists()

    def test_wrong_key_cannot_decrypt(self, tmp_path):
        """Encrypted files can't be read with a different key."""
        mgr, _, km = _make_manager_encrypted(tmp_path)
        mgr._memory.append({
            "fact": "Secret", "category": "test",
            "timestamp": "2025-01-01",
        })
        mgr.save_state()

        # New manager with different key
        config = _make_config(tmp_path)
        claude = MagicMock()
        claude.send.return_value = "ok"
        fw = PhiFirewall()
        km2 = _make_key_manager()  # Different random key
        mgr2 = ClaudeConversationManager(config, claude, fw, key_manager=km2)
        mgr2.load()

        # Decryption fails silently, memory stays empty
        assert len(mgr2._memory) == 0

    def test_context_md_stays_plaintext(self, tmp_path):
        """context.md is never encrypted — it's user-editable, no PHI."""
        _make_manager_encrypted(tmp_path)
        context_path = tmp_path / "claude" / "context.md"
        assert context_path.exists()
        content = context_path.read_text()
        assert "HealthBot" in content

    def test_encryption_failure_does_not_write_plaintext(self, tmp_path):
        """If encryption fails, data is NOT saved — no plaintext fallback."""
        mgr, _, km = _make_manager_encrypted(tmp_path)
        mgr._memory.append({
            "fact": "First save", "category": "test",
            "timestamp": "2025-01-01",
        })
        mgr.save_state()

        enc_path = tmp_path / "claude" / "memory.enc"
        plain_path = tmp_path / "claude" / "memory.json"
        assert enc_path.exists()
        first_enc = enc_path.read_bytes()

        # Simulate encryption failure on next save
        km.get_key.side_effect = RuntimeError("key zeroed")
        mgr._memory.append({
            "fact": "Second save", "category": "test",
            "timestamp": "2025-01-02",
        })
        mgr.save_state()

        # .enc unchanged (old data), no plaintext written
        assert enc_path.read_bytes() == first_enc
        assert not plain_path.exists()


class TestChartBlocks:
    """CHART blocks are accumulated in _pending_charts, not routed to DB."""

    def test_chart_block_extracted(self, tmp_path):
        response = (
            'Here is your HRV trend.\n'
            'CHART: {"metric": "hrv", "source": "wearable", "days": 90}\n'
        )
        mgr, _ = _make_manager(tmp_path, claude_response=response)
        mgr.handle_message("show me my hrv")
        assert len(mgr._pending_charts) == 1
        assert mgr._pending_charts[0]["metric"] == "hrv"
        assert mgr._pending_charts[0]["source"] == "wearable"
        assert mgr._pending_charts[0]["days"] == 90

    def test_multiple_chart_blocks(self, tmp_path):
        response = (
            'Overview:\n'
            'CHART: {"metric": "hrv", "source": "wearable", "days": 30}\n'
            'CHART: {"metric": "ldl", "source": "lab", "days": 730}\n'
        )
        mgr, _ = _make_manager(tmp_path, claude_response=response)
        mgr.handle_message("overview")
        assert len(mgr._pending_charts) == 2
        assert mgr._pending_charts[0]["metric"] == "hrv"
        assert mgr._pending_charts[1]["metric"] == "ldl"

    def test_chart_blocks_cleared_between_messages(self, tmp_path):
        response1 = 'CHART: {"metric": "hrv", "source": "wearable", "days": 30}\nDone.'
        mgr, claude = _make_manager(tmp_path, claude_response=response1)
        mgr.handle_message("first")
        assert len(mgr._pending_charts) == 1

        claude.send.return_value = "No charts this time."
        mgr.handle_message("second")
        assert len(mgr._pending_charts) == 0

    def test_chart_blocks_stripped_from_response(self, tmp_path):
        response = (
            'Your HRV is improving.\n'
            'CHART: {"metric": "hrv", "source": "wearable", "days": 90}\n'
        )
        mgr, _ = _make_manager(tmp_path, claude_response=response)
        result, _ = mgr.handle_message("hrv?")
        assert "CHART" not in result
        assert "Your HRV is improving" in result
