"""Tests for the Self-Organizing Health Intelligence system.

Covers:
- ClinicalDocRouter: Claude CLI routing with mixed blocks
- health_records_ext: raw vault insert + query + encryption
- clean_health_records_ext: sync worker
- analysis_rule: lifecycle (upsert, query, deactivate, supersede)
- HEALTH_DATA block: parsed and stored via conversation routing
- ANALYSIS_RULE block: parsed and stored via conversation routing
- context injection: build_prompt includes both new sections
- append_health_sections: all data types now included
- clinical extraction fallback: pending_routing + user warning
"""
from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from healthbot.data.clean_db import CleanDB, PhiDetectedError
from healthbot.security.phi_firewall import PhiFirewall

# ── Fixtures ──────────────────────────────────────────


@pytest.fixture()
def phi_firewall():
    return PhiFirewall()


@pytest.fixture()
def clean_db(tmp_path, phi_firewall):
    db = CleanDB(tmp_path / "clean.db", phi_firewall=phi_firewall)
    db.open()
    yield db
    db.close()


# ── health_records_ext roundtrip (Clean DB) ──────────


class TestCleanHealthRecordsExt:
    def test_upsert_and_query(self, clean_db):
        clean_db.upsert_health_record_ext(
            record_id="ext1",
            data_type="allergy",
            label="Penicillin",
            value="severe",
            date_effective="2023-06-15",
            source="patient_reported",
            details='{"reaction": "anaphylaxis"}',
            tags="drug_allergy",
        )
        records = clean_db.get_health_records_ext()
        assert len(records) == 1
        assert records[0]["data_type"] == "allergy"
        assert records[0]["label"] == "Penicillin"
        assert records[0]["value"] == "severe"

    def test_filter_by_type(self, clean_db):
        clean_db.upsert_health_record_ext(
            record_id="ext1", data_type="allergy", label="Penicillin",
        )
        clean_db.upsert_health_record_ext(
            record_id="ext2", data_type="procedure", label="Appendectomy",
        )
        allergies = clean_db.get_health_records_ext(data_type="allergy")
        assert len(allergies) == 1
        assert allergies[0]["label"] == "Penicillin"

    def test_pii_blocked(self, clean_db):
        with pytest.raises(PhiDetectedError):
            clean_db.upsert_health_record_ext(
                record_id="ext1",
                data_type="allergy",
                label="SSN: 123-45-6789",
            )


# ── Analysis rule lifecycle ──────────────────────────


class TestAnalysisRules:
    def test_upsert_and_query(self, clean_db):
        clean_db.upsert_analysis_rule(
            name="allergy_med_check",
            scope="allergy,medication",
            rule="Cross-reference all allergies with medications.",
            priority="high",
        )
        rules = clean_db.get_active_analysis_rules()
        assert len(rules) == 1
        assert rules[0]["name"] == "allergy_med_check"
        assert rules[0]["priority"] == "high"

    def test_deactivate(self, clean_db):
        clean_db.upsert_analysis_rule(
            name="old_rule", scope="labs", rule="Check iron levels.",
        )
        assert len(clean_db.get_active_analysis_rules()) == 1
        clean_db.deactivate_analysis_rule("old_rule")
        assert len(clean_db.get_active_analysis_rules()) == 0

    def test_supersede_via_deactivate(self, clean_db):
        clean_db.upsert_analysis_rule(
            name="rule_v1", scope="labs", rule="Version 1.",
        )
        clean_db.deactivate_analysis_rule("rule_v1")
        clean_db.upsert_analysis_rule(
            name="rule_v2", scope="labs", rule="Version 2.",
        )
        active = clean_db.get_active_analysis_rules()
        assert len(active) == 1
        assert active[0]["name"] == "rule_v2"

    def test_pii_blocked(self, clean_db):
        with pytest.raises(PhiDetectedError):
            clean_db.upsert_analysis_rule(
                name="bad_rule",
                scope="labs",
                rule="Check for SSN: 123-45-6789",
            )


# ── Summary builders ─────────────────────────────────


class TestSummaryBuilders:
    def test_health_records_ext_in_summary(self, clean_db):
        clean_db.upsert_health_record_ext(
            record_id="ext1",
            data_type="allergy",
            label="Penicillin",
            value="severe",
        )
        sections = clean_db.get_health_summary_sections()
        assert "health_records_ext" in sections
        assert "Penicillin" in sections["health_records_ext"]

    def test_analysis_rules_in_summary(self, clean_db):
        clean_db.upsert_analysis_rule(
            name="test_rule",
            scope="labs",
            rule="Monitor iron levels.",
            priority="high",
        )
        sections = clean_db.get_health_summary_sections()
        assert "analysis_rules" in sections
        assert "test_rule" in sections["analysis_rules"]

    def test_markdown_includes_new_sections(self, clean_db):
        clean_db.upsert_health_record_ext(
            record_id="ext1",
            data_type="allergy",
            label="Penicillin",
        )
        markdown = clean_db.get_health_summary_markdown()
        assert "Penicillin" in markdown


# ── Block routing (HEALTH_DATA + ANALYSIS_RULE) ─────


class TestBlockRouting:
    def _make_mgr(self, tmp_path, clean_db):
        config = MagicMock()
        config.vault_home = tmp_path
        config.clean_db_path = clean_db._path
        fw = PhiFirewall()
        claude = MagicMock()
        claude.send.return_value = "OK"
        km = MagicMock()
        km.get_key.return_value = os.urandom(32)
        km.get_clean_key.return_value = None

        from healthbot.llm.claude_conversation import ClaudeConversationManager
        mgr = ClaudeConversationManager(config, claude, fw, key_manager=km)
        mgr._db = MagicMock()
        mgr._db.insert_health_record_ext = MagicMock()
        mgr._user_id = 1
        mgr._clean_db_available = True
        mgr.load()
        return mgr

    def test_health_data_block_routed(self, tmp_path, clean_db):
        mgr = self._make_mgr(tmp_path, clean_db)
        from healthbot.llm.conversation_routing import handle_health_data_block
        block = {
            "type": "allergy",
            "label": "Penicillin",
            "value": "severe",
            "date": "2023-06-15",
            "source": "patient_reported",
            "details": {"reaction": "anaphylaxis"},
            "tags": ["drug_allergy"],
        }
        handle_health_data_block(mgr, block)
        # Verify raw vault insert was called
        mgr._db.insert_health_record_ext.assert_called_once()

    def test_analysis_rule_block_routed(self, tmp_path, clean_db):
        mgr = self._make_mgr(tmp_path, clean_db)
        from healthbot.llm.conversation_routing import handle_analysis_rule_block
        block = {
            "name": "allergy_med_check",
            "scope": "allergy,medication",
            "rule": "Cross-reference allergies with meds.",
            "priority": "high",
        }
        handle_analysis_rule_block(mgr, block)
        # Verify rule was stored in clean DB
        rules = clean_db.get_active_analysis_rules()
        assert len(rules) == 1
        assert rules[0]["name"] == "allergy_med_check"

    def test_analysis_rule_supersedes(self, tmp_path, clean_db):
        mgr = self._make_mgr(tmp_path, clean_db)
        from healthbot.llm.conversation_routing import handle_analysis_rule_block
        # Create initial rule
        handle_analysis_rule_block(mgr, {
            "name": "rule_v1", "scope": "labs",
            "rule": "Old rule.",
        })
        # Supersede it
        handle_analysis_rule_block(mgr, {
            "name": "rule_v2", "scope": "labs",
            "rule": "New rule.", "supersedes": "rule_v1",
        })
        rules = clean_db.get_active_analysis_rules()
        assert len(rules) == 1
        assert rules[0]["name"] == "rule_v2"


# ── Block pattern parsing ────────────────────────────


class TestBlockPattern:
    def test_health_data_block_parsed(self, tmp_path):
        from healthbot.llm.claude_conversation import _BLOCK_PATTERN
        text = (
            'Some analysis.\n'
            'HEALTH_DATA: {"type": "allergy", "label": "Penicillin", '
            '"value": "severe"}\n'
        )
        matches = list(_BLOCK_PATTERN.finditer(text))
        assert len(matches) == 1
        assert matches[0].group(1) == "HEALTH_DATA"
        data = json.loads(matches[0].group(2))
        assert data["type"] == "allergy"

    def test_analysis_rule_block_parsed(self, tmp_path):
        from healthbot.llm.claude_conversation import _BLOCK_PATTERN
        text = (
            'ANALYSIS_RULE: {"name": "test_rule", "scope": "labs", '
            '"rule": "Monitor iron.", "priority": "high"}\n'
        )
        matches = list(_BLOCK_PATTERN.finditer(text))
        assert len(matches) == 1
        assert matches[0].group(1) == "ANALYSIS_RULE"

    def test_new_blocks_skip_flat_memory(self, tmp_path):
        """HEALTH_DATA and ANALYSIS_RULE blocks should not be added to flat memory."""
        config = MagicMock()
        config.vault_home = tmp_path
        fw = PhiFirewall()
        claude = MagicMock()
        # Return a response with both new block types
        claude.send.return_value = (
            'Here is the analysis.\n'
            'HEALTH_DATA: {"type": "allergy", "label": "Test", "value": "mild"}\n'
            'ANALYSIS_RULE: {"name": "rule1", "scope": "labs", "rule": "Check."}\n'
        )
        from healthbot.llm.claude_conversation import ClaudeConversationManager
        mgr = ClaudeConversationManager(config, claude, fw)
        mgr.load()
        # Extract insights
        _, blocks = mgr._extract_insights(claude.send.return_value)
        assert len(blocks) == 2
        # _store_insight for these types should NOT add to _memory
        initial_memory_len = len(mgr._memory)
        for block in blocks:
            mgr._store_insight(block)
        assert len(mgr._memory) == initial_memory_len


# ── Context injection ────────────────────────────────


class TestContextInjection:
    def test_append_health_sections_includes_all_types(self, clean_db):
        """Verify all 6 previously missing types are now included."""
        from healthbot.llm.conversation_context import append_health_sections

        # Populate sections
        sections = {
            "header": "# Header",
            "demographics": "## Demographics",
            "labs": "## Labs",
            "labs_summary": "## Labs Summary",
            "medications": "## Meds",
            "wearable_detail": "## Wearable",
            "wearable_summary": "## Wearable summary",
            "hypotheses": "## Hypotheses",
            "health_context": "## Context",
            "workouts": "## Workouts data",
            "genetics": "## Genetics data",
            "goals": "## Goals data",
            "med_reminders": "## Reminders data",
            "providers": "## Providers data",
            "appointments": "## Appointments data",
            "health_records_ext": "## Extended records",
            "analysis_rules": "## Analysis rules",
            "user_memory": "## User memory",
        }

        mgr = MagicMock()
        mgr._health_sections = sections

        parts: list[str] = []
        with patch(
            "healthbot.nlu.medical_classifier.classify_medical_category",
            return_value="general",
        ):
            append_health_sections(mgr, parts, "test query")

        joined = "\n".join(parts)
        assert "Workouts data" in joined
        assert "Genetics data" in joined
        assert "Goals data" in joined
        assert "Reminders data" in joined
        assert "Providers data" in joined
        assert "Appointments data" in joined
        assert "Extended records" in joined
        assert "Analysis rules" in joined


# ── Clinical doc router ──────────────────────────────


class TestClinicalDocRouter:
    @patch("healthbot.llm.anonymizer.Anonymizer")
    def test_route_mixed_blocks(self, mock_anon_cls):
        """Mock Claude response with mixed blocks routes to correct tables.

        ClinicalDocRouter now anonymizes text before sending to Claude (M25).
        We mock the Anonymizer to avoid canary token checks in tests.
        """
        from healthbot.ingest.clinical_doc_router import ClinicalDocRouter

        # Mock Anonymizer so anonymize() returns text unchanged and
        # assert_safe() does not raise
        mock_anon = MagicMock(spec=[])
        mock_anon.anonymize = MagicMock(return_value=("Sample text", False))
        mock_anon.assert_safe = MagicMock(return_value=None)
        mock_anon_cls.return_value = mock_anon

        mock_claude = MagicMock()
        mock_claude.send.return_value = (
            'OBSERVATION: {"test": "PHQ-9", "value": "14", "unit": "score", '
            '"date": "2024-01-15", "flag": "moderate"}\n'
            'MEDICATION: {"name": "Sertraline", "dose": "50mg", '
            '"status": "active", "date": "2024-01-15"}\n'
            'CONDITION: {"name": "MDD", "status": "confirmed", '
            '"evidence": "PHQ-9 score"}\n'
            'HEALTH_DATA: {"type": "allergy", "label": "Penicillin", '
            '"value": "severe"}\n'
            'ANALYSIS_RULE: {"name": "depression_monitor", "scope": "psych", '
            '"rule": "Track PHQ-9 scores over time.", "priority": "high"}\n'
        )

        mock_db = MagicMock()
        mock_db.insert_observation = MagicMock()
        mock_db.insert_medication = MagicMock()
        mock_db.insert_health_record_ext = MagicMock()

        mock_clean = MagicMock()
        mock_clean.upsert_analysis_rule = MagicMock()

        fw = PhiFirewall()

        router = ClinicalDocRouter(
            claude_client=mock_claude,
            db=mock_db,
            clean_db=mock_clean,
            phi_firewall=fw,
        )
        result = router.route_document("Sample text", user_id=1, doc_id="doc1")

        assert result.observations == 1
        assert result.medications == 1
        assert result.conditions == 1
        assert result.health_data == 1
        assert result.analysis_rules == 1
        assert result.total == 5

    def test_claude_unavailable_returns_error(self):
        from healthbot.ingest.clinical_doc_router import ClinicalDocRouter

        mock_claude = MagicMock()
        mock_claude.send.side_effect = RuntimeError("Claude CLI not available")

        router = ClinicalDocRouter(
            claude_client=mock_claude,
            db=MagicMock(),
            clean_db=None,
            phi_firewall=PhiFirewall(),
        )
        result = router.route_document("text", user_id=1, doc_id="doc1")
        assert result.routing_error
        assert result.total == 0


# ── Sync worker for health_records_ext ───────────────


class TestSyncHealthRecordsExt:
    def test_sync_worker(self, clean_db):
        from healthbot.data.clean_sync_workers import SyncReport
        from healthbot.data.clean_sync_workers_ext import sync_health_records_ext

        mock_raw = MagicMock()
        mock_raw.get_health_records_ext.return_value = [
            {
                "id": "ext1",
                "data_type": "allergy",
                "label": "Penicillin",
                "data": {
                    "label": "Penicillin",
                    "value": "severe",
                    "unit": "",
                    "date": "2023-06-15",
                    "source": "patient_reported",
                    "details": "",
                    "tags": "drug_allergy",
                },
            },
        ]

        report = SyncReport()
        ids = sync_health_records_ext(
            mock_raw,
            anonymize=lambda x: x,
            clean=clean_db,
            report=report,
            user_id=1,
        )

        assert ids == {"ext1"}
        assert report.health_records_ext_synced == 1
        records = clean_db.get_health_records_ext()
        assert len(records) == 1
        assert records[0]["label"] == "Penicillin"


# ── Clinical extraction no-claude queues ─────────────


class TestClinicalExtractionFallback:
    def test_no_claude_marks_pending_routing(self):
        """When Claude CLI is unavailable, document should be marked pending."""
        from healthbot.ingest.telegram_pdf_ingest import IngestResult

        mock_db = MagicMock()
        mock_db.update_document_routing_status = MagicMock()

        from healthbot.ingest.telegram_pdf_ingest import TelegramPdfIngest
        ingest = TelegramPdfIngest.__new__(TelegramPdfIngest)
        ingest._db = mock_db
        ingest._fw = PhiFirewall()
        ingest._on_progress = None
        ingest._config = None

        result = IngestResult()

        # Mock _try_claude_routing to return False (Claude unavailable)
        with patch.object(ingest, "_try_claude_routing", return_value=False):
            ingest._try_clinical_extraction(
                result, "blob1", "doc1", 1, "test.pdf",
                preextracted_text="This is a clinical document with enough text to process.",
            )

        # Should have been marked as pending_routing
        mock_db.update_document_routing_status.assert_called_once()
        call_args = mock_db.update_document_routing_status.call_args
        assert call_args[0][0] == "doc1"
        assert call_args[1]["status"] == "pending_routing"

        # Should have a warning
        assert len(result.warnings) == 1
        assert "queued for processing" in result.warnings[0]
