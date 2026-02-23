"""Tests for background analysis engine, evidence bridge, and source citations."""
from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, TriageLevel
from healthbot.llm.background_analysis import BackgroundAnalysisEngine
from healthbot.security.key_manager import KeyManager

PASSPHRASE = "test-bg-analysis-passphrase"


@pytest.fixture
def db_setup(tmp_path: Path):
    """Create a real encrypted DB for watermark / prompt builder tests."""
    vault_home = tmp_path / "vault"
    vault_home.mkdir()
    config = Config(vault_home=vault_home)
    config.ensure_dirs()

    km = KeyManager(config)
    km.setup(PASSPHRASE)

    db = HealthDB(config, km)
    db.open()
    db.run_migrations()

    yield db, config, km


# -- Watermark tests --


class TestWatermarks:
    """Watermark read/write against vault_meta."""

    def test_get_missing_watermark(self, db_setup):
        db, config, _ = db_setup
        engine = BackgroundAnalysisEngine(db, config)
        assert engine.get_watermark("nonexistent") == ""

    def test_set_and_get_watermark(self, db_setup):
        db, config, _ = db_setup
        engine = BackgroundAnalysisEngine(db, config)
        engine.set_watermark("bg_last_lab_count", "42")
        assert engine.get_watermark("bg_last_lab_count") == "42"

    def test_watermark_overwrite(self, db_setup):
        db, config, _ = db_setup
        engine = BackgroundAnalysisEngine(db, config)
        engine.set_watermark("bg_last_lab_count", "10")
        engine.set_watermark("bg_last_lab_count", "20")
        assert engine.get_watermark("bg_last_lab_count") == "20"


# -- Health synthesis prompt tests --


class TestHealthSynthesisPrompt:
    """build_health_synthesis_prompt() tests."""

    def test_returns_none_when_no_new_data(self, db_setup):
        db, config, _ = db_setup
        engine = BackgroundAnalysisEngine(db, config)
        # First call with no data → sets watermarks to 0
        result = engine.build_health_synthesis_prompt(user_id=123)
        # No labs, no wearable → should be None
        assert result is None

    def test_returns_prompt_when_force(self, db_setup):
        db, config, _ = db_setup
        engine = BackgroundAnalysisEngine(db, config)
        result = engine.build_health_synthesis_prompt(user_id=123, force=True)
        assert result is not None
        assert "Background health review" in result
        assert "TASKS:" in result

    def test_returns_prompt_when_new_labs(self, db_setup):
        db, config, km = db_setup
        # Insert a lab result
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=4.2,
            unit="mIU/L",
            reference_low=0.4,
            reference_high=4.0,
            date_collected=date(2025, 12, 1),
            triage_level=TriageLevel.NORMAL,
        )
        db.insert_observation(lab, user_id=123)

        engine = BackgroundAnalysisEngine(db, config)
        result = engine.build_health_synthesis_prompt(user_id=123)
        assert result is not None
        assert "1 new lab result" in result

    def test_no_prompt_on_second_call_after_commit(self, db_setup):
        db, config, km = db_setup
        # Insert lab
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=4.2,
            unit="mIU/L",
            date_collected=date(2025, 12, 1),
            triage_level=TriageLevel.NORMAL,
        )
        db.insert_observation(lab, user_id=123)

        engine = BackgroundAnalysisEngine(db, config)
        # First call → returns prompt
        result1 = engine.build_health_synthesis_prompt(user_id=123)
        assert result1 is not None
        # Simulate successful response → commit watermarks
        engine.commit_health_watermarks()
        # Second call → no new data
        result2 = engine.build_health_synthesis_prompt(user_id=123)
        assert result2 is None

    def test_prompt_returned_again_without_commit(self, db_setup):
        """Without committing, watermarks are not advanced."""
        db, config, km = db_setup
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=4.2,
            unit="mIU/L",
            date_collected=date(2025, 12, 1),
            triage_level=TriageLevel.NORMAL,
        )
        db.insert_observation(lab, user_id=123)

        engine = BackgroundAnalysisEngine(db, config)
        result1 = engine.build_health_synthesis_prompt(user_id=123)
        assert result1 is not None
        # Do NOT commit — simulating failed response
        result2 = engine.build_health_synthesis_prompt(user_id=123)
        assert result2 is not None  # Still returns prompt

    def test_detects_change_after_lab_deletion(self, db_setup):
        """After labs are deleted, count changes → triggers analysis."""
        db, config, km = db_setup
        # Insert 2 labs
        for i in range(2):
            lab = LabResult(
                id=f"lab_{i}",
                test_name="TSH",
                canonical_name="tsh",
                value=4.0 + i,
                unit="mIU/L",
                date_collected=date(2025, 12, 1),
                triage_level=TriageLevel.NORMAL,
            )
            db.insert_observation(lab, user_id=123)

        engine = BackgroundAnalysisEngine(db, config)
        result = engine.build_health_synthesis_prompt(user_id=123)
        assert result is not None
        engine.commit_health_watermarks()

        # Delete one lab (simulating /delete_labs)
        db.conn.execute("DELETE FROM observations WHERE obs_id = 'lab_0'")
        db.conn.commit()

        # Count changed (2 → 1) → should trigger analysis
        result2 = engine.build_health_synthesis_prompt(user_id=123)
        assert result2 is not None

    def test_prompt_includes_hypothesis_count(self, db_setup):
        db, config, km = db_setup
        # Insert a lab so there's new data
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=100.0,
            unit="mg/dL",
            date_collected=date(2025, 12, 1),
            triage_level=TriageLevel.NORMAL,
        )
        db.insert_observation(lab, user_id=123)

        engine = BackgroundAnalysisEngine(db, config)
        result = engine.build_health_synthesis_prompt(user_id=123, force=True)
        assert "active hypotheses" in result


# -- Research synthesis prompt tests --


class TestResearchSynthesisPrompt:
    """build_research_synthesis_prompt() tests."""

    def test_returns_none_when_no_articles(self, db_setup):
        db, config, _ = db_setup
        engine = BackgroundAnalysisEngine(db, config)
        result = engine.build_research_synthesis_prompt(user_id=123)
        assert result is None

    def test_returns_prompt_when_new_articles(self, db_setup):
        db, config, _ = db_setup
        # Insert a fake evidence entry
        from healthbot.research.external_evidence_store import (
            ExternalEvidenceStore,
        )

        store = ExternalEvidenceStore(db)
        store.store(
            source="pubmed",
            query="hypothyroidism recent advances",
            result={"title": "Test Article", "pmid": "12345678"},
            condition_related=True,
        )

        engine = BackgroundAnalysisEngine(db, config)
        result = engine.build_research_synthesis_prompt(user_id=123)
        assert result is not None
        assert "Background research synthesis" in result
        assert "1 new article was" in result

    def test_no_prompt_on_second_call_after_commit(self, db_setup):
        db, config, _ = db_setup
        from healthbot.research.external_evidence_store import (
            ExternalEvidenceStore,
        )

        store = ExternalEvidenceStore(db)
        store.store(
            source="pubmed",
            query="hypothyroidism",
            result={"title": "Article 1"},
            condition_related=True,
        )

        engine = BackgroundAnalysisEngine(db, config)
        result1 = engine.build_research_synthesis_prompt(user_id=123)
        assert result1 is not None
        engine.commit_research_watermarks()
        result2 = engine.build_research_synthesis_prompt(user_id=123)
        assert result2 is None


# -- Alert extraction tests --


class TestExtractAlert:
    """extract_alert() tests."""

    def test_no_alert_in_normal_response(self):
        assert BackgroundAnalysisEngine.extract_alert(
            "Everything looks good. No concerns."
        ) is None

    def test_extract_alert_from_prefix(self):
        response = (
            "ALERT: TSH 4.8 is above optimal range — consider dose adjustment.\n\n"
            "The rest of the analysis..."
        )
        alert = BackgroundAnalysisEngine.extract_alert(response)
        assert alert is not None
        assert "TSH 4.8" in alert

    def test_alert_truncated_to_200_chars(self):
        response = "ALERT: " + "x" * 300
        alert = BackgroundAnalysisEngine.extract_alert(response)
        assert len(alert) <= 200

    def test_extract_alert_from_urgent_action(self):
        response = (
            'Some analysis text.\n'
            'ACTION: {"test": "anti-TPO antibodies", '
            '"reason": "Confirm autoimmune thyroiditis", '
            '"urgency": "urgent"}'
        )
        alert = BackgroundAnalysisEngine.extract_alert(response)
        assert alert is not None
        assert "Confirm autoimmune thyroiditis" in alert

    def test_no_alert_from_routine_action(self):
        response = (
            'ACTION: {"test": "vitamin D", '
            '"reason": "Annual check", '
            '"urgency": "routine"}'
        )
        assert BackgroundAnalysisEngine.extract_alert(response) is None

    def test_empty_response(self):
        assert BackgroundAnalysisEngine.extract_alert("") is None
        assert BackgroundAnalysisEngine.extract_alert(None) is None

    def test_alert_case_insensitive_prefix(self):
        response = "alert: Low ferritin detected."
        alert = BackgroundAnalysisEngine.extract_alert(response)
        assert alert is not None
        assert "Low ferritin" in alert


# -- Evidence bridge tests --


class TestEvidenceBridge:
    """Test _append_research_evidence() in ClaudeConversationManager."""

    def test_evidence_appears_in_prompt(self, db_setup):
        """When evidence store has articles, they appear in the prompt."""
        db, config, km = db_setup
        from healthbot.research.external_evidence_store import (
            ExternalEvidenceStore,
        )

        store = ExternalEvidenceStore(db)
        store.store(
            source="pubmed",
            query="subclinical hypothyroidism",
            result={
                "title": "Subclinical Hypothyroidism Management",
                "journal": "NEJM",
                "year": "2025",
                "pmid": "99887766",
                "abstract": "Recent advances in diagnosis...",
            },
            condition_related=True,
        )

        from healthbot.llm.claude_conversation import ClaudeConversationManager
        from healthbot.security.phi_firewall import PhiFirewall

        claude = MagicMock()
        claude.send.return_value = "Test response."
        fw = PhiFirewall()
        mgr = ClaudeConversationManager(config, claude, fw, key_manager=km)
        mgr.load()
        mgr._db = db
        mgr._user_id = 123

        # Build prompt and check for research library
        parts: list[str] = []
        mgr._append_research_evidence(parts, "thyroid")
        text = "\n".join(parts)
        assert "RESEARCH LIBRARY" in text
        assert "PMID:99887766" in text
        assert "NEJM" in text

    def test_no_evidence_section_when_empty(self, db_setup):
        """No RESEARCH LIBRARY section when no articles exist."""
        db, config, km = db_setup

        from healthbot.llm.claude_conversation import ClaudeConversationManager
        from healthbot.security.phi_firewall import PhiFirewall

        claude = MagicMock()
        claude.send.return_value = "Test response."
        fw = PhiFirewall()
        mgr = ClaudeConversationManager(config, claude, fw, key_manager=km)
        mgr.load()
        mgr._db = db
        mgr._user_id = 123

        parts: list[str] = []
        mgr._append_research_evidence(parts, "anything")
        assert len(parts) == 0


# -- System prompt tests --


class TestSystemPromptUpdates:
    """Verify citation and cross-referencing instructions exist."""

    def test_context_template_has_citation_section(self):
        from healthbot.llm.claude_context import CLAUDE_CONTEXT_TEMPLATE

        assert "## Citing sources" in CLAUDE_CONTEXT_TEMPLATE
        assert "PMID" in CLAUDE_CONTEXT_TEMPLATE

    def test_context_template_has_cross_referencing(self):
        from healthbot.llm.claude_context import CLAUDE_CONTEXT_TEMPLATE

        assert "## Cross-referencing (critical)" in CLAUDE_CONTEXT_TEMPLATE
        assert "cross-reference against the patient" in CLAUDE_CONTEXT_TEMPLATE

    def test_context_template_has_research_library_mention(self):
        from healthbot.llm.claude_context import CLAUDE_CONTEXT_TEMPLATE

        assert "Cached research articles" in CLAUDE_CONTEXT_TEMPLATE

    def test_old_template_detected_for_upgrade(self, tmp_path):
        """Templates without citation sections trigger auto-upgrade."""
        from healthbot.llm.claude_context import (
            _maybe_upgrade_template,
        )

        # Write a template with the old signature (no cross-referencing)
        old_content = (
            "## When you need to research\n"
            "You have WebSearch and WebFetch. Use them when:\n"
            "- I ask about a condition, drug interaction, or supplement\n"
            "- You want to cite a specific guideline or study\n"
            "- You need current treatment protocols\n\n"
            "## Medical Intelligence Protocol\n"
            "Some old content..."
        )
        ctx_path = tmp_path / "context.md"
        ctx_path.write_text(old_content)

        _maybe_upgrade_template(ctx_path)

        new_content = ctx_path.read_text()
        assert "## Citing sources" in new_content
        assert "## Cross-referencing" in new_content
