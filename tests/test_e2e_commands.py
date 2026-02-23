"""End-to-end integration tests for Telegram command handlers.

Tests verify the full pipeline from handler invocation through
business logic to database state, using mock Telegram objects.
All LLM calls are mocked; database and encryption are real.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.bot.handlers import Handlers
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall

# ── Helpers ──────────────────────────────────────────────────────────

def _mock_update(user_id: int = 123, text: str = "") -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat.id = 456
    update.effective_chat.send_message = AsyncMock()
    update.effective_chat.send_action = AsyncMock()
    update.message.text = text
    update.message.reply_text = AsyncMock()
    update.message.reply_photo = AsyncMock()
    update.message.reply_document = AsyncMock()
    update.message.delete = AsyncMock()
    update.message.document = None
    return update


def _mock_context(*args_list: str) -> MagicMock:
    ctx = MagicMock()
    ctx.args = list(args_list)
    ctx.bot = AsyncMock()
    ctx.bot.get_file = AsyncMock()
    return ctx


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def handlers(config: Config, key_manager: KeyManager, db: HealthDB) -> Handlers:
    """Fully-wired Handlers with real DB + encryption, vault already unlocked."""
    db.run_migrations()
    fw = PhiFirewall()
    h = Handlers(config, key_manager, fw)
    # Inject already-open DB so handlers skip lazy init
    h._core._db = db
    return h


# ── Test: /start ─────────────────────────────────────────────────────

class TestStartCommand:
    @pytest.mark.asyncio
    async def test_start_shows_commands(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.start(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        # Vault is unlocked (fixture), so returns returning-unlocked variant
        assert "Welcome back" in reply
        assert "unlocked" in reply.lower()
        assert "/insights" in reply
        assert "/help" in reply

    @pytest.mark.asyncio
    async def test_help_shows_all_commands(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.help_cmd(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "/template" in reply
        assert "/evidence" in reply
        assert "/profile" in reply
        assert "/backup" in reply
        assert "/reset" in reply


# ── Test: vault lock guard ───────────────────────────────────────────

class TestVaultLockGuard:
    """Commands requiring unlock should refuse when vault is locked."""

    @pytest.mark.asyncio
    async def test_insights_locked(self, handlers) -> None:
        handlers._core._km.lock()
        update = _mock_update()
        ctx = _mock_context()
        await handlers.insights(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_trend_locked(self, handlers) -> None:
        handlers._core._km.lock()
        update = _mock_update()
        ctx = _mock_context("glucose")
        await handlers.trend(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_profile_locked(self, handlers) -> None:
        handlers._core._km.lock()
        update = _mock_update()
        ctx = _mock_context()
        await handlers.profile(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_evidence_locked(self, handlers) -> None:
        handlers._core._km.lock()
        update = _mock_update()
        ctx = _mock_context()
        await handlers.evidence(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_template_locked(self, handlers) -> None:
        handlers._core._km.lock()
        update = _mock_update()
        ctx = _mock_context()
        await handlers.template(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_hypotheses_locked(self, handlers) -> None:
        handlers._core._km.lock()
        update = _mock_update()
        ctx = _mock_context()
        await handlers.hypotheses(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_oura_locked(self, handlers) -> None:
        handlers._core._km.lock()
        update = _mock_update()
        ctx = _mock_context()
        await handlers.sync_oura(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()


# ── Test: /insights flow ─────────────────────────────────────────────

class TestInsightsFlow:
    @pytest.mark.asyncio
    async def test_insights_returns_dashboard(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.insights(update, ctx)
        # Should have at least one reply_text call (dashboard text)
        assert update.message.reply_text.call_count >= 1


# ── Test: /trend flow ────────────────────────────────────────────────

class TestTrendFlow:
    @pytest.mark.asyncio
    async def test_trend_no_args(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.trend(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "Usage" in reply

    @pytest.mark.asyncio
    async def test_trend_no_data(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context("glucose")
        await handlers.trend(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "not enough" in reply.lower() or "no data" in reply.lower()

    @pytest.mark.asyncio
    async def test_trend_hrv_uses_wearable(self, handlers, db) -> None:
        """'/trend hrv' should use WearableTrendAnalyzer, not lab TrendAnalyzer."""
        import uuid
        from datetime import date, timedelta

        from healthbot.data.models import WhoopDaily

        today = date.today()
        for i in range(10):
            wd = WhoopDaily(
                id=uuid.uuid4().hex,
                date=today - timedelta(days=9 - i),
                hrv=80.0 - i * 3,
            )
            db.insert_wearable_daily(wd, user_id=123)

        update = _mock_update()
        ctx = _mock_context("hrv")
        await handlers.trend(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "HRV" in reply
        assert "TREND" in reply
        assert "Monthly" in reply or "direction" in reply.lower() or "decreasing" in reply.lower()

    @pytest.mark.asyncio
    async def test_trend_sleep_alias(self, handlers, db) -> None:
        """'/trend sleep' should resolve to sleep_score wearable metric."""
        import uuid
        from datetime import date, timedelta

        from healthbot.data.models import WhoopDaily

        today = date.today()
        for i in range(7):
            wd = WhoopDaily(
                id=uuid.uuid4().hex,
                date=today - timedelta(days=6 - i),
                sleep_score=85.0 - i * 5,
            )
            db.insert_wearable_daily(wd, user_id=123)

        update = _mock_update()
        ctx = _mock_context("sleep")
        await handlers.trend(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "SLEEP" in reply.upper()

    @pytest.mark.asyncio
    async def test_trend_hrv_no_data(self, handlers) -> None:
        """'/trend hrv' with no wearable data should say not enough."""
        update = _mock_update()
        ctx = _mock_context("hrv")
        await handlers.trend(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "not enough" in reply.lower()


# ── Test: /log + /undo flow ──────────────────────────────────────────

class TestLogUndoFlow:
    @pytest.mark.asyncio
    async def test_log_event_stores_observation(self, handlers, db) -> None:
        update = _mock_update()
        ctx = _mock_context("headache", "today", "moderate")
        await handlers.log_event(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "logged" in reply.lower() or "headache" in reply.lower()

        # Verify stored in DB
        rows = db.query_observations(record_type="user_event", limit=10)
        assert len(rows) >= 1

    @pytest.mark.asyncio
    async def test_log_empty_usage(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.log_event(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "Usage" in reply

    @pytest.mark.asyncio
    async def test_undo_nothing(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.undo(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "nothing" in reply.lower()


# ── Test: /profile flow ──────────────────────────────────────────────

class TestProfileFlow:
    @pytest.mark.asyncio
    async def test_profile_returns_sections(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.profile(update, ctx)
        # Gather all reply text
        replies = [
            call[0][0]
            for call in update.message.reply_text.call_args_list
        ]
        full = "\n".join(replies)
        assert "HEALTH PROFILE" in full


# ── Test: /hypotheses flow ───────────────────────────────────────────

class TestHypothesesFlow:
    @pytest.mark.asyncio
    async def test_hypotheses_empty(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.hypotheses(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "no" in reply.lower()

    @pytest.mark.asyncio
    async def test_hypotheses_with_data(self, handlers, db) -> None:
        user_id = 123
        db.insert_hypothesis(user_id, {
            "title": "Iron Deficiency",
            "confidence": 0.6,
            "evidence_for": ["Low ferritin"],
            "evidence_against": [],
            "missing_tests": ["iron", "tibc"],
        })
        update = _mock_update(user_id=user_id)
        ctx = _mock_context()
        await handlers.hypotheses(update, ctx)
        replies = [
            call[0][0]
            for call in update.message.reply_text.call_args_list
        ]
        full = "\n".join(replies)
        assert "Iron Deficiency" in full

    @pytest.mark.asyncio
    async def test_hypotheses_ruleout(self, handlers, db) -> None:
        user_id = 123
        db.insert_hypothesis(user_id, {
            "title": "Thyroid Issue",
            "confidence": 0.4,
            "evidence_for": ["Fatigue"],
            "evidence_against": [],
            "missing_tests": ["tsh"],
        })
        update = _mock_update(user_id=user_id)
        ctx = _mock_context("ruleout", "1", "TSH was normal")
        await handlers.hypotheses(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "ruled out" in reply.lower()

        # Verify status changed in DB
        hyps = db.get_all_hypotheses(user_id)
        assert any(h.get("_status") == "ruled_out" for h in hyps)

    @pytest.mark.asyncio
    async def test_hypotheses_confirm(self, handlers, db) -> None:
        user_id = 123
        db.insert_hypothesis(user_id, {
            "title": "Vitamin D Deficiency",
            "confidence": 0.8,
            "evidence_for": ["Low vitamin D", "Fatigue"],
            "evidence_against": [],
            "missing_tests": [],
        })
        update = _mock_update(user_id=user_id)
        ctx = _mock_context("confirm", "1", "Lab confirmed")
        await handlers.hypotheses(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "confirmed" in reply.lower()

        hyps = db.get_all_hypotheses(user_id)
        assert any(h.get("_status") == "confirmed" for h in hyps)


# ── Test: /template flow ─────────────────────────────────────────────

class TestTemplateFlow:
    @pytest.mark.asyncio
    async def test_template_list(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.template(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "template" in reply.lower()
        assert "pots" in reply.lower()
        assert "thyroid" in reply.lower()

    @pytest.mark.asyncio
    async def test_template_generate_pots(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context("pots")
        await handlers.template(update, ctx)
        replies = [
            call[0][0]
            for call in update.message.reply_text.call_args_list
        ]
        full = "\n".join(replies)
        assert "POTS" in full or "Postural" in full

    @pytest.mark.asyncio
    async def test_template_unknown(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context("nonexistent")
        await handlers.template(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "unknown" in reply.lower() or "not found" in reply.lower()


# ── Test: /evidence flow ─────────────────────────────────────────────

class TestEvidenceFlow:
    @pytest.mark.asyncio
    async def test_evidence_empty(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.evidence(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "no" in reply.lower()

    @pytest.mark.asyncio
    async def test_evidence_with_data(self, handlers, db) -> None:
        from healthbot.research.external_evidence_store import ExternalEvidenceStore
        store = ExternalEvidenceStore(db)
        store.store("pubmed", "vitamin d deficiency symptoms", "Summary of findings...")

        update = _mock_update()
        ctx = _mock_context()
        await handlers.evidence(update, ctx)
        replies = [
            call[0][0]
            for call in update.message.reply_text.call_args_list
        ]
        full = "\n".join(replies)
        assert "vitamin d" in full.lower() or "pubmed" in full.lower()

    @pytest.mark.asyncio
    async def test_evidence_detail(self, handlers, db) -> None:
        from healthbot.research.external_evidence_store import ExternalEvidenceStore
        store = ExternalEvidenceStore(db)
        store.store("claude_cli", "iron absorption factors", "Detailed research text...")

        update = _mock_update()
        ctx = _mock_context("1")
        await handlers.evidence(update, ctx)
        replies = [
            call[0][0]
            for call in update.message.reply_text.call_args_list
        ]
        full = "\n".join(replies)
        assert "research" in full.lower() or "iron" in full.lower() or "Source" in full


# ── Test: /oura flow (mocked API) ───────────────────────────────────

class TestOuraFlow:
    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_oura_sync_success(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()

        with patch("healthbot.importers.oura_client.OuraClient") as mock_cls:
            mock_instance = MagicMock()
            mock_instance.sync_daily = AsyncMock(return_value=5)
            mock_cls.return_value = mock_instance

            with patch("healthbot.security.keychain.Keychain"):
                with patch("healthbot.security.vault.Vault"):
                    await handlers.sync_oura(update, ctx)

        replies = [
            call[0][0]
            for call in update.message.reply_text.call_args_list
        ]
        full = "\n".join(replies)
        assert "oura" in full.lower()
        assert "5" in full or "complete" in full.lower()


# ── Test: /lock flow ─────────────────────────────────────────────────

class TestLockFlow:
    @pytest.mark.asyncio
    async def test_lock_wipes_session(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.lock(update, ctx)
        # Lock now sends via chat.send_message (after chat wipe)
        sent = update.effective_chat.send_message.call_args[0][0]
        assert "locked" in sent.lower()
        assert not handlers._core._km.is_unlocked


# ── Test: /overdue ───────────────────────────────────────────────────

class TestOverdueFlow:
    @pytest.mark.asyncio
    async def test_overdue_returns_text(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.overdue(update, ctx)
        assert update.message.reply_text.call_count >= 1


# ── Test: /correlate ─────────────────────────────────────────────────

class TestCorrelateFlow:
    @pytest.mark.asyncio
    async def test_correlate_returns_text(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.correlate(update, ctx)
        assert update.message.reply_text.call_count >= 1


# ── Test: /gaps ──────────────────────────────────────────────────────

class TestGapsFlow:
    @pytest.mark.asyncio
    async def test_gaps_returns_text(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.gaps(update, ctx)
        assert update.message.reply_text.call_count >= 1


# ── Test: /ask ───────────────────────────────────────────────────────

class TestAskFlow:
    @pytest.mark.asyncio
    async def test_ask_no_args(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.ask(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "Usage" in reply

    @pytest.mark.asyncio
    async def test_ask_no_results(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context("random", "query", "nothing")
        await handlers.ask(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "no matching" in reply.lower() or "no results" in reply.lower()


# ── Test: /research_cloud ────────────────────────────────────────────

class TestResearchCloudFlow:
    @pytest.mark.asyncio
    async def test_research_no_topic(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context()
        await handlers.research_cloud(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "Usage" in reply

    @pytest.mark.asyncio
    async def test_research_phi_blocked(self, handlers) -> None:
        update = _mock_update()
        ctx = _mock_context("John", "Smith", "SSN", "123-45-6789")
        await handlers.research_cloud(update, ctx)
        reply = update.message.reply_text.call_args[0][0]
        assert "blocked" in reply.lower() or "phi" in reply.lower()
