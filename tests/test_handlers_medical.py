"""Tests for healthbot.bot.handlers_medical — medical tracking commands."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_medical import MedicalHandlers
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_handlers(config: Config, key_manager: KeyManager) -> MedicalHandlers:
    core = HandlerCore(config, key_manager, PhiFirewall())
    return MedicalHandlers(core)


def _mock_update(user_id: int = 123) -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat.send_action = AsyncMock()
    update.message.reply_text = AsyncMock()
    update.message.reply_document = AsyncMock()
    return update


def _mock_context(args: list[str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.args = args or []
    return ctx


class TestDoctorprep:
    @pytest.mark.asyncio
    async def test_doctorprep_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.doctorprep(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_doctorprep_responds(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.doctorprep(update, _mock_context())
        assert update.message.reply_text.called


class TestResearchCloud:
    @pytest.mark.asyncio
    async def test_research_no_args_shows_usage(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.research_cloud(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()

    @pytest.mark.asyncio
    async def test_research_blocks_phi(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        # SSN triggers PHI detection
        await handlers.research_cloud(
            update, _mock_context(["SSN", "123-45-6789"])
        )
        reply = update.message.reply_text.call_args[0][0]
        assert "phi" in reply.lower() or "blocked" in reply.lower()

    @pytest.mark.asyncio
    async def test_research_calls_client(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch(
            "healthbot.research.claude_cli_client.ClaudeCLIResearchClient"
        ) as mock_cls:
            mock_cls.return_value.research.return_value = "Research result"
            with patch(
                "healthbot.research.external_evidence_store.ExternalEvidenceStore"
            ) as mock_store_cls:
                mock_store_cls.return_value.store = MagicMock()
                await handlers.research_cloud(
                    update, _mock_context(["vitamin", "d", "deficiency"])
                )
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("research" in t.lower() for t in texts)


class TestDoctorpacket:
    @pytest.mark.asyncio
    async def test_doctorpacket_generates_pdf(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.export.pdf_generator.DoctorPacketPdf") as mock_pdf:
            mock_pdf.return_value.generate.return_value = b"%PDF-fake"
            await handlers.doctorpacket(update, _mock_context())
        assert update.message.reply_document.called or update.message.reply_text.called

    @pytest.mark.asyncio
    async def test_doctorpacket_error_fallback(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.export.pdf_generator.DoctorPacketPdf") as mock_pdf:
            mock_pdf.return_value.generate.side_effect = RuntimeError("PDF fail")
            await handlers.doctorpacket(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("error" in t.lower() or "doctorprep" in t.lower() for t in texts)


class TestInteractions:
    @pytest.mark.asyncio
    async def test_interactions_responds(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.interactions(update, _mock_context())
        assert update.message.reply_text.called


class TestEvidence:
    @pytest.mark.asyncio
    async def test_evidence_no_args_lists(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.evidence(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "no cached" in reply.lower() or "evidence" in reply.lower()


class TestTemplate:
    @pytest.mark.asyncio
    async def test_template_no_args_lists(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.template(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "template" in reply.lower()


class TestHypotheses:
    @pytest.mark.asyncio
    async def test_hypotheses_no_args_lists_active(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.hypotheses(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "no" in reply.lower() or "hypothes" in reply.lower()

    @pytest.mark.asyncio
    async def test_hypotheses_all_subcommand(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.hypotheses(update, _mock_context(["all"]))
        assert update.message.reply_text.called

    @pytest.mark.asyncio
    async def test_hypotheses_ruleout_no_number(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.hypotheses(update, _mock_context(["ruleout"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()

    @pytest.mark.asyncio
    async def test_hypotheses_unknown_subcommand(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.hypotheses(update, _mock_context(["bogus"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()


class TestLogEvent:
    @pytest.mark.asyncio
    async def test_log_no_args_shows_usage(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.log_event(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()

    @pytest.mark.asyncio
    async def test_log_stores_event(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.log_event(
            update, _mock_context(["headache", "moderate"])
        )
        assert update.message.reply_text.called


class TestUndo:
    @pytest.mark.asyncio
    async def test_undo_nothing_logged_says_nothing(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.undo(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "nothing" in reply.lower()
