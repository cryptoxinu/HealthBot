"""Tests for vault lock guards on new Phase 3-4 commands."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_health import HealthHandlers
from healthbot.bot.handlers_session import SessionHandlers
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_health(config: Config, key_manager: KeyManager) -> HealthHandlers:
    core = HandlerCore(config, key_manager, PhiFirewall())
    return HealthHandlers(core)


def _make_session(config: Config, key_manager: KeyManager) -> SessionHandlers:
    core = HandlerCore(config, key_manager, PhiFirewall())
    return SessionHandlers(core)


def _mock_update(user_id: int = 123) -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat.send_action = AsyncMock()
    update.message.reply_text = AsyncMock()
    update.message.reply_document = AsyncMock()
    update.message.reply_photo = AsyncMock()
    return update


def _mock_context(args: list[str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.args = args or []
    ctx.bot = MagicMock()
    return ctx


class TestVaultLockGuards:
    """Commands requiring vault unlock must reject when locked."""

    @pytest.mark.asyncio
    async def test_workouts_requires_unlock(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        key_manager.lock()
        handlers = _make_health(config, key_manager)
        update = _mock_update()
        await handlers.workouts(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_weeklyreport_requires_unlock(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        key_manager.lock()
        handlers = _make_health(config, key_manager)
        update = _mock_update()
        await handlers.weeklyreport(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_monthlyreport_requires_unlock(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        key_manager.lock()
        handlers = _make_health(config, key_manager)
        update = _mock_update()
        await handlers.monthlyreport(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_integrity_requires_unlock(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        key_manager.lock()
        handlers = _make_session(config, key_manager)
        update = _mock_update()
        await handlers.integrity(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()
