"""Tests for user-friendly error messages in handlers."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_health import HealthHandlers
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_handlers(config: Config, key_manager: KeyManager) -> HealthHandlers:
    core = HandlerCore(config, key_manager, PhiFirewall())
    return HealthHandlers(core)


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
    return ctx


class TestWeeklyReportValidation:
    """Validate input args for /weeklyreport."""

    @pytest.mark.asyncio
    async def test_invalid_string_arg(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.weeklyreport(update, _mock_context(["abc"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "Invalid argument" in reply
        assert "Usage" in reply

    @pytest.mark.asyncio
    async def test_out_of_range_days(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.weeklyreport(update, _mock_context(["200"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "Invalid range" in reply


class TestMonthlyReportValidation:
    """Validate input args for /monthlyreport."""

    @pytest.mark.asyncio
    async def test_invalid_string_arg(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.monthlyreport(update, _mock_context(["xyz"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "Invalid argument" in reply

    @pytest.mark.asyncio
    async def test_out_of_range_days(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.monthlyreport(update, _mock_context(["500"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "Invalid range" in reply


class TestWorkoutsNoData:
    """Verify helpful message when no workouts exist."""

    @pytest.mark.asyncio
    async def test_no_workouts_helpful_message(
        self, config: Config, key_manager: KeyManager, db,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.workouts(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "No workouts found" in reply
        assert "Apple Health" in reply
