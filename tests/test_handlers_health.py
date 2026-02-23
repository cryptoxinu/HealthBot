"""Tests for healthbot.bot.handlers_health — health analysis commands."""
from __future__ import annotations

import uuid
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_health import HealthHandlers
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, TriageLevel, Workout
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
    update.message.reply_photo = AsyncMock()
    return update


def _mock_context(args: list[str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.args = args or []
    return ctx


def _insert_lab(db: HealthDB, name: str = "glucose", value: float = 100.0) -> str:
    lab = LabResult(
        id="", test_name=name, canonical_name=name,
        value=value, unit="mg/dL",
        date_collected=date(2025, 1, 15),
        triage_level=TriageLevel.NORMAL,
    )
    return db.insert_observation(lab)


class TestInsights:
    @pytest.mark.asyncio
    async def test_insights_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.insights(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_insights_returns_dashboard(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        _insert_lab(db)
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.insights(update, _mock_context())
        assert update.message.reply_text.called


class TestTrend:
    @pytest.mark.asyncio
    async def test_trend_no_args_shows_usage(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.trend(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()

    @pytest.mark.asyncio
    async def test_trend_with_test_name(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        _insert_lab(db)
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.trend(update, _mock_context(["glucose"]))
        assert update.message.reply_text.called


class TestAsk:
    @pytest.mark.asyncio
    async def test_ask_no_args_shows_usage(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.ask(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "usage" in reply.lower()

    @pytest.mark.asyncio
    async def test_ask_emergency_keyword(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.ask(update, _mock_context(["chest", "pain", "crushing"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "emergency" in reply.lower() or "911" in reply

    @pytest.mark.asyncio
    async def test_ask_searches_records(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        _insert_lab(db)
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.ask(update, _mock_context(["glucose"]))
        assert update.message.reply_text.called


class TestOverdue:
    @pytest.mark.asyncio
    async def test_overdue_responds(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.overdue(update, _mock_context())
        assert update.message.reply_text.called


class TestCorrelate:
    @pytest.mark.asyncio
    async def test_correlate_responds(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.correlate(update, _mock_context())
        assert update.message.reply_text.called


class TestGaps:
    @pytest.mark.asyncio
    async def test_gaps_responds(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.gaps(update, _mock_context())
        assert update.message.reply_text.called


class TestHealthReview:
    @pytest.mark.asyncio
    async def test_healthreview_responds(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.healthreview(update, _mock_context())
        assert update.message.reply_text.called


class TestProfile:
    @pytest.mark.asyncio
    async def test_profile_responds(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.profile(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        combined = " ".join(texts)
        assert "HEALTH PROFILE" in combined

    @pytest.mark.asyncio
    async def test_profile_with_data(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        _insert_lab(db)
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.profile(update, _mock_context())
        assert update.message.reply_text.called


class TestWorkouts:
    @pytest.mark.asyncio
    async def test_workouts_no_data(
        self, config: Config, key_manager: KeyManager, db: HealthDB,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.workouts(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "No workouts found" in reply

    @pytest.mark.asyncio
    async def test_workouts_with_data(
        self, config: Config, key_manager: KeyManager, db: HealthDB,
    ) -> None:
        wo = Workout(
            id=uuid.uuid4().hex,
            sport_type="running",
            start_time=datetime.now(),
            duration_minutes=30.0,
            calories_burned=300.0,
            source="apple_health",
        )
        db.insert_workout(wo, user_id=123)
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.workouts(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        combined = " ".join(texts)
        assert "Running" in combined

    @pytest.mark.asyncio
    async def test_workouts_filter_by_activity(
        self, config: Config, key_manager: KeyManager, db: HealthDB,
    ) -> None:
        wo = Workout(
            id=uuid.uuid4().hex,
            sport_type="cycling",
            start_time=datetime.now(),
            duration_minutes=60.0,
            source="apple_health",
        )
        db.insert_workout(wo, user_id=123)
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.workouts(update, _mock_context(["cycling"]))
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        combined = " ".join(texts)
        assert "Cycling" in combined

    @pytest.mark.asyncio
    async def test_workouts_custom_days(
        self, config: Config, key_manager: KeyManager, db: HealthDB,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.workouts(update, _mock_context(["7"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "7 days" in reply


class TestWeeklyReport:
    @pytest.mark.asyncio
    async def test_weeklyreport_generates_pdf(
        self, config: Config, key_manager: KeyManager, db: HealthDB,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.weeklyreport(update, _mock_context())
        # Should send a document (PDF)
        assert update.message.reply_document.called

    @pytest.mark.asyncio
    async def test_weeklyreport_invalid_args(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.weeklyreport(update, _mock_context(["notanumber"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "Invalid" in reply


class TestMonthlyReport:
    @pytest.mark.asyncio
    async def test_monthlyreport_generates_pdf(
        self, config: Config, key_manager: KeyManager, db: HealthDB,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.monthlyreport(update, _mock_context())
        assert update.message.reply_document.called

    @pytest.mark.asyncio
    async def test_monthlyreport_invalid_args(
        self, config: Config, key_manager: KeyManager,
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.monthlyreport(update, _mock_context(["xyz"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "Invalid" in reply
