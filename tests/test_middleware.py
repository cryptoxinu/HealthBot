"""Tests for bot/middleware.py — decorators for Handlers class methods."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, PropertyMock

import pytest

from healthbot.bot.middleware import (
    _rate_limits,
    rate_limited,
    require_auth,
    require_unlocked,
)


@pytest.fixture(autouse=True)
def clear_rate_limits():
    """Clear rate limit state between tests."""
    _rate_limits.clear()
    yield
    _rate_limits.clear()


def _mock_update(user_id: int = 123):
    update = MagicMock()
    update.effective_user.id = user_id
    update.message.reply_text = AsyncMock()
    return update


def _mock_self(unlocked: bool = True, allowed_ids: list[int] | None = None):
    """Mock a Handlers instance with _km and _config."""
    self = MagicMock()
    type(self._km).is_unlocked = PropertyMock(return_value=unlocked)
    self._km.touch = MagicMock()
    self._config.allowed_user_ids = allowed_ids or []
    self._check_auth = MagicMock(
        side_effect=lambda update: (
            not self._config.allowed_user_ids
            or (update.effective_user.id if update.effective_user else 0)
            in self._config.allowed_user_ids
        )
    )
    return self


class TestRequireUnlocked:
    @pytest.mark.asyncio
    async def test_passes_when_unlocked(self):
        @require_unlocked
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self(unlocked=True)
        update = _mock_update()
        result = await handler(mock_self, update, MagicMock())
        assert result == "ok"
        mock_self._km.touch.assert_called_once()

    @pytest.mark.asyncio
    async def test_blocks_when_locked(self):
        @require_unlocked
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self(unlocked=False)
        update = _mock_update()
        result = await handler(mock_self, update, MagicMock())
        assert result is None
        update.message.reply_text.assert_called_once()
        assert "locked" in update.message.reply_text.call_args[0][0].lower()


class TestRequireAuth:
    @pytest.mark.asyncio
    async def test_allowed_user_passes(self):
        @require_auth
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self(allowed_ids=[123])
        update = _mock_update(user_id=123)
        result = await handler(mock_self, update, MagicMock())
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_disallowed_user_blocked(self):
        @require_auth
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self(allowed_ids=[123])
        update = _mock_update(user_id=999)
        result = await handler(mock_self, update, MagicMock())
        assert result is None
        update.message.reply_text.assert_called_once()
        assert "unauthorized" in update.message.reply_text.call_args[0][0].lower()

    @pytest.mark.asyncio
    async def test_empty_allowlist_allows_all(self):
        @require_auth
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self(allowed_ids=[])
        update = _mock_update(user_id=999)
        result = await handler(mock_self, update, MagicMock())
        assert result == "ok"


class TestRateLimited:
    @pytest.mark.asyncio
    async def test_under_limit_passes(self):
        @rate_limited(max_per_minute=5)
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self()
        update = _mock_update()
        result = await handler(mock_self, update, MagicMock())
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_over_limit_blocked(self):
        @rate_limited(max_per_minute=3)
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self()
        update = _mock_update()
        for _ in range(3):
            await handler(mock_self, update, MagicMock())
        result = await handler(mock_self, update, MagicMock())
        assert result is None
        assert "rate limit" in update.message.reply_text.call_args[0][0].lower()

    @pytest.mark.asyncio
    async def test_different_users_separate_limits(self):
        @rate_limited(max_per_minute=2)
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self()
        update1 = _mock_update(user_id=111)
        update2 = _mock_update(user_id=222)

        # User 1 hits limit
        for _ in range(2):
            await handler(mock_self, update1, MagicMock())
        result = await handler(mock_self, update1, MagicMock())
        assert result is None

        # User 2 still has quota
        result = await handler(mock_self, update2, MagicMock())
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_old_entries_expire(self):
        @rate_limited(max_per_minute=2)
        async def handler(self, update, context):
            return "ok"

        mock_self = _mock_self()
        update = _mock_update()
        # Fill limit
        for _ in range(2):
            await handler(mock_self, update, MagicMock())

        # Manually expire entries
        import time
        _rate_limits[123] = [time.time() - 120]  # 2 minutes ago

        result = await handler(mock_self, update, MagicMock())
        assert result == "ok"
