"""Middleware decorators for Telegram command handlers.

These decorators work with methods on the Handlers class. They access
self._km (KeyManager) and self._config (Config) from the handler instance.
"""
from __future__ import annotations

import functools
import time
from collections.abc import Callable
from typing import Any

# Rate limiting state (per user)
_rate_limits: dict[int, list[float]] = {}
_MAX_TRACKED_USERS = 100


def clear_rate_limits() -> None:
    """Clear all rate limit state. Used by tests."""
    _rate_limits.clear()


def require_unlocked(func: Callable) -> Callable:
    """Decorator: check vault is unlocked before handler runs."""
    @functools.wraps(func)
    async def wrapper(self, update, context, *args, **kwargs) -> Any:
        # Auth gate — must pass before any state access
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        # Store bot reference for proactive wipe on lock
        core = getattr(self, '_core', None)
        if core:
            core._bot = context.bot
        if not self._km.is_unlocked:
            # Check if passive timeout just triggered lock (wipe needed)
            if core and getattr(core, '_pending_wipe', False) is True:
                core._pending_wipe = False
                await core.wipe_session_chat(context.bot)
                try:
                    await update.message.delete()
                except Exception:
                    pass
                await update.effective_chat.send_message(
                    "Session expired. Vault locked. Chat cleared.\n"
                    "Send /unlock to start a new session."
                )
            else:
                await update.message.reply_text(
                    "Vault is locked. Send /unlock first."
                )
            return
        self._km.touch()
        return await func(self, update, context, *args, **kwargs)
    return wrapper


def require_auth(func: Callable) -> Callable:
    """Decorator: check user_id is in allowlist."""
    @functools.wraps(func)
    async def wrapper(self, update, context, *args, **kwargs) -> Any:
        if not self._check_auth(update):
            await update.message.reply_text("Unauthorized.")
            return
        return await func(self, update, context, *args, **kwargs)
    return wrapper


def rate_limited(max_per_minute: int = 20) -> Callable:
    """Decorator factory: rate limit per user."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(self, update, context, *args, **kwargs) -> Any:
            user_id = update.effective_user.id if update.effective_user else 0
            now = time.time()

            if user_id not in _rate_limits:
                _rate_limits[user_id] = []

            # Clean old entries
            _rate_limits[user_id] = [
                t for t in _rate_limits[user_id] if now - t < 60
            ]

            if len(_rate_limits[user_id]) >= max_per_minute:
                await update.message.reply_text(
                    "Rate limit exceeded. Please wait a moment."
                )
                return

            _rate_limits[user_id].append(now)

            # Cap tracked users to prevent unbounded growth
            if len(_rate_limits) > _MAX_TRACKED_USERS:
                # Remove users with no recent activity
                stale = [
                    uid for uid, ts in _rate_limits.items()
                    if not ts or now - max(ts) > 300
                ]
                for uid in stale:
                    del _rate_limits[uid]

            return await func(self, update, context, *args, **kwargs)
        return wrapper
    return decorator
