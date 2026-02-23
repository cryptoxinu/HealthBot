"""Typing indicator keepalive for long-running operations.

Telegram's typing indicator expires after ~5 seconds. For Ollama calls
(30-120s), we need to keep re-sending it so the user knows the bot is working.
"""
from __future__ import annotations

import asyncio

from telegram.constants import ChatAction


class TypingIndicator:
    """Async context manager that keeps sending TYPING action every 4 seconds."""

    def __init__(self, chat, interval: float = 4.0) -> None:
        self._chat = chat
        self._interval = interval
        self._task: asyncio.Task | None = None

    async def __aenter__(self):
        await self._chat.send_action(ChatAction.TYPING)
        self._task = asyncio.create_task(self._keepalive())
        return self

    async def __aexit__(self, *exc):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _keepalive(self):
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self._chat.send_action(ChatAction.TYPING)
            except Exception:
                pass
