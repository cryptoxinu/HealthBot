"""Message routing for non-command Telegram messages.

Routes passphrase entry, document uploads, and free-text conversation
to appropriate handlers. Manages passphrase-awaiting state.
"""
from healthbot.bot.message_router.intent_interceptor import (
    _DELETE_LABS_PATTERN,
    _ONBOARD_PATTERN,
    _OURA_AUTH_PATTERN,
    _PAUSE_OVERDUE_PATTERN,
    _RESET_PATTERN,
    _RESTART_PATTERN,
    _SAVE_MESSAGE_PATTERN,
    _STATUS_CHECK_PATTERN,
    _TROUBLESHOOT_PATTERN,
    _UNPAUSE_OVERDUE_PATTERN,
    _UNSAVE_MESSAGE_PATTERN,
    _VISUAL_HEALTH_PATTERN,
    _WEARABLE_STATUS_PATTERN,
    _WHOOP_AUTH_PATTERN,
)
from healthbot.bot.message_router.router_core import MessageRouter

__all__ = [
    "MessageRouter",
    "_DELETE_LABS_PATTERN",
    "_ONBOARD_PATTERN",
    "_OURA_AUTH_PATTERN",
    "_PAUSE_OVERDUE_PATTERN",
    "_RESET_PATTERN",
    "_RESTART_PATTERN",
    "_SAVE_MESSAGE_PATTERN",
    "_STATUS_CHECK_PATTERN",
    "_TROUBLESHOOT_PATTERN",
    "_UNPAUSE_OVERDUE_PATTERN",
    "_UNSAVE_MESSAGE_PATTERN",
    "_VISUAL_HEALTH_PATTERN",
    "_WEARABLE_STATUS_PATTERN",
    "_WHOOP_AUTH_PATTERN",
]
