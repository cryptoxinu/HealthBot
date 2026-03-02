"""Telegram command handlers — thin facade delegating to sub-handler groups.

All handlers check session lock status before processing.
Message routing (passphrase, PDFs, free text) is delegated to MessageRouter.

Commands are routed via _ROUTING table to avoid 60+ boilerplate one-liner
delegation methods.  Forwarding stubs are generated at class load time from
the routing dict so that hasattr() and inspect.iscoroutinefunction() work
on the class itself (required by tests and python-telegram-bot registration).
"""
from __future__ import annotations

from telegram import Update
from telegram.ext import ContextTypes

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_data import DataHandlers
from healthbot.bot.handlers_health import HealthHandlers
from healthbot.bot.handlers_medical import MedicalHandlers
from healthbot.bot.handlers_research import ResearchHandlers
from healthbot.bot.handlers_session import SessionHandlers
from healthbot.config import Config
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


class Handlers:
    """Facade delegating commands to specialized handler groups."""

    # Maps command method name → sub-handler attribute name.
    _ROUTING: dict[str, str] = {
        # Session commands
        "start": "_session", "help_cmd": "_session", "unlock": "_session",
        "lock": "_session", "feedback": "_session", "backup": "_session",
        "rekey": "_session", "version": "_session", "audit": "_session",
        "integrity": "_session", "restart": "_session", "ingest": "_session",
        "upload": "_session", "finish": "_session", "digest": "_session",
        "refresh": "_session", "claude_auth": "_session",
        "auth_status": "_session", "pii_alerts": "_session",
        "privacy": "_session", "redacted": "_session", "snooze": "_session",
        "preferences": "_session", "tokenusage": "_session",
        # Health analysis commands
        "memory": "_health", "insights": "_health", "summary": "_health",
        "trend": "_health", "ask": "_health", "overdue": "_health",
        "correlate": "_health", "gaps": "_health", "symptoms": "_health",
        "healthreview": "_health", "profile": "_health", "aboutme": "_health",
        "labs": "_health", "recommend": "_health", "screenings": "_health",
        "sleeprec": "_health", "stress": "_health", "goals": "_health",
        "timeline": "_health", "report": "_health", "emergency": "_health",
        "workouts": "_health", "weeklyreport": "_health",
        "monthlyreport": "_health", "analyze": "_health",
        "score": "_health", "wearable_chart": "_health",
        "sleep_chart": "_health", "lab_heatmap": "_health",
        "scatter": "_health", "trends_chart": "_health",
        # Medical tracking commands
        "doctorprep": "_medical", "research_cloud": "_medical",
        "doctorpacket": "_medical", "interactions": "_medical",
        "evidence": "_medical", "template": "_medical",
        "hypotheses": "_medical", "sideeffects": "_medical",
        "retests": "_medical", "supplements": "_medical",
        "comorbidity": "_medical", "effectiveness": "_medical",
        "log_event": "_medical", "undo": "_medical", "doctors": "_medical",
        "appointments": "_medical", "remind": "_medical",
        "reminders": "_medical",
        # Data import/export commands
        "wearable_status": "_data", "whoop_auth": "_data",
        "sync_all": "_data", "oura_auth": "_data", "sync_oura": "_data",
        "connectors": "_data",
        # Research commands
        "deep": "_research",
        "savedmessages": "_session",
        "apple_sync": "_data", "import_health": "_data",
        "import_mychart": "_data", "export_fhir": "_data",
        "ai_export": "_data", "rescan": "_data", "docs": "_data",
        "import_fasten": "_data", "scrub_pii": "_data", "cleansync": "_data",
        "debug": "_data", "genetics": "_data",
    }

    def __init__(
        self,
        config: Config,
        key_manager: KeyManager,
        phi_firewall: PhiFirewall,
    ) -> None:
        self._core = HandlerCore(config, key_manager, phi_firewall)
        self._core.log_capability_manifest()
        self._session = SessionHandlers(self._core)
        self._health = HealthHandlers(self._core)
        self._medical = MedicalHandlers(self._core)
        self._research = ResearchHandlers(self._core)
        self._data = DataHandlers(self._core)

    def wire_scheduler(self, job_queue: object) -> None:
        self._core.wire_scheduler(job_queue)

    def wire_unlock_callback(self) -> None:
        self._core.wire_unlock_callback()

    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._core.handle_message(update, context)

    # -- Special routing (not standard sub-handler delegation) --

    async def delete_labs(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._core._router._reset_handlers.delete_labs(update, context)

    async def delete_doc(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self._core._router._reset_handlers.delete_doc(update, context)

    # -- Identity commands (delegated to IdentityHandlers via app.py) --
    # /identity, /identity_check, /identity_clear are registered directly
    # in app.py like OnboardHandlers and ResetHandlers.


def _make_forwarder(handler_attr: str, method_name: str):
    """Create an async forwarding stub for the routing table."""
    async def _forward(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await getattr(getattr(self, handler_attr), method_name)(update, context)
    _forward.__name__ = method_name
    _forward.__qualname__ = f"Handlers.{method_name}"
    return _forward


for _name, _attr in Handlers._ROUTING.items():
    setattr(Handlers, _name, _make_forwarder(_attr, _name))
