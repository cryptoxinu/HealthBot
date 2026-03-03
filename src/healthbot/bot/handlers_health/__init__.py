"""Health analysis command handlers — package facade.

Re-exports ``HealthHandlers`` so that all existing imports continue to work::

    from healthbot.bot.handlers_health import HealthHandlers
"""
from __future__ import annotations

from telegram import Update

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_health._wearable_aliases import _WEARABLE_ALIASES
from healthbot.bot.handlers_health.analysis import AnalysisMixin
from healthbot.bot.handlers_health.charting import ChartingMixin
from healthbot.bot.handlers_health.lab_browser import LabBrowserMixin
from healthbot.bot.handlers_health.medical_qa import MedicalQAMixin
from healthbot.bot.handlers_health.memory_system import MemorySystemMixin
from healthbot.bot.handlers_health.profile_mgmt import ProfileMgmtMixin
from healthbot.bot.handlers_health.reporting import ReportingMixin


class HealthHandlers(
    MemorySystemMixin,
    AnalysisMixin,
    LabBrowserMixin,
    MedicalQAMixin,
    ReportingMixin,
    ChartingMixin,
    ProfileMgmtMixin,
):
    """Health analysis and dashboard commands."""

    def __init__(self, core: HandlerCore) -> None:
        self._core = core

    @property
    def _km(self):
        return self._core._km

    def _check_auth(self, update: Update) -> bool:
        return self._core._check_auth(update)


__all__ = ["HealthHandlers", "_WEARABLE_ALIASES"]
