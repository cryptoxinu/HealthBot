"""Data import/export and wearable sync command handlers."""
from __future__ import annotations

from healthbot.bot.handler_core import HandlerCore

from .apple_sync import AppleSyncMixin
from .clean_sync import CleanSyncMixin
from .connectors import ConnectorsMixin
from .export import ExportMixin
from .genetics_analysis import GeneticsAnalysisMixin
from .health_import import HealthImportMixin
from .setup_handler import SetupHandlerMixin
from .wearable_sync import WearableSyncMixin


class DataHandlers(
    SetupHandlerMixin,
    WearableSyncMixin,
    AppleSyncMixin,
    HealthImportMixin,
    ExportMixin,
    CleanSyncMixin,
    GeneticsAnalysisMixin,
    ConnectorsMixin,
):
    """Data import, export, and wearable synchronization commands."""

    def __init__(self, core: HandlerCore) -> None:
        self._core = core
        # Per-user wearable credential setup state
        self._setup_state: dict[int, dict] = {}

    @property
    def _km(self):
        return self._core._km

    def _check_auth(self, update) -> bool:
        return self._core._check_auth(update)


__all__ = ["DataHandlers"]
