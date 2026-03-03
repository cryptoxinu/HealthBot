"""Clean DB — Tier 2 anonymized data store.

Separate SQLite database for pre-anonymized health data. No PII ever written.
Accessible to Claude Code, OpenClaw, or any AI via MCP server.

Encryption uses HKDF-derived "clean key" from master key, NOT the master
key directly. This provides cryptographic separation between tiers.

Every text field is validated by PhiFirewall before write — if PII is
detected, the write is rejected with PhiDetectedError.
"""
from __future__ import annotations

from healthbot.data.clean_db.db_core import (
    CleanDBCore,
    EncryptionError,
    PhiDetectedError,
)
from healthbot.data.clean_db.demographics import DemographicsMixin
from healthbot.data.clean_db.health_context import HealthContextMixin
from healthbot.data.clean_db.hypotheses import HypothesesMixin
from healthbot.data.clean_db.medications import MedicationsMixin
from healthbot.data.clean_db.memory import MemoryMixin
from healthbot.data.clean_db.misc import MiscMixin
from healthbot.data.clean_db.observations import ObservationsMixin
from healthbot.data.clean_db.reporting import ReportingMixin
from healthbot.data.clean_db.search import SearchMixin
from healthbot.data.clean_db.wearables import WearablesMixin


class CleanDB(
    ReportingMixin,
    SearchMixin,
    MemoryMixin,
    HealthContextMixin,
    MiscMixin,
    HypothesesMixin,
    DemographicsMixin,
    WearablesMixin,
    MedicationsMixin,
    ObservationsMixin,
    CleanDBCore,
):
    """Anonymized health data store (Tier 2). Zero PII."""

    pass


__all__ = [
    "CleanDB",
    "PhiDetectedError",
    "EncryptionError",
]
