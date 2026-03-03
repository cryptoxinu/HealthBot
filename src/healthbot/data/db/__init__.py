"""Encrypted database operations.

Wraps sqlite3 with field-level AES-256-GCM encryption for sensitive data.
Each encrypted field uses AAD (Additional Authenticated Data) including
table name, column name, and row ID to prevent ciphertext swapping.

This package splits the monolithic HealthDB class into domain-specific
mixin modules while preserving full backward compatibility:

    from healthbot.data.db import HealthDB

continues to work with all public methods available.
"""
from healthbot.data.db.db_core import HealthDBCore
from healthbot.data.db.documents import DocumentsMixin
from healthbot.data.db.genetics import GeneticsMixin
from healthbot.data.db.identity import IdentityMixin
from healthbot.data.db.medications import MedicationsMixin
from healthbot.data.db.misc import MiscMixin
from healthbot.data.db.observations import ObservationsMixin
from healthbot.data.db.providers import ProvidersMixin
from healthbot.data.db.search_index import SearchIndexMixin
from healthbot.data.db.wearables import WearablesMixin
from healthbot.data.db.workouts import WorkoutsMixin


class HealthDB(
    DocumentsMixin,
    ObservationsMixin,
    MedicationsMixin,
    WearablesMixin,
    WorkoutsMixin,
    GeneticsMixin,
    ProvidersMixin,
    IdentityMixin,
    SearchIndexMixin,
    MiscMixin,
    HealthDBCore,
):
    """Encrypted health data database."""

    pass


__all__ = ["HealthDB"]
