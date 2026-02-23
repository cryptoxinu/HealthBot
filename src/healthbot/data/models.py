"""Domain models as dataclasses.

These represent the in-memory, decrypted form of health records.
They are never serialized to disk unencrypted.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum


class RecordType(Enum):
    LAB_RESULT = "lab_result"
    MEDICATION = "medication"
    VITAL_SIGN = "vital_sign"
    APPOINTMENT = "appointment"
    NOTE = "note"
    APPLE_HEALTH = "apple_health"
    WHOOP_RECOVERY = "whoop_recovery"
    WHOOP_SLEEP = "whoop_sleep"
    WHOOP_WORKOUT = "whoop_workout"
    CONCERN = "concern"


class TriageLevel(Enum):
    NORMAL = "normal"
    WATCH = "watch"
    URGENT = "urgent"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


@dataclass
class LabResult:
    id: str
    test_name: str
    canonical_name: str = ""
    value: float | str = ""
    unit: str = ""
    reference_low: float | None = None
    reference_high: float | None = None
    reference_text: str = ""
    date_collected: date | None = None
    date_reported: date | None = None
    lab_name: str = ""
    ordering_provider: str = ""
    specimen: str = ""
    fasting: bool | None = None
    confidence: float = 1.0
    source_blob_id: str = ""
    source_page: int = 0
    source_section: str = ""
    triage_level: TriageLevel = TriageLevel.NORMAL
    flag: str = ""  # "H", "L", "HH", "LL", etc.


@dataclass
class Medication:
    id: str
    name: str
    brand_name: str = ""
    dose: str = ""
    unit: str = ""
    route: str = ""
    frequency: str = ""
    prescriber: str = ""
    start_date: date | None = None
    end_date: date | None = None
    status: str = "active"
    source_blob_id: str = ""
    source_page: int = 0


@dataclass
class VitalSign:
    id: str
    type: str
    value: str
    unit: str
    timestamp: datetime | None = None
    source: str = ""
    source_blob_id: str = ""


@dataclass
class WhoopDaily:
    id: str
    date: date
    provider: str = "whoop"
    hrv: float | None = None
    rhr: float | None = None
    resp_rate: float | None = None
    spo2: float | None = None
    skin_temp: float | None = None
    sleep_score: float | None = None
    recovery_score: float | None = None
    strain: float | None = None
    sleep_duration_min: int | None = None
    rem_min: int | None = None
    deep_min: int | None = None
    light_min: int | None = None
    calories: float | None = None
    # Phase O: enhanced WHOOP data
    sleep_latency_min: float | None = None
    wake_episodes: int | None = None
    sleep_efficiency_pct: float | None = None
    workout_sport_name: str | None = None
    workout_avg_hr: float | None = None
    workout_max_hr: float | None = None


@dataclass
class Workout:
    id: str
    sport_type: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    duration_minutes: float | None = None
    distance_km: float | None = None
    calories_burned: float | None = None
    avg_heart_rate: float | None = None
    max_heart_rate: float | None = None
    min_heart_rate: float | None = None
    source: str = "apple_health"


@dataclass
class Concern:
    id: str
    title: str
    severity: TriageLevel = TriageLevel.WATCH
    evidence: list[dict] = field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    status: str = "active"


@dataclass
class ExternalEvidence:
    id: str
    source: str  # "pubmed", "claude_cli"
    query_hash: str
    prompt_sanitized: str
    result_json: dict = field(default_factory=dict)
    created_at: datetime | None = None


@dataclass
class Document:
    id: str
    source: str  # "telegram_pdf", "apple_health", "whoop", "mychart"
    sha256: str
    received_at: datetime | None = None
    enc_blob_path: str = ""
    filename: str = ""
    mime_type: str = ""
    size_bytes: int = 0
    page_count: int = 0
    meta: dict = field(default_factory=dict)


@dataclass
class Citation:
    record_id: str
    source_type: str
    source_blob_id: str
    page_number: int = 0
    section: str = ""
    date_collected: str = ""
    lab_or_provider: str = ""

    def format(self) -> str:
        """Format for display: [Source, Provider, Date, p.X]"""
        parts = [self.source_type]
        if self.lab_or_provider:
            parts.append(self.lab_or_provider)
        if self.date_collected:
            parts.append(self.date_collected)
        if self.page_number:
            parts.append(f"p.{self.page_number}")
        if self.section:
            parts.append(self.section)
        return f"[{', '.join(parts)}]"


@dataclass
class MemoryEntry:
    """A single memory entry (STM conversation message or LTM fact)."""

    id: str
    user_id: int
    role: str  # 'user'/'assistant' (STM) or category (LTM)
    content: str
    created_at: datetime | None = None
    consolidated: bool = False
    source: str = ""  # 'conversation', 'lab_ingestion', 'consolidation'


@dataclass
class Hypothesis:
    """A medical hypothesis being tracked with evidence."""

    id: str
    user_id: int
    title: str  # "POTS (Postural Orthostatic Tachycardia Syndrome)"
    confidence: float = 0.0  # 0.0 - 1.0
    evidence_for: list[str] = field(default_factory=list)
    evidence_against: list[str] = field(default_factory=list)
    missing_tests: list[str] = field(default_factory=list)
    notes: str = ""
    status: str = "active"  # 'active', 'confirmed', 'ruled_out'
    created_at: datetime | None = None
    updated_at: datetime | None = None
