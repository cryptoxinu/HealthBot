"""Data models for the Telegram PDF ingestion pipeline."""
from __future__ import annotations

from dataclasses import dataclass, field

from healthbot.data.models import LabResult


@dataclass
class IngestResult:
    blob_id: str = ""
    doc_id: str = ""
    lab_results: list[LabResult] = field(default_factory=list)
    triage_summary: str = ""
    quality_summary: str = ""
    clinical_summary: str = ""
    clinical_facts_count: int = 0
    clinical_pii_blocked: int = 0
    doc_type: str = ""
    redacted_blob_id: str = ""
    warnings: list[str] = field(default_factory=list)
    missing_date: bool = False
    success: bool = False
    is_duplicate: bool = False
    is_rescan: bool = False
    rescan_new: int = 0
    rescan_existing: int = 0
    cross_doc_dupes: int = 0
    alerts: list = field(default_factory=list)
