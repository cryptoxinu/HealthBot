"""Unified medical timeline — chronological view of all health events.

Aggregates lab results, medications, symptoms, wearable anomalies,
document uploads, hypotheses, and medical journal entries into a
single sorted timeline.

All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass(frozen=True, order=True)
class TimelineEvent:
    """A single event in the medical timeline.

    Sorted by date descending (most recent first).
    """

    sort_key: str = field(repr=False)  # ISO date for ordering
    date: str
    category: str          # "lab", "medication", "symptom", "wearable",
                           # "document", "hypothesis", "journal"
    title: str
    detail: str = ""
    severity: str = ""     # "normal", "watch", "urgent", "" for N/A
    source_id: str = ""    # obs_id, doc_id, etc.


# Categories users can filter by
TIMELINE_CATEGORIES = {
    "lab", "medication", "symptom", "wearable",
    "document", "hypothesis", "journal",
}


class MedicalTimeline:
    """Build a unified timeline from all health data sources."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def build(
        self,
        user_id: int,
        months: int = 12,
        categories: set[str] | None = None,
        limit: int = 200,
    ) -> list[TimelineEvent]:
        """Build a chronological timeline.

        Args:
            user_id: User to build timeline for.
            months: How far back to look (0 = all time).
            categories: Filter to specific categories (None = all).
            limit: Max events to return.

        Returns:
            List of TimelineEvent sorted by date descending.
        """
        cats = categories or TIMELINE_CATEGORIES
        events: list[TimelineEvent] = []

        cutoff = ""
        if months > 0:
            from datetime import timedelta
            cutoff = (date.today() - timedelta(days=months * 30)).isoformat()

        if "lab" in cats:
            events.extend(self._gather_labs(user_id, cutoff))
        if "symptom" in cats:
            events.extend(self._gather_symptoms(user_id, cutoff))
        if "medication" in cats:
            events.extend(self._gather_medications(user_id))
        if "wearable" in cats:
            events.extend(self._gather_wearable_anomalies(user_id, cutoff))
        if "document" in cats:
            events.extend(self._gather_documents(user_id, cutoff))
        if "hypothesis" in cats:
            events.extend(self._gather_hypotheses(user_id, cutoff))
        if "journal" in cats:
            events.extend(self._gather_journal(user_id, cutoff))

        # Sort descending by date, then by title for stable ordering
        events.sort(key=lambda e: e.sort_key, reverse=True)
        return events[:limit]

    def _gather_labs(
        self, user_id: int, cutoff: str,
    ) -> list[TimelineEvent]:
        """Gather lab result events."""
        kwargs: dict = {
            "record_type": "lab_result",
            "limit": 500,
            "user_id": user_id,
        }
        if cutoff:
            kwargs["start_date"] = cutoff
        rows = self._db.query_observations(**kwargs)
        events: list[TimelineEvent] = []
        for row in rows:
            dt = row.get("date_collected", "")
            if not dt:
                continue
            name = row.get("test_name", row.get("canonical_name", ""))
            val = row.get("value", "")
            unit = row.get("unit", "")
            flag = row.get("flag", "")
            triage = row.get("triage_level", "normal")

            flag_str = f" [{flag}]" if flag else ""
            detail = f"{val} {unit}{flag_str}".strip()

            ref_lo = row.get("reference_low", "")
            ref_hi = row.get("reference_high", "")
            if ref_lo and ref_hi:
                detail += f" (ref: {ref_lo}-{ref_hi})"

            severity = "normal"
            if triage in ("urgent", "critical", "emergency"):
                severity = "urgent"
            elif triage == "watch" or flag:
                severity = "watch"

            events.append(TimelineEvent(
                sort_key=dt,
                date=dt,
                category="lab",
                title=name,
                detail=detail,
                severity=severity,
                source_id=row.get("_meta", {}).get("obs_id", ""),
            ))
        return events

    def _gather_symptoms(
        self, user_id: int, cutoff: str,
    ) -> list[TimelineEvent]:
        """Gather user-logged symptom/event entries."""
        kwargs: dict = {
            "record_type": "user_event",
            "limit": 200,
            "user_id": user_id,
        }
        if cutoff:
            kwargs["start_date"] = cutoff
        rows = self._db.query_observations(**kwargs)
        events: list[TimelineEvent] = []
        for row in rows:
            dt = row.get("date_effective", "")
            if not dt:
                continue
            category_name = row.get("symptom_category", "general")
            text = row.get("cleaned_text", row.get("raw_text", ""))
            severity_str = row.get("severity", "")

            title = f"Symptom: {category_name}"
            detail = text
            if severity_str:
                detail = f"[{severity_str}] {text}"

            events.append(TimelineEvent(
                sort_key=dt,
                date=dt,
                category="symptom",
                title=title,
                detail=detail,
                severity="watch" if severity_str == "severe" else "normal",
            ))
        return events

    def _gather_medications(self, user_id: int) -> list[TimelineEvent]:
        """Gather medication start/stop events."""
        meds = self._db.get_active_medications(user_id=user_id)
        events: list[TimelineEvent] = []
        for med in meds:
            name = med.get("name", "Unknown")
            dose = med.get("dose", "")
            unit = med.get("unit", "")
            freq = med.get("frequency", "")
            dose_str = f" {dose} {unit}".strip() if dose else ""
            freq_str = f" ({freq})" if freq else ""

            start = med.get("start_date", "")
            if start:
                events.append(TimelineEvent(
                    sort_key=start,
                    date=start,
                    category="medication",
                    title=f"Started: {name}",
                    detail=f"{name}{dose_str}{freq_str}",
                ))

            end = med.get("end_date", "")
            if end:
                events.append(TimelineEvent(
                    sort_key=end,
                    date=end,
                    category="medication",
                    title=f"Stopped: {name}",
                    detail=f"Discontinued {name}",
                ))
        return events

    def _gather_wearable_anomalies(
        self, user_id: int, cutoff: str,
    ) -> list[TimelineEvent]:
        """Gather notable wearable data points (anomalies only)."""
        try:
            from healthbot.reasoning.wearable_trends import (
                WearableTrendAnalyzer,
            )
            analyzer = WearableTrendAnalyzer(self._db)
            anomalies = analyzer.detect_anomalies(
                days=365, user_id=user_id,
            )
        except Exception:
            return []

        events: list[TimelineEvent] = []
        for a in anomalies:
            dt = str(a.date) if hasattr(a, "date") else ""
            if not dt:
                continue
            if cutoff and dt < cutoff:
                continue
            events.append(TimelineEvent(
                sort_key=dt,
                date=dt,
                category="wearable",
                title=f"Anomaly: {a.display_name}",
                detail=a.message,
                severity=a.severity,
            ))
        return events

    def _gather_documents(
        self, user_id: int, cutoff: str,
    ) -> list[TimelineEvent]:
        """Gather document upload events."""
        docs = self._db.list_documents(user_id=user_id)
        events: list[TimelineEvent] = []
        for doc in docs:
            dt = doc.get("received_at", "")
            if not dt:
                continue
            # Normalize datetime to date-only for sorting
            dt_date = dt[:10] if len(dt) >= 10 else dt
            if cutoff and dt_date < cutoff:
                continue
            filename = doc.get("filename", "document")
            source = doc.get("source", "")
            pages = doc.get("page_count", 0)
            page_str = f", {pages} pages" if pages else ""
            events.append(TimelineEvent(
                sort_key=dt_date,
                date=dt_date,
                category="document",
                title=f"Uploaded: {filename}",
                detail=f"Source: {source}{page_str}",
                source_id=doc.get("doc_id", ""),
            ))
        return events

    def _gather_hypotheses(
        self, user_id: int, cutoff: str,
    ) -> list[TimelineEvent]:
        """Gather hypothesis creation/update events."""
        try:
            hyps = self._db.get_all_hypotheses(user_id)
        except Exception:
            return []
        events: list[TimelineEvent] = []
        for h in hyps:
            dt = h.get("_created_at", "")
            if not dt:
                continue
            dt_date = dt[:10] if len(dt) >= 10 else dt
            if cutoff and dt_date < cutoff:
                continue
            title_text = h.get("title", "Unknown")
            confidence = h.get("confidence", 0)
            status = h.get("status", "active")
            try:
                conf_pct = f"{float(confidence) * 100:.0f}%"
            except (ValueError, TypeError):
                conf_pct = "?"

            events.append(TimelineEvent(
                sort_key=dt_date,
                date=dt_date,
                category="hypothesis",
                title=f"Hypothesis: {title_text}",
                detail=f"Status: {status}, confidence: {conf_pct}",
            ))
        return events

    def _gather_journal(
        self, user_id: int, cutoff: str,
    ) -> list[TimelineEvent]:
        """Gather medical journal entries (permanent health record)."""
        entries = self._db.query_journal(user_id, limit=100)
        events: list[TimelineEvent] = []
        for entry in entries:
            dt = entry.get("_timestamp", "")
            if not dt:
                continue
            dt_date = dt[:10] if len(dt) >= 10 else dt
            if cutoff and dt_date < cutoff:
                continue
            speaker = entry.get("speaker", "")
            content = entry.get("content", "")
            category_name = entry.get("_category", "")

            # Truncate long content
            short = content[:120] + "..." if len(content) > 120 else content
            prefix = "You" if speaker == "user" else "HealthBot"
            cat_str = f" [{category_name}]" if category_name else ""

            events.append(TimelineEvent(
                sort_key=dt_date,
                date=dt_date,
                category="journal",
                title=f"{prefix}{cat_str}",
                detail=short,
            ))
        return events


def format_timeline(
    events: list[TimelineEvent],
    compact: bool = True,
) -> str:
    """Format timeline events for display.

    Args:
        events: Sorted list of TimelineEvent.
        compact: True for one-line-per-event, False for full detail.

    Returns:
        Formatted string.
    """
    if not events:
        return (
            "No events found in your medical timeline.\n\n"
            "Upload lab PDFs, log symptoms with /log, or sync wearables "
            "to build your timeline."
        )

    lines = ["MEDICAL TIMELINE", "-" * 30]
    current_date = ""

    severity_icon = {
        "urgent": "!",
        "watch": "~",
        "normal": " ",
        "": " ",
    }

    cat_icon = {
        "lab": "L",
        "medication": "M",
        "symptom": "S",
        "wearable": "W",
        "document": "D",
        "hypothesis": "H",
        "journal": "J",
    }

    for event in events:
        # Date header when date changes
        if event.date != current_date:
            current_date = event.date
            lines.append(f"\n  {current_date}")

        sev = severity_icon.get(event.severity, " ")
        cat = cat_icon.get(event.category, "?")

        lines.append(f"    {sev}[{cat}] {event.title}")
        if not compact and event.detail:
            lines.append(f"         {event.detail}")

    # Legend
    lines.append("\n" + "-" * 30)
    lines.append(
        "Legend: [L]ab [M]ed [S]ymptom [W]earable "
        "[D]oc [H]ypothesis [J]ournal"
    )
    lines.append("Severity: ! urgent  ~ watch")

    return "\n".join(lines)
