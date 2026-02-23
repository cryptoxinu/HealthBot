"""Symptom frequency, trending, and correlation analytics.

Analyzes logged symptom events for patterns. Deterministic — no LLM.
"""
from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")

# All known symptom categories (from event_logger.py)
SYMPTOM_CATEGORIES = (
    "dizziness", "headache", "fatigue", "pain", "nausea",
    "sleep", "mood", "digestive", "heart", "general",
)


@dataclass
class SymptomFrequency:
    """Frequency summary for a symptom category."""

    category: str
    total_count: int
    weeks_active: int
    avg_per_week: float
    severities: dict[str, int] = field(default_factory=dict)
    most_recent: str = ""


@dataclass
class SymptomOverview:
    """Overview of all symptom categories."""

    categories: list[SymptomFrequency] = field(default_factory=list)
    total_events: int = 0
    days_covered: int = 90


class SymptomAnalyzer:
    """Analyze logged symptom events for patterns."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def overview(
        self, user_id: int, days: int = 90,
    ) -> SymptomOverview:
        """Get overview of all symptom categories in the last N days."""
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        events = self._db.query_observations(
            record_type="user_event",
            start_date=cutoff,
            limit=500,
            user_id=user_id,
        )

        # Group by category
        by_cat: dict[str, list[dict]] = {}
        for ev in events:
            cat = ev.get("symptom_category", ev.get("canonical_name", "general"))
            by_cat.setdefault(cat, []).append(ev)

        categories: list[SymptomFrequency] = []
        for cat, cat_events in sorted(by_cat.items(), key=lambda x: -len(x[1])):
            freq = self._compute_frequency(cat, cat_events, days)
            categories.append(freq)

        return SymptomOverview(
            categories=categories,
            total_events=len(events),
            days_covered=days,
        )

    def frequency(
        self, user_id: int, category: str, days: int = 90,
    ) -> SymptomFrequency | None:
        """Get detailed frequency for a specific symptom category."""
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        events = self._db.query_observations(
            record_type="user_event",
            canonical_name=category,
            start_date=cutoff,
            limit=500,
            user_id=user_id,
        )
        if not events:
            return None

        return self._compute_frequency(category, events, days)

    def _compute_frequency(
        self, category: str, events: list[dict], days: int,
    ) -> SymptomFrequency:
        """Compute frequency stats from a list of events."""
        total = len(events)
        weeks = max(days / 7, 1)

        # Count severities
        severities: Counter[str] = Counter()
        dates_seen: set[str] = set()
        most_recent = ""

        for ev in events:
            sev = ev.get("severity", "")
            if sev:
                severities[sev] += 1
            dt = ev.get("date_effective", ev.get("_meta", {}).get("date_effective", ""))
            if dt:
                dates_seen.add(dt[:10])  # YYYY-MM-DD
                if not most_recent or dt > most_recent:
                    most_recent = dt[:10]

        # Count unique weeks with at least one event
        week_set: set[str] = set()
        for dt_str in dates_seen:
            try:
                d = date.fromisoformat(dt_str)
                week_set.add(d.strftime("%G-W%V"))
            except ValueError:
                pass

        return SymptomFrequency(
            category=category,
            total_count=total,
            weeks_active=len(week_set),
            avg_per_week=round(total / weeks, 1),
            severities=dict(severities),
            most_recent=most_recent,
        )


def format_overview(overview: SymptomOverview) -> str:
    """Format symptom overview for display."""
    if not overview.categories:
        return f"No symptoms logged in the last {overview.days_covered} days."

    lines = [
        f"Symptom Overview (last {overview.days_covered} days, "
        f"{overview.total_events} total events):\n",
    ]
    for freq in overview.categories:
        sev_parts = []
        for s in ("severe", "moderate", "mild"):
            if s in freq.severities:
                sev_parts.append(f"{freq.severities[s]} {s}")
        sev_str = f" ({', '.join(sev_parts)})" if sev_parts else ""
        lines.append(
            f"  {freq.category}: {freq.total_count}x "
            f"({freq.avg_per_week}/week){sev_str}"
        )
        if freq.most_recent:
            lines.append(f"    Last: {freq.most_recent}")

    return "\n".join(lines)


def format_frequency(freq: SymptomFrequency) -> str:
    """Format detailed frequency for a single category."""
    lines = [
        f"{freq.category.title()} — Detailed Analysis:\n",
        f"  Total events: {freq.total_count}",
        f"  Average: {freq.avg_per_week}/week",
        f"  Active weeks: {freq.weeks_active}",
    ]
    if freq.severities:
        sev_parts = [f"{v}x {k}" for k, v in sorted(freq.severities.items())]
        lines.append(f"  Severity breakdown: {', '.join(sev_parts)}")
    if freq.most_recent:
        lines.append(f"  Most recent: {freq.most_recent}")

    return "\n".join(lines)
