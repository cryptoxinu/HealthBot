"""Natural language date parsing.

Uses dateutil for structured dates and custom code for relative
and named date expressions. Returns date | None (never raises).

Also provides resolve_temporal() which extracts date ranges from
natural language queries for time-bounded health data filtering.
"""
from __future__ import annotations

import calendar
import re
from datetime import date, timedelta
from typing import TypedDict

# Weekday name -> weekday number (Monday=0 ... Sunday=6)
_WEEKDAY_MAP: dict[str, int] = {
    "monday": 0, "mon": 0,
    "tuesday": 1, "tue": 1, "tues": 1,
    "wednesday": 2, "wed": 2,
    "thursday": 3, "thu": 3, "thurs": 3,
    "friday": 4, "fri": 4,
    "saturday": 5, "sat": 5,
    "sunday": 6, "sun": 6,
}

# Relative day patterns
_RELATIVE_DAY_RE = re.compile(
    r"\b(\d+)\s+(days?|weeks?|months?)\s+ago\b", re.IGNORECASE
)

_LAST_WEEKDAY_RE = re.compile(
    r"\blast\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday"
    r"|mon|tue|tues|wed|thu|thurs|fri|sat|sun)\b",
    re.IGNORECASE,
)

_LAST_PERIOD_RE = re.compile(
    r"\blast\s+(week|month)\b", re.IGNORECASE
)


def _most_recent_weekday(target_weekday: int, today: date) -> date:
    """Find the most recent occurrence of a weekday before today."""
    days_back = (today.weekday() - target_weekday) % 7
    if days_back == 0:
        days_back = 7  # "last Tuesday" when today is Tuesday means 7 days ago
    return today - timedelta(days=days_back)


def _resolve_named_date(text: str, today: date) -> date | None:
    """Resolve named holidays to the most recent past occurrence."""
    lower = text.strip().lower()

    holidays: dict[str, tuple[int, int]] = {
        "christmas eve": (12, 24),
        "christmas": (12, 25),
        "xmas eve": (12, 24),
        "xmas": (12, 25),
        "new year's eve": (12, 31),
        "new years eve": (12, 31),
        "new year's": (1, 1),
        "new years": (1, 1),
        "new year's day": (1, 1),
        "new years day": (1, 1),
        "halloween": (10, 31),
        "valentine's day": (2, 14),
        "valentines day": (2, 14),
        "independence day": (7, 4),
        "fourth of july": (7, 4),
        "4th of july": (7, 4),
        "thanksgiving": (11, 0),  # Special: 4th Thursday of November
        "st patrick's day": (3, 17),
        "st patricks day": (3, 17),
        "memorial day": (5, 0),  # Special: last Monday of May
        "labor day": (9, 0),  # Special: 1st Monday of September
    }

    for name, (month, day) in holidays.items():
        if name not in lower:
            continue

        if name == "thanksgiving":
            return _nth_weekday(today, 11, 3, 4)  # 4th Thursday of November
        if name == "memorial day":
            return _last_weekday_of_month(today, 5, 0)  # Last Monday of May
        if name == "labor day":
            return _nth_weekday(today, 9, 0, 1)  # 1st Monday of September

        candidate = date(today.year, month, day)
        if candidate > today:
            candidate = date(today.year - 1, month, day)
        return candidate

    return None


def _nth_weekday(today: date, month: int, weekday: int, n: int) -> date:
    """Find the nth weekday of a given month, most recent past occurrence."""
    year = today.year
    for _ in range(2):
        first = date(year, month, 1)
        offset = (weekday - first.weekday()) % 7
        candidate = first + timedelta(days=offset + 7 * (n - 1))
        if candidate <= today:
            return candidate
        year -= 1
    return date(year, month, 1)  # fallback


def _last_weekday_of_month(today: date, month: int, weekday: int) -> date:
    """Find last weekday of month, most recent past occurrence."""
    year = today.year
    for _ in range(2):
        last_day = date(year, month, calendar.monthrange(year, month)[1])
        offset = (last_day.weekday() - weekday) % 7
        candidate = last_day - timedelta(days=offset)
        if candidate <= today:
            return candidate
        year -= 1
    return date(year, month, 1)  # fallback


def parse_date(text: str) -> date | None:
    """Parse a natural language date expression.

    Handles relative dates, named holidays, and structured dates.
    Returns None if unparseable. Never raises.
    """
    if not text or not text.strip():
        return None

    stripped = text.strip().lower()
    today = date.today()

    # --- Simple relative words (search within text) ---
    # "day before yesterday" must be checked before "yesterday"
    if re.search(r"\bday\s+before\s+yesterday\b", stripped):
        return today - timedelta(days=2)
    if re.search(r"\b(?:today|now)\b", stripped):
        return today
    if re.search(r"\byesterday\b", stripped):
        return today - timedelta(days=1)

    # --- "N days/weeks/months ago" ---
    m = _RELATIVE_DAY_RE.search(stripped)
    if m:
        amount = int(m.group(1))
        unit = m.group(2).lower().rstrip("s")
        if unit == "day":
            return today - timedelta(days=amount)
        if unit == "week":
            return today - timedelta(weeks=amount)
        if unit == "month":
            # Approximate: 30 days per month
            target_month = today.month - amount
            target_year = today.year
            while target_month < 1:
                target_month += 12
                target_year -= 1
            max_day = calendar.monthrange(target_year, target_month)[1]
            day = min(today.day, max_day)
            return date(target_year, target_month, day)
        return None

    # --- "last Tuesday" ---
    m = _LAST_WEEKDAY_RE.search(stripped)
    if m:
        day_name = m.group(1).lower()
        target_wd = _WEEKDAY_MAP.get(day_name)
        if target_wd is not None:
            return _most_recent_weekday(target_wd, today)

    # --- "last week" / "last month" ---
    m = _LAST_PERIOD_RE.search(stripped)
    if m:
        period = m.group(1).lower()
        if period == "week":
            return today - timedelta(weeks=1)
        if period == "month":
            target_month = today.month - 1
            target_year = today.year
            if target_month < 1:
                target_month = 12
                target_year -= 1
            max_day = calendar.monthrange(target_year, target_month)[1]
            day = min(today.day, max_day)
            return date(target_year, target_month, day)

    # --- Named holidays ---
    named = _resolve_named_date(stripped, today)
    if named is not None:
        return named

    # --- Structured dates via dateutil ---
    try:
        from dateutil.parser import parse as dateutil_parse

        result = dateutil_parse(text, fuzzy=True)
        parsed = result.date()
        # Validate: reject phantom dates from fuzzy parsing.
        # Dates before 1900 or more than 2 years in the future are almost
        # certainly artifacts of fuzzy matching on random text.
        if parsed.year < 1900:
            return None
        if parsed > today + timedelta(days=730):
            return None
        return parsed
    except Exception:
        return None


# ── Temporal query resolver ──────────────────────────────────────


class TemporalRange(TypedDict, total=False):
    """Result of resolve_temporal(). All fields optional."""

    start: str  # ISO date string (YYYY-MM-DD)
    end: str  # ISO date string (YYYY-MM-DD)
    direction: str  # "past" | "future"


# Month name → number
_MONTH_NAMES: dict[str, int] = {
    "january": 1, "jan": 1, "february": 2, "feb": 2,
    "march": 3, "mar": 3, "april": 4, "apr": 4,
    "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10, "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# Patterns for temporal phrases in queries
_LAST_N_RE = re.compile(
    r"\b(?:last|past|previous)\s+(\d+)\s+(days?|weeks?|months?|years?)\b",
    re.IGNORECASE,
)
_LAST_PERIOD_QUERY_RE = re.compile(
    r"\b(?:last|past|previous)\s+(week|month|year)\b", re.IGNORECASE,
)
_SINCE_RE = re.compile(
    r"\bsince\s+(\w+)(?:\s+(\d{4}))?\b", re.IGNORECASE,
)
_IN_MONTH_RE = re.compile(
    r"\bin\s+(january|february|march|april|may|june|july|august"
    r"|september|october|november|december"
    r"|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)"
    r"(?:\s+(\d{4}))?\b",
    re.IGNORECASE,
)
_N_AGO_RE = re.compile(
    r"\b(\d+)\s+(days?|weeks?|months?|years?)\s+ago\b", re.IGNORECASE,
)
_FUTURE_PERIOD_RE = re.compile(
    r"\b(?:next|coming)\s+(week|month|year|appointment)\b",
    re.IGNORECASE,
)
_FUTURE_STANDALONE_RE = re.compile(
    r"\b(?:upcoming|coming\s+up)\b",
    re.IGNORECASE,
)
_RECENTLY_RE = re.compile(
    r"\b(?:recently|recent|lately)\b", re.IGNORECASE,
)


def resolve_temporal(query: str) -> TemporalRange | None:
    """Extract a date range from a natural language query.

    Returns a TemporalRange dict with start/end ISO dates and direction,
    or None if no temporal phrase is detected. Never raises.

    Examples:
        "how were my labs last month?" → {"start": "2026-02-01", "end": "2026-02-28", ...}
        "labs since January"          → {"start": "2026-01-01", "end": "2026-03-01", ...}
        "results 2 weeks ago"         → {"start": "2026-02-15", "end": "2026-02-15", ...}
        "next appointment"            → {"direction": "future"}
    """
    if not query or not query.strip():
        return None

    text = query.strip().lower()
    today = date.today()

    # --- "last/past N days/weeks/months/years" ---
    m = _LAST_N_RE.search(text)
    if m:
        amount = int(m.group(1))
        unit = m.group(2).lower().rstrip("s")
        start = _subtract_unit(today, amount, unit)
        return {"start": start.isoformat(), "end": today.isoformat(), "direction": "past"}

    # --- "last/past week/month/year" (no number) ---
    m = _LAST_PERIOD_QUERY_RE.search(text)
    if m:
        period = m.group(1).lower()
        if period == "week":
            start = today - timedelta(weeks=1)
        elif period == "month":
            start = _subtract_unit(today, 1, "month")
        else:  # year
            start = _subtract_unit(today, 1, "year")
        return {"start": start.isoformat(), "end": today.isoformat(), "direction": "past"}

    # --- "N days/weeks/months ago" (point-in-time, use ±window) ---
    m = _N_AGO_RE.search(text)
    if m:
        amount = int(m.group(1))
        unit = m.group(2).lower().rstrip("s")
        point = _subtract_unit(today, amount, unit)
        # Use a ±3 day window around the point for fuzzy matching
        start = point - timedelta(days=3)
        end = point + timedelta(days=3)
        if end > today:
            end = today
        return {"start": start.isoformat(), "end": end.isoformat(), "direction": "past"}

    # --- "since January [2025]" ---
    m = _SINCE_RE.search(text)
    if m:
        month_num = _MONTH_NAMES.get(m.group(1).lower())
        if month_num:
            year = (int(m.group(2)) if m.group(2)
                    else _resolve_year(month_num, today, preposition="since"))
            start = date(year, month_num, 1)
            return {
                "start": start.isoformat(),
                "end": today.isoformat(),
                "direction": "past",
            }

    # --- "in March [2025]" ---
    m = _IN_MONTH_RE.search(text)
    if m:
        month_num = _MONTH_NAMES.get(m.group(1).lower())
        if month_num:
            year = (int(m.group(2)) if m.group(2)
                    else _resolve_year(month_num, today, preposition="in"))
            start = date(year, month_num, 1)
            last_day = calendar.monthrange(year, month_num)[1]
            end = date(year, month_num, last_day)
            if end > today:
                end = today
            return {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "direction": "past",
            }

    # --- "yesterday" ---
    if re.search(r"\byesterday\b", text):
        yesterday = today - timedelta(days=1)
        return {
            "start": yesterday.isoformat(),
            "end": yesterday.isoformat(),
            "direction": "past",
        }

    # --- "recently" / "recent" / "lately" ---
    if _RECENTLY_RE.search(text):
        start = today - timedelta(days=14)
        return {"start": start.isoformat(), "end": today.isoformat(), "direction": "past"}

    # --- Forward-looking: "next week/month/appointment" ---
    m = _FUTURE_PERIOD_RE.search(text)
    if m:
        period = m.group(1).lower()
        if period == "week":
            end = today + timedelta(weeks=1)
            return {
                "start": today.isoformat(),
                "end": end.isoformat(),
                "direction": "future",
            }
        if period == "month":
            end_month = today.month + 1
            end_year = today.year
            if end_month > 12:
                end_month = 1
                end_year += 1
            last_day = calendar.monthrange(end_year, end_month)[1]
            end = date(end_year, end_month, last_day)
            return {
                "start": today.isoformat(),
                "end": end.isoformat(),
                "direction": "future",
            }
        # "next appointment"
        return {"direction": "future"}

    # --- "upcoming" / "coming up" (standalone) ---
    if _FUTURE_STANDALONE_RE.search(text):
        return {"direction": "future"}

    return None


def _subtract_unit(today: date, amount: int, unit: str) -> date:
    """Subtract N days/weeks/months/years from today."""
    if unit == "day":
        return today - timedelta(days=amount)
    if unit == "week":
        return today - timedelta(weeks=amount)
    if unit == "month":
        target_month = today.month - amount
        target_year = today.year
        while target_month < 1:
            target_month += 12
            target_year -= 1
        max_day = calendar.monthrange(target_year, target_month)[1]
        day = min(today.day, max_day)
        return date(target_year, target_month, day)
    if unit == "year":
        try:
            return today.replace(year=today.year - amount)
        except ValueError:
            # Feb 29 in non-leap year
            return today.replace(year=today.year - amount, day=28)
    return today


def _resolve_year(
    month_num: int, today: date, preposition: str = "in",
) -> int:
    """Resolve which year a bare month name refers to.

    Uses the preposition to disambiguate:
    - "since March" always refers to the past (last year if month > current)
    - "in March" refers to the current year if the month hasn't passed,
      otherwise last year (assumes the most recent occurrence)
    """
    preposition = preposition.lower().strip()
    if preposition == "since":
        # "since" always means past — if month hasn't occurred yet, last year
        if month_num > today.month:
            return today.year - 1
        return today.year
    # "in" — default: assume most recent past occurrence
    if month_num > today.month:
        return today.year - 1
    return today.year
