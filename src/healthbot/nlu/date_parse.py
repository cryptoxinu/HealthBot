"""Natural language date parsing.

Uses dateutil for structured dates and custom code for relative
and named date expressions. Returns date | None (never raises).
"""
from __future__ import annotations

import calendar
import re
from datetime import date, timedelta

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
        return result.date()
    except Exception:
        return None
