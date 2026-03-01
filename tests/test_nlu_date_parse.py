"""Tests for natural language date parsing."""
from __future__ import annotations

from datetime import date, timedelta

from healthbot.nlu.date_parse import parse_date, resolve_temporal


class TestRelativeDates:
    """Test relative date expressions."""

    def test_today(self) -> None:
        assert parse_date("today") == date.today()

    def test_now(self) -> None:
        assert parse_date("now") == date.today()

    def test_yesterday(self) -> None:
        assert parse_date("yesterday") == date.today() - timedelta(days=1)

    def test_day_before_yesterday(self) -> None:
        assert parse_date("day before yesterday") == date.today() - timedelta(days=2)

    def test_days_ago(self) -> None:
        assert parse_date("3 days ago") == date.today() - timedelta(days=3)

    def test_one_day_ago(self) -> None:
        assert parse_date("1 day ago") == date.today() - timedelta(days=1)

    def test_weeks_ago(self) -> None:
        assert parse_date("2 weeks ago") == date.today() - timedelta(weeks=2)

    def test_months_ago(self) -> None:
        result = parse_date("1 month ago")
        assert result is not None
        # Should be roughly 30 days ago (within the previous month)
        today = date.today()
        expected_month = today.month - 1 if today.month > 1 else 12
        assert result.month == expected_month

    def test_last_week(self) -> None:
        assert parse_date("last week") == date.today() - timedelta(weeks=1)

    def test_last_month(self) -> None:
        result = parse_date("last month")
        assert result is not None
        today = date.today()
        expected_month = today.month - 1 if today.month > 1 else 12
        assert result.month == expected_month


class TestLastWeekday:
    """Test 'last [weekday]' expressions."""

    def test_last_tuesday(self) -> None:
        result = parse_date("last Tuesday")
        assert result is not None
        today = date.today()
        assert result < today
        assert result.weekday() == 1  # Tuesday

    def test_last_monday(self) -> None:
        result = parse_date("last Monday")
        assert result is not None
        assert result.weekday() == 0  # Monday
        assert result < date.today()

    def test_last_friday(self) -> None:
        result = parse_date("last Friday")
        assert result is not None
        assert result.weekday() == 4  # Friday
        assert result < date.today()

    def test_last_weekday_never_returns_today(self) -> None:
        """'last X' when today IS X should return 7 days ago, not today."""
        today = date.today()
        weekday_name = [
            "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Saturday", "Sunday",
        ][today.weekday()]
        result = parse_date(f"last {weekday_name}")
        assert result is not None
        assert result == today - timedelta(days=7)


class TestNamedDates:
    """Test named holiday resolution."""

    def test_christmas(self) -> None:
        result = parse_date("christmas")
        assert result is not None
        assert result.month == 12
        assert result.day == 25

    def test_christmas_eve(self) -> None:
        result = parse_date("christmas eve")
        assert result is not None
        assert result.month == 12
        assert result.day == 24

    def test_halloween(self) -> None:
        result = parse_date("halloween")
        assert result is not None
        assert result.month == 10
        assert result.day == 31

    def test_new_years_day(self) -> None:
        result = parse_date("new year's day")
        assert result is not None
        assert result.month == 1
        assert result.day == 1

    def test_new_years_eve(self) -> None:
        result = parse_date("new year's eve")
        assert result is not None
        assert result.month == 12
        assert result.day == 31

    def test_named_date_resolves_to_past(self) -> None:
        """Named dates should never return a future date."""
        result = parse_date("christmas")
        assert result is not None
        assert result <= date.today()


class TestStructuredDates:
    """Test structured date parsing via dateutil."""

    def test_iso_format(self) -> None:
        result = parse_date("2024-03-15")
        assert result == date(2024, 3, 15)

    def test_us_format(self) -> None:
        result = parse_date("March 15, 2024")
        assert result == date(2024, 3, 15)

    def test_slash_format(self) -> None:
        result = parse_date("03/15/2024")
        assert result == date(2024, 3, 15)

    def test_fuzzy_date_in_sentence(self) -> None:
        result = parse_date("it happened on January 5th 2024")
        assert result is not None
        assert result.month == 1
        assert result.day == 5


class TestEdgeCases:
    """Test edge cases and invalid input."""

    def test_empty_string(self) -> None:
        assert parse_date("") is None

    def test_none_like_empty(self) -> None:
        assert parse_date("   ") is None

    def test_nonsense(self) -> None:
        assert parse_date("not a date at all xyz") is None

    def test_abbreviations(self) -> None:
        result = parse_date("last fri")
        assert result is not None
        assert result.weekday() == 4  # Friday


class TestResolveTemporalPast:
    """Test resolve_temporal() for past-looking queries."""

    def test_last_month_range(self) -> None:
        result = resolve_temporal("how were my labs last month?")
        assert result is not None
        assert result["direction"] == "past"
        start = date.fromisoformat(result["start"])
        end = date.fromisoformat(result["end"])
        assert end == date.today()
        # Start should be roughly one month ago
        today = date.today()
        expected_month = today.month - 1 if today.month > 1 else 12
        assert start.month == expected_month

    def test_last_week_range(self) -> None:
        result = resolve_temporal("labs from last week")
        assert result is not None
        assert result["direction"] == "past"
        start = date.fromisoformat(result["start"])
        assert start == date.today() - timedelta(weeks=1)

    def test_last_n_days(self) -> None:
        result = resolve_temporal("results from the last 30 days")
        assert result is not None
        start = date.fromisoformat(result["start"])
        assert start == date.today() - timedelta(days=30)

    def test_past_3_months(self) -> None:
        result = resolve_temporal("trends over the past 3 months")
        assert result is not None
        assert result["direction"] == "past"
        start = date.fromisoformat(result["start"])
        today = date.today()
        # 3 months back
        target_month = today.month - 3
        target_year = today.year
        while target_month < 1:
            target_month += 12
            target_year -= 1
        assert start.month == target_month
        assert start.year == target_year

    def test_n_months_ago(self) -> None:
        result = resolve_temporal("labs from 2 months ago")
        assert result is not None
        assert result["direction"] == "past"
        # Should return a window around the point
        start = date.fromisoformat(result["start"])
        end = date.fromisoformat(result["end"])
        assert start < end

    def test_since_month_name(self) -> None:
        result = resolve_temporal("labs since January")
        assert result is not None
        start = date.fromisoformat(result["start"])
        assert start.month == 1
        assert start.day == 1
        assert result["end"] == date.today().isoformat()

    def test_since_month_with_year(self) -> None:
        result = resolve_temporal("labs since March 2025")
        assert result is not None
        start = date.fromisoformat(result["start"])
        assert start == date(2025, 3, 1)

    def test_in_month(self) -> None:
        result = resolve_temporal("my labs in January")
        assert result is not None
        start = date.fromisoformat(result["start"])
        end = date.fromisoformat(result["end"])
        assert start.month == 1
        assert start.day == 1
        # End should be last day of January (or today if in the future)
        today = date.today()
        year = today.year if 1 <= today.month else today.year - 1
        expected_end = min(date(year, 1, 31), today)
        assert end == expected_end

    def test_in_month_with_year(self) -> None:
        result = resolve_temporal("labs in March 2025")
        assert result is not None
        start = date.fromisoformat(result["start"])
        assert start == date(2025, 3, 1)
        end = date.fromisoformat(result["end"])
        assert end.month == 3
        assert end.day == 31

    def test_yesterday(self) -> None:
        result = resolve_temporal("what were my labs yesterday")
        assert result is not None
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        assert result["start"] == yesterday
        assert result["direction"] == "past"

    def test_recently(self) -> None:
        result = resolve_temporal("any recent lab changes?")
        assert result is not None
        start = date.fromisoformat(result["start"])
        assert start == date.today() - timedelta(days=14)
        assert result["direction"] == "past"

    def test_last_year(self) -> None:
        result = resolve_temporal("trends from last year")
        assert result is not None
        start = date.fromisoformat(result["start"])
        today = date.today()
        assert start.year == today.year - 1


class TestResolveTemporalFuture:
    """Test resolve_temporal() for forward-looking queries."""

    def test_next_week(self) -> None:
        result = resolve_temporal("what should I do next week")
        assert result is not None
        assert result["direction"] == "future"
        end = date.fromisoformat(result["end"])
        assert end == date.today() + timedelta(weeks=1)

    def test_next_appointment(self) -> None:
        result = resolve_temporal("when is my next appointment")
        assert result is not None
        assert result["direction"] == "future"

    def test_upcoming(self) -> None:
        result = resolve_temporal("upcoming appointments")
        assert result is not None
        assert result["direction"] == "future"


class TestResolveTemporalNoMatch:
    """Test that non-temporal queries return None."""

    def test_plain_question(self) -> None:
        assert resolve_temporal("what is my cholesterol level") is None

    def test_empty(self) -> None:
        assert resolve_temporal("") is None

    def test_whitespace(self) -> None:
        assert resolve_temporal("   ") is None

    def test_no_temporal_signal(self) -> None:
        assert resolve_temporal("explain my liver panel results") is None
