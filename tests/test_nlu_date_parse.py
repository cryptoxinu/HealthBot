"""Tests for natural language date parsing."""
from __future__ import annotations

from datetime import date, timedelta

from healthbot.nlu.date_parse import parse_date


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
