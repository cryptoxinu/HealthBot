"""Tests for overdue notification pause state management."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from healthbot.bot.overdue_pause import (
    get_pause_until,
    is_overdue_paused,
    parse_duration,
    unpause_overdue,
)
from healthbot.bot.overdue_pause import pause_overdue as do_pause


@pytest.fixture()
def config(tmp_path):
    """Config mock with vault_home pointing to tmp_path."""
    cfg = MagicMock()
    cfg.vault_home = tmp_path
    (tmp_path / "config").mkdir()
    return cfg


class TestIsOverduePaused:
    def test_no_file_returns_false(self, config):
        assert is_overdue_paused(config) is False

    def test_active_pause_returns_true(self, config):
        deadline = (datetime.now(UTC) + timedelta(days=7)).isoformat()
        pause_file = config.vault_home / "config" / "overdue_pause.json"
        pause_file.write_text(json.dumps({"paused_until": deadline}))
        assert is_overdue_paused(config) is True

    def test_expired_pause_returns_false_and_cleans_up(self, config):
        deadline = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        pause_file = config.vault_home / "config" / "overdue_pause.json"
        pause_file.write_text(json.dumps({"paused_until": deadline}))
        assert is_overdue_paused(config) is False
        assert not pause_file.exists()

    def test_null_paused_until_returns_false(self, config):
        pause_file = config.vault_home / "config" / "overdue_pause.json"
        pause_file.write_text(json.dumps({"paused_until": None}))
        assert is_overdue_paused(config) is False

    def test_corrupted_file_returns_false(self, config):
        pause_file = config.vault_home / "config" / "overdue_pause.json"
        pause_file.write_text("NOT VALID JSON {{{")
        assert is_overdue_paused(config) is False


class TestGetPauseUntil:
    def test_no_file_returns_none(self, config):
        assert get_pause_until(config) is None

    def test_active_pause_returns_deadline(self, config):
        deadline = datetime.now(UTC) + timedelta(days=3)
        do_pause(config, timedelta(days=3))
        result = get_pause_until(config)
        assert result is not None
        assert abs((result - deadline).total_seconds()) < 2

    def test_expired_returns_none(self, config):
        deadline = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        pause_file = config.vault_home / "config" / "overdue_pause.json"
        pause_file.write_text(json.dumps({"paused_until": deadline}))
        assert get_pause_until(config) is None


class TestPauseOverdue:
    def test_creates_file_with_deadline(self, config):
        deadline = do_pause(config, timedelta(weeks=2))
        pause_file = config.vault_home / "config" / "overdue_pause.json"
        assert pause_file.exists()
        data = json.loads(pause_file.read_text())
        stored = datetime.fromisoformat(data["paused_until"])
        assert abs((stored - deadline).total_seconds()) < 1

    def test_deadline_in_future(self, config):
        deadline = do_pause(config, timedelta(hours=6))
        assert deadline > datetime.now(UTC)


class TestUnpauseOverdue:
    def test_removes_file_returns_true(self, config):
        do_pause(config, timedelta(days=5))
        assert unpause_overdue(config) is True
        assert not (config.vault_home / "config" / "overdue_pause.json").exists()

    def test_no_file_returns_false(self, config):
        assert unpause_overdue(config) is False


class TestParseDuration:
    def test_hours(self):
        assert parse_duration("3 hours") == timedelta(hours=3)

    def test_hours_abbrev(self):
        assert parse_duration("12 hrs") == timedelta(hours=12)

    def test_days(self):
        assert parse_duration("5 days") == timedelta(days=5)

    def test_day_singular(self):
        assert parse_duration("1 day") == timedelta(days=1)

    def test_weeks(self):
        assert parse_duration("2 weeks") == timedelta(weeks=2)

    def test_weeks_abbrev(self):
        assert parse_duration("4 wks") == timedelta(weeks=4)

    def test_months(self):
        assert parse_duration("1 month") == timedelta(days=30)

    def test_months_abbrev(self):
        assert parse_duration("3 mons") == timedelta(days=90)

    def test_invalid_returns_none(self):
        assert parse_duration("foo bar baz") is None

    def test_zero_returns_none(self):
        assert parse_duration("0 days") is None

    def test_embedded_in_sentence(self):
        assert parse_duration("pause for 2 weeks please") == timedelta(weeks=2)
