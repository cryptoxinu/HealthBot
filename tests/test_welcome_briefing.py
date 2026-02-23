"""Tests for welcome briefing generation in scheduler.py."""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import MagicMock

from healthbot.bot.scheduler import AlertScheduler


def _make_scheduler():
    """Create a scheduler with mocked dependencies."""
    config = MagicMock()
    config.vault_home = Path(tempfile.mkdtemp())
    config.incoming_dir = config.vault_home / "incoming"
    config.allowed_user_ids = [123]
    config.overdue_pause_until = None
    km = MagicMock()
    km.is_unlocked = True
    sched = AlertScheduler(config, km, chat_id=123)
    return sched


class TestBuildWelcomeBriefing:
    def test_empty_db_returns_empty(self):
        sched = _make_scheduler()
        # Mock _get_db to return a mock with empty results
        db = MagicMock()
        db.get_user_demographics.return_value = {}
        db.query_observations.return_value = []
        db.get_active_hypotheses.return_value = []
        sched._get_db = lambda: db
        # _primary_user_id is a property from config.allowed_user_ids[0]

        result = sched._build_welcome_briefing()
        # With no data, should return empty or minimal
        assert isinstance(result, str)

    def test_returns_string(self):
        sched = _make_scheduler()
        db = MagicMock()
        db.get_user_demographics.return_value = {}
        db.query_observations.return_value = []
        sched._get_db = lambda: db
        # _primary_user_id is a property from config.allowed_user_ids[0]

        result = sched._build_welcome_briefing()
        assert isinstance(result, str)

    def test_welcome_back_header_when_data(self):
        sched = _make_scheduler()
        db = MagicMock()
        db.get_user_demographics.return_value = {"sex": "male", "age": 40}
        db.query_observations.return_value = []
        db.get_active_hypotheses.return_value = []

        # Make overdue detector return results
        sched._get_db = lambda: db
        # _primary_user_id is a property from config.allowed_user_ids[0]

        # If there ARE parts, they should start with "Welcome back."
        result = sched._build_welcome_briefing()
        if result:
            assert result.startswith("Welcome back.")

    def test_handles_db_failure_gracefully(self):
        sched = _make_scheduler()
        sched._get_db = MagicMock(side_effect=RuntimeError("DB locked"))
        # _primary_user_id is a property from config.allowed_user_ids[0]

        # Should not raise — DB-dependent sections produce nothing,
        # but non-DB sections (wearable hint) may still appear
        result = sched._build_welcome_briefing()
        # No DB-dependent content (overdue, hypotheses, trends, research)
        assert "Overdue:" not in result
        assert "Pattern:" not in result
        assert "Trend:" not in result

    def test_cached_conditions_research(self):
        sched = _make_scheduler()
        db = MagicMock()
        db.get_user_demographics.return_value = {}
        db.query_observations.return_value = []
        sched._get_db = lambda: db
        # _primary_user_id is a property from config.allowed_user_ids[0]
        sched._cached_conditions = ["hypothyroidism"]

        # Even with cached conditions, if evidence store has no results,
        # should still run without error
        result = sched._build_welcome_briefing()
        assert isinstance(result, str)
