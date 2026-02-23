"""Tests for /profile command and profile_radar_chart."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FakeDomainScore:
    domain: str
    label: str
    score: float
    tests_found: int
    tests_total: int
    issues: list[str]


class TestProfileRadarChart:
    def test_generates_png(self):
        from healthbot.export.chart_generator import profile_radar_chart

        scores = [
            FakeDomainScore("metabolic", "Metabolic", 85, 3, 5, []),
            FakeDomainScore("cardiovascular", "Cardiovascular", 70, 2, 4, []),
            FakeDomainScore("blood", "Blood", 90, 4, 5, []),
            FakeDomainScore("liver", "Liver", 60, 2, 5, []),
            FakeDomainScore("thyroid", "Thyroid", 50, 1, 3, []),
            FakeDomainScore("nutrition", "Nutrition", 75, 3, 5, []),
            FakeDomainScore("inflammation", "Inflammation", 95, 2, 2, []),
        ]
        result = profile_radar_chart(scores)
        assert result is not None
        assert isinstance(result, bytes)
        assert len(result) > 1000  # Should be a real PNG
        assert result[:4] == b"\x89PNG"

    def test_returns_none_for_empty(self):
        from healthbot.export.chart_generator import profile_radar_chart

        assert profile_radar_chart([]) is None

    def test_returns_none_for_too_few(self):
        from healthbot.export.chart_generator import profile_radar_chart

        scores = [
            FakeDomainScore("a", "A", 50, 1, 1, []),
            FakeDomainScore("b", "B", 60, 1, 1, []),
        ]
        assert profile_radar_chart(scores) is None


class TestProfileHandler:
    """Basic tests for profile handler registration."""

    def test_profile_in_app(self):
        """Verify profile command is registered."""
        from healthbot.bot.handlers import Handlers

        assert hasattr(Handlers, "profile")

    def test_profile_handler_callable(self):
        """Handler is an async method."""
        import inspect

        from healthbot.bot.handlers import Handlers

        assert inspect.iscoroutinefunction(Handlers.profile)

    def test_hypotheses_in_app(self):
        """Verify hypotheses command is registered."""
        from healthbot.bot.handlers import Handlers

        assert hasattr(Handlers, "hypotheses")

    def test_help_text_includes_profile(self):
        """The /help text should mention /profile."""
        import inspect

        from healthbot.bot.handlers_session import SessionHandlers

        src = inspect.getsource(SessionHandlers.help_cmd)
        assert "/profile" in src

    def test_help_text_includes_hypotheses(self):
        """The /help text should mention /hypotheses."""
        import inspect

        from healthbot.bot.handlers_session import SessionHandlers

        src = inspect.getsource(SessionHandlers.help_cmd)
        assert "/hypotheses" in src
