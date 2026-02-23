"""Tests for chart_generator — in-memory PNG generation."""
from __future__ import annotations

from healthbot.export.chart_generator import (
    dashboard_chart,
    multi_trend_chart,
    trend_chart,
    workout_summary_chart,
)
from healthbot.reasoning.insights import DomainScore
from healthbot.reasoning.trends import TrendResult

_PNG_HEADER = b"\x89PNG"


def _make_trend(
    name: str = "LDL",
    canonical: str = "ldl",
    direction: str = "increasing",
    values: list[tuple[str, float]] | None = None,
) -> TrendResult:
    vals = [
        ("2024-01-15", 100.0),
        ("2024-04-15", 110.0),
        ("2024-07-15", 120.0),
        ("2024-10-15", 130.0),
    ] if values is None else values
    return TrendResult(
        test_name=name,
        canonical_name=canonical,
        direction=direction,
        slope=0.11,
        r_squared=0.95,
        data_points=len(vals),
        first_date=vals[0][0] if vals else "",
        last_date=vals[-1][0] if vals else "",
        first_value=vals[0][1] if vals else 0.0,
        last_value=vals[-1][1] if vals else 0.0,
        pct_change=30.0,
        values=vals,
    )


def _make_scores() -> list[DomainScore]:
    return [
        DomainScore("metabolic", "Metabolic Health", 92.0, 5, 5, []),
        DomainScore("cardiovascular", "Cardiovascular", 65.0, 4, 4, ["LDL high"]),
        DomainScore("blood", "Blood Health", 88.0, 5, 5, []),
        DomainScore("liver", "Liver Function", 100.0, 3, 5, []),
        DomainScore("thyroid", "Thyroid", 55.0, 2, 3, ["TSH elevated"]),
        DomainScore("nutrition", "Nutrition", 78.0, 4, 5, []),
        DomainScore("inflammation", "Inflammation", 100.0, 1, 2, []),
    ]


class TestTrendChart:
    def test_returns_valid_png(self):
        result = trend_chart(_make_trend())
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty_trend(self):
        assert trend_chart(None) is None

    def test_returns_none_for_no_values(self):
        t = _make_trend(values=[])
        assert trend_chart(t) is None

    def test_returns_none_for_single_value(self):
        t = _make_trend(values=[("2024-01-15", 100.0)])
        assert trend_chart(t) is None

    def test_decreasing_trend(self):
        t = _make_trend(
            direction="decreasing",
            values=[("2024-01-15", 130.0), ("2024-07-15", 100.0)],
        )
        result = trend_chart(t)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_stable_trend(self):
        t = _make_trend(direction="stable")
        result = trend_chart(t)
        assert result is not None


class TestDashboardChart:
    def test_returns_valid_png(self):
        result = dashboard_chart(_make_scores())
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty(self):
        assert dashboard_chart([]) is None

    def test_single_domain(self):
        scores = [DomainScore("metabolic", "Metabolic Health", 85.0, 5, 5, [])]
        result = dashboard_chart(scores)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_all_red_scores(self):
        scores = [
            DomainScore("metabolic", "Metabolic", 30.0, 5, 5, []),
            DomainScore("cardio", "Cardiovascular", 45.0, 4, 4, []),
        ]
        result = dashboard_chart(scores)
        assert result is not None


class TestMultiTrendChart:
    def test_returns_valid_png(self):
        trends = [
            _make_trend("LDL", "ldl", "increasing"),
            _make_trend("Glucose", "glucose", "decreasing"),
            _make_trend("TSH", "tsh", "stable"),
        ]
        result = multi_trend_chart(trends)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty(self):
        assert multi_trend_chart([]) is None

    def test_single_trend(self):
        result = multi_trend_chart([_make_trend()])
        assert result is not None

    def test_respects_max_panels(self):
        trends = [_make_trend(f"Test{i}", f"test{i}") for i in range(10)]
        result = multi_trend_chart(trends, max_panels=4)
        assert result is not None
        assert result[:4] == _PNG_HEADER


class TestWorkoutSummaryChart:
    def test_returns_valid_png(self):
        by_sport = {
            "running": [
                {"duration_minutes": 30, "calories_burned": 300},
                {"duration_minutes": 45, "calories_burned": 450},
            ],
            "cycling": [
                {"duration_minutes": 60, "calories_burned": 500},
            ],
        }
        result = workout_summary_chart(by_sport)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty(self):
        assert workout_summary_chart({}) is None
