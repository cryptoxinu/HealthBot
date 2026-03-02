"""Tests for chart_generator and chart_generator_ext — in-memory PNG generation."""
from __future__ import annotations

from healthbot.export.chart_generator import (
    dashboard_chart,
    multi_trend_chart,
    trend_chart,
    workout_summary_chart,
)
from healthbot.export.chart_generator_ext import (
    composite_score_chart,
    correlation_scatter_chart,
    lab_heatmap_chart,
    sleep_architecture_chart,
    wearable_sparklines_chart,
)
from healthbot.export.chart_health_card import health_card
from healthbot.reasoning.health_score import CompositeHealthScore
from healthbot.reasoning.insights import DomainScore
from healthbot.reasoning.trends import TrendResult
from healthbot.reasoning.wearable_trends import WearableTrendResult

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


def _make_wearable_trend(
    metric: str = "hrv",
    display: str = "HRV",
    direction: str = "increasing",
    values: list[tuple[str, float]] | None = None,
) -> WearableTrendResult:
    vals = values or [
        ("2025-01-01", 45.0),
        ("2025-01-08", 48.0),
        ("2025-01-15", 50.0),
        ("2025-01-22", 53.0),
        ("2025-01-29", 55.0),
    ]
    return WearableTrendResult(
        metric_name=metric,
        display_name=display,
        direction=direction,
        slope=0.4,
        r_squared=0.92,
        data_points=len(vals),
        first_date=vals[0][0],
        last_date=vals[-1][0],
        first_value=vals[0][1],
        last_value=vals[-1][1],
        pct_change=22.2,
        values=vals,
    )


class TestWearableTrendChart:
    """trend_chart() should work with WearableTrendResult via alias properties."""

    def test_alias_properties_map_correctly(self):
        wt = _make_wearable_trend(metric="hrv", display="HRV")
        assert wt.test_name == "HRV"
        assert wt.canonical_name == "hrv"

    def test_returns_valid_png(self):
        wt = _make_wearable_trend()
        result = trend_chart(wt)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_decreasing_wearable_trend(self):
        wt = _make_wearable_trend(
            direction="decreasing",
            values=[("2025-01-01", 60.0), ("2025-01-15", 45.0)],
        )
        result = trend_chart(wt)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_insufficient_data_returns_none(self):
        wt = _make_wearable_trend(values=[("2025-01-01", 50.0)])
        assert trend_chart(wt) is None


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


# ── Extended chart tests (chart_generator_ext.py) ─────────────────


def _make_composite_score(**overrides) -> CompositeHealthScore:
    defaults = {
        "overall": 78.5,
        "grade": "B",
        "breakdown": {
            "biomarker": 85.0,
            "recovery": 70.0,
            "trend_trajectory": 60.0,
            "anomaly_penalty": 100.0,
        },
        "trend_direction": "improving",
        "limiting_factors": [],
        "data_coverage": {"biomarker": True, "recovery": True},
    }
    defaults.update(overrides)
    return CompositeHealthScore(**defaults)


def _make_wearable_data(days: int = 14) -> list[dict]:
    rows = []
    for i in range(days):
        rows.append({
            "_date": f"2025-01-{i + 1:02d}",
            "hrv": 45 + i,
            "rhr": 62 - i * 0.3,
            "sleep_score": 70 + i,
            "recovery_score": 60 + i * 2,
            "strain": 10 + i * 0.5,
            "sleep_duration_min": 420 + i * 5,
            "deep_min": 60 + i,
            "rem_min": 90 + i,
        })
    return rows


class TestCompositeScoreChart:
    def test_returns_valid_png(self):
        result = composite_score_chart(_make_composite_score())
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_none(self):
        assert composite_score_chart(None) is None

    def test_returns_none_for_empty_breakdown(self):
        s = _make_composite_score(overall=0, breakdown={})
        assert composite_score_chart(s) is None

    def test_high_score(self):
        s = _make_composite_score(overall=95, grade="A+")
        result = composite_score_chart(s)
        assert result is not None

    def test_low_score(self):
        s = _make_composite_score(overall=30, grade="F")
        result = composite_score_chart(s)
        assert result is not None


class TestWearableSparklines:
    def test_returns_valid_png(self):
        data = _make_wearable_data(14)
        result = wearable_sparklines_chart(data)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty(self):
        assert wearable_sparklines_chart([]) is None

    def test_custom_metrics(self):
        data = _make_wearable_data(10)
        result = wearable_sparklines_chart(data, metrics=["hrv", "rhr"])
        assert result is not None

    def test_few_days(self):
        data = _make_wearable_data(3)
        result = wearable_sparklines_chart(data, days=3)
        assert result is not None


class TestSleepArchitecture:
    def test_returns_valid_png(self):
        data = _make_wearable_data(30)
        result = sleep_architecture_chart(data)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty(self):
        assert sleep_architecture_chart([]) is None

    def test_returns_none_for_no_sleep_data(self):
        data = [{"_date": "2025-01-01", "hrv": 50}]
        assert sleep_architecture_chart(data) is None

    def test_custom_days(self):
        data = _make_wearable_data(7)
        result = sleep_architecture_chart(data, days=7)
        assert result is not None


class TestLabHeatmap:
    def test_returns_valid_png(self):
        lab_data = [
            {"test_name": "LDL", "date": "2024-01-15", "value": 100, "ref_low": 0, "ref_high": 130},
            {"test_name": "LDL", "date": "2024-06-15", "value": 145, "ref_low": 0, "ref_high": 130},
            {"test_name": "HDL", "date": "2024-01-15", "value": 55, "ref_low": 40, "ref_high": 100},
            {"test_name": "HDL", "date": "2024-06-15", "value": 60, "ref_low": 40, "ref_high": 100},
        ]
        result = lab_heatmap_chart(lab_data)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty(self):
        assert lab_heatmap_chart([]) is None

    def test_returns_none_for_single_entry(self):
        lab_data = [
            {"test_name": "LDL", "date": "2024-01-15", "value": 100},
        ]
        assert lab_heatmap_chart(lab_data) is None

    def test_with_reference_ranges(self):
        lab_data = [
            {"test_name": "TSH", "date": "2024-01-01", "value": 2.5},
            {"test_name": "TSH", "date": "2024-06-01", "value": 5.5},
        ]
        ref = {"TSH": (0.4, 4.0)}
        result = lab_heatmap_chart(lab_data, reference_ranges=ref)
        assert result is not None


class TestCorrelationScatter:
    def test_returns_valid_png(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]
        result = correlation_scatter_chart(x, y, "HRV", "Sleep Score", r_value=0.99)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_returns_none_for_empty(self):
        assert correlation_scatter_chart([], [], "X", "Y") is None

    def test_returns_none_for_too_few_points(self):
        assert correlation_scatter_chart([1.0, 2.0], [3.0, 4.0], "X", "Y") is None

    def test_returns_none_for_length_mismatch(self):
        assert correlation_scatter_chart([1.0, 2.0, 3.0], [4.0, 5.0], "X", "Y") is None

    def test_no_r_value(self):
        x = [1.0, 2.0, 3.0, 4.0]
        y = [10.0, 8.0, 6.0, 4.0]
        result = correlation_scatter_chart(x, y, "Strain", "Recovery")
        assert result is not None


# ── Health card tests (chart_health_card.py) ──────────────────────


class TestHealthCard:
    def test_full_data_returns_png(self):
        composite = _make_composite_score()
        scores = _make_scores()
        wearable = _make_wearable_data(14)
        trend = _make_trend()
        result = health_card(composite, scores, wearable, trend)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_partial_data_still_produces_png(self):
        """At least one panel has data -> should produce a PNG."""
        composite = _make_composite_score()
        result = health_card(composite, None, None, None)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_all_none_returns_none(self):
        result = health_card(None, None, None, None)
        assert result is None

    def test_only_radar_data(self):
        scores = _make_scores()
        result = health_card(None, scores, None, None)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_only_wearable_data(self):
        wearable = _make_wearable_data(7)
        result = health_card(None, None, wearable, None)
        assert result is not None
        assert result[:4] == _PNG_HEADER

    def test_only_trend_data(self):
        trend = _make_trend()
        result = health_card(None, None, None, trend)
        assert result is not None
        assert result[:4] == _PNG_HEADER
