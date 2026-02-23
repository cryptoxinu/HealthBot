"""Tests for sleep optimization recommendations."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.sleep_recommendations import (
    SLEEP_DEFICIT_RECOMMENDATIONS,
    SleepRecommender,
    format_sleep_recommendations,
)


def _make_db(wearables: list[dict] | None = None) -> MagicMock:
    db = MagicMock()
    wearables = wearables or []
    db.query_wearable_daily.return_value = wearables
    return db


def _day(
    dt: str,
    deep_pct: float = 20.0,
    rem_pct: float = 22.0,
    duration_min: float = 480.0,
    efficiency: float = 90.0,
    latency: float = 10.0,
) -> dict:
    return {
        "_date": dt,
        "deep_sleep_pct": deep_pct,
        "rem_sleep_pct": rem_pct,
        "sleep_duration_min": duration_min,
        "sleep_efficiency": efficiency,
        "sleep_latency_min": latency,
    }


class TestRecommendationKB:
    def test_all_categories_have_tips(self):
        for cat, tips in SLEEP_DEFICIT_RECOMMENDATIONS.items():
            assert len(tips) >= 2, f"{cat} has too few tips"

    def test_all_tips_have_citations(self):
        for cat, tips in SLEEP_DEFICIT_RECOMMENDATIONS.items():
            for tip in tips:
                assert tip.citation, f"{cat}: {tip.category} missing citation"


class TestInsufficientData:
    def test_no_data(self):
        db = _make_db([])
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)
        assert recs == []

    def test_two_days(self):
        db = _make_db([_day("2024-01-01"), _day("2024-01-02")])
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)
        assert recs == []


class TestDeficitDetection:
    def test_low_deep_sleep(self):
        days = [_day(f"2024-01-0{i}", deep_pct=10.0) for i in range(1, 6)]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)

        deep = [r for r in recs if r.deficit_type == "deep_low"]
        assert len(deep) == 1
        assert "10%" in deep[0].current_value
        assert len(deep[0].tips) >= 3

    def test_low_rem(self):
        days = [_day(f"2024-01-0{i}", rem_pct=10.0) for i in range(1, 6)]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)

        rem = [r for r in recs if r.deficit_type == "rem_low"]
        assert len(rem) == 1

    def test_low_duration(self):
        days = [_day(f"2024-01-0{i}", duration_min=360.0) for i in range(1, 6)]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)

        dur = [r for r in recs if r.deficit_type == "duration_low"]
        assert len(dur) == 1
        assert "6.0h" in dur[0].current_value

    def test_low_efficiency(self):
        days = [_day(f"2024-01-0{i}", efficiency=75.0) for i in range(1, 6)]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)

        eff = [r for r in recs if r.deficit_type == "efficiency_low"]
        assert len(eff) == 1

    def test_high_latency(self):
        days = [_day(f"2024-01-0{i}", latency=35.0) for i in range(1, 6)]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)

        lat = [r for r in recs if r.deficit_type == "latency_high"]
        assert len(lat) == 1

    def test_good_sleep_no_recs(self):
        days = [_day(f"2024-01-0{i}") for i in range(1, 6)]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)
        assert recs == []

    def test_multiple_deficits(self):
        days = [
            _day(f"2024-01-0{i}", deep_pct=8.0, rem_pct=10.0, duration_min=350)
            for i in range(1, 6)
        ]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)
        assert len(recs) == 3


class TestFormatting:
    def test_format_empty(self):
        result = format_sleep_recommendations([])
        assert "No sleep deficits" in result

    def test_format_with_recs(self):
        days = [_day(f"2024-01-0{i}", deep_pct=10.0) for i in range(1, 6)]
        db = _make_db(days)
        recommender = SleepRecommender(db)
        recs = recommender.get_recommendations(user_id=1)

        result = format_sleep_recommendations(recs)
        assert "SLEEP OPTIMIZATION" in result
        assert "Low Deep Sleep" in result
        assert "Ref:" in result
