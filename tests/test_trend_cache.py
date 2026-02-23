"""Tests for trend caching (Phase 5)."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.reasoning.trends import TrendAnalyzer, TrendResult


@pytest.fixture()
def mock_db():
    db = MagicMock()
    db.conn = MagicMock()
    return db


@pytest.fixture()
def analyzer(mock_db):
    return TrendAnalyzer(mock_db)


def _make_trend(canonical_name: str = "glucose") -> TrendResult:
    return TrendResult(
        test_name="Glucose",
        canonical_name=canonical_name,
        direction="increasing",
        slope=0.5,
        r_squared=0.85,
        data_points=5,
        first_date="2025-01-01",
        last_date="2025-06-01",
        first_value=90.0,
        last_value=110.0,
        pct_change=22.2,
        values=[("2025-01-01", 90.0), ("2025-06-01", 110.0)],
    )


class TestAnalyzeTestCached:
    def test_returns_cached_when_valid(self, analyzer, mock_db):
        """Cache hit with matching last_date returns cached result."""
        mock_db.conn.execute.return_value.fetchone.return_value = {
            "direction": "increasing",
            "slope": 0.5,
            "r_squared": 0.85,
            "data_points": 5,
            "first_date": "2025-01-01",
            "last_date": "2025-06-01",
            "first_value": 90.0,
            "last_value": 110.0,
            "pct_change": 22.2,
        }
        # Latest observation matches cache last_date
        mock_db.query_observations.return_value = [
            {"date_collected": "2025-06-01", "value": 110.0},
        ]

        result = analyzer.analyze_test_cached("glucose", user_id=1)
        assert result is not None
        assert result.direction == "increasing"
        assert result.last_date == "2025-06-01"

    def test_recomputes_when_stale(self, analyzer, mock_db):
        """Cache hit but newer data exists -> recomputes."""
        mock_db.conn.execute.return_value.fetchone.return_value = {
            "direction": "increasing",
            "slope": 0.5,
            "r_squared": 0.85,
            "data_points": 5,
            "first_date": "2025-01-01",
            "last_date": "2025-06-01",
            "first_value": 90.0,
            "last_value": 110.0,
            "pct_change": 22.2,
        }
        # Latest observation is NEWER than cache
        mock_db.query_observations.return_value = [
            {"date_collected": "2025-07-15", "value": 115.0},
        ]

        with patch.object(
            analyzer, "analyze_test", return_value=_make_trend(),
        ) as mock_analyze:
            with patch.object(analyzer, "_store_cached"):
                analyzer.analyze_test_cached("glucose", user_id=1)
                mock_analyze.assert_called_once()

    def test_cache_miss_recomputes(self, analyzer, mock_db):
        """No cache entry -> recomputes."""
        mock_db.conn.execute.return_value.fetchone.return_value = None

        with patch.object(
            analyzer, "analyze_test", return_value=_make_trend(),
        ) as mock_analyze:
            with patch.object(analyzer, "_store_cached"):
                analyzer.analyze_test_cached("glucose", user_id=1)
                mock_analyze.assert_called_once()


class TestInvalidateCache:
    def test_invalidates_specific_tests(self, analyzer, mock_db):
        mock_db.conn.execute.return_value.rowcount = 2
        count = analyzer.invalidate_cache(
            {"glucose", "ferritin"}, user_id=1,
        )
        assert count == 2
        mock_db.conn.commit.assert_called_once()

    def test_invalidates_all_for_user(self, analyzer, mock_db):
        mock_db.conn.execute.return_value.rowcount = 10
        count = analyzer.invalidate_cache(user_id=1)
        assert count == 10

    def test_invalidates_all(self, analyzer, mock_db):
        mock_db.conn.execute.return_value.rowcount = 50
        count = analyzer.invalidate_cache()
        assert count == 50


class TestDetectAllTrendsUsesCached:
    def test_detect_all_uses_cached(self, analyzer, mock_db):
        """detect_all_trends should use analyze_test_cached."""
        mock_db.conn.execute.return_value.fetchall.return_value = [
            {"canonical_name": "glucose"},
        ]

        with patch.object(
            analyzer, "analyze_test_cached",
            return_value=_make_trend(),
        ) as mock_cached:
            results = analyzer.detect_all_trends(user_id=1)
            mock_cached.assert_called_once_with("glucose", 24, user_id=1)
            assert len(results) == 1
