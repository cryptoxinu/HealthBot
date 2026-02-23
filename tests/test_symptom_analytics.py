"""Tests for symptom analytics module."""
from __future__ import annotations

from datetime import date, timedelta

from healthbot.reasoning.event_logger import EventLogger, ParsedEvent
from healthbot.reasoning.symptom_analytics import (
    SymptomAnalyzer,
    SymptomFrequency,
    SymptomOverview,
    format_frequency,
    format_overview,
)


def _insert_symptom(db, category: str, severity: str = "moderate", days_ago: int = 0):
    """Helper to insert a symptom event into the DB via EventLogger."""
    dt = date.today() - timedelta(days=days_ago)
    event = ParsedEvent(
        raw_text=f"{category} {severity}",
        cleaned_text=f"{category} {severity}",
        symptom_category=category,
        severity=severity,
        date_effective=dt,
    )
    logger = EventLogger(db)
    logger.store(event, user_id=0)


class TestSymptomAnalyzer:
    def test_empty_overview(self, db) -> None:
        analyzer = SymptomAnalyzer(db)
        overview = analyzer.overview(user_id=0)
        assert overview.total_events == 0
        assert overview.categories == []

    def test_overview_with_events(self, db) -> None:
        _insert_symptom(db, "headache", "severe", days_ago=1)
        _insert_symptom(db, "headache", "mild", days_ago=3)
        _insert_symptom(db, "fatigue", "moderate", days_ago=2)

        analyzer = SymptomAnalyzer(db)
        overview = analyzer.overview(user_id=0)
        assert overview.total_events == 3
        assert len(overview.categories) == 2
        # Headache should be first (most frequent)
        assert overview.categories[0].category == "headache"
        assert overview.categories[0].total_count == 2

    def test_frequency_specific_category(self, db) -> None:
        _insert_symptom(db, "dizziness", "moderate", days_ago=0)
        _insert_symptom(db, "dizziness", "severe", days_ago=7)
        _insert_symptom(db, "headache", "mild", days_ago=1)

        analyzer = SymptomAnalyzer(db)
        freq = analyzer.frequency(user_id=0, category="dizziness")
        assert freq is not None
        assert freq.category == "dizziness"
        assert freq.total_count == 2

    def test_frequency_nonexistent_category(self, db) -> None:
        analyzer = SymptomAnalyzer(db)
        freq = analyzer.frequency(user_id=0, category="nausea")
        assert freq is None

    def test_weeks_active_count(self, db) -> None:
        # Events in 2 different weeks
        _insert_symptom(db, "pain", "mild", days_ago=0)
        _insert_symptom(db, "pain", "moderate", days_ago=8)

        analyzer = SymptomAnalyzer(db)
        freq = analyzer.frequency(user_id=0, category="pain")
        assert freq is not None
        assert freq.weeks_active >= 1

    def test_avg_per_week(self, db) -> None:
        for i in range(7):
            _insert_symptom(db, "fatigue", "mild", days_ago=i)

        analyzer = SymptomAnalyzer(db)
        freq = analyzer.frequency(user_id=0, category="fatigue", days=7)
        assert freq is not None
        assert freq.avg_per_week == 7.0


class TestFormatOverview:
    def test_empty_overview(self) -> None:
        overview = SymptomOverview()
        output = format_overview(overview)
        assert "No symptoms logged" in output

    def test_formatted_output(self) -> None:
        overview = SymptomOverview(
            categories=[
                SymptomFrequency(
                    category="headache",
                    total_count=5,
                    weeks_active=3,
                    avg_per_week=1.7,
                    severities={"severe": 2, "moderate": 3},
                    most_recent="2025-12-10",
                ),
            ],
            total_events=5,
            days_covered=90,
        )
        output = format_overview(overview)
        assert "headache" in output
        assert "5x" in output
        assert "1.7/week" in output
        assert "2 severe" in output
        assert "2025-12-10" in output


class TestFormatFrequency:
    def test_formatted_output(self) -> None:
        freq = SymptomFrequency(
            category="fatigue",
            total_count=10,
            weeks_active=4,
            avg_per_week=2.5,
            severities={"mild": 6, "moderate": 4},
            most_recent="2025-12-11",
        )
        output = format_frequency(freq)
        assert "Fatigue" in output
        assert "10" in output
        assert "2.5/week" in output
        assert "2025-12-11" in output
