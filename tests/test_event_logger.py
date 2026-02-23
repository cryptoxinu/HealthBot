"""Tests for event logger parsing and storage."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from healthbot.reasoning.event_logger import EventLogger, ParsedEvent


@pytest.fixture
def mock_db() -> MagicMock:
    """Mock database with _encrypt and conn."""
    db = MagicMock()
    db._encrypt.return_value = b"encrypted_data"
    db.conn.execute.return_value = None
    db.conn.commit.return_value = None
    return db


@pytest.fixture
def logger(mock_db: MagicMock) -> EventLogger:
    return EventLogger(db=mock_db)


class TestEventParsing:
    """Test free-form text parsing into structured events."""

    def test_headache_category(self, logger: EventLogger) -> None:
        event = logger.parse("I have a terrible headache")
        assert event.symptom_category == "headache"

    def test_dizziness_category(self, logger: EventLogger) -> None:
        event = logger.parse("feeling dizzy and lightheaded")
        assert event.symptom_category == "dizziness"

    def test_fatigue_category(self, logger: EventLogger) -> None:
        event = logger.parse("so tired and exhausted today")
        assert event.symptom_category == "fatigue"

    def test_pain_category(self, logger: EventLogger) -> None:
        event = logger.parse("lower back pain since morning")
        assert event.symptom_category == "pain"

    def test_nausea_category(self, logger: EventLogger) -> None:
        event = logger.parse("feeling nauseous after lunch")
        assert event.symptom_category == "nausea"

    def test_sleep_category(self, logger: EventLogger) -> None:
        event = logger.parse("can't sleep at all lately")
        assert event.symptom_category == "sleep"

    def test_mood_category(self, logger: EventLogger) -> None:
        event = logger.parse("feeling very anxious today")
        assert event.symptom_category == "mood"

    def test_digestive_category(self, logger: EventLogger) -> None:
        event = logger.parse("bloating and constipation")
        assert event.symptom_category == "digestive"

    def test_heart_category(self, logger: EventLogger) -> None:
        event = logger.parse("having palpitations again")
        assert event.symptom_category == "heart"

    def test_general_fallback(self, logger: EventLogger) -> None:
        event = logger.parse("something weird happened")
        assert event.symptom_category == "general"

    def test_severity_extraction_severe(self, logger: EventLogger) -> None:
        event = logger.parse("severe headache all day")
        assert event.severity == "severe"

    def test_severity_extraction_mild(self, logger: EventLogger) -> None:
        event = logger.parse("mild nausea this morning")
        assert event.severity == "mild"

    def test_severity_extraction_moderate(self, logger: EventLogger) -> None:
        event = logger.parse("moderate back pain")
        assert event.severity == "moderate"

    def test_no_severity_defaults_empty(self, logger: EventLogger) -> None:
        event = logger.parse("headache today")
        assert event.severity == ""

    def test_strips_log_prefix(self, logger: EventLogger) -> None:
        event = logger.parse("log headache yesterday")
        assert not event.cleaned_text.lower().startswith("log")

    def test_strips_note_prefix(self, logger: EventLogger) -> None:
        event = logger.parse("note mild dizziness")
        assert not event.cleaned_text.lower().startswith("note")

    def test_strips_record_prefix(self, logger: EventLogger) -> None:
        event = logger.parse("record nausea this morning")
        assert not event.cleaned_text.lower().startswith("record")

    def test_date_defaults_to_today(self, logger: EventLogger) -> None:
        event = logger.parse("headache right now")
        assert event.date_effective == date.today()

    def test_date_extraction_yesterday(self, logger: EventLogger) -> None:
        from datetime import timedelta

        event = logger.parse("headache yesterday")
        assert event.date_effective == date.today() - timedelta(days=1)

    def test_preserves_raw_text(self, logger: EventLogger) -> None:
        text = "log severe headache yesterday"
        event = logger.parse(text)
        assert event.raw_text == text


class TestEventStorage:
    """Test event storage in the encrypted DB."""

    def test_store_calls_encrypt(
        self, logger: EventLogger, mock_db: MagicMock
    ) -> None:
        event = ParsedEvent(
            raw_text="headache",
            cleaned_text="headache",
            symptom_category="headache",
            severity="mild",
            date_effective=date.today(),
        )
        obs_id = logger.store(event, user_id=1)
        assert isinstance(obs_id, str)
        assert len(obs_id) == 32  # uuid hex
        mock_db._encrypt.assert_called_once()

    def test_store_inserts_into_db(
        self, logger: EventLogger, mock_db: MagicMock
    ) -> None:
        event = ParsedEvent(
            raw_text="headache",
            cleaned_text="headache",
            symptom_category="headache",
            severity="mild",
            date_effective=date.today(),
        )
        logger.store(event, user_id=1)
        mock_db.conn.execute.assert_called_once()
        args = mock_db.conn.execute.call_args
        sql = args[0][0]
        assert "INSERT INTO observations" in sql
        params = args[0][1]
        assert params[1] == "user_event"  # record_type
        assert params[2] == "headache"  # canonical_name = symptom_category

    def test_store_commits(
        self, logger: EventLogger, mock_db: MagicMock
    ) -> None:
        event = ParsedEvent(
            raw_text="test",
            cleaned_text="test",
            symptom_category="general",
            severity="",
            date_effective=date.today(),
        )
        logger.store(event, user_id=1)
        mock_db.conn.commit.assert_called_once()


class TestEventConfirmation:
    """Test human-readable confirmation formatting."""

    def test_format_with_severity(self, logger: EventLogger) -> None:
        event = ParsedEvent(
            raw_text="log severe headache",
            cleaned_text="severe headache",
            symptom_category="headache",
            severity="severe",
            date_effective=date(2024, 3, 15),
        )
        text = logger.format_confirmation(event)
        assert "Logged: severe headache" in text
        assert "Category: headache" in text
        assert "Severity: severe" in text
        assert "Date: 2024-03-15" in text

    def test_format_without_severity(self, logger: EventLogger) -> None:
        event = ParsedEvent(
            raw_text="headache",
            cleaned_text="headache",
            symptom_category="headache",
            severity="",
            date_effective=date(2024, 3, 15),
        )
        text = logger.format_confirmation(event)
        assert "Logged: headache" in text
        assert "Severity" not in text
