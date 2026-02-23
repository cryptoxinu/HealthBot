"""Tests for /identity Telegram command handlers."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from healthbot.bot.handlers_identity import IdentityHandlers


@pytest.fixture
def mock_db():
    """Mock HealthDB."""
    db = MagicMock()
    db._fields: list[dict] = []

    def upsert(user_id, field_key, value, field_type):
        db._fields[:] = [f for f in db._fields if f["field_key"] != field_key]
        db._fields.append({
            "field_key": field_key,
            "value": value,
            "type": field_type,
        })
        return "test-id"

    def get_fields(user_id):
        return list(db._fields)

    def delete_all(user_id):
        count = len(db._fields)
        db._fields.clear()
        return count

    db.upsert_identity_field = MagicMock(side_effect=upsert)
    db.get_identity_fields = MagicMock(side_effect=get_fields)
    db.delete_identity_field = MagicMock(return_value=True)
    db.delete_all_identity_fields = MagicMock(side_effect=delete_all)
    return db


@pytest.fixture
def handlers(mock_db):
    config = MagicMock()
    km = MagicMock()
    km.is_unlocked = True
    return IdentityHandlers(
        config=config,
        key_manager=km,
        get_db=lambda: mock_db,
        check_auth=lambda _: True,
    )


@pytest.fixture
def update():
    u = MagicMock()
    u.effective_user.id = 42
    u.message.text = ""
    u.message.reply_text = AsyncMock()
    u.effective_chat.send_message = AsyncMock()
    return u


@pytest.fixture
def context():
    ctx = MagicMock()
    ctx.args = []
    return ctx


class TestIdentitySurvey:
    """Test multi-step identity survey flow."""

    @pytest.mark.asyncio
    async def test_identity_starts_survey(self, handlers, update, context) -> None:
        await handlers.identity(update, context)
        # Should start survey — first question about name
        calls = update.message.reply_text.call_args_list
        text = " ".join(str(c) for c in calls)
        assert "name" in text.lower()

    @pytest.mark.asyncio
    async def test_survey_answer_advances(self, handlers, update, context) -> None:
        await handlers.identity(update, context)
        assert handlers.is_active(42)

        # Answer first question
        update.message.text = "John Smith"
        handled = await handlers.handle_answer(update, context)
        assert handled
        # Should advance to next question (email)
        last_reply = update.message.reply_text.call_args[0][0]
        assert "email" in last_reply.lower()

    @pytest.mark.asyncio
    async def test_survey_skip(self, handlers, update, context) -> None:
        await handlers.identity(update, context)

        update.message.text = "skip"
        handled = await handlers.handle_answer(update, context)
        assert handled
        # Should advance to next question

    @pytest.mark.asyncio
    async def test_survey_cancel(self, handlers, update, context) -> None:
        await handlers.identity(update, context)
        assert handlers.is_active(42)

        update.message.text = "cancel"
        handled = await handlers.handle_answer(update, context)
        assert handled
        assert not handlers.is_active(42)

    @pytest.mark.asyncio
    async def test_survey_completion(self, handlers, update, context, mock_db) -> None:
        await handlers.identity(update, context)

        # Answer all 5 questions
        answers = ["John Smith", "john@test.com", "1990-03-15", "skip", "skip"]
        for ans in answers:
            update.message.text = ans
            await handlers.handle_answer(update, context)

        assert not handlers.is_active(42)
        # Check completion message
        last_reply = update.message.reply_text.call_args[0][0]
        assert "SAVED" in last_reply

    @pytest.mark.asyncio
    async def test_not_active_when_no_session(self, handlers) -> None:
        assert not handlers.is_active(42)

    @pytest.mark.asyncio
    async def test_handle_answer_returns_false_when_inactive(
        self, handlers, update, context,
    ) -> None:
        update.message.text = "some text"
        result = await handlers.handle_answer(update, context)
        assert result is False


class TestIdentityCheck:
    """Test /identity_check command."""

    @pytest.mark.asyncio
    async def test_check_with_no_args(self, handlers, update, context) -> None:
        context.args = []
        await handlers.identity_check(update, context)
        reply = update.message.reply_text.call_args[0][0]
        assert "Usage" in reply

    @pytest.mark.asyncio
    async def test_check_detects_stored_name(
        self, handlers, update, context, mock_db,
    ) -> None:
        # Pre-populate identity
        mock_db._fields.append({
            "field_key": "full_name",
            "value": "John Smith",
            "type": "name",
        })
        context.args = ["John", "Smith", "saw", "the", "doctor"]
        await handlers.identity_check(update, context)
        reply = update.message.reply_text.call_args[0][0]
        assert "match" in reply.lower()

    @pytest.mark.asyncio
    async def test_check_no_match(
        self, handlers, update, context, mock_db,
    ) -> None:
        mock_db._fields.append({
            "field_key": "full_name",
            "value": "John Smith",
            "type": "name",
        })
        context.args = ["Hemoglobin", "is", "14.2"]
        await handlers.identity_check(update, context)
        reply = update.message.reply_text.call_args[0][0]
        assert "No identity-based matches" in reply


class TestIdentityClear:
    """Test /identity_clear command."""

    @pytest.mark.asyncio
    async def test_clear_with_data(
        self, handlers, update, context, mock_db,
    ) -> None:
        mock_db._fields.append({
            "field_key": "full_name",
            "value": "John Smith",
            "type": "name",
        })
        await handlers.identity_clear(update, context)
        reply = update.message.reply_text.call_args[0][0]
        assert "cleared" in reply.lower()

    @pytest.mark.asyncio
    async def test_clear_with_no_data(self, handlers, update, context) -> None:
        await handlers.identity_clear(update, context)
        reply = update.message.reply_text.call_args[0][0]
        assert "No identity profile" in reply


class TestVaultLock:
    """Test vault lock clears sessions."""

    @pytest.mark.asyncio
    async def test_vault_lock_clears_sessions(
        self, handlers, update, context,
    ) -> None:
        await handlers.identity(update, context)
        assert handlers.is_active(42)
        handlers.on_vault_lock()
        assert not handlers.is_active(42)


class TestDOBNormalization:
    """Test DOB normalization to ISO format."""

    def test_iso_passthrough(self, handlers) -> None:
        assert handlers._normalize_dob("1990-03-15") == "1990-03-15"

    def test_slash_format(self, handlers) -> None:
        assert handlers._normalize_dob("03/15/1990") == "1990-03-15"

    def test_text_month(self, handlers) -> None:
        assert handlers._normalize_dob("March 15, 1990") == "1990-03-15"

    def test_unrecognized_passthrough(self, handlers) -> None:
        assert handlers._normalize_dob("born in 1990") == "born in 1990"


class TestMultiValueStorage:
    """Test multi-value field storage (family names, custom PII)."""

    @pytest.mark.asyncio
    async def test_family_names_split(
        self, handlers, update, context, mock_db,
    ) -> None:
        await handlers.identity(update, context)

        # Skip to family question (index 3)
        for _ in range(3):
            update.message.text = "skip"
            await handlers.handle_answer(update, context)

        # Answer family question with comma-separated names
        update.message.text = "Sarah Johnson, Mike Thompson"
        await handlers.handle_answer(update, context)

        # Check that both were stored
        stored_keys = [f["field_key"] for f in mock_db._fields]
        assert "family:0" in stored_keys
        assert "family:1" in stored_keys
