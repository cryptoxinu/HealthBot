"""Tests for /import Apple Health command."""
from __future__ import annotations

import zipfile
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.config import Config


def _make_apple_health_zip() -> bytes:
    """Create a minimal Apple Health export ZIP in memory."""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("apple_health_export/export.xml", "<HealthData></HealthData>")
    return buf.getvalue()


def _make_non_health_zip() -> bytes:
    """Create a ZIP that is NOT an Apple Health export."""
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("readme.txt", "not a health export")
    return buf.getvalue()


class TestImportCommand:
    """Tests for the import_health handler."""

    @pytest.mark.asyncio
    async def test_no_zips_found(self, tmp_path: Path) -> None:
        """Should tell user no ZIPs found when incoming/ is empty."""
        from healthbot.bot.handlers import Handlers

        config = MagicMock(spec=Config)
        config.incoming_dir = tmp_path
        config.allowed_user_ids = [1]
        km = MagicMock()
        km.is_unlocked = True
        fw = MagicMock()

        handlers = Handlers(config, km, fw)

        update = MagicMock()
        update.effective_user.id = 1
        update.effective_chat = AsyncMock()
        update.message.reply_text = AsyncMock()
        context = MagicMock()

        await handlers.import_health(update, context)

        call_text = update.message.reply_text.call_args[0][0]
        assert "No ZIP files found" in call_text

    @pytest.mark.asyncio
    async def test_import_valid_zip(self, tmp_path: Path) -> None:
        """Should import a valid Apple Health ZIP and move to processed/."""
        from healthbot.bot.handlers import Handlers

        config = MagicMock(spec=Config)
        config.incoming_dir = tmp_path
        config.db_path = tmp_path / "health.db"
        config.blobs_dir = tmp_path / "blobs"
        config.allowed_user_ids = [1]
        config.privacy_mode = "relaxed"
        km = MagicMock()
        km.is_unlocked = True
        fw = MagicMock()

        # Write a valid ZIP
        zip_path = tmp_path / "export.zip"
        zip_path.write_bytes(_make_apple_health_zip())

        handlers = Handlers(config, km, fw)

        update = MagicMock()
        update.effective_user.id = 1
        update.effective_chat = AsyncMock()
        update.message.reply_text = AsyncMock()
        context = MagicMock()

        fake_vitals = [MagicMock() for _ in range(42)]
        fake_workouts = [MagicMock() for _ in range(3)]

        def _insert_vitals(batch, existing, uid, result):
            result.records_imported += len(batch)
            result.types_found["lab_result"] = 30
            result.types_found["vital_sign"] = 12

        def _insert_workouts(batch, existing, uid, result):
            result.workouts_imported += len(batch)

        with patch("healthbot.bot.handler_core.HandlerCore._get_db") as mock_db, \
             patch(
                 "healthbot.ingest.apple_health_import.AppleHealthImporter"
             ) as mock_imp_cls, \
             patch("healthbot.security.vault.Vault"):
            mock_db.return_value = MagicMock()
            inst = mock_imp_cls.return_value
            inst.parse_zip_bytes.return_value = (
                fake_vitals, fake_workouts, None,
            )
            inst.insert_vitals_batch.side_effect = _insert_vitals
            inst.insert_workouts_batch.side_effect = _insert_workouts

            await handlers.import_health(update, context)

        # Should have reported the import
        texts = [
            call[0][0] for call in update.message.reply_text.call_args_list
        ]
        assert any("42 vitals" in t for t in texts)

        # ZIP should have been moved to processed/
        assert not zip_path.exists()
        assert (tmp_path / "processed" / "export.zip").exists()

    @pytest.mark.asyncio
    async def test_skip_non_health_zip(self, tmp_path: Path) -> None:
        """Should skip ZIPs that don't contain export.xml."""
        from healthbot.bot.handlers import Handlers

        config = MagicMock(spec=Config)
        config.incoming_dir = tmp_path
        config.allowed_user_ids = [1]
        km = MagicMock()
        km.is_unlocked = True
        fw = MagicMock()

        # Write a non-health ZIP
        zip_path = tmp_path / "random.zip"
        zip_path.write_bytes(_make_non_health_zip())

        handlers = Handlers(config, km, fw)

        update = MagicMock()
        update.effective_user.id = 1
        update.effective_chat = AsyncMock()
        update.message.reply_text = AsyncMock()
        context = MagicMock()

        await handlers.import_health(update, context)

        texts = [
            call[0][0] for call in update.message.reply_text.call_args_list
        ]
        assert any("not an Apple Health export" in t for t in texts)

    @pytest.mark.asyncio
    async def test_import_error_handled(self, tmp_path: Path) -> None:
        """Should catch and report errors for individual ZIPs."""
        from healthbot.bot.handlers import Handlers

        config = MagicMock(spec=Config)
        config.incoming_dir = tmp_path
        config.db_path = tmp_path / "health.db"
        config.blobs_dir = tmp_path / "blobs"
        config.allowed_user_ids = [1]
        km = MagicMock()
        km.is_unlocked = True
        fw = MagicMock()

        zip_path = tmp_path / "bad.zip"
        zip_path.write_bytes(_make_apple_health_zip())

        handlers = Handlers(config, km, fw)

        update = MagicMock()
        update.effective_user.id = 1
        update.effective_chat = AsyncMock()
        update.message.reply_text = AsyncMock()
        context = MagicMock()

        with patch("healthbot.bot.handler_core.HandlerCore._get_db") as mock_db, \
             patch(
                 "healthbot.ingest.apple_health_import.AppleHealthImporter"
             ) as mock_imp_cls, \
             patch("healthbot.security.vault.Vault"):
            mock_db.return_value = MagicMock()
            mock_imp_cls.return_value.import_from_zip_bytes.side_effect = (
                RuntimeError("corrupt zip")
            )

            await handlers.import_health(update, context)

        texts = [
            call[0][0] for call in update.message.reply_text.call_args_list
        ]
        assert any("Error importing" in t for t in texts)
