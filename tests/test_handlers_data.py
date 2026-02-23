"""Tests for healthbot.bot.handlers_data — import/export/sync commands."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.bot.handler_core import HandlerCore
from healthbot.bot.handlers_data import DataHandlers
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall


def _make_handlers(config: Config, key_manager: KeyManager) -> DataHandlers:
    core = HandlerCore(config, key_manager, PhiFirewall())
    return DataHandlers(core)


def _mock_update(user_id: int = 123) -> MagicMock:
    update = MagicMock()
    update.effective_user.id = user_id
    update.effective_chat.send_action = AsyncMock()
    update.message.reply_text = AsyncMock()
    update.message.reply_document = AsyncMock()
    return update


def _mock_context(args: list[str] | None = None) -> MagicMock:
    ctx = MagicMock()
    ctx.args = args or []
    return ctx


@pytest.mark.slow
class TestSyncWhoop:
    @pytest.mark.asyncio
    async def test_whoop_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.sync_whoop(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_whoop_sync_success(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.importers.whoop_client.WhoopClient") as mock_cls:
            mock_cls.return_value.sync_daily = AsyncMock(return_value=5)
            await handlers.sync_whoop(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("5" in t and "record" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_whoop_custom_days(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.importers.whoop_client.WhoopClient") as mock_cls:
            mock_cls.return_value.sync_daily = AsyncMock(return_value=3)
            await handlers.sync_whoop(update, _mock_context(["7"]))
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("7 days" in t for t in texts)

    @pytest.mark.asyncio
    async def test_whoop_auth_error(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        from healthbot.importers.whoop_client import WhoopAuthError
        with patch("healthbot.importers.whoop_client.WhoopClient") as mock_cls:
            mock_cls.return_value.sync_daily = AsyncMock(
                side_effect=WhoopAuthError("no token")
            )
            await handlers.sync_whoop(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("auth" in t.lower() for t in texts)


@pytest.mark.slow
class TestSyncOura:
    @pytest.mark.asyncio
    async def test_oura_sync_success(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.importers.oura_client.OuraClient") as mock_cls:
            mock_cls.return_value.sync_daily = AsyncMock(return_value=10)
            await handlers.sync_oura(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("10" in t and "record" in t.lower() for t in texts)

    @pytest.mark.asyncio
    async def test_oura_auth_error(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        from healthbot.importers.oura_client import OuraAuthError
        with patch("healthbot.importers.oura_client.OuraClient") as mock_cls:
            mock_cls.return_value.sync_daily = AsyncMock(
                side_effect=OuraAuthError("no token")
            )
            await handlers.sync_oura(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("auth" in t.lower() for t in texts)


class TestImportHealth:
    @pytest.mark.asyncio
    async def test_import_no_zips(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.import_health(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "no zip" in reply.lower()

    @pytest.mark.asyncio
    async def test_import_processes_zip(
        self, config: Config, key_manager: KeyManager, db: HealthDB, tmp_vault: Path
    ) -> None:
        import io
        import zipfile

        # Create a fake Apple Health ZIP
        incoming = config.incoming_dir
        incoming.mkdir(parents=True, exist_ok=True)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("apple_health_export/export.xml", "<HealthData></HealthData>")
        zip_path = incoming / "export.zip"
        zip_path.write_bytes(buf.getvalue())

        handlers = _make_handlers(config, key_manager)
        update = _mock_update()

        fake_vitals = [MagicMock() for _ in range(5)]

        def _insert_vitals(batch, existing, uid, result):
            result.records_imported += len(batch)
            result.types_found["heart_rate"] = 3
            result.types_found["steps"] = 2

        with patch(
            "healthbot.ingest.apple_health_import.AppleHealthImporter"
        ) as mock_cls:
            inst = mock_cls.return_value
            inst.parse_zip_bytes.return_value = (fake_vitals, [], None)
            inst.insert_vitals_batch.side_effect = _insert_vitals
            await handlers.import_health(update, _mock_context())

        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("5" in t for t in texts)


class TestImportMychart:
    @pytest.mark.asyncio
    async def test_mychart_no_files(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.import_mychart(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "no mychart" in reply.lower()

    @pytest.mark.asyncio
    async def test_mychart_imports_xml(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        incoming = config.incoming_dir
        incoming.mkdir(parents=True, exist_ok=True)
        (incoming / "results.xml").write_text("<ClinicalDocument/>")

        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.ingest.mychart_import.MyChartImporter") as mock_cls:
            mock_cls.return_value.import_ccda_bytes.return_value = {
                "labs": 3, "meds": 1
            }
            await handlers.import_mychart(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("3" in t and "lab" in t.lower() for t in texts)


class TestExportFhir:
    @pytest.mark.asyncio
    async def test_export_unknown_format(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.export_fhir(update, _mock_context(["xml"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "unknown" in reply.lower()

    @pytest.mark.asyncio
    async def test_export_fhir_success(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.export.fhir_export.FhirExporter") as mock_cls:
            mock_cls.return_value.export_json.return_value = '{"resourceType": "Bundle"}'
            await handlers.export_fhir(update, _mock_context(["fhir"]))
        assert update.message.reply_document.called

    @pytest.mark.asyncio
    async def test_export_fhir_error(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.export.fhir_export.FhirExporter") as mock_cls:
            mock_cls.return_value.export_json.side_effect = RuntimeError("fail")
            await handlers.export_fhir(update, _mock_context(["fhir"]))
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("failed" in t.lower() for t in texts)


class TestDocs:
    @pytest.mark.asyncio
    async def test_docs_no_documents(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        handlers._core._db = db
        update = _mock_update()
        await handlers.docs(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "no documents" in reply.lower()

    @pytest.mark.asyncio
    async def test_docs_lists_documents(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        from healthbot.data.models import Document

        handlers = _make_handlers(config, key_manager)
        handlers._core._db = db
        doc = Document(
            id="doc1", source="telegram_pdf", sha256="abc123",
            filename="bloodwork.pdf", size_bytes=2048,
        )
        db.insert_document(doc, user_id=123)
        update = _mock_update(user_id=123)
        await handlers.docs(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "bloodwork.pdf" in reply
        assert "2 KB" in reply

    @pytest.mark.asyncio
    async def test_docs_download_by_number(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        from healthbot.data.models import Document

        handlers = _make_handlers(config, key_manager)
        handlers._core._db = db
        doc = Document(
            id="doc2", source="telegram_pdf", sha256="def456",
            enc_blob_path="blob123", filename="labs.pdf", size_bytes=1024,
        )
        db.insert_document(doc, user_id=123)
        update = _mock_update(user_id=123)
        with patch("healthbot.security.vault.Vault") as mock_vault_cls:
            mock_vault_cls.return_value.retrieve_blob.return_value = b"%PDF-test"
            await handlers.docs(update, _mock_context(["1"]))
        assert update.message.reply_document.called

    @pytest.mark.asyncio
    async def test_docs_invalid_number(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        from healthbot.data.models import Document

        handlers = _make_handlers(config, key_manager)
        handlers._core._db = db
        doc = Document(
            id="doc3", source="telegram_pdf", sha256="ghi789",
            filename="test.pdf", size_bytes=512,
        )
        db.insert_document(doc, user_id=123)
        update = _mock_update(user_id=123)
        await handlers.docs(update, _mock_context(["99"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "invalid" in reply.lower()

    @pytest.mark.asyncio
    async def test_docs_missing_blob(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        from healthbot.data.models import Document

        handlers = _make_handlers(config, key_manager)
        handlers._core._db = db
        doc = Document(
            id="doc4", source="telegram_pdf", sha256="jkl012",
            enc_blob_path="", filename="empty.pdf", size_bytes=0,
        )
        db.insert_document(doc, user_id=123)
        update = _mock_update(user_id=123)
        await handlers.docs(update, _mock_context(["1"]))
        reply = update.message.reply_text.call_args[0][0]
        assert "no stored file" in reply.lower()

    @pytest.mark.asyncio
    async def test_docs_null_size_bytes(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        """Ensure listing doesn't crash when size_bytes is NULL."""
        handlers = _make_handlers(config, key_manager)
        handlers._core._db = db
        # Insert directly with NULL size_bytes
        db.conn.execute(
            """INSERT INTO documents (doc_id, source, sha256, received_at,
               filename, user_id) VALUES (?, ?, ?, ?, ?, ?)""",
            ("doc5", "telegram_pdf", "mno345", "2026-01-01T00:00:00", "null_size.pdf", 123),
        )
        db.conn.commit()
        update = _mock_update(user_id=123)
        await handlers.docs(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "null_size.pdf" in reply


# ---------------------------------------------------------------------------
# _get_connected_sources
# ---------------------------------------------------------------------------

class TestGetConnectedSources:
    def test_no_credentials(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        with patch("healthbot.security.keychain.Keychain") as mock_kc:
            mock_kc.return_value.retrieve.return_value = None
            result = handlers._get_connected_sources()
        assert result == []

    def test_whoop_only(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)

        def _retrieve(key: str):
            if key == "whoop_client_id":
                return "12345678-1234-1234-1234-123456789abc"
            return None

        with patch("healthbot.security.keychain.Keychain") as mock_kc:
            mock_kc.return_value.retrieve.side_effect = _retrieve
            result = handlers._get_connected_sources()
        assert len(result) == 1
        assert result[0]["name"] == "WHOOP"
        assert result[0]["provider"] == "whoop"

    def test_multiple_sources(
        self, config: Config, key_manager: KeyManager, tmp_path: Path
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        # Set up Apple Health path
        apple_dir = tmp_path / "apple_export"
        apple_dir.mkdir()
        handlers._core._config.apple_health_export_path = str(apple_dir)

        def _retrieve(key: str):
            if key == "whoop_client_id":
                return "12345678-1234-1234-1234-123456789abc"
            if key == "oura_client_id":
                return "abcdef12-3456-7890-abcd-ef1234567890"
            return None

        with patch("healthbot.security.keychain.Keychain") as mock_kc:
            mock_kc.return_value.retrieve.side_effect = _retrieve
            result = handlers._get_connected_sources()
        assert len(result) == 3
        names = [s["name"] for s in result]
        assert "WHOOP" in names
        assert "Oura Ring" in names
        assert "Apple Health" in names

    def test_invalid_credential_skipped(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        """Short or space-containing credentials are not valid."""
        handlers = _make_handlers(config, key_manager)

        def _retrieve(key: str):
            if key == "whoop_client_id":
                return "too short"
            return None

        with patch("healthbot.security.keychain.Keychain") as mock_kc:
            mock_kc.return_value.retrieve.side_effect = _retrieve
            result = handlers._get_connected_sources()
        assert result == []


# ---------------------------------------------------------------------------
# sync_all
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestSyncAll:
    @pytest.mark.asyncio
    async def test_sync_all_no_sources(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch.object(handlers, "_get_connected_sources", return_value=[]):
            await handlers.sync_all(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "no data sources" in reply.lower()
        assert "/connectors" in reply

    @pytest.mark.asyncio
    async def test_sync_all_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.sync_all(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_sync_all_single_provider(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        sources = [{"name": "WHOOP", "provider": "whoop"}]
        whoop_result = {"name": "WHOOP", "count": 7, "error": None}
        with (
            patch.object(handlers, "_get_connected_sources", return_value=sources),
            patch.object(handlers, "_sync_provider_whoop", return_value=whoop_result),
            patch.object(handlers, "_post_sync_claude_analysis", new_callable=AsyncMock),
        ):
            await handlers.sync_all(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("Syncing WHOOP" in t for t in texts)
        assert any("7 records" in t for t in texts)

    @pytest.mark.asyncio
    async def test_sync_all_multiple_providers_concurrent(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        sources = [
            {"name": "WHOOP", "provider": "whoop"},
            {"name": "Oura Ring", "provider": "oura"},
        ]
        whoop_result = {"name": "WHOOP", "count": 5, "error": None}
        oura_result = {"name": "Oura Ring", "count": 10, "error": None}
        with (
            patch.object(handlers, "_get_connected_sources", return_value=sources),
            patch.object(handlers, "_sync_provider_whoop", return_value=whoop_result),
            patch.object(handlers, "_sync_provider_oura", return_value=oura_result),
            patch.object(
                handlers, "_post_sync_claude_analysis", new_callable=AsyncMock,
            ) as mock_claude,
        ):
            await handlers.sync_all(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        # Both providers reported
        assert any("WHOOP: 5 records" in t for t in texts)
        assert any("Oura Ring: 10 records" in t for t in texts)
        # Claude analysis called for the provider with most data (Oura Ring = 10)
        mock_claude.assert_called_once()
        call_args = mock_claude.call_args
        assert call_args[0][2] == 10  # count
        assert call_args[0][4] == "Oura Ring"  # name

    @pytest.mark.asyncio
    async def test_sync_all_provider_error(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        sources = [{"name": "WHOOP", "provider": "whoop"}]
        whoop_result = {"name": "WHOOP", "count": 0, "error": "auth error: expired"}
        with (
            patch.object(handlers, "_get_connected_sources", return_value=sources),
            patch.object(handlers, "_sync_provider_whoop", return_value=whoop_result),
        ):
            await handlers.sync_all(update, _mock_context())
        texts = [c[0][0] for c in update.message.reply_text.call_args_list]
        assert any("auth error" in t for t in texts)

    @pytest.mark.asyncio
    async def test_sync_all_custom_days(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        sources = [{"name": "WHOOP", "provider": "whoop"}]
        whoop_result = {"name": "WHOOP", "count": 3, "error": None}
        with (
            patch.object(handlers, "_get_connected_sources", return_value=sources),
            patch.object(
                handlers, "_sync_provider_whoop", return_value=whoop_result,
            ) as mock_whoop,
            patch.object(handlers, "_post_sync_claude_analysis", new_callable=AsyncMock),
        ):
            await handlers.sync_all(update, _mock_context(["30"]))
        # Verify days=30 was passed to the provider
        mock_whoop.assert_called_once_with(30, 123)

    @pytest.mark.asyncio
    async def test_sync_all_skips_claude_for_apple_health(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        """Apple Health doesn't use wearable_daily, so no Claude analysis."""
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        sources = [{"name": "Apple Health", "provider": "apple_health"}]
        apple_result = {"name": "Apple Health", "count": 50, "error": None}
        with (
            patch.object(handlers, "_get_connected_sources", return_value=sources),
            patch.object(handlers, "_sync_provider_apple", return_value=apple_result),
            patch.object(
                handlers, "_post_sync_claude_analysis", new_callable=AsyncMock,
            ) as mock_claude,
        ):
            await handlers.sync_all(update, _mock_context())
        mock_claude.assert_not_called()


# ---------------------------------------------------------------------------
# _sync_provider_apple error reporting
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestSyncProviderApple:
    @pytest.mark.asyncio
    async def test_apple_no_export_path(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        handlers._core._config.apple_health_export_path = ""
        result = await handlers._sync_provider_apple(123)
        assert result["error"] == "not configured"

    @pytest.mark.asyncio
    async def test_apple_path_not_found(
        self, config: Config, key_manager: KeyManager, tmp_path: Path
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        handlers._core._config.apple_health_export_path = str(
            tmp_path / "nonexistent"
        )
        result = await handlers._sync_provider_apple(123)
        assert result["error"] == "path not found"

    @pytest.mark.asyncio
    async def test_apple_no_json_files(
        self, config: Config, key_manager: KeyManager, tmp_path: Path
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        apple_dir = tmp_path / "apple_export"
        apple_dir.mkdir()
        handlers._core._config.apple_health_export_path = str(apple_dir)
        result = await handlers._sync_provider_apple(123)
        assert result["error"] == "no files"

    @pytest.mark.asyncio
    async def test_apple_all_files_fail(
        self, config: Config, key_manager: KeyManager, db: HealthDB, tmp_path: Path
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        apple_dir = tmp_path / "apple_export"
        apple_dir.mkdir()
        (apple_dir / "data1.json").write_text("{}")
        (apple_dir / "data2.json").write_text("{}")
        handlers._core._config.apple_health_export_path = str(apple_dir)

        with patch(
            "healthbot.importers.apple_health_auto.AppleHealthAutoImporter"
        ) as mock_cls:
            mock_cls.return_value.import_from_json.side_effect = ValueError("bad")
            result = await handlers._sync_provider_apple(123)
        assert result["count"] == 0
        assert "2 file(s) failed" in result["error"]

    @pytest.mark.asyncio
    async def test_apple_partial_success(
        self, config: Config, key_manager: KeyManager, db: HealthDB, tmp_path: Path
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        apple_dir = tmp_path / "apple_export"
        apple_dir.mkdir()
        (apple_dir / "good.json").write_text("{}")
        (apple_dir / "bad.json").write_text("{}")
        handlers._core._config.apple_health_export_path = str(apple_dir)

        mock_result = MagicMock()
        mock_result.imported = 5
        call_count = 0

        def _import_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ValueError("bad file")
            return mock_result

        with patch(
            "healthbot.importers.apple_health_auto.AppleHealthAutoImporter"
        ) as mock_cls:
            mock_cls.return_value.import_from_json.side_effect = _import_side_effect
            result = await handlers._sync_provider_apple(123)
        # Partial success — 5 records from one file, one error
        assert result["count"] == 5
        assert result["error"] is None


# ---------------------------------------------------------------------------
# connectors
# ---------------------------------------------------------------------------

@pytest.mark.slow
class TestConnectors:
    @pytest.mark.asyncio
    async def test_connectors_nothing_configured(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        with patch("healthbot.security.keychain.Keychain") as mock_kc:
            mock_kc.return_value.retrieve.return_value = None
            await handlers.connectors(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "DATA CONNECTORS" in reply
        assert "Not configured" in reply
        assert "/whoop_auth" in reply
        assert "/oura_auth" in reply

    @pytest.mark.asyncio
    async def test_connectors_whoop_connected(
        self, config: Config, key_manager: KeyManager, db: HealthDB
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()

        def _retrieve(key: str):
            if key == "whoop_client_id":
                return "12345678-1234-1234-1234-123456789abc"
            return None

        with patch("healthbot.security.keychain.Keychain") as mock_kc:
            mock_kc.return_value.retrieve.side_effect = _retrieve
            await handlers.connectors(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "WHOOP: Connected" in reply
        assert "Oura Ring: Not configured" in reply
        assert "/sync" in reply

    @pytest.mark.asyncio
    async def test_connectors_requires_unlock(
        self, config: Config, key_manager: KeyManager
    ) -> None:
        key_manager.lock()
        handlers = _make_handlers(config, key_manager)
        update = _mock_update()
        await handlers.connectors(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "locked" in reply.lower()

    @pytest.mark.asyncio
    async def test_connectors_apple_health_with_pending(
        self, config: Config, key_manager: KeyManager, db: HealthDB, tmp_path: Path
    ) -> None:
        handlers = _make_handlers(config, key_manager)
        apple_dir = tmp_path / "apple_export"
        apple_dir.mkdir()
        (apple_dir / "data1.json").write_text("{}")
        (apple_dir / "data2.json").write_text("{}")
        handlers._core._config.apple_health_export_path = str(apple_dir)

        update = _mock_update()
        with patch("healthbot.security.keychain.Keychain") as mock_kc:
            mock_kc.return_value.retrieve.return_value = None
            await handlers.connectors(update, _mock_context())
        reply = update.message.reply_text.call_args[0][0]
        assert "2 pending files" in reply
