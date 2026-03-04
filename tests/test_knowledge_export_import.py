"""Tests for knowledge export/import (brain backup).

Covers:
- Export: 6 stores, PII redaction, counts
- Import: validation, dedup, PII belt-and-suspenders
- Encrypted round-trip
- Telegram handler integration
- Upload detection
"""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.data.db import HealthDB
from healthbot.export.knowledge_export import KnowledgeExporter
from healthbot.ingest.knowledge_import import (
    ImportReport,
    KnowledgeImporter,
    is_knowledge_export,
)
from healthbot.security.phi_firewall import PhiFirewall

TEST_USER_ID = 12345


# ── Fixtures ─────────────────────────────────────────────────────


@pytest.fixture
def exporter(db, config, key_manager, phi_firewall):
    return KnowledgeExporter(db, config, key_manager, phi_firewall)


@pytest.fixture
def importer(db, config, key_manager, phi_firewall):
    return KnowledgeImporter(db, config, key_manager, phi_firewall)


def _seed_ltm(db, user_id=TEST_USER_ID):
    """Insert a few LTM facts."""
    db.insert_ltm(user_id, "medical", "Patient has iron deficiency")
    db.insert_ltm(user_id, "demographic", "Age 35, male")


def _seed_hypothesis(db, user_id=TEST_USER_ID):
    """Insert a hypothesis."""
    db.insert_hypothesis(user_id, {
        "title": "Subclinical hypothyroidism",
        "confidence": 0.65,
        "evidence_for": ["Elevated TSH 6.2"],
        "evidence_against": [],
        "missing_tests": ["Free T4", "Anti-TPO"],
        "notes": "Monitor over 3 months",
    })


def _seed_journal(db, user_id=TEST_USER_ID):
    """Insert a journal entry."""
    db.insert_journal_entry(
        user_id, "user", "I've been feeling tired lately",
        category="symptom", source="conversation",
    )


# ══════════════════════════════════════════════════════════════════
# PHASE 1: Export tests
# ══════════════════════════════════════════════════════════════════


class TestExportLTM:
    def test_export_ltm_facts(self, exporter, db):
        _seed_ltm(db)
        data, counts = exporter.export_all(TEST_USER_ID)
        payload = json.loads(data)
        assert payload["format"] == "healthbot_knowledge_export"
        assert payload["version"] == 1
        assert payload["mode"] == "plain"
        assert counts["ltm_facts"] == 2
        facts = payload["stores"]["ltm_facts"]
        assert len(facts) == 2
        assert any("iron" in f["fact"].lower() or "REDACTED" in f["fact"] for f in facts)

    def test_export_empty_stores(self, exporter):
        data, counts = exporter.export_all(TEST_USER_ID)
        payload = json.loads(data)
        assert all(len(v) == 0 for v in payload["stores"].values())
        assert sum(counts.values()) == 0


class TestExportHypotheses:
    def test_export_hypotheses(self, exporter, db):
        _seed_hypothesis(db)
        data, counts = exporter.export_all(TEST_USER_ID)
        payload = json.loads(data)
        assert counts["hypotheses"] >= 1
        hyps = payload["stores"]["hypotheses"]
        assert len(hyps) >= 1
        h = hyps[0]
        assert "title" in h
        assert "confidence" in h
        assert "evidence_for" in h


class TestExportJournal:
    def test_export_journal(self, exporter, db):
        _seed_journal(db)
        data, counts = exporter.export_all(TEST_USER_ID)
        assert counts["medical_journal"] >= 1
        payload = json.loads(data)
        entries = payload["stores"]["medical_journal"]
        assert len(entries) >= 1
        assert entries[0]["speaker"] in ("user", "bot")


class TestExportKnowledgeBase:
    def test_export_knowledge_base(self, exporter, db):
        from healthbot.research.knowledge_base import KnowledgeBase
        kb = KnowledgeBase(db)
        kb.store_finding("vitamin D", "Low vitamin D linked to fatigue", "pubmed")
        data, counts = exporter.export_all(TEST_USER_ID)
        assert counts["knowledge_base"] >= 1
        payload = json.loads(data)
        entries = payload["stores"]["knowledge_base"]
        assert len(entries) >= 1


class TestExportEvidence:
    def test_export_external_evidence(self, exporter, db):
        from healthbot.research.external_evidence_store import ExternalEvidenceStore
        store = ExternalEvidenceStore(db)
        store.store("pubmed", "ferritin deficiency treatment", "Take iron supplements")
        data, counts = exporter.export_all(TEST_USER_ID)
        assert counts["external_evidence"] >= 1


class TestExportPIIRedaction:
    def test_plain_mode_redacts_phi(self, db, config, key_manager):
        """PHI in text fields should be redacted in plain mode."""
        fw = PhiFirewall()
        # Insert LTM with a phone number (PHI)
        db.insert_ltm(TEST_USER_ID, "contact", "Call me at 555-123-4567")
        exporter = KnowledgeExporter(db, config, key_manager, fw)
        data, _ = exporter.export_all(TEST_USER_ID, mode="plain")
        payload = json.loads(data)
        facts = payload["stores"]["ltm_facts"]
        # The phone number should be redacted
        for f in facts:
            if "contact" in f.get("category", ""):
                assert "555-123-4567" not in f["fact"]


# ══════════════════════════════════════════════════════════════════
# PHASE 2: Import tests
# ══════════════════════════════════════════════════════════════════


def _make_export_payload(**overrides):
    """Build a minimal valid export JSON."""
    base = {
        "format": "healthbot_knowledge_export",
        "version": 1,
        "exported_at": "2026-03-03T00:00:00+00:00",
        "mode": "plain",
        "stores": {
            "ltm_facts": [],
            "hypotheses": [],
            "medical_journal": [],
            "claude_insights": [],
            "knowledge_base": [],
            "external_evidence": [],
        },
    }
    base.update(overrides)
    return json.dumps(base).encode("utf-8")


class TestImportValidation:
    def test_reject_invalid_json(self, importer):
        report = importer.import_bytes(b"not json at all", TEST_USER_ID)
        assert report.total_imported == 0
        assert len(report.errors) > 0

    def test_reject_wrong_format(self, importer):
        data = json.dumps({"format": "something_else"}).encode()
        report = importer.import_bytes(data, TEST_USER_ID)
        assert report.total_imported == 0
        assert "Unknown format" in report.errors[0]

    def test_import_empty_stores(self, importer):
        data = _make_export_payload()
        report = importer.import_bytes(data, TEST_USER_ID)
        assert report.total_imported == 0
        assert len(report.errors) == 0


class TestImportLTM:
    def test_import_ltm_facts(self, importer, db):
        payload = _make_export_payload()
        stores = json.loads(payload)
        stores["stores"]["ltm_facts"] = [
            {"category": "medical", "fact": "Has celiac disease", "source": "import"},
            {"category": "lifestyle", "fact": "Runs 5k daily", "source": "import"},
        ]
        data = json.dumps(stores).encode()
        report = importer.import_bytes(data, TEST_USER_ID)
        assert report.ltm_facts == 2

    def test_ltm_dedup_exact_match(self, importer, db):
        """Duplicate (category, fact) should be skipped."""
        db.insert_ltm(TEST_USER_ID, "medical", "Has celiac disease")
        stores = json.loads(_make_export_payload())
        stores["stores"]["ltm_facts"] = [
            {"category": "medical", "fact": "Has celiac disease", "source": "import"},
        ]
        data = json.dumps(stores).encode()
        report = importer.import_bytes(data, TEST_USER_ID)
        assert report.ltm_facts == 0
        assert report.duplicates_skipped == 1


class TestImportHypotheses:
    def test_import_hypothesis(self, importer, db):
        stores = json.loads(_make_export_payload())
        stores["stores"]["hypotheses"] = [
            {
                "title": "Iron deficiency anemia",
                "status": "active",
                "confidence": 0.7,
                "evidence_for": ["Low ferritin"],
                "evidence_against": [],
                "missing_tests": ["Serum iron"],
                "notes": "",
            },
        ]
        data = json.dumps(stores).encode()
        report = importer.import_bytes(data, TEST_USER_ID)
        assert report.hypotheses == 1

    def test_hypothesis_dedup_via_upsert(self, importer, db):
        """Matching hypothesis title should merge, not duplicate."""
        _seed_hypothesis(db)  # "Subclinical hypothyroidism"
        stores = json.loads(_make_export_payload())
        stores["stores"]["hypotheses"] = [
            {
                "title": "Subclinical hypothyroidism",
                "status": "active",
                "confidence": 0.8,
                "evidence_for": ["TSH trending up"],
                "evidence_against": [],
                "missing_tests": [],
                "notes": "",
            },
        ]
        data = json.dumps(stores).encode()
        report = importer.import_bytes(data, TEST_USER_ID)
        # upsert_hypothesis merges — count reflects the call was made
        assert report.hypotheses == 1
        # Should still only have 1 hypothesis total
        all_hyps = db.get_all_hypotheses(TEST_USER_ID)
        assert len(all_hyps) == 1


class TestImportJournal:
    def test_import_journal_entries(self, importer, db):
        stores = json.loads(_make_export_payload())
        stores["stores"]["medical_journal"] = [
            {
                "speaker": "user",
                "content": "Started new medication",
                "category": "medication",
                "timestamp": "2026-01-15T10:00:00",
                "source": "conversation",
            },
        ]
        data = json.dumps(stores).encode()
        report = importer.import_bytes(data, TEST_USER_ID)
        assert report.medical_journal == 1

    def test_journal_dedup_timestamp_speaker(self, importer, db):
        """Same (timestamp, speaker) should be skipped."""
        db.insert_journal_entry(
            TEST_USER_ID, "user", "Started new medication",
            category="medication", source="conversation",
        )
        existing = db.query_journal(TEST_USER_ID)
        ts = existing[0].get("_timestamp", existing[0].get("timestamp", ""))

        stores = json.loads(_make_export_payload())
        stores["stores"]["medical_journal"] = [
            {
                "speaker": "user",
                "content": "Started new medication",
                "timestamp": ts,
                "source": "conversation",
            },
        ]
        data = json.dumps(stores).encode()
        report = importer.import_bytes(data, TEST_USER_ID)
        assert report.medical_journal == 0
        assert report.duplicates_skipped == 1


class TestImportPII:
    def test_pii_redacted_on_import(self, db, config, key_manager):
        """PHI in imported text should be caught and redacted."""
        fw = PhiFirewall()
        imp = KnowledgeImporter(db, config, key_manager, fw)

        stores = json.loads(_make_export_payload())
        stores["stores"]["ltm_facts"] = [
            {
                "category": "contact",
                "fact": "SSN is 123-45-6789",
                "source": "import",
            },
        ]
        data = json.dumps(stores).encode()
        report = imp.import_bytes(data, TEST_USER_ID)
        assert report.pii_redacted >= 1
        # The fact was still imported (redacted)
        assert report.ltm_facts == 1


# ══════════════════════════════════════════════════════════════════
# PHASE 3: Encrypted round-trip tests
# ══════════════════════════════════════════════════════════════════


class TestEncryptedRoundTrip:
    def test_export_encrypted_import(self, exporter, db, tmp_path):
        """Export encrypted → import with password → data restored."""
        _seed_ltm(db)
        _seed_hypothesis(db)

        # Export encrypted
        enc_bytes, counts = exporter.export_all(
            TEST_USER_ID, mode="encrypted", password="test-pw-123",
        )
        assert counts["ltm_facts"] == 2
        assert counts["hypotheses"] >= 1

        # Verify it's not valid JSON (it's encrypted)
        with pytest.raises((json.JSONDecodeError, UnicodeDecodeError)):
            json.loads(enc_bytes)

        # Import on a fresh DB (separate path to avoid sharing)
        from healthbot.config import Config

        fresh_vault = tmp_path / "fresh_vault"
        fresh_vault.mkdir()
        (fresh_vault / "db").mkdir()
        fresh_config = Config(vault_home=fresh_vault)
        fresh_db = HealthDB(fresh_config, exporter._km)
        fresh_db.open()
        fresh_db.run_migrations()
        try:
            imp = KnowledgeImporter(
                fresh_db, fresh_config, exporter._km,
                PhiFirewall(),
            )
            report = imp.import_bytes(enc_bytes, TEST_USER_ID, password="test-pw-123")
            assert report.ltm_facts == 2
            assert report.hypotheses >= 1
        finally:
            fresh_db.close()

    def test_wrong_password_fails(self, exporter, db):
        """Wrong password should produce an error, not silently fail."""
        _seed_ltm(db)
        enc_bytes, _ = exporter.export_all(
            TEST_USER_ID, mode="encrypted", password="correct",
        )
        imp = KnowledgeImporter(db, exporter._config, exporter._km, PhiFirewall())
        report = imp.import_bytes(enc_bytes, TEST_USER_ID, password="wrong")
        assert report.total_imported == 0
        assert any("Decryption failed" in e or "decrypt" in e.lower() for e in report.errors)


# ══════════════════════════════════════════════════════════════════
# PHASE 4: Telegram export handler tests
# ══════════════════════════════════════════════════════════════════


class TestExportHandler:
    @pytest.mark.asyncio
    async def test_export_knowledge_plain(self, config, key_manager, phi_firewall):
        """The /export_knowledge handler should send a JSON file."""
        from healthbot.bot.handler_core import HandlerCore

        core = MagicMock(spec=HandlerCore)
        core._config = config
        core._km = key_manager
        core._fw = phi_firewall

        # Mock a DB with no data
        mock_db = MagicMock()
        mock_db.get_ltm_by_user.return_value = []
        mock_db.get_all_hypotheses.return_value = []
        mock_db.query_journal.return_value = []
        mock_db.conn = MagicMock()
        mock_db.conn.execute.return_value.fetchall.return_value = []
        core._get_db.return_value = mock_db
        core._check_auth.return_value = True

        from healthbot.bot.handlers_data.export import ExportMixin

        mixin = ExportMixin()
        mixin._core = core
        mixin._km = key_manager
        mixin._check_auth = lambda u: True

        update = MagicMock()
        update.effective_user.id = TEST_USER_ID
        update.effective_chat = AsyncMock()
        update.message.reply_text = AsyncMock()
        update.message.reply_document = AsyncMock()

        context = MagicMock()
        context.args = []

        # The handler uses TypingIndicator — mock it
        with patch("healthbot.bot.handlers_data.export.TypingIndicator") as mock_typing:
            mock_typing.return_value.__aenter__ = AsyncMock()
            mock_typing.return_value.__aexit__ = AsyncMock()
            await mixin.export_knowledge(update, context)

        update.message.reply_document.assert_called_once()

    @pytest.mark.asyncio
    async def test_export_knowledge_encrypted(self, config, key_manager, phi_firewall):
        """The /export_knowledge password X handler should send an .enc file."""
        from healthbot.bot.handler_core import HandlerCore

        core = MagicMock(spec=HandlerCore)
        core._config = config
        core._km = key_manager
        core._fw = phi_firewall

        mock_db = MagicMock()
        mock_db.get_ltm_by_user.return_value = []
        mock_db.get_all_hypotheses.return_value = []
        mock_db.query_journal.return_value = []
        mock_db.conn = MagicMock()
        mock_db.conn.execute.return_value.fetchall.return_value = []
        core._get_db.return_value = mock_db
        core._check_auth.return_value = True

        from healthbot.bot.handlers_data.export import ExportMixin

        mixin = ExportMixin()
        mixin._core = core
        mixin._km = key_manager
        mixin._check_auth = lambda u: True

        update = MagicMock()
        update.effective_user.id = TEST_USER_ID
        update.effective_chat = AsyncMock()
        update.message.reply_text = AsyncMock()
        update.message.reply_document = AsyncMock()

        context = MagicMock()
        context.args = ["password", "my-secret"]

        with patch("healthbot.bot.handlers_data.export.TypingIndicator") as mock_typing:
            mock_typing.return_value.__aenter__ = AsyncMock()
            mock_typing.return_value.__aexit__ = AsyncMock()
            await mixin.export_knowledge(update, context)

        update.message.reply_document.assert_called_once()
        # The file should be named .enc
        call_kwargs = update.message.reply_document.call_args
        doc = (
            call_kwargs.kwargs.get("document")
            or call_kwargs[1].get("document")
            or call_kwargs[0][0]
        )
        assert doc.name == "knowledge_export.enc"


# ══════════════════════════════════════════════════════════════════
# PHASE 5: Upload detection tests
# ══════════════════════════════════════════════════════════════════


class TestIsKnowledgeExport:
    def test_detects_valid_export(self):
        data = _make_export_payload()
        assert is_knowledge_export(data) is True

    def test_rejects_random_json(self):
        data = json.dumps({"hello": "world"}).encode()
        assert is_knowledge_export(data) is False

    def test_rejects_binary(self):
        assert is_knowledge_export(b"\x00\x01\x02\x03") is False


class TestNestedDictRedaction:
    """Fix 1: _redact_record must handle nested dicts and mixed lists."""

    def test_plain_export_redacts_nested_dict(self, db, config, key_manager):
        """Nested dict values (e.g. result_json) must be redacted."""
        fw = PhiFirewall()
        exporter = KnowledgeExporter(db, config, key_manager, fw)

        # Directly test _redact_record with nested dict containing PHI
        record = {
            "source": "pubmed",
            "result": {
                "title": "Call 555-123-4567 for results",
                "authors": "Normal text",
            },
        }
        redacted = exporter._redact_record(record)
        # Phone number in nested dict should be redacted
        assert "555-123-4567" not in redacted["result"]["title"]

    def test_redact_list_of_dicts(self, db, config, key_manager):
        """Lists containing dicts should have their dicts redacted."""
        fw = PhiFirewall()
        exporter = KnowledgeExporter(db, config, key_manager, fw)

        record = {
            "items": [
                {"note": "SSN is 123-45-6789"},
                "Call 555-123-4567",
                42,
            ],
        }
        redacted = exporter._redact_record(record)
        assert "123-45-6789" not in redacted["items"][0]["note"]
        assert "555-123-4567" not in redacted["items"][1]
        assert redacted["items"][2] == 42


class TestEncryptedExportWithoutPassword:
    """Fix 2: export_all(mode='encrypted', password=None) must raise."""

    def test_encrypted_no_password_raises(self, exporter):
        with pytest.raises(ValueError, match="Encrypted export requires a password"):
            exporter.export_all(TEST_USER_ID, mode="encrypted", password=None)

    def test_encrypted_empty_password_raises(self, exporter):
        with pytest.raises(ValueError, match="Encrypted export requires a password"):
            exporter.export_all(TEST_USER_ID, mode="encrypted", password="")


class TestExportPasswordDeletion:
    """Fix 3: /export_knowledge password X should delete the message."""

    @pytest.mark.asyncio
    async def test_password_message_deleted(self, config, key_manager, phi_firewall):
        from healthbot.bot.handler_core import HandlerCore
        from healthbot.bot.handlers_data.export import ExportMixin

        core = MagicMock(spec=HandlerCore)
        core._config = config
        core._km = key_manager
        core._fw = phi_firewall
        mock_db = MagicMock()
        mock_db.get_ltm_by_user.return_value = []
        mock_db.get_all_hypotheses.return_value = []
        mock_db.query_journal.return_value = []
        mock_db.conn = MagicMock()
        mock_db.conn.execute.return_value.fetchall.return_value = []
        core._get_db.return_value = mock_db

        mixin = ExportMixin()
        mixin._core = core
        mixin._km = key_manager
        mixin._check_auth = lambda u: True

        update = MagicMock()
        update.effective_user.id = TEST_USER_ID
        update.effective_chat = AsyncMock()
        update.message.reply_text = AsyncMock()
        update.message.reply_document = AsyncMock()
        update.message.delete = AsyncMock()

        context = MagicMock()
        context.args = ["password", "secret123"]

        with patch("healthbot.bot.handlers_data.export.TypingIndicator") as mock_typing:
            mock_typing.return_value.__aenter__ = AsyncMock()
            mock_typing.return_value.__aexit__ = AsyncMock()
            await mixin.export_knowledge(update, context)

        # The message containing the password should have been deleted
        update.message.delete.assert_called_once()


class TestImportReport:
    def test_summary_with_data(self):
        report = ImportReport(
            ltm_facts=5, hypotheses=2, knowledge_base=3,
            duplicates_skipped=10, pii_redacted=1,
        )
        s = report.summary()
        assert "5 LTM facts" in s
        assert "2 hypotheses" in s
        assert "3 KB entries" in s
        assert "10 duplicates" in s
        assert "Redacted PII" in s

    def test_summary_empty(self):
        report = ImportReport()
        assert "No new records" in report.summary()

    def test_total_imported(self):
        report = ImportReport(ltm_facts=3, medical_journal=7)
        assert report.total_imported == 10
