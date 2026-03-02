"""Tests for AI-ready anonymized health data export."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from healthbot.export.ai_export import AiExporter, ValidationReport
from healthbot.llm.anonymizer import Anonymizer
from healthbot.security.phi_firewall import PhiFirewall


def _make_db(**overrides):
    """Create a mock DB with default empty returns."""
    db = MagicMock()
    db.get_user_demographics.return_value = overrides.get("demographics", {})
    db.query_observations.return_value = overrides.get("labs", [])
    db.get_active_medications.return_value = overrides.get("medications", [])
    db.query_wearable_daily.return_value = overrides.get("wearables", [])
    db.get_active_hypotheses.return_value = overrides.get("hypotheses", [])
    db.get_ltm_by_user.return_value = overrides.get("ltm", [])
    db.query_journal.return_value = overrides.get("journal", [])
    db.get_genetic_variant_count.return_value = 0
    db.get_genetic_variants.return_value = []
    return db


def _make_exporter(db=None, ollama=None):
    """Create an AiExporter with real anonymizer and phi_firewall (no NER)."""
    db = db or _make_db()
    fw = PhiFirewall()
    anon = Anonymizer(phi_firewall=fw, use_ner=False)
    return AiExporter(db=db, anonymizer=anon, phi_firewall=fw, ollama=ollama)


@patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
class TestAiExporterAssembly:
    """Layer 1: assembly-time stripping."""

    def test_empty_db_produces_valid_markdown(self, _mock_canary):
        exporter = _make_exporter()
        result = exporter.export(user_id=1)
        assert "# Health Data Export" in result.markdown
        assert result.validation.layer1_passed
        assert result.validation.layer2_passed

    def test_exact_age_in_export(self, _mock_canary):
        db = _make_db(demographics={"age": 33, "sex": "Male"})
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        age_line = [x for x in result.markdown.split("\n") if "Age" in x][0]
        assert "Age**: 33" in age_line

    def test_dob_never_appears(self, _mock_canary):
        db = _make_db(demographics={"age": 40, "dob": "1985-03-15"})
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "1985-03-15" not in result.markdown
        assert "1985" not in result.markdown

    def test_lab_name_kept_provider_stripped(self, _mock_canary):
        """Lab brand (Quest) is medical metadata → kept. Provider → stripped."""
        db = _make_db(labs=[{
            "test_name": "Glucose",
            "canonical_name": "glucose",
            "value": 95.0,
            "unit": "mg/dL",
            "reference_low": 70.0,
            "reference_high": 100.0,
            "flag": "",
            "lab_name": "Quest Diagnostics",
            "ordering_provider": "Dr. John Smith",
            "_meta": {"date_effective": "2024-11-15"},
        }])
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "Quest Diagnostics" in result.markdown  # Lab brand kept
        assert "Smith" not in result.markdown  # Provider stripped
        assert "Glucose" in result.markdown
        assert "95" in result.markdown

    def test_prescriber_stripped(self, _mock_canary):
        db = _make_db(medications=[{
            "name": "Metformin",
            "dose": "500",
            "unit": "mg",
            "frequency": "twice daily",
            "prescriber": "Dr. Jane Williams",
        }])
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "Williams" not in result.markdown
        assert "Metformin" in result.markdown
        assert "500 mg" in result.markdown

    def test_lab_dates_preserved(self, _mock_canary):
        db = _make_db(labs=[{
            "test_name": "TSH",
            "value": 2.5,
            "unit": "mIU/L",
            "flag": "",
            "_meta": {"date_effective": "2024-11-15"},
        }])
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "2024-11-15" in result.markdown

    def test_wearable_format(self, _mock_canary):
        wearable = {
            "_date": date.today().isoformat(),
            "hrv": 45.0,
            "rhr": 58.0,
            "sleep_score": 82.0,
            "recovery_score": 75.0,
            "strain": 12.5,
        }
        db = _make_db(wearables=[wearable])
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "45" in result.markdown
        assert "58" in result.markdown

    def test_exact_height_weight_in_export(self, _mock_canary):
        # 1.78m = 70.08 inches -> 5'10"
        # 75kg = 165.3 lbs
        db = _make_db(demographics={"height_m": 1.78, "weight_kg": 75.0, "bmi": 23.7})
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "1.78" in result.markdown
        assert "75.0" in result.markdown
        assert "5'10\"" in result.markdown
        assert "165 lbs" in result.markdown


@patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
class TestAiExporterValidationLayer2:
    """Layer 2: regex + NER scan catches PII in free text."""

    def test_ssn_in_journal_caught(self, _mock_canary):
        db = _make_db(journal=[{
            "_timestamp": "2024-11-15 10:00:00",
            "_category": "note",
            "speaker": "user",
            "content": "My SSN is 123-45-6789 and I feel tired.",
        }])
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "123-45-6789" not in result.markdown

    def test_phone_in_context_caught(self, _mock_canary):
        db = _make_db(ltm=[{
            "_category": "medical",
            "_source": "conversation",
            "fact": "Call me at 555-123-4567 for results.",
        }])
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "555-123-4567" not in result.markdown

    def test_email_in_journal_caught(self, _mock_canary):
        db = _make_db(journal=[{
            "_timestamp": "2024-11-15 10:00:00",
            "_category": "note",
            "speaker": "user",
            "content": "Send results to john@example.com please.",
        }])
        exporter = _make_exporter(db)
        result = exporter.export(user_id=1)
        assert "john@example.com" not in result.markdown


@patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
class TestAiExporterValidationLayer3:
    """Layer 3: LLM scan (Ollama)."""

    def test_skipped_when_no_ollama(self, _mock_canary):
        exporter = _make_exporter(ollama=None)
        result = exporter.export(user_id=1)
        assert result.validation.layer3_passed is None

    def test_skipped_when_ollama_down(self, _mock_canary):
        ollama = MagicMock()
        ollama.is_available.return_value = False
        exporter = _make_exporter(ollama=ollama)
        result = exporter.export(user_id=1)
        assert result.validation.layer3_passed is None

    def test_clean_passes(self, _mock_canary):
        ollama = MagicMock()
        ollama.is_available.return_value = True
        ollama.send.return_value = '{"found": false}'
        exporter = _make_exporter(ollama=ollama)
        result = exporter.export(user_id=1)
        assert result.validation.layer3_passed is True

    def test_pii_found_triggers_redaction(self, _mock_canary):
        db = _make_db(ltm=[{
            "_category": "medical",
            "_source": "conversation",
            "fact": "Sees Anderson for cardiology checkups.",
        }])
        ollama = MagicMock()
        ollama.is_available.return_value = True
        ollama.send.return_value = json.dumps({
            "found": True,
            "items": [{"text": "Anderson", "type": "provider_name"}],
        })
        exporter = _make_exporter(db, ollama=ollama)
        result = exporter.export(user_id=1)
        assert "Anderson" not in result.markdown
        assert result.validation.layer3_passed is False

    def test_bad_json_fails_safe(self, _mock_canary):
        ollama = MagicMock()
        ollama.is_available.return_value = True
        ollama.send.return_value = "I couldn't parse that request properly"
        exporter = _make_exporter(ollama=ollama)
        result = exporter.export(user_id=1)
        # Should not crash; layer3 skipped due to parse error
        assert result.validation.layer3_passed is None
        assert any("error" in w.lower() for w in result.validation.warnings)

    def test_json_in_code_block_parsed(self, _mock_canary):
        ollama = MagicMock()
        ollama.is_available.return_value = True
        ollama.send.return_value = '```json\n{"found": false}\n```'
        exporter = _make_exporter(ollama=ollama)
        result = exporter.export(user_id=1)
        assert result.validation.layer3_passed is True


class TestValidationReport:
    def test_summary_format(self):
        report = ValidationReport()
        report.add(1, "dob", "DOB excluded", "stripped")
        report.layer1_passed = True
        report.layer2_passed = True
        report.layer3_passed = None

        summary = report.summary()
        assert "Layer 1" in summary
        assert "PASS" in summary
        assert "SKIPPED" in summary
        assert "DOB excluded" in summary

    def test_summary_counts_stripped_and_redacted(self):
        report = ValidationReport()
        report.add(1, "dob", "DOB excluded", "stripped")
        report.add(1, "age", "Age banded", "stripped")
        report.add(2, "phi", "PII redacted", "redacted")

        summary = report.summary()
        assert "2 fields stripped" in summary
        assert "1 items redacted" in summary


@patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
class TestAiExportToFile:
    def test_file_saved_with_correct_extension(self, _mock_canary, tmp_path):
        exporter = _make_exporter()
        result = exporter.export_to_file(user_id=1, exports_dir=tmp_path)
        assert result.file_path is not None
        assert result.file_path.exists()
        assert result.file_path.suffix == ".md"
        assert "health_export_ai_" in result.file_path.name
        content = result.file_path.read_text()
        assert "# Health Data Export" in content

    def test_file_in_correct_directory(self, _mock_canary, tmp_path):
        subdir = tmp_path / "exports"
        exporter = _make_exporter()
        result = exporter.export_to_file(user_id=1, exports_dir=subdir)
        assert result.file_path.parent == subdir
        assert subdir.exists()


class TestAutoAiExport:
    """Scheduled auto-export via AlertScheduler."""

    @pytest.mark.slow
    @pytest.mark.asyncio
    async def test_auto_export_sends_file(self, tmp_path) -> None:
        from healthbot.bot.scheduler import AlertScheduler
        from healthbot.config import Config

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        config.exports_dir = tmp_path / "exports"
        config.ollama_model = "test"
        config.ollama_url = "http://localhost:11434"
        config.ollama_timeout = 30
        config.allowed_user_ids = [123]
        km = MagicMock()
        km.is_unlocked = True

        scheduler = AlertScheduler(config, km, chat_id=123)

        context = MagicMock()
        context.bot = AsyncMock()

        mock_result = MagicMock()
        mock_result.markdown = "# Test Export"
        mock_result.file_path = tmp_path / "exports" / "test.md"
        mock_result.file_path.parent.mkdir(parents=True, exist_ok=True)
        mock_result.file_path.write_text("# Test Export")
        mock_result.validation = MagicMock()
        mock_result.validation.summary.return_value = "All clear"

        with patch(
            "healthbot.export.ai_export.AiExporter"
        ) as mock_cls, patch.object(
            scheduler, "_get_db", return_value=MagicMock(),
        ):
            mock_cls.return_value.export_to_file.return_value = mock_result
            await scheduler._auto_ai_export(context)

        context.bot.send_document.assert_called_once()

    @pytest.mark.asyncio
    async def test_auto_export_skips_when_locked(self, tmp_path) -> None:
        from healthbot.bot.scheduler import AlertScheduler
        from healthbot.config import Config

        config = MagicMock(spec=Config)
        config.vault_home = tmp_path
        config.allowed_user_ids = [123]
        km = MagicMock()
        km.is_unlocked = False

        scheduler = AlertScheduler(config, km, chat_id=123)
        context = MagicMock()
        context.bot = AsyncMock()

        await scheduler._auto_ai_export(context)

        context.bot.send_document.assert_not_called()

    def test_config_defaults_off(self):
        from healthbot.config import Config

        config = Config()
        assert config.auto_ai_export is False
        assert config.auto_ai_export_interval == 86400
