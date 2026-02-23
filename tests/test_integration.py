"""End-to-end integration test.

Full lifecycle: setup -> unlock -> insert data -> query -> search index ->
triage -> trends -> insights -> overdue -> correlations -> doctor prep ->
backup -> restore -> lock.
"""
from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import pytest

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import (
    LabResult,
    Medication,
    TriageLevel,
    WhoopDaily,
)
from healthbot.normalize.lab_normalizer import normalize_test_name
from healthbot.reasoning.correlate import CorrelationEngine
from healthbot.reasoning.doctor_prep import DoctorPrepEngine
from healthbot.reasoning.insights import InsightEngine
from healthbot.reasoning.overdue import OverdueDetector
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine
from healthbot.research.research_packet import build_research_packet
from healthbot.security.key_manager import KeyManager, LockedError
from healthbot.security.phi_firewall import PhiFirewall
from healthbot.security.vault import Vault
from healthbot.vault_ops.backup import VaultBackup
from healthbot.vault_ops.restore import VaultRestore

PASSPHRASE = "integration-test-passphrase-2025"


class TestFullLifecycle:
    """Test the complete HealthBot lifecycle end-to-end."""

    def test_full_lifecycle(self, tmp_path: Path) -> None:
        vault_home = tmp_path / "vault"
        vault_home.mkdir()
        config = Config(vault_home=vault_home)
        config.ensure_dirs()

        # -- Phase 1: Setup & Unlock --
        km = KeyManager(config)
        km.setup(PASSPHRASE)
        assert km.is_unlocked

        vault = Vault(config.blobs_dir, km)
        db = HealthDB(config, km)
        db.open()
        db.run_migrations()

        # -- Phase 2: Insert Lab Results --
        labs = [
            LabResult(
                id="lab1", test_name="Glucose", canonical_name="glucose",
                value=85.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                date_collected=date(2025, 1, 15), source_blob_id="blob1",
                source_page=1, source_section="Chemistry",
            ),
            LabResult(
                id="lab2", test_name="Glucose", canonical_name="glucose",
                value=95.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                date_collected=date(2025, 6, 15), source_blob_id="blob1",
                source_page=1, source_section="Chemistry",
            ),
            LabResult(
                id="lab3", test_name="Glucose", canonical_name="glucose",
                value=108.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                flag="H", date_collected=date(2025, 12, 1), source_blob_id="blob1",
                source_page=1, source_section="Chemistry",
                triage_level=TriageLevel.URGENT,
            ),
            LabResult(
                id="lab4", test_name="LDL Cholesterol", canonical_name="ldl",
                value=145.0, unit="mg/dL", reference_low=0.0, reference_high=130.0,
                flag="H", date_collected=date(2025, 6, 15), source_blob_id="blob1",
                source_page=2, source_section="Lipid Panel",
                triage_level=TriageLevel.URGENT,
            ),
        ]
        for lab in labs:
            db.insert_observation(lab)
            db.upsert_search_text(
                doc_id=lab.id, record_type="lab_result",
                date_effective=lab.date_collected.isoformat() if lab.date_collected else None,
                text=f"{lab.test_name} {lab.value} {lab.unit} {lab.reference_text}",
            )

        # -- Phase 3: Insert Medication --
        med = Medication(
            id="med1", name="Metformin", dose="500mg",
            frequency="twice daily", route="oral", status="active",
            start_date=date(2025, 7, 1),
        )
        db.insert_medication(med)

        # -- Phase 4: Insert Wearable Data --
        for i in range(5):
            wd = WhoopDaily(
                id=f"wd{i}", date=date(2025, 6, 10 + i), provider="whoop",
                hrv=50.0 + i * 2, rhr=55 - i,
                recovery_score=70.0 + i * 5, sleep_score=75.0 + i * 2,
                strain=10.0 + i, sleep_duration_min=420 + i * 10,
            )
            db.insert_wearable_daily(wd)

        # -- Phase 5: Triage --
        triage = TriageEngine()
        assert triage.classify(labs[0]) == TriageLevel.NORMAL
        assert triage.classify(labs[2]) == TriageLevel.URGENT  # 108 > ref_high 100

        level, msg = triage.check_emergency_keywords("I have severe chest pain")
        assert level == TriageLevel.EMERGENCY
        assert "911" in msg

        level2, _ = triage.check_emergency_keywords("my glucose is 108")
        assert level2 is None

        # -- Phase 6: Trends --
        trends = TrendAnalyzer(db)
        trend = trends.analyze_test("glucose")
        assert trend is not None
        assert trend.data_points == 3
        assert trend.direction == "increasing"
        assert trend.pct_change > 0
        formatted = trends.format_trend(trend)
        assert "Glucose" in formatted

        # -- Phase 7: Insights --
        insights = InsightEngine(db, triage, trends)
        scores = insights.compute_domain_scores()
        assert len(scores) > 0
        metabolic = next((s for s in scores if s.domain == "metabolic"), None)
        assert metabolic is not None
        assert metabolic.tests_found > 0

        dashboard = insights.generate_dashboard()
        assert "HEALTH DASHBOARD" in dashboard
        assert "NOTABLE TRENDS" in dashboard

        # -- Phase 8: Overdue --
        overdue = OverdueDetector(db)
        overdue_items = overdue.check_overdue()
        # glucose was tested Dec 2025, so may not be overdue depending on test date
        formatted_overdue = overdue.format_reminders(overdue_items)
        assert isinstance(formatted_overdue, str)

        # -- Phase 9: Correlations --
        corr_engine = CorrelationEngine(db)
        corrs = corr_engine.auto_discover()
        assert isinstance(corrs, list)
        formatted_corrs = corr_engine.format_correlations(corrs)
        assert isinstance(formatted_corrs, str)

        # -- Phase 10: Doctor Prep --
        doctor = DoctorPrepEngine(db, triage, trends, overdue)
        prep = doctor.generate_prep()
        assert "DOCTOR VISIT PREPARATION" in prep
        assert "ACTIVE MEDICATIONS" in prep
        assert "SUGGESTED QUESTIONS" in prep

        # -- Phase 11: Vault Blob Operations --
        blob_id = vault.store_blob(b"fake PDF content for test")
        assert vault.blob_exists(blob_id)
        retrieved = vault.retrieve_blob(blob_id)
        assert retrieved == b"fake PDF content for test"

        # -- Phase 12: PHI Firewall --
        fw = PhiFirewall()
        assert fw.contains_phi("SSN: 123-45-6789")
        assert not fw.contains_phi("glucose level 108 mg/dL")
        redacted = fw.redact("Call 123-45-6789")
        assert "123-45-6789" not in redacted

        # -- Phase 13: Research Packet --
        packet = build_research_packet("What causes high glucose?", "glucose 108", fw)
        assert not packet.blocked
        assert packet.query == "What causes high glucose?"

        blocked = build_research_packet("Patient SSN 123-45-6789 has diabetes", "", fw)
        assert blocked.blocked

        # -- Phase 14: Lab Normalization --
        assert normalize_test_name("Hemoglobin A1c") == "hba1c"

        # -- Phase 15: Backup --
        db.close()
        vb = VaultBackup(config, km)
        backup_path = vb.create_backup()
        assert backup_path.exists()
        assert backup_path.stat().st_size > 0
        content = backup_path.read_bytes()
        assert b"Glucose" not in content  # Encrypted, no plaintext

        backups = vb.list_backups()
        assert len(backups) == 1

        # -- Phase 16: Lock & Verify --
        km.lock()
        assert not km.is_unlocked
        with pytest.raises(LockedError):
            km.get_key()

        # -- Phase 17: Re-unlock & Verify Data Persists --
        assert km.unlock(PASSPHRASE)
        db2 = HealthDB(config, km)
        db2.open()
        db2.run_migrations()
        obs = db2.get_observation("lab1")
        assert obs is not None
        assert obs.get("test_name") == "Glucose"
        assert obs.get("value") == 85.0

        meds = db2.get_active_medications()
        assert len(meds) == 1
        assert meds[0].get("name") == "Metformin"

        wearables = db2.query_wearable_daily()
        assert len(wearables) == 5

        db2.close()
        km.lock()


class TestRestoreFromBackup:
    """Test backup -> restore -> verify cycle."""

    def test_restore_roundtrip(self, tmp_path: Path) -> None:
        # Create and populate vault
        vault_home = tmp_path / "original"
        vault_home.mkdir()
        config = Config(vault_home=vault_home)
        config.ensure_dirs()

        km = KeyManager(config)
        km.setup(PASSPHRASE)

        vault = Vault(config.blobs_dir, km)
        db = HealthDB(config, km)
        db.open()
        db.run_migrations()

        lab = LabResult(
            id="restore_test", test_name="TSH", canonical_name="tsh",
            value=2.5, unit="mIU/L",
            date_collected=date(2025, 3, 1),
        )
        db.insert_observation(lab)
        blob_id = vault.store_blob(b"encrypted document data")
        db.close()

        # Backup
        vb = VaultBackup(config, km)
        backup_path = vb.create_backup()
        km.lock()

        # Restore to new location
        restore_home = tmp_path / "restored"
        restore_home.mkdir()
        config_r = Config(vault_home=restore_home)
        config_r.ensure_dirs()

        # Copy manifest (needed for key derivation salt)
        shutil.copy2(config.manifest_path, config_r.manifest_path)

        km_r = KeyManager(config_r)
        vr = VaultRestore(config_r, km_r)
        vr.restore(backup_path, PASSPHRASE)

        # Verify restored data
        assert km_r.unlock(PASSPHRASE)
        db_r = HealthDB(config_r, km_r)
        db_r.open()
        db_r.run_migrations()

        obs = db_r.get_observation("restore_test")
        assert obs is not None
        assert obs.get("test_name") == "TSH"
        assert obs.get("value") == 2.5

        vault_r = Vault(config_r.blobs_dir, km_r)
        data = vault_r.retrieve_blob(blob_id)
        assert data == b"encrypted document data"

        db_r.close()
        km_r.lock()
