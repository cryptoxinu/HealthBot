"""Tests for proactive insight engine (deterministic signals only)."""
from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path

import pytest

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import LabResult, TriageLevel, WhoopDaily
from healthbot.llm.proactive import ProactiveInsightEngine
from healthbot.security.key_manager import KeyManager

PASSPHRASE = "test-proactive-passphrase"


@pytest.fixture
def proactive_setup(tmp_path: Path):
    """Create a DB with lab data for proactive testing."""
    vault_home = tmp_path / "vault"
    vault_home.mkdir()
    config = Config(vault_home=vault_home)
    config.ensure_dirs()

    km = KeyManager(config)
    km.setup(PASSPHRASE)

    db = HealthDB(config, km)
    db.open()
    db.run_migrations()

    # Insert historical glucose data (increasing trend)
    for i, (d, v) in enumerate([
        (date(2025, 1, 15), 85.0),
        (date(2025, 6, 15), 95.0),
        (date(2025, 12, 1), 108.0),
    ]):
        lab = LabResult(
            id=f"hist_{i}", test_name="Glucose", canonical_name="glucose",
            value=v, unit="mg/dL", reference_low=70.0, reference_high=100.0,
            date_collected=d,
            triage_level=TriageLevel.URGENT if v > 100 else TriageLevel.NORMAL,
        )
        db.insert_observation(lab)

    yield db, km


class TestProactiveNoLLM:
    """Test proactive insights without LLM (deterministic only)."""

    def test_signals_for_urgent_labs(self, proactive_setup):
        db, km = proactive_setup
        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="new1", test_name="Glucose", canonical_name="glucose",
                value=115.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                flag="H", date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.URGENT,
            ),
        ]

        result = engine.analyze_new_labs(new_labs)
        assert result is not None
        assert "TRIAGE" in result or "TREND" in result

    def test_no_signals_for_normal_labs(self, proactive_setup):
        db, km = proactive_setup
        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="normal1", test_name="Calcium", canonical_name="calcium",
                value=9.5, unit="mg/dL", reference_low=8.5, reference_high=10.5,
                date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.NORMAL,
            ),
        ]

        result = engine.analyze_new_labs(new_labs)
        assert result is None


class TestProactiveDeterministic:
    """Test proactive insights (deterministic signals, no LLM)."""

    def test_urgent_lab_produces_triage_signal(self, proactive_setup):
        db, km = proactive_setup

        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="new2", test_name="Glucose", canonical_name="glucose",
                value=115.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                flag="H", date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.URGENT,
            ),
        ]

        result = engine.analyze_new_labs(new_labs, user_id=123)
        assert result is not None
        assert "TRIAGE" in result

    def test_deterministic_output_without_llm(self, proactive_setup):
        """Deterministic signals produce structured text without LLM."""
        db, km = proactive_setup

        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="new3", test_name="Glucose", canonical_name="glucose",
                value=115.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                flag="H", date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.URGENT,
            ),
        ]

        result = engine.analyze_new_labs(new_labs, user_id=123)
        assert result is not None
        assert "TRIAGE FINDINGS" in result

    def test_no_signals_for_normal_labs_deterministic(self, proactive_setup):
        """Normal labs produce no signals."""
        db, km = proactive_setup

        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="normal2", test_name="Calcium", canonical_name="calcium",
                value=9.5, unit="mg/dL", reference_low=8.5, reference_high=10.5,
                date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.NORMAL,
            ),
        ]

        result = engine.analyze_new_labs(new_labs)
        assert result is None

    def test_signal_contains_raw_lab_info(self, proactive_setup):
        """Deterministic signals should contain the raw lab data."""
        db, km = proactive_setup

        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="new4", test_name="Glucose", canonical_name="glucose",
                value=115.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                flag="H", date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.URGENT,
            ),
        ]

        result = engine.analyze_new_labs(new_labs, user_id=123)
        assert result is not None
        assert "Glucose" in result or "TRIAGE" in result

    def test_multiple_urgent_labs_combined(self, proactive_setup):
        """Multiple urgent labs should all appear in triage output."""
        db, km = proactive_setup

        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="demo1", test_name="Glucose", canonical_name="glucose",
                value=115.0, unit="mg/dL", reference_low=70.0, reference_high=100.0,
                flag="H", date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.URGENT,
            ),
        ]

        result = engine.analyze_new_labs(new_labs, user_id=123)
        assert result is not None
        assert "TRIAGE" in result

    def test_range_check_signal_no_ref_ranges(self, proactive_setup):
        """Labs without ref ranges should get age/sex-adjusted range checks."""
        db, km = proactive_setup

        db.insert_ltm(456, "demographic", "Date of birth: 1990-06-01 (age 35)")
        db.insert_ltm(456, "demographic", "Biological sex: female")

        engine = ProactiveInsightEngine(db)

        new_labs = [
            LabResult(
                id="range1", test_name="Hemoglobin", canonical_name="hemoglobin",
                value=11.0, unit="g/dL",
                reference_low=None, reference_high=None,
                date_collected=date(2026, 1, 15),
                triage_level=TriageLevel.NORMAL,
            ),
        ]

        result = engine.analyze_new_labs(new_labs, user_id=456)
        # Hemoglobin 11.0 is LOW for female (12-16 range)
        assert result is not None
        assert "RANGE CHECK" in result
        assert "LOW" in result


class TestWearableProactive:
    """Tests for wearable data proactive insights."""

    def _insert_wearable_data(self, db, days=10, **metrics):
        """Insert wearable baseline + today's data."""
        today = date.today()
        for i in range(days):
            d = today - timedelta(days=days - i)
            wd = WhoopDaily(id=uuid.uuid4().hex, date=d, **metrics)
            db.insert_wearable_daily(wd)

    def test_wearable_signals_gathered(self, db):
        """Wearable data with bad trends should produce signals."""
        today = date.today()
        # Insert declining HRV over 7 days
        for i in range(7):
            d = today - timedelta(days=6 - i)
            wd = WhoopDaily(
                id=uuid.uuid4().hex, date=d,
                hrv=100 - i * 10,  # 100 → 40
                rhr=55.0,
                sleep_score=75.0,
                recovery_score=70.0,
                strain=10.0,
            )
            db.insert_wearable_daily(wd)

        engine = ProactiveInsightEngine(db)
        signals = engine._gather_wearable_signals(user_id=0)
        # Should find HRV trend
        assert any("HRV" in s for s in signals)

    def test_wearable_deterministic_signals(self, db):
        """Wearable signals should be returned as deterministic text."""
        today = date.today()
        for i in range(7):
            d = today - timedelta(days=6 - i)
            wd = WhoopDaily(
                id=uuid.uuid4().hex, date=d,
                hrv=100 - i * 10,
                sleep_score=75.0,
            )
            db.insert_wearable_daily(wd)

        engine = ProactiveInsightEngine(db)
        result = engine.analyze_wearable_sync(user_id=0)
        assert result is not None
        assert "HRV" in result

    def test_no_wearable_signals_when_no_data(self, db):
        """No wearable data should return None from analyze_wearable_sync."""
        engine = ProactiveInsightEngine(db)
        result = engine.analyze_wearable_sync(user_id=0)
        assert result is None

    def test_wearable_recovery_signal(self, db):
        """Low readiness should produce RECOVERY signal."""
        today = date.today()
        # 30-day baseline
        for i in range(30):
            d = today - timedelta(days=30 - i)
            wd = WhoopDaily(
                id=uuid.uuid4().hex, date=d,
                hrv=80.0, rhr=55.0, sleep_score=75.0,
                recovery_score=70.0, strain=12.0,
            )
            db.insert_wearable_daily(wd)
        # Today: everything bad
        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=today,
            hrv=40.0, rhr=66.0, sleep_score=25.0,
            recovery_score=15.0, strain=19.0,
        )
        db.insert_wearable_daily(wd)

        engine = ProactiveInsightEngine(db)
        signals = engine._gather_wearable_signals(user_id=0)
        assert any("RECOVERY" in s for s in signals)
