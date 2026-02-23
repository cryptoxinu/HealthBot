"""Tests for medication side effect monitor."""
from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

from healthbot.reasoning.side_effect_monitor import (
    SIDE_EFFECT_PROFILES,
    SideEffectAlert,
    SideEffectMonitor,
    format_alerts,
    format_watch_list,
)


def _make_db(
    meds: list[dict], observations: list[dict] | None = None,
) -> MagicMock:
    db = MagicMock()
    db.get_active_medications.return_value = meds
    observations = observations or []

    def query_obs(
        record_type=None, canonical_name=None,
        start_date=None, end_date=None,
        triage_level=None, limit=200, user_id=None,
    ):
        results = []
        for obs in observations:
            if canonical_name and obs.get("canonical_name") != canonical_name:
                continue
            results.append(obs)
        results.sort(
            key=lambda x: x.get("date_collected", ""), reverse=True,
        )
        return results[:limit]

    db.query_observations.side_effect = query_obs
    return db


def _obs(
    name: str, value: float, dt: str, flag: str = "",
) -> dict:
    return {
        "canonical_name": name,
        "value": value,
        "date_collected": dt,
        "_date_effective": dt,
        "flag": flag,
    }


class TestWatchList:
    def test_statin_watch(self):
        meds = [{"name": "atorvastatin 40mg"}]
        db = _make_db(meds)
        monitor = SideEffectMonitor(db)
        watches = monitor.get_watch_list(user_id=1)

        effects = [w.effect for w in watches]
        assert "muscle pain/myalgia" in effects
        assert "liver enzyme elevation" in effects

    def test_metformin_watch(self):
        meds = [{"name": "metformin 1000mg"}]
        db = _make_db(meds)
        monitor = SideEffectMonitor(db)
        watches = monitor.get_watch_list(user_id=1)

        effects = [w.effect for w in watches]
        assert "B12 deficiency" in effects

    def test_unknown_med_no_watches(self):
        meds = [{"name": "randomdrug 50mg"}]
        db = _make_db(meds)
        monitor = SideEffectMonitor(db)
        watches = monitor.get_watch_list(user_id=1)
        assert watches == []

    def test_lab_marker_never_checked(self):
        meds = [{"name": "atorvastatin"}]
        db = _make_db(meds, [])
        monitor = SideEffectMonitor(db)
        watches = monitor.get_watch_list(user_id=1)

        ck_watch = [w for w in watches if w.lab_marker == "ck"]
        assert len(ck_watch) == 1
        assert ck_watch[0].last_checked == "never"
        assert ck_watch[0].months_since == -1

    def test_lab_marker_checked_recently(self):
        recent = (date.today() - timedelta(days=30)).isoformat()
        meds = [{"name": "atorvastatin"}]
        obs = [_obs("ck", 150.0, recent)]
        db = _make_db(meds, obs)
        monitor = SideEffectMonitor(db)
        watches = monitor.get_watch_list(user_id=1)

        ck_watch = [w for w in watches if w.lab_marker == "ck"]
        assert len(ck_watch) == 1
        assert ck_watch[0].last_checked == recent
        assert ck_watch[0].months_since == 1

    def test_multiple_meds(self):
        meds = [
            {"name": "atorvastatin"},
            {"name": "metformin"},
        ]
        db = _make_db(meds)
        monitor = SideEffectMonitor(db)
        watches = monitor.get_watch_list(user_id=1)
        assert len(watches) >= 3  # statin(2) + metformin(2)


class TestActiveConcerns:
    def test_statin_high_ck(self):
        meds = [{"name": "atorvastatin 40mg"}]
        obs = [_obs("ck", 1200.0, date.today().isoformat(), "H")]
        db = _make_db(meds, obs)
        monitor = SideEffectMonitor(db)
        alerts = monitor.check_active_concerns(user_id=1)

        ck_alerts = [a for a in alerts if a.lab_marker == "ck"]
        assert len(ck_alerts) == 1
        assert ck_alerts[0].severity == "watch"
        assert "muscle" in ck_alerts[0].effect

    def test_statin_very_high_ck(self):
        meds = [{"name": "atorvastatin"}]
        obs = [_obs("ck", 5000.0, date.today().isoformat(), "HH")]
        db = _make_db(meds, obs)
        monitor = SideEffectMonitor(db)
        alerts = monitor.check_active_concerns(user_id=1)

        ck_alerts = [a for a in alerts if a.lab_marker == "ck"]
        assert len(ck_alerts) == 1
        assert ck_alerts[0].severity == "urgent"

    def test_metformin_low_b12(self):
        meds = [{"name": "metformin 500mg"}]
        obs = [_obs("vitamin_b12", 180.0, date.today().isoformat(), "L")]
        db = _make_db(meds, obs)
        monitor = SideEffectMonitor(db)
        alerts = monitor.check_active_concerns(user_id=1)

        b12_alerts = [a for a in alerts if a.lab_marker == "vitamin_b12"]
        assert len(b12_alerts) == 1
        assert "B12" in b12_alerts[0].effect

    def test_ace_high_potassium(self):
        meds = [{"name": "lisinopril 10mg"}]
        obs = [_obs("potassium", 5.8, date.today().isoformat(), "H")]
        db = _make_db(meds, obs)
        monitor = SideEffectMonitor(db)
        alerts = monitor.check_active_concerns(user_id=1)

        k_alerts = [a for a in alerts if a.lab_marker == "potassium"]
        assert len(k_alerts) == 1

    def test_normal_labs_no_alerts(self):
        meds = [{"name": "atorvastatin"}]
        obs = [_obs("ck", 150.0, date.today().isoformat(), "")]
        db = _make_db(meds, obs)
        monitor = SideEffectMonitor(db)
        alerts = monitor.check_active_concerns(user_id=1)
        assert alerts == []

    def test_no_lab_data_no_alerts(self):
        meds = [{"name": "atorvastatin"}]
        db = _make_db(meds, [])
        monitor = SideEffectMonitor(db)
        alerts = monitor.check_active_concerns(user_id=1)
        assert alerts == []


class TestFormatting:
    def test_format_empty_watch_list(self):
        result = format_watch_list([])
        assert "No medications" in result

    def test_format_watch_list_with_data(self):
        from healthbot.reasoning.side_effect_monitor import SideEffectWatch
        watches = [
            SideEffectWatch(
                med_name="atorvastatin",
                drug_key="statin",
                effect="muscle pain/myalgia",
                frequency="common",
                lab_marker="ck",
                monitoring_note="Check CK if symptoms.",
                last_checked="never",
                months_since=-1,
            ),
        ]
        result = format_watch_list(watches)
        assert "atorvastatin" in result
        assert "muscle pain" in result
        assert "never checked" in result

    def test_format_empty_alerts(self):
        assert format_alerts([]) == ""

    def test_format_alerts_with_data(self):
        alerts = [
            SideEffectAlert(
                med_name="atorvastatin",
                drug_key="statin",
                effect="muscle pain/myalgia",
                lab_marker="ck",
                lab_value="1200",
                lab_flag="H",
                monitoring_note="Check CK.",
                citation="Test citation.",
                severity="watch",
            ),
        ]
        result = format_alerts(alerts)
        assert "atorvastatin" in result
        assert "muscle pain" in result
        assert "HIGH" in result


class TestKBCoverage:
    def test_all_profiles_have_citations(self):
        for p in SIDE_EFFECT_PROFILES:
            assert p.citation, (
                f"{p.drug_key}: {p.effect} missing citation"
            )

    def test_all_lab_directions_valid(self):
        for p in SIDE_EFFECT_PROFILES:
            if p.lab_marker:
                assert p.lab_direction in ("increase", "decrease")
            else:
                assert p.lab_direction == ""

    def test_all_frequencies_valid(self):
        for p in SIDE_EFFECT_PROFILES:
            assert p.frequency in ("common", "occasional", "rare")
