"""Tests for lab alerts engine."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.lab_alerts import CRITICAL_THRESHOLDS, LabAlertEngine


def _make_db(latest=None, previous=None):
    """Create a mock DB returning lab observations."""
    db = MagicMock()
    call_count = {"n": 0}

    def fake_query(**kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return latest or []
        return previous or []

    db.query_observations.side_effect = fake_query
    return db


def _obs(name, value, unit="mg/dL", flag="", date="2024-06-15"):
    return {
        "canonical_name": name,
        "value": value,
        "unit": unit,
        "test_name": name.upper(),
        "flag": flag,
        "_meta": {"date_effective": date},
    }


class TestLabAlerts:

    def test_no_labs_no_alerts(self):
        db = _make_db()
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)
        assert not report.has_alerts

    def test_critical_high_glucose(self):
        db = _make_db(latest=[_obs("glucose", 550)])
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)
        assert report.critical_count >= 1
        assert any(
            a.canonical_name == "glucose" and a.alert_type == "critical"
            for a in report.alerts
        )

    def test_critical_low_potassium(self):
        db = _make_db(latest=[_obs("potassium", 2.0, "mEq/L")])
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)
        assert report.critical_count >= 1

    def test_alerts_sorted_by_severity(self):
        db = _make_db(latest=[
            _obs("glucose", 550),
            _obs("potassium", 4.0),
        ])
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)
        if len(report.alerts) > 1:
            order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
            severities = [order.get(a.severity, 99) for a in report.alerts]
            assert severities == sorted(severities)

    def test_critical_thresholds_populated(self):
        assert "glucose" in CRITICAL_THRESHOLDS
        assert "potassium" in CRITICAL_THRESHOLDS
        assert "troponin" in CRITICAL_THRESHOLDS


class TestQualitativeAlerts:
    """Tests for qualitative change alerts."""

    def test_qualitative_change_detected_fires_alert(self):
        """Flipping from 'Not Detected' to 'Detected' fires a high alert."""
        latest = [_obs("jak2_v617f_mutation", "Detected", "", "A")]
        # _get_previous_labs picks the 2nd occurrence per test name
        previous = [
            _obs("jak2_v617f_mutation", "Detected", "", "A"),
            _obs("jak2_v617f_mutation", "Not Detected", ""),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 1
        assert qual_alerts[0].severity == "high"
        assert qual_alerts[0].canonical_name == "jak2_v617f_mutation"
        assert "Detected" in qual_alerts[0].message

    def test_qualitative_positive_fires_alert(self):
        """Flipping from 'Negative' to 'Positive' fires a high alert."""
        latest = [_obs("hbsag", "Positive", "", "A")]
        previous = [
            _obs("hbsag", "Positive", "", "A"),
            _obs("hbsag", "Negative", ""),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 1
        assert qual_alerts[0].severity == "high"

    def test_qualitative_reactive_fires_alert(self):
        """Flipping to 'Reactive' fires a high alert."""
        latest = [_obs("rpr", "Reactive", "", "A")]
        previous = [
            _obs("rpr", "Reactive", "", "A"),
            _obs("rpr", "Non-Reactive", ""),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 1

    def test_qualitative_no_change_no_alert(self):
        """Same qualitative value between panels fires no alert."""
        latest = [_obs("jak2_v617f_mutation", "Not Detected", "")]
        previous = [
            _obs("jak2_v617f_mutation", "Not Detected", ""),
            _obs("jak2_v617f_mutation", "Not Detected", ""),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 0

    def test_qualitative_normal_change_no_alert(self):
        """Change to a normal value (Not Detected) does not fire an alert."""
        latest = [_obs("jak2_v617f_mutation", "Not Detected", "")]
        previous = [
            _obs("jak2_v617f_mutation", "Not Detected", ""),
            _obs("jak2_v617f_mutation", "Detected", "", "A"),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 0

    def test_qualitative_skips_numeric_values(self):
        """Numeric values are not checked by qualitative alert engine."""
        latest = [_obs("glucose", 150)]
        previous = [
            _obs("glucose", 150),
            _obs("glucose", 90),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 0

    def test_qualitative_mutation_detected_fires(self):
        """'Mutation Detected' (multi-word) fires alert."""
        latest = [_obs("braf_v600e", "Mutation Detected", "", "A")]
        previous = [
            _obs("braf_v600e", "Mutation Detected", "", "A"),
            _obs("braf_v600e", "No Mutation Detected", ""),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 1

    def test_qualitative_heterozygous_fires(self):
        """'Heterozygous' fires alert."""
        latest = [_obs("factor_v_leiden", "Heterozygous", "", "A")]
        previous = [
            _obs("factor_v_leiden", "Heterozygous", "", "A"),
            _obs("factor_v_leiden", "Wild Type", ""),
        ]
        db = _make_db(latest=latest, previous=previous)
        engine = LabAlertEngine(db)
        report = engine.scan(user_id=0)

        qual_alerts = [a for a in report.alerts if a.alert_type == "qualitative_change"]
        assert len(qual_alerts) == 1
        assert qual_alerts[0].severity == "high"
