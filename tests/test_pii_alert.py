"""Tests for PII alert service."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from healthbot.security.pii_alert import PiiAlertService


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Reset singleton between tests."""
    PiiAlertService.reset_instance()
    yield
    PiiAlertService.reset_instance()


class TestPiiAlertService:

    def test_record_increments_stats(self):
        svc = PiiAlertService()
        svc.record("name", "claude_cli")
        stats = svc.get_stats()
        assert stats.total_alerts == 1
        assert stats.by_category["name"] == 1
        assert stats.by_destination["claude_cli"] == 1

    def test_multiple_records(self):
        svc = PiiAlertService()
        svc.record("SSN", "research")
        svc.record("name", "mcp")
        svc.record("SSN", "clean_db")
        stats = svc.get_stats()
        assert stats.total_alerts == 3
        assert stats.by_category["SSN"] == 2
        assert stats.by_category["name"] == 1
        assert stats.by_destination["research"] == 1

    def test_get_recent_newest_first(self):
        svc = PiiAlertService()
        svc.record("name", "a")
        svc.record("SSN", "b")
        svc.record("email", "c")
        recent = svc.get_recent(2)
        assert len(recent) == 2
        assert recent[0].category == "email"
        assert recent[1].category == "SSN"

    def test_empty_report(self):
        svc = PiiAlertService()
        report = svc.format_report()
        assert "No PII leaks detected" in report

    def test_report_with_alerts(self):
        svc = PiiAlertService()
        svc.record("name", "claude_cli")
        svc.record("SSN", "research")
        report = svc.format_report()
        assert "Total alerts: 2" in report
        assert "name:" in report
        assert "SSN:" in report

    def test_log_file_written(self, tmp_path: Path):
        svc = PiiAlertService(log_dir=tmp_path)
        svc.record("phone", "mcp")
        log_file = tmp_path / "pii_alerts.log"
        assert log_file.exists()
        content = log_file.read_text()
        entry = json.loads(content.strip())
        assert entry["category"] == "phone"
        assert entry["destination"] == "mcp"
        assert entry["blocked"] is True

    def test_notify_callback_called(self):
        messages = []
        svc = PiiAlertService()
        svc.set_notify_callback(lambda msg: messages.append(msg))
        svc.record("name", "outbound")
        assert len(messages) == 1
        assert "PII ALERT" in messages[0]
        assert "name" in messages[0]

    def test_notify_callback_failure_nonfatal(self):
        def bad_cb(msg: str) -> None:
            raise RuntimeError("notification failed")

        svc = PiiAlertService()
        svc.set_notify_callback(bad_cb)
        # Should not raise
        svc.record("name", "outbound")
        assert svc.get_stats().total_alerts == 1

    def test_singleton(self, tmp_path: Path):
        svc1 = PiiAlertService.get_instance(log_dir=tmp_path)
        svc2 = PiiAlertService.get_instance()
        assert svc1 is svc2

    def test_last_alert_timestamp(self):
        svc = PiiAlertService()
        svc.record("name", "outbound")
        stats = svc.get_stats()
        assert stats.last_alert != ""
        assert "T" in stats.last_alert  # ISO format


class TestPiiAlertIntegration:

    def test_anonymizer_assert_safe_triggers_alert(self):
        """Verify assert_safe failure records a PII alert."""
        from healthbot.llm.anonymizer import AnonymizationError, Anonymizer
        from healthbot.security.phi_firewall import PhiFirewall

        fw = PhiFirewall()
        anon = Anonymizer(phi_firewall=fw, use_ner=False)
        svc = PiiAlertService.get_instance()

        # Text with SSN should trigger assert_safe failure
        text_with_ssn = "Patient SSN: 123-45-6789"
        with pytest.raises(AnonymizationError):
            anon.assert_safe(text_with_ssn)

        stats = svc.get_stats()
        assert stats.total_alerts >= 1

    def test_research_packet_phi_triggers_alert(self):
        """Verify research_packet hard-block records a PII alert."""
        from healthbot.research.research_packet import build_research_packet
        from healthbot.security.phi_firewall import PhiFirewall

        fw = PhiFirewall()
        svc = PiiAlertService.get_instance()

        packet = build_research_packet(
            raw_query="Check results for SSN 123-45-6789",
            firewall=fw,
        )
        assert packet.blocked
        stats = svc.get_stats()
        assert stats.total_alerts >= 1
        assert "research" in stats.by_destination
