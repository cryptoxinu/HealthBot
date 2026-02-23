"""Tests for log scrubber PHI redaction."""
from __future__ import annotations

import logging

from healthbot.security.log_scrubber import PhiScrubFilter, setup_logging
from healthbot.security.phi_firewall import PhiFirewall


class TestLogScrubber:
    """Test that log output is PHI-free."""

    def test_scrubs_ssn_from_log(self) -> None:
        fw = PhiFirewall()
        filt = PhiScrubFilter(fw)
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Processing SSN: 123-45-6789", args=(), exc_info=None,
        )
        filt.filter(record)
        assert "123-45-6789" not in record.msg
        assert "[REDACTED-ssn]" in record.msg

    def test_scrubs_email_from_log(self) -> None:
        fw = PhiFirewall()
        filt = PhiScrubFilter(fw)
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="User email: patient@hospital.com", args=(), exc_info=None,
        )
        filt.filter(record)
        assert "patient@hospital.com" not in record.msg

    def test_scrubs_log_args(self) -> None:
        fw = PhiFirewall()
        filt = PhiScrubFilter(fw)
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="Data: %s", args=("SSN: 999-88-7777",), exc_info=None,
        )
        filt.filter(record)
        assert "999-88-7777" not in str(record.args)

    def test_clean_logs_unchanged(self) -> None:
        fw = PhiFirewall()
        filt = PhiScrubFilter(fw)
        msg = "Ingested 5 lab results from PDF"
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg=msg, args=(), exc_info=None,
        )
        filt.filter(record)
        assert record.msg == msg

    def test_setup_logging_creates_logger(self, tmp_path) -> None:
        fw = PhiFirewall()
        logger = setup_logging(tmp_path / "logs", fw)
        assert logger.name == "healthbot"
        assert len(logger.handlers) >= 1
