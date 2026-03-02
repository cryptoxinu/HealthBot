"""Tests for research clients and PHI firewall enforcement."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from healthbot.config import Config
from healthbot.research.claude_cli_client import ClaudeCLIResearchClient
from healthbot.research.research_packet import build_research_packet
from healthbot.security.phi_firewall import PhiFirewall


@pytest.fixture
def fw() -> PhiFirewall:
    return PhiFirewall()


class TestResearchPacket:
    """Test ResearchQueryPacket building and PHI blocking."""

    def test_clean_query_passes(self, fw):
        """Clean query should not be blocked."""
        packet = build_research_packet("What is normal cholesterol?", firewall=fw)
        assert not packet.blocked
        assert packet.query == "What is normal cholesterol?"

    def test_phi_in_query_blocks(self, fw):
        """PHI in query must hard-block (not sanitize-and-send)."""
        packet = build_research_packet(
            "What does John Smith's glucose of 250 mean? SSN: 123-45-6789",
            firewall=fw,
        )
        assert packet.blocked
        assert "PHI detected" in packet.block_reason

    def test_phi_in_context_blocks(self, fw):
        """PHI in context must also hard-block."""
        packet = build_research_packet(
            "What is elevated TSH?",
            context="Patient: Jane Doe, DOB: 03/15/1985",
            firewall=fw,
        )
        assert packet.blocked

    def test_blocked_packet_has_no_query(self, fw):
        """Blocked packets must not contain the original query."""
        packet = build_research_packet(
            "Analyze SSN: 123-45-6789",
            firewall=fw,
        )
        assert packet.query == ""
        assert packet.context == ""


class TestClaudeCLIClient:
    """Test Claude CLI client PHI enforcement."""

    def test_blocks_phi_query(self, fw):
        """Claude CLI must not be called when PHI is detected."""
        config = Config()
        client = ClaudeCLIResearchClient(config, fw)

        with patch("subprocess.run") as mock_run:
            result = client.research("Patient: John Smith has glucose 250")
            mock_run.assert_not_called()
            assert "blocked" in result.lower()

    def test_subprocess_not_called_on_block(self, fw):
        """Verify subprocess is never invoked for blocked queries."""
        config = Config()
        client = ClaudeCLIResearchClient(config, fw)

        call_count = 0
        original_run = __import__("subprocess").run

        def counting_run(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return original_run(*args, **kwargs)

        with patch("subprocess.run", side_effect=counting_run):
            # Use a valid SSN (area 123 is valid; 999 is excluded by regex)
            client.research("SSN: 123-45-6789 what does this mean?")
            assert call_count == 0, "subprocess.run should not be called for blocked queries"

    @pytest.mark.slow
    def test_clean_query_would_call_cli(self, fw):
        """Clean query should attempt CLI call (may fail if CLI not installed)."""
        config = Config()
        client = ClaudeCLIResearchClient(config, fw)
        # CLI won't be found in test env, but we verify it tries
        result = client.research("What are normal hemoglobin levels?")
        # Should not say "blocked"
        assert "blocked" not in result.lower() or "not found" in result.lower()
