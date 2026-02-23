"""Tests for research query packet builder."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.research.research_packet import ResearchQueryPacket, build_research_packet


class TestResearchQueryPacket:
    def test_dataclass_defaults(self):
        pkt = ResearchQueryPacket(
            query="test", query_hash="abc", context="", created_at="now",
        )
        assert pkt.blocked is False
        assert pkt.block_reason == ""

    def test_blocked_packet_fields(self):
        pkt = ResearchQueryPacket(
            query="", query_hash="abc", context="", created_at="now",
            blocked=True, block_reason="PHI detected",
        )
        assert pkt.blocked is True
        assert pkt.block_reason == "PHI detected"


class TestBuildResearchPacket:
    def test_clean_query_passes(self):
        pkt = build_research_packet("what causes high cholesterol")
        assert pkt.blocked is False
        assert pkt.query == "what causes high cholesterol"
        assert pkt.query_hash  # non-empty hash
        assert pkt.created_at  # timestamp present

    def test_clean_query_with_context(self):
        pkt = build_research_packet("metformin interactions", context="patient takes lisinopril")
        assert pkt.blocked is False
        assert pkt.query == "metformin interactions"
        assert pkt.context == "patient takes lisinopril"

    def test_phi_in_query_blocks(self):
        pkt = build_research_packet("John Smith SSN 123-45-6789 cholesterol")
        assert pkt.blocked is True
        assert "query" in pkt.block_reason.lower()
        assert pkt.query == ""
        assert pkt.context == ""

    def test_phi_in_context_blocks(self):
        pkt = build_research_packet(
            "what causes high cholesterol",
            context="Patient John Smith DOB 01/15/1980",
        )
        assert pkt.blocked is True
        assert "context" in pkt.block_reason.lower()
        assert pkt.query == ""

    def test_hash_is_deterministic(self):
        pkt1 = build_research_packet("same query")
        pkt2 = build_research_packet("same query")
        assert pkt1.query_hash == pkt2.query_hash

    def test_hash_differs_for_different_queries(self):
        pkt1 = build_research_packet("query one")
        pkt2 = build_research_packet("query two")
        assert pkt1.query_hash != pkt2.query_hash

    def test_custom_firewall(self):
        fw = MagicMock()
        fw.contains_phi.return_value = False
        pkt = build_research_packet("test query", firewall=fw)
        assert pkt.blocked is False
        fw.contains_phi.assert_called()

    def test_custom_firewall_blocks(self):
        fw = MagicMock()
        fw.contains_phi.return_value = True
        pkt = build_research_packet("test query", firewall=fw)
        assert pkt.blocked is True

    def test_empty_context_skips_context_check(self):
        """Empty context should not trigger PHI check on context."""
        fw = MagicMock()
        fw.contains_phi.return_value = False
        build_research_packet("test query", context="", firewall=fw)
        # Should only be called once (for query), not for empty context
        assert fw.contains_phi.call_count == 1

    def test_blocked_packet_still_has_hash(self):
        pkt = build_research_packet("SSN 123-45-6789")
        assert pkt.blocked is True
        assert len(pkt.query_hash) == 16
