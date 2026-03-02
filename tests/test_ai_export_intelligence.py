"""Tests for AI export intelligence sections."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from healthbot.export.ai_export import AiExporter
from healthbot.security.phi_firewall import PhiFirewall


def _make_exporter():
    """Create AiExporter with fully mocked DB."""
    db = MagicMock()
    db.get_user_demographics.return_value = {"age": 30, "sex": "male"}
    db.query_observations.return_value = []
    db.get_active_medications.return_value = []
    db.query_wearable_daily.return_value = []
    db.get_active_hypotheses.return_value = []
    db.get_ltm_by_user.return_value = []
    db.query_journal.return_value = []
    db.get_genetic_variant_count.return_value = 0

    from healthbot.llm.anonymizer import Anonymizer

    fw = PhiFirewall()
    anon = Anonymizer(phi_firewall=fw, use_ner=False)
    return AiExporter(db=db, anonymizer=anon, phi_firewall=fw), db


@patch("healthbot.llm.anonymizer.Anonymizer._verify_canary")
class TestAiExportIntelligence:
    """Verify all 4 intelligence sections exist in export."""

    def test_export_has_trends_section(self, _mock_canary):
        exporter, db = _make_exporter()
        result = exporter.export(user_id=0)
        assert "## Lab Trends" in result.markdown

    def test_export_has_interactions_section(self, _mock_canary):
        exporter, db = _make_exporter()
        result = exporter.export(user_id=0)
        assert "## Drug-Lab Interactions" in result.markdown

    def test_export_has_intelligence_gaps_section(self, _mock_canary):
        exporter, db = _make_exporter()
        result = exporter.export(user_id=0)
        assert "## Intelligence Gaps" in result.markdown

    def test_export_has_panel_gaps_section(self, _mock_canary):
        exporter, db = _make_exporter()
        result = exporter.export(user_id=0)
        assert "## Panel Gaps" in result.markdown

    def test_export_has_all_intelligence_sections(self, _mock_canary):
        """All 4 intelligence sections present in a single export."""
        exporter, db = _make_exporter()
        result = exporter.export(user_id=0)
        md = result.markdown
        assert "## Lab Trends" in md
        assert "## Drug-Lab Interactions" in md
        assert "## Intelligence Gaps" in md
        assert "## Panel Gaps" in md
        assert "## Wearable Data" in md
        assert "## Health Hypotheses" in md

    def test_trends_populated_when_data_exists(self, _mock_canary):
        """Trends section should have content when TrendAnalyzer finds trends."""

        exporter, db = _make_exporter()

        mock_trend = MagicMock()
        mock_trend.direction = "increasing"
        mock_trend.canonical_name = "glucose"
        mock_trend.first_value = 95
        mock_trend.last_value = 115
        mock_trend.pct_change = 21.0
        mock_trend.data_points = 8
        mock_trend.first_date = "2024-03-15"
        mock_trend.last_date = "2024-11-15"
        mock_trend.age_context = ""

        with patch(
            "healthbot.export.ai_export.TrendAnalyzer",
            create=True,
        ):
            import healthbot.reasoning.trends as trends_mod
            orig = trends_mod.TrendAnalyzer
            try:
                mock_analyzer = MagicMock()
                mock_analyzer.detect_all_trends.return_value = [mock_trend]
                trends_mod.TrendAnalyzer = lambda db: mock_analyzer
                result = exporter.export(user_id=0)
                assert "glucose" in result.markdown
                assert "+21.0%" in result.markdown
            finally:
                trends_mod.TrendAnalyzer = orig
