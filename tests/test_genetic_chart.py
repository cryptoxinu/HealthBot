"""Tests for genetic_risk_chart() in export/chart_generator.py."""
from __future__ import annotations

from dataclasses import dataclass, field

from healthbot.export.chart_generator import genetic_risk_chart


@dataclass
class MockFinding:
    rsid: str = "rs1800562"
    gene: str = "HFE"
    user_genotype: str = "AG"
    condition: str = "Hereditary hemochromatosis"
    risk_level: str = "carrier"
    clinical_notes: list[str] = field(default_factory=list)
    affected_labs: list[str] = field(default_factory=list)
    research_keywords: list[str] = field(default_factory=list)


class TestGeneticRiskChart:
    def test_returns_png_bytes(self):
        findings = [
            MockFinding(gene="HFE", condition="Hemochromatosis", risk_level="elevated"),
            MockFinding(gene="MTHFR", condition="Folate deficiency", risk_level="moderate"),
            MockFinding(gene="F5", condition="Factor V Leiden", risk_level="carrier"),
        ]
        result = genetic_risk_chart(findings)
        assert result is not None
        assert isinstance(result, bytes)
        # PNG magic bytes
        assert result[:4] == b"\x89PNG"

    def test_empty_findings_returns_none(self):
        assert genetic_risk_chart([]) is None

    def test_all_normal_returns_none(self):
        findings = [MockFinding(risk_level="normal")]
        assert genetic_risk_chart(findings) is None

    def test_single_finding(self):
        findings = [
            MockFinding(gene="CYP2D6", condition="Poor metabolizer", risk_level="elevated"),
        ]
        result = genetic_risk_chart(findings)
        assert result is not None
        assert result[:4] == b"\x89PNG"

    def test_protective_finding_included(self):
        findings = [
            MockFinding(gene="APOE", condition="Alzheimer risk", risk_level="protective"),
        ]
        result = genetic_risk_chart(findings)
        assert result is not None
