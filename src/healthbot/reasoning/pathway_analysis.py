"""Genetic pathway analysis — group SNP findings by biological pathway.

Deterministic analysis. No LLM. Groups genetic risk findings into
biological pathways and scores cumulative impact.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger("healthbot")

PATHWAY_DEFINITIONS: dict[str, dict] = {
    "methylation": {
        "name": "Methylation Cycle",
        "description": "DNA methylation, homocysteine metabolism, neurotransmitter synthesis",
        "key_genes": ["MTHFR", "COMT", "MTR", "MTRR", "CBS", "BHMT"],
    },
    "detoxification": {
        "name": "Detoxification & Antioxidant Defense",
        "description": "Phase I/II liver detoxification, oxidative stress management",
        "key_genes": ["CYP1A2", "SOD2", "GPX1", "GSTP1", "GSTM1"],
    },
    "cardiovascular": {
        "name": "Cardiovascular Risk",
        "description": "Lipid metabolism, clotting, vascular function",
        "key_genes": ["APOE", "LPA", "PCSK9", "CETP", "F5", "ACE", "AGT", "NOS3", "9p21"],
    },
    "inflammation": {
        "name": "Inflammatory Response",
        "description": "Cytokine production, immune regulation, autoimmunity",
        "key_genes": ["TNF", "IL6", "HLA-DQ2", "HLA-DQ8"],
    },
    "nutrient_metabolism": {
        "name": "Nutrient Metabolism",
        "description": "Vitamin absorption, fatty acid conversion, lactose tolerance",
        "key_genes": ["VDR", "FTO", "FADS1", "BCMO1", "MCM6"],
    },
    "iron_homeostasis": {
        "name": "Iron Homeostasis",
        "description": "Iron absorption, storage, and overload risk",
        "key_genes": ["HFE"],
    },
    "pharmacogenomics": {
        "name": "Pharmacogenomics",
        "description": "Drug metabolism enzyme variants affecting medication response",
        "key_genes": [
            "CYP2D6", "CYP2C19", "CYP2C9", "VKORC1", "SLCO1B1",
            "DPYD", "UGT1A1", "NAT2", "TPMT", "CYP3A4",
        ],
    },
}


@dataclass
class PathwayReport:
    """Analysis of a single biological pathway."""
    pathway_id: str
    pathway_name: str
    description: str
    total_snps_in_pathway: int
    risk_snps_found: int
    findings: list  # list[GeneticRiskFinding]
    impact_score: float  # 0-10
    narrative: str


class PathwayAnalysisEngine:
    """Compute pathway-level impact from individual SNP findings."""

    def __init__(self, db: object) -> None:
        self._db = db

    def analyze(self, user_id: int) -> list[PathwayReport]:
        """Group genetic findings by pathway and score impact."""
        from healthbot.reasoning.genetic_risk import (
            _RULES_BY_RSID,
            SNP_RULES,
            GeneticRiskEngine,
        )

        # Get all findings
        engine = GeneticRiskEngine(self._db)
        findings = engine.scan_variants(user_id)

        # Count total SNPs per pathway from catalog
        pathway_totals: dict[str, int] = {}
        for rule in SNP_RULES:
            pw = rule.get("pathway", "other")
            pathway_totals[pw] = pathway_totals.get(pw, 0) + 1

        # Group findings by pathway
        pathway_findings: dict[str, list] = {}
        for f in findings:
            rule = _RULES_BY_RSID.get(f.rsid, {})
            pw = rule.get("pathway", "other")
            pathway_findings.setdefault(pw, []).append(f)

        # Build reports for defined pathways
        reports = []
        for pw_id, pw_def in PATHWAY_DEFINITIONS.items():
            found = pathway_findings.get(pw_id, [])
            total = pathway_totals.get(pw_id, 0)
            risk_count = len(found)

            # Score: 0 risk = 0, 1 = 3, 2 = 5, 3 = 7, 4+ = 8-10
            if risk_count == 0:
                score = 0.0
            elif risk_count == 1:
                score = 3.0
            elif risk_count == 2:
                score = 5.0
            elif risk_count == 3:
                score = 7.0
            else:
                score = min(10.0, 7.0 + risk_count)

            # Boost for "elevated" (homozygous) findings
            elevated_count = sum(1 for f in found if f.risk_level == "elevated")
            score = min(10.0, score + elevated_count * 0.5)

            narrative = self._build_narrative(pw_def, found, score)

            reports.append(PathwayReport(
                pathway_id=pw_id,
                pathway_name=pw_def["name"],
                description=pw_def["description"],
                total_snps_in_pathway=total,
                risk_snps_found=risk_count,
                findings=found,
                impact_score=round(score, 1),
                narrative=narrative,
            ))

        # Sort by impact score descending
        reports.sort(key=lambda r: r.impact_score, reverse=True)
        return reports

    def _build_narrative(self, pw_def: dict, findings: list, score: float) -> str:
        """Build human-readable narrative for a pathway."""
        if not findings:
            return f"No risk variants detected in {pw_def['name'].lower()} pathway."

        genes = ", ".join(sorted(set(f.gene for f in findings)))

        if score >= 7:
            intensity = "significant cumulative impact"
        elif score >= 4:
            intensity = "moderate impact"
        else:
            intensity = "mild impact"

        return (
            f"{len(findings)} variant(s) in {pw_def['name'].lower()} pathway "
            f"({genes}) — {intensity}. "
            f"See individual findings for specific recommendations."
        )

    def format_report(self, reports: list[PathwayReport]) -> str:
        """Format pathway analysis for display."""
        active = [r for r in reports if r.risk_snps_found > 0]
        if not active:
            return "No pathway impacts detected from genetic variants."

        lines = ["PATHWAY ANALYSIS", "-" * 40]

        for r in active:
            # Pick icon based on score range
            if r.impact_score >= 7:
                icon = "!!"
            elif r.impact_score >= 4:
                icon = "!"
            else:
                icon = "~"

            lines.append(f"  [{icon}] {r.pathway_name}: {r.impact_score}/10 "
                        f"({r.risk_snps_found}/{r.total_snps_in_pathway} variants)")
            lines.append(f"      {r.narrative}")

            for f in r.findings:
                lines.append(f"      - {f.gene} {f.rsid} ({f.risk_level})")

            lines.append("")

        inactive = [r for r in reports if r.risk_snps_found == 0 and r.total_snps_in_pathway > 0]
        if inactive:
            names = ", ".join(r.pathway_name for r in inactive)
            lines.append(f"  [=] No variants found in: {names}")

        return "\n".join(lines)
