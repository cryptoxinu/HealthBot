"""Deterministic genetic risk engine.

Maps known SNP variants to health implications. All logic is deterministic
(no LLM calls). Follows the pattern from hypothesis_generator.py.

Individual SNP queries sent for research are NOT identifying — millions
of people share each variant. Never batch more than 3-5 related SNPs
per research query. Never attach demographics to genetic queries.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("healthbot")


@dataclass
class GeneticRiskFinding:
    """A single genetic risk finding."""

    rsid: str
    gene: str
    user_genotype: str
    condition: str
    risk_level: str          # "elevated", "carrier", "moderate", "protective", "normal"
    clinical_notes: list[str] = field(default_factory=list)
    affected_labs: list[str] = field(default_factory=list)
    research_keywords: list[str] = field(default_factory=list)


# Load SNP catalog from JSON (same pattern as reference_ranges.py)
_CATALOG_PATH = Path(__file__).resolve().parent.parent / "data" / "snp_catalog.json"


def _load_catalog() -> list[dict]:
    with open(_CATALOG_PATH) as f:
        return json.load(f)


SNP_RULES: list[dict] = _load_catalog()

# Index rules by rsid for fast lookup
_RULES_BY_RSID: dict[str, dict] = {rule["rsid"]: rule for rule in SNP_RULES}


@dataclass
class CrossReferenceInsight:
    """Enriched genetic-lab correlation with actionable insight."""

    finding: GeneticRiskFinding
    matching_labs: list[dict]
    insight: str
    action_items: list[str]
    severity: str  # "high", "medium", "low"


# Enriched cross-reference rules: (rsid, canonical_lab_name) -> insight template
_CROSS_REF_RULES: dict[tuple[str, str], dict] = {
    ("rs1800562", "ferritin"): {
        "insight": (
            "Elevated ferritin with HFE C282Y variant — iron overload risk confirmed"
        ),
        "action_items": [
            "Monitor ferritin quarterly",
            "Consider therapeutic phlebotomy evaluation",
            "Check transferrin saturation",
        ],
        "severity": "high",
    },
    ("rs1800562", "transferrin_saturation"): {
        "insight": "Elevated transferrin saturation with HFE C282Y — active iron overload",
        "action_items": [
            "Urgent hematology referral",
            "Therapeutic phlebotomy likely indicated",
            "Monitor ferritin and TSAT together",
        ],
        "severity": "high",
    },
    ("rs2228570", "vitamin_d"): {
        "insight": "Low vitamin D with VDR variant — impaired vitamin D receptor activity",
        "action_items": [
            "Target 60-80 ng/mL (higher than standard)",
            "Consider 5000 IU/day vitamin D3 with K2",
            "Recheck in 3 months",
        ],
        "severity": "medium",
    },
    ("rs1801133", "homocysteine"): {
        "insight": "Elevated homocysteine with MTHFR C677T — impaired folate metabolism confirmed",
        "action_items": [
            "Methylfolate (5-MTHF) 1000mcg/day instead of folic acid",
            "Add methylcobalamin B12",
            "Recheck homocysteine in 3 months",
        ],
        "severity": "high",
    },
    ("rs1801133", "folate"): {
        "insight": (
            "Low folate with MTHFR C677T — reduced enzyme activity affecting folate"
        ),
        "action_items": [
            "Switch to methylfolate (5-MTHF) supplementation",
            "Avoid folic acid (synthetic) — use active forms",
            "Check homocysteine as functional marker",
        ],
        "severity": "medium",
    },
    ("rs429358", "ldl"): {
        "insight": "Elevated LDL with APOE e4 variant — genetically driven lipid pattern",
        "action_items": [
            "ApoB as primary lipid target",
            "Consider dietary fat modification",
            "Evaluate statin candidacy with cardiologist",
        ],
        "severity": "high",
    },
    ("rs429358", "apob"): {
        "insight": "Elevated ApoB with APOE e4 — increased atherogenic particle count",
        "action_items": [
            "Target ApoB < 80 mg/dL (high risk) or < 60 mg/dL (very high risk)",
            "Consider PCSK9 inhibitor evaluation if statin-resistant",
            "Monitor Lp(a) as additional risk marker",
        ],
        "severity": "high",
    },
    ("rs6025", "d_dimer"): {
        "insight": "Elevated D-dimer with Factor V Leiden — thrombotic risk marker active",
        "action_items": [
            "Urgent hematology consultation",
            "Evaluate for DVT/PE if symptomatic",
            "Review anticoagulation status",
        ],
        "severity": "high",
    },
    ("rs1799945", "ferritin"): {
        "insight": "Elevated ferritin with HFE H63D — iron accumulation in progress",
        "action_items": [
            "Check C282Y status (compound heterozygosity risk)",
            "Monitor ferritin and TSAT every 6 months",
            "Limit iron-fortified foods and red meat",
        ],
        "severity": "medium",
    },
    ("rs4149056", "creatine_kinase"): {
        "insight": "Elevated CK with SLCO1B1 variant — statin myopathy risk increased",
        "action_items": [
            "Discuss statin dose reduction with prescriber",
            "Consider rosuvastatin or pravastatin (lower SLCO1B1 sensitivity)",
            "Monitor CK if on any statin",
        ],
        "severity": "high",
    },
    ("rs10455872", "ldl"): {
        "insight": (
            "Elevated LDL with Lp(a) genetic variant — genetically driven CV risk"
        ),
        "action_items": [
            "Measure Lp(a) directly (one-time test)",
            "Consider PCSK9 inhibitor evaluation",
            "Aggressive LDL management warranted",
        ],
        "severity": "high",
    },
    ("rs1800629", "hs_crp"): {
        "insight": (
            "Elevated hs-CRP with TNF-alpha variant — genetic inflammation risk"
        ),
        "action_items": [
            "Anti-inflammatory dietary pattern recommended",
            "Monitor hs-CRP trend quarterly",
            "Evaluate omega-3 index and supplementation",
        ],
        "severity": "medium",
    },
    ("rs9939609", "hba1c"): {
        "insight": "Elevated HbA1c with FTO obesity variant — metabolic risk compounding",
        "action_items": [
            "Fasting insulin and HOMA-IR assessment",
            "Structured weight management program",
            "Monitor glucose trends closely",
        ],
        "severity": "medium",
    },
    ("rs174547", "triglycerides"): {
        "insight": "Abnormal triglycerides with FADS1 variant — altered fatty acid conversion",
        "action_items": [
            "Consider direct EPA/DHA supplementation (bypass conversion)",
            "Omega-3 index testing recommended",
            "Reduce omega-6 intake",
        ],
        "severity": "medium",
    },
}


class GeneticRiskEngine:
    """Scan genetic variants against curated risk rules."""

    def __init__(self, db: object) -> None:
        self._db = db

    def scan_variants(self, user_id: int) -> list[GeneticRiskFinding]:
        """Check stored variants against all SNP rules.

        Returns list of findings sorted by risk level (elevated first).
        """
        # Only query the rsids we have rules for
        target_rsids = list(_RULES_BY_RSID.keys())
        variants = self._db.get_genetic_variants(user_id, rsids=target_rsids)
        if not variants:
            return []

        findings: list[GeneticRiskFinding] = []
        for var in variants:
            rsid = var.get("_rsid", "")
            genotype = var.get("genotype", "")
            rule = _RULES_BY_RSID.get(rsid)
            if not rule or not genotype:
                continue

            risk_level = rule["risk_genotypes"].get(genotype)
            if not risk_level:
                continue  # Normal genotype for this SNP

            notes = rule.get("clinical_notes", {})
            clinical = notes.get(risk_level, [])

            findings.append(GeneticRiskFinding(
                rsid=rsid,
                gene=rule["gene"],
                user_genotype=genotype,
                condition=rule["condition"],
                risk_level=risk_level,
                clinical_notes=list(clinical),
                affected_labs=list(rule.get("affected_labs", [])),
                research_keywords=list(rule.get("research_keywords", [])),
            ))

        # Sort: elevated first, then moderate, then carrier
        order = {"elevated": 0, "moderate": 1, "carrier": 2, "protective": 3}
        findings.sort(key=lambda f: order.get(f.risk_level, 99))
        return findings

    def cross_reference_labs(
        self, findings: list[GeneticRiskFinding], user_id: int,
    ) -> list[dict]:
        """Cross-reference genetic risks with actual lab values.

        Returns list of {finding, matching_labs} dicts where genetic risk
        aligns with abnormal lab results.
        """
        if not findings:
            return []

        # Gather all affected lab names
        all_lab_names: set[str] = set()
        for f in findings:
            all_lab_names.update(f.affected_labs)

        if not all_lab_names:
            return []

        # Query recent observations
        labs = self._db.query_observations(
            record_type="lab_result", user_id=user_id,
        )
        if not labs:
            return []

        # Build map of canonical_name -> most recent flagged result
        flagged: dict[str, dict] = {}
        for lab in labs:
            name = (lab.get("canonical_name") or "").lower()
            flag = lab.get("flag", "")
            if name in all_lab_names and flag and flag.lower() not in ("", "normal"):
                if name not in flagged:
                    flagged[name] = lab

        if not flagged:
            return []

        correlations = []
        for finding in findings:
            matches = []
            for lab_name in finding.affected_labs:
                if lab_name in flagged:
                    matches.append(flagged[lab_name])
            if matches:
                correlations.append({
                    "finding": finding,
                    "matching_labs": matches,
                })

        return correlations

    def cross_reference_labs_enriched(
        self, findings: list[GeneticRiskFinding], user_id: int,
    ) -> list[CrossReferenceInsight]:
        """Enriched cross-reference with actionable insights.

        Calls basic cross_reference_labs(), then enhances each correlation
        with specific insight text and action items from _CROSS_REF_RULES.
        """
        basic = self.cross_reference_labs(findings, user_id)
        if not basic:
            return []

        insights: list[CrossReferenceInsight] = []
        for corr in basic:
            finding: GeneticRiskFinding = corr["finding"]
            matching_labs: list[dict] = corr["matching_labs"]

            # Check for specific enrichment rules
            best_rule = None
            for lab in matching_labs:
                lab_name = (lab.get("canonical_name") or "").lower()
                key = (finding.rsid, lab_name)
                if key in _CROSS_REF_RULES:
                    best_rule = _CROSS_REF_RULES[key]
                    break

            if best_rule:
                insights.append(CrossReferenceInsight(
                    finding=finding,
                    matching_labs=matching_labs,
                    insight=best_rule["insight"],
                    action_items=list(best_rule["action_items"]),
                    severity=best_rule["severity"],
                ))
            else:
                # Generic insight fallback
                lab_names = ", ".join(
                    (lab.get("canonical_name") or "unknown") for lab in matching_labs
                )
                insights.append(CrossReferenceInsight(
                    finding=finding,
                    matching_labs=matching_labs,
                    insight=f"Abnormal {lab_names} with {finding.gene} {finding.condition} — "
                            f"genetic predisposition may be contributing",
                    action_items=[
                        f"Monitor {lab_names} trend closely",
                        f"Discuss {finding.gene} variant with provider",
                    ],
                    severity="medium",
                ))

        # Sort by severity
        sev_order = {"high": 0, "medium": 1, "low": 2}
        insights.sort(key=lambda i: sev_order.get(i.severity, 99))
        return insights

    def format_summary(self, findings: list[GeneticRiskFinding]) -> str:
        """Format findings as human-readable text for Telegram."""
        if not findings:
            return "No significant genetic risk variants found in your data."

        lines: list[str] = []
        for f in findings:
            icon = {"elevated": "!!", "moderate": "!", "carrier": "~"}.get(
                f.risk_level, "",
            )
            lines.append(f"{icon} {f.gene} — {f.condition}")
            lines.append(f"   Genotype: {f.user_genotype} ({f.risk_level})")
            for note in f.clinical_notes[:2]:
                lines.append(f"   - {note}")
            if f.affected_labs:
                lines.append(f"   Labs to monitor: {', '.join(f.affected_labs)}")
            lines.append("")

        return "\n".join(lines).strip()

    def build_research_query(self, finding: GeneticRiskFinding) -> str:
        """Build an anonymized research query for a genetic finding.

        The query contains NO PII — only the rsID, genotype, and gene name,
        which are shared by millions of people.
        """
        keywords = " ".join(finding.research_keywords[:3])
        return (
            f"Health implications of {finding.rsid} {finding.user_genotype} "
            f"({finding.gene} {finding.condition}). "
            f"Focus on: {keywords}. "
            f"Include clinical significance, monitoring recommendations, "
            f"and any drug interactions."
        )
