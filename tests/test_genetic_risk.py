"""Tests for genetic risk engine."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.genetic_risk import (
    _RULES_BY_RSID,
    SNP_RULES,
    GeneticRiskEngine,
    GeneticRiskFinding,
)


def _make_db(variants=None, labs=None):
    """Create a mock DB with genetic variants and labs."""
    db = MagicMock()
    db.get_genetic_variants.return_value = variants or []
    db.get_genetic_variant_count.return_value = len(variants or [])
    db.query_observations.return_value = labs or []
    return db


def _variant(rsid: str, genotype: str) -> dict:
    """Create a mock variant dict as returned by db.get_genetic_variants()."""
    return {
        "_id": f"test_{rsid}",
        "_rsid": rsid,
        "_chromosome": "1",
        "_position": 0,
        "_source": "tellmegen",
        "genotype": genotype,
        "source": "tellmegen",
    }


class TestGeneticRiskEngine:

    def test_no_variants_returns_empty(self):
        db = _make_db(variants=[])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert findings == []

    def test_hfe_c282y_homozygous_detected(self):
        db = _make_db(variants=[_variant("rs1800562", "AA")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 1
        assert findings[0].gene == "HFE"
        assert findings[0].risk_level == "elevated"
        assert "hemochromatosis" in findings[0].condition.lower()

    def test_hfe_c282y_carrier_detected(self):
        db = _make_db(variants=[_variant("rs1800562", "AG")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 1
        assert findings[0].risk_level == "carrier"

    def test_hfe_c282y_normal_not_flagged(self):
        db = _make_db(variants=[_variant("rs1800562", "GG")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 0

    def test_mthfr_c677t_homozygous(self):
        db = _make_db(variants=[_variant("rs1801133", "AA")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 1
        assert findings[0].gene == "MTHFR"
        assert findings[0].risk_level == "elevated"
        assert "homocysteine" in findings[0].affected_labs

    def test_multiple_findings_sorted_by_severity(self):
        db = _make_db(variants=[
            _variant("rs1801133", "AG"),   # MTHFR moderate
            _variant("rs1800562", "AA"),   # HFE elevated
            _variant("rs4988235", "CC"),   # Lactose moderate
        ])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 3
        assert findings[0].risk_level == "elevated"  # HFE first
        assert findings[1].risk_level == "moderate"   # MTHFR or lactose

    def test_factor_v_leiden_detected(self):
        db = _make_db(variants=[_variant("rs6025", "AG")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 1
        assert "Factor V" in findings[0].condition

    def test_cyp2d6_pharmacogenomics(self):
        db = _make_db(variants=[_variant("rs3892097", "AA")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 1
        assert findings[0].gene == "CYP2D6"
        assert any("codeine" in n.lower() for n in findings[0].clinical_notes)

    def test_unknown_rsid_ignored(self):
        db = _make_db(variants=[_variant("rs9999999", "AG")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        assert len(findings) == 0


class TestGeneticRiskCrossReference:

    def test_hfe_with_high_ferritin_correlates(self):
        db = _make_db(
            variants=[_variant("rs1800562", "AG")],
            labs=[{
                "test_name": "Ferritin",
                "canonical_name": "ferritin",
                "value": 450,
                "unit": "ng/mL",
                "flag": "H",
                "_meta": {"date_effective": "2024-11-15"},
            }],
        )
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        correlations = engine.cross_reference_labs(findings, user_id=1)
        assert len(correlations) == 1
        assert correlations[0]["finding"].gene == "HFE"
        assert correlations[0]["matching_labs"][0]["canonical_name"] == "ferritin"

    def test_no_correlation_when_labs_normal(self):
        db = _make_db(
            variants=[_variant("rs1800562", "AG")],
            labs=[{
                "test_name": "Ferritin",
                "canonical_name": "ferritin",
                "value": 100,
                "unit": "ng/mL",
                "flag": "",
                "_meta": {"date_effective": "2024-11-15"},
            }],
        )
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        correlations = engine.cross_reference_labs(findings, user_id=1)
        assert len(correlations) == 0

    def test_no_labs_returns_empty(self):
        db = _make_db(variants=[_variant("rs1800562", "AG")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        correlations = engine.cross_reference_labs(findings, user_id=1)
        assert len(correlations) == 0


class TestGeneticRiskFormatting:

    def test_format_summary_empty(self):
        db = _make_db()
        engine = GeneticRiskEngine(db)
        result = engine.format_summary([])
        assert "no significant" in result.lower()

    def test_format_summary_with_findings(self):
        db = _make_db(variants=[_variant("rs1800562", "AG")])
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id=1)
        result = engine.format_summary(findings)
        assert "HFE" in result
        assert "carrier" in result

    def test_research_query_has_no_pii(self):
        finding = GeneticRiskFinding(
            rsid="rs1800562",
            gene="HFE",
            user_genotype="AG",
            condition="Hereditary hemochromatosis (HFE C282Y)",
            risk_level="carrier",
            clinical_notes=["Monitor ferritin"],
            affected_labs=["ferritin"],
            research_keywords=["HFE C282Y", "hereditary hemochromatosis"],
        )
        db = _make_db()
        engine = GeneticRiskEngine(db)
        query = engine.build_research_query(finding)
        assert "rs1800562" in query
        assert "AG" in query
        assert "HFE" in query
        # Should NOT contain any PII patterns
        import re
        assert not re.search(r"\b\d{3}-\d{2}-\d{4}\b", query)  # No SSN
        assert "@" not in query  # No email
        assert not re.search(r"\b\d{3}[-.)]\d{3}", query)  # No phone


class TestSNPRulesIntegrity:

    def test_all_rules_have_required_fields(self):
        for rule in SNP_RULES:
            assert "rsid" in rule, "Missing rsid in rule"
            assert "gene" in rule, f"Missing gene in {rule['rsid']}"
            assert "risk_genotypes" in rule, f"Missing risk_genotypes in {rule['rsid']}"
            assert "condition" in rule, f"Missing condition in {rule['rsid']}"
            assert "clinical_notes" in rule, f"Missing clinical_notes in {rule['rsid']}"

    def test_all_rsids_start_with_rs(self):
        for rule in SNP_RULES:
            assert rule["rsid"].startswith("rs"), f"Invalid rsid: {rule['rsid']}"

    def test_rules_indexed_correctly(self):
        for rule in SNP_RULES:
            assert rule["rsid"] in _RULES_BY_RSID
            assert _RULES_BY_RSID[rule["rsid"]] is rule

    def test_no_duplicate_rsids(self):
        rsids = [r["rsid"] for r in SNP_RULES]
        assert len(rsids) == len(set(rsids)), "Duplicate rsids in SNP_RULES"
