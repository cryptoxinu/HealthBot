"""Tests for SNP catalog JSON and genetic risk engine loading."""
from __future__ import annotations

import json
from pathlib import Path

from healthbot.reasoning.genetic_risk import _RULES_BY_RSID, SNP_RULES

CATALOG_PATH = (
    Path(__file__).resolve().parent.parent
    / "src" / "healthbot" / "data" / "snp_catalog.json"
)


class TestSnpCatalog:

    def test_json_loads(self):
        with open(CATALOG_PATH) as f:
            data = json.load(f)
        assert isinstance(data, list)
        assert len(data) >= 40

    def test_no_duplicate_rsids(self):
        rsids = [r["rsid"] for r in SNP_RULES]
        assert len(rsids) == len(set(rsids))

    def test_all_entries_have_required_fields(self):
        required = {"rsid", "gene", "variant_name", "risk_genotypes", "condition", "clinical_notes"}
        for rule in SNP_RULES:
            missing = required - set(rule.keys())
            assert not missing, f"{rule['rsid']} missing: {missing}"

    def test_all_entries_have_new_fields(self):
        for rule in SNP_RULES:
            assert "confidence_level" in rule, f"{rule['rsid']} missing confidence_level"
            assert rule["confidence_level"] in ("strong", "moderate", "preliminary")
            assert "pathway" in rule, f"{rule['rsid']} missing pathway"

    def test_original_snps_preserved(self):
        originals = [
            "rs1800562", "rs1799945", "rs1801133", "rs1801131",
            "rs429358", "rs6025", "rs762551", "rs2228570",
            "rs3892097", "rs4244285", "rs4988235",
        ]
        for rsid in originals:
            assert rsid in _RULES_BY_RSID, f"Original SNP {rsid} missing from catalog"

    def test_rules_by_rsid_matches_rules(self):
        assert len(_RULES_BY_RSID) == len(SNP_RULES)

    def test_pharmacogenomics_pathway_present(self):
        pgx = [r for r in SNP_RULES if r.get("pathway") == "pharmacogenomics"]
        assert len(pgx) >= 10, f"Expected >=10 pharmacogenomics entries, got {len(pgx)}"
