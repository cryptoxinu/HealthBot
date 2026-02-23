"""Tests for pathway analysis engine."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.pathway_analysis import (
    PATHWAY_DEFINITIONS,
    PathwayAnalysisEngine,
)


def _make_db(variants=None, labs=None):
    db = MagicMock()
    db.get_genetic_variants.return_value = variants or []
    db.get_genetic_variant_count.return_value = len(variants or [])
    db.query_observations.return_value = labs or []
    return db


def _variant(rsid: str, genotype: str) -> dict:
    return {
        "_id": f"test_{rsid}",
        "_rsid": rsid,
        "_chromosome": "1",
        "_position": 0,
        "_source": "tellmegen",
        "genotype": genotype,
        "source": "tellmegen",
    }


class TestPathwayAnalysis:

    def test_no_variants_all_zero(self):
        db = _make_db()
        engine = PathwayAnalysisEngine(db)
        reports = engine.analyze(user_id=1)
        assert len(reports) == len(PATHWAY_DEFINITIONS)
        assert all(r.risk_snps_found == 0 for r in reports)
        assert all(r.impact_score == 0.0 for r in reports)

    def test_iron_pathway_detected(self):
        db = _make_db(variants=[_variant("rs1800562", "AA")])  # HFE C282Y elevated
        engine = PathwayAnalysisEngine(db)
        reports = engine.analyze(user_id=1)
        iron = next((r for r in reports if r.pathway_id == "iron_homeostasis"), None)
        assert iron is not None
        assert iron.risk_snps_found == 1
        assert iron.impact_score > 0

    def test_multiple_variants_score_higher(self):
        db = _make_db(variants=[
            _variant("rs1801133", "AA"),   # MTHFR C677T elevated
            _variant("rs1801131", "CC"),   # MTHFR A1298C moderate
        ])
        engine = PathwayAnalysisEngine(db)
        reports = engine.analyze(user_id=1)
        meth = next((r for r in reports if r.pathway_id == "methylation"), None)
        assert meth is not None
        assert meth.risk_snps_found == 2
        assert meth.impact_score >= 5.0

    def test_reports_sorted_by_score(self):
        db = _make_db(variants=[
            _variant("rs1800562", "AA"),   # iron
            _variant("rs1801133", "AA"),   # methylation
            _variant("rs1801131", "CC"),   # methylation
        ])
        engine = PathwayAnalysisEngine(db)
        reports = engine.analyze(user_id=1)
        scores = [r.impact_score for r in reports]
        assert scores == sorted(scores, reverse=True)

    def test_format_report_empty(self):
        db = _make_db()
        engine = PathwayAnalysisEngine(db)
        reports = engine.analyze(user_id=1)
        text = engine.format_report(reports)
        assert "no pathway" in text.lower()

    def test_format_report_with_findings(self):
        db = _make_db(variants=[_variant("rs1800562", "AA")])
        engine = PathwayAnalysisEngine(db)
        reports = engine.analyze(user_id=1)
        text = engine.format_report(reports)
        assert "PATHWAY ANALYSIS" in text
        assert "Iron" in text

    def test_pathway_definitions_complete(self):
        expected = {"methylation", "detoxification", "cardiovascular",
                    "inflammation", "nutrient_metabolism", "iron_homeostasis",
                    "pharmacogenomics"}
        assert set(PATHWAY_DEFINITIONS.keys()) == expected
