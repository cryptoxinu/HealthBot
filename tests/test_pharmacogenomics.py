"""Tests for pharmacogenomics engine."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.pharmacogenomics import (
    MetabolizerStatus,
    PharmacogenomicsEngine,
)


def _make_db(variants=None, meds=None):
    db = MagicMock()
    db.get_genetic_variants.return_value = variants or []

    def fake_query(**kwargs):
        record_type = kwargs.get("record_type", "")
        if record_type == "medication":
            return meds or []
        return []

    db.query_observations.side_effect = fake_query
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


class TestPharmacogenomics:

    def test_no_variants_all_normal(self):
        db = _make_db()
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        assert report.total_enzymes_checked == 10
        assert report.actionable_count == 0
        assert all(ep.status == MetabolizerStatus.NORMAL for ep in report.enzyme_profiles)

    def test_cyp2d6_poor_metabolizer(self):
        db = _make_db(variants=[_variant("rs3892097", "AA")])
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        cyp2d6 = next(ep for ep in report.enzyme_profiles if ep.enzyme == "CYP2D6")
        assert cyp2d6.status == MetabolizerStatus.POOR
        assert report.actionable_count >= 1
        assert "codeine" in cyp2d6.clinical_note.lower()

    def test_cyp2d6_intermediate_metabolizer(self):
        db = _make_db(variants=[_variant("rs3892097", "AG")])
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        cyp2d6 = next(ep for ep in report.enzyme_profiles if ep.enzyme == "CYP2D6")
        assert cyp2d6.status == MetabolizerStatus.INTERMEDIATE

    def test_drug_flag_when_on_codeine(self):
        db = _make_db(
            variants=[_variant("rs3892097", "AA")],
            meds=[{"name": "codeine", "test_name": "codeine"}],
        )
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        assert len(report.drug_flags) >= 1
        codeine_flag = next((f for f in report.drug_flags if f.drug_name == "codeine"), None)
        assert codeine_flag is not None
        assert codeine_flag.severity == "high"

    def test_no_drug_flags_without_meds(self):
        db = _make_db(variants=[_variant("rs3892097", "AA")])
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        assert report.drug_flags == []

    def test_multiple_enzymes_detected(self):
        db = _make_db(variants=[
            _variant("rs3892097", "AA"),   # CYP2D6 poor
            _variant("rs4244285", "AG"),   # CYP2C19 intermediate
        ])
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        assert report.actionable_count == 2
        cyp2c19 = next(ep for ep in report.enzyme_profiles if ep.enzyme == "CYP2C19")
        assert cyp2c19.status == MetabolizerStatus.INTERMEDIATE

    def test_format_report_no_findings(self):
        db = _make_db()
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        text = engine.format_report(report)
        assert "no actionable" in text.lower()

    def test_format_report_with_findings(self):
        db = _make_db(variants=[_variant("rs3892097", "AA")])
        engine = PharmacogenomicsEngine(db)
        report = engine.profile(user_id=1)
        text = engine.format_report(report)
        assert "PHARMACOGENOMICS" in text
        assert "CYP2D6" in text
