"""Tests for CleanDB query methods — deduped fetch and record_type filter."""
from __future__ import annotations

import pytest

from healthbot.data.clean_db import CleanDB
from healthbot.security.phi_firewall import PhiFirewall


@pytest.fixture()
def phi_firewall():
    return PhiFirewall()


@pytest.fixture()
def clean_db(tmp_path, phi_firewall):
    db = CleanDB(tmp_path / "clean.db", phi_firewall=phi_firewall)
    db.open()
    yield db
    db.close()


class TestGetLatestPerTest:
    """Tests for _get_latest_per_test() deduplication."""

    def test_returns_one_row_per_unique_test(self, clean_db):
        """Each canonical_name should appear at most once."""
        # Insert two values for the same test on different dates
        clean_db.upsert_observation(
            "obs1", canonical_name="WBC", test_name="WBC",
            value="5.0", unit="K/uL", date_effective="2024-06-01",
        )
        clean_db.upsert_observation(
            "obs2", canonical_name="WBC", test_name="WBC",
            value="6.0", unit="K/uL", date_effective="2024-07-01",
        )
        clean_db.upsert_observation(
            "obs3", canonical_name="Glucose", test_name="Glucose",
            value="95", unit="mg/dL", date_effective="2024-06-01",
        )

        labs = clean_db._get_latest_per_test()
        names = [lab["canonical_name"] for lab in labs]
        assert sorted(names) == ["Glucose", "WBC"]
        # WBC should be the newer value
        wbc = next(lab for lab in labs if lab["canonical_name"] == "WBC")
        assert wbc["value"] == "6.0"

    def test_qualitative_results_visible_alongside_many_numeric(self, clean_db):
        """Qualitative tests must appear even when 50+ numeric results exist
        on a newer date."""
        # Insert 50 numeric results on a recent date
        for i in range(50):
            clean_db.upsert_observation(
                f"num_{i}", canonical_name=f"NumericTest_{i}",
                test_name=f"NumericTest_{i}",
                value=str(100 + i), unit="mg/dL",
                date_effective="2024-08-01",
            )
        # Insert qualitative results on an older date
        for name in ["JAK2 V617F", "CALR Mutation", "HBsAg"]:
            clean_db.upsert_observation(
                f"qual_{name}", canonical_name=name, test_name=name,
                value="Negative", unit="", date_effective="2024-01-15",
                reference_text="Negative", flag="normal",
            )

        labs = clean_db._get_latest_per_test(limit=200)
        found_names = {lab["canonical_name"] for lab in labs}
        assert "JAK2 V617F" in found_names
        assert "CALR Mutation" in found_names
        assert "HBsAg" in found_names
        # All 50 numeric + 3 qualitative = 53 total
        assert len(labs) == 53

    def test_excludes_non_lab_observations(self, clean_db):
        """_get_latest_per_test should only return lab_result records."""
        clean_db.upsert_observation(
            "lab1", canonical_name="WBC", test_name="WBC",
            value="5.0", unit="K/uL", date_effective="2024-06-01",
            record_type="lab_result",
        )
        clean_db.upsert_observation(
            "vital1", canonical_name="Blood Pressure", test_name="BP",
            value="120/80", unit="mmHg", date_effective="2024-06-01",
            record_type="vital_sign",
        )

        labs = clean_db._get_latest_per_test()
        assert len(labs) == 1
        assert labs[0]["canonical_name"] == "WBC"

    def test_rn_column_not_in_results(self, clean_db):
        """The synthetic ROW_NUMBER column should be stripped."""
        clean_db.upsert_observation(
            "obs1", canonical_name="WBC", test_name="WBC",
            value="5.0", unit="K/uL", date_effective="2024-06-01",
        )
        labs = clean_db._get_latest_per_test()
        assert len(labs) == 1
        assert "rn" not in labs[0]


class TestGetLabResultsRecordTypeFilter:
    """Tests for record_type filter in get_lab_results()."""

    def test_only_returns_lab_result_type(self, clean_db):
        """get_lab_results() should filter to record_type='lab_result'."""
        clean_db.upsert_observation(
            "lab1", canonical_name="WBC", test_name="WBC",
            value="5.0", unit="K/uL", date_effective="2024-06-01",
            record_type="lab_result",
        )
        clean_db.upsert_observation(
            "vital1", canonical_name="Heart Rate", test_name="HR",
            value="72", unit="bpm", date_effective="2024-06-01",
            record_type="vital_sign",
        )
        clean_db.upsert_observation(
            "clinical1", canonical_name="Assessment", test_name="Assessment",
            value="Normal", unit="", date_effective="2024-06-01",
            record_type="clinical_note",
        )

        labs = clean_db.get_lab_results()
        assert len(labs) == 1
        assert labs[0]["canonical_name"] == "WBC"

    def test_filter_works_with_test_name_search(self, clean_db):
        """record_type filter should combine with test_name search."""
        clean_db.upsert_observation(
            "lab1", canonical_name="WBC", test_name="WBC",
            value="5.0", unit="K/uL", date_effective="2024-06-01",
            record_type="lab_result",
        )
        clean_db.upsert_observation(
            "vital1", canonical_name="WBC-like Vital", test_name="WBC check",
            value="ok", unit="", date_effective="2024-06-01",
            record_type="vital_sign",
        )

        labs = clean_db.get_lab_results(test_name="WBC")
        assert len(labs) == 1
        assert labs[0]["record_type"] == "lab_result"
