"""Tests for data quality engine and reference ranges."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import LabResult
from healthbot.reasoning.data_quality import DataQualityEngine, DataQualityIssue
from healthbot.reasoning.reference_ranges import (
    DEFAULT_RANGES,
    FASTING_TESTS,
    convert_unit,
    get_default_range,
)

# ---------------------------------------------------------------------------
# Reference Ranges
# ---------------------------------------------------------------------------


class TestReferenceRanges:
    """Tests for the reference range lookup and unit conversion helpers."""

    def test_known_test_returns_range(self):
        r = get_default_range("glucose")
        assert r is not None
        assert r["low"] == 70.0
        assert r["high"] == 100.0
        assert r["unit"] == "mg/dL"

    def test_unknown_test_returns_none(self):
        assert get_default_range("made_up_test_xyz") is None

    def test_fasting_tests_defined(self):
        assert "glucose" in FASTING_TESTS
        assert "triglycerides" in FASTING_TESTS

    def test_default_ranges_cover_common_panels(self):
        """Ensure metabolic, lipid, CBC, thyroid, vitamins are present."""
        for name in ("sodium", "ldl", "hemoglobin", "tsh", "vitamin_d"):
            assert name in DEFAULT_RANGES, f"{name} missing from DEFAULT_RANGES"

    def test_convert_unit_same_unit(self):
        assert convert_unit(100.0, "mg/dL", "mg/dL") == 100.0

    def test_convert_unit_known_conversion(self):
        # mmol/L -> mg/dL for glucose: ~18x
        result = convert_unit(5.5, "mmol/L", "mg/dL")
        assert result is not None
        assert 98 < result < 100  # 5.5 * 18.0182 ≈ 99.1

    def test_convert_unit_unknown_returns_none(self):
        assert convert_unit(10.0, "foo_unit", "bar_unit") is None


# ---------------------------------------------------------------------------
# Data Quality Engine — Fasting
# ---------------------------------------------------------------------------


class TestCheckFasting:
    """Fasting flag validation for fasting-required tests."""

    def test_missing_fasting_flag_on_glucose(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            fasting=None,
        )
        issue = dq.check_fasting(lab)
        assert issue is not None
        assert issue.issue_type == "missing_fasting_flag"
        assert issue.severity == "warning"

    def test_fasting_flag_present(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            fasting=True,
        )
        assert dq.check_fasting(lab) is None

    def test_non_fasting_test_no_issue(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.1,
            unit="mIU/L",
            fasting=None,
        )
        assert dq.check_fasting(lab) is None


# ---------------------------------------------------------------------------
# Data Quality Engine — Unit Mismatch
# ---------------------------------------------------------------------------


class TestCheckUnitMismatch:
    """Detect when a new result uses a different unit than historical data."""

    def _insert_lab(self, db, canonical: str, value: float, unit: str, day: int):
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name=canonical.title(),
            canonical_name=canonical,
            value=value,
            unit=unit,
            date_collected=date(2024, 1, day),
        )
        db.insert_observation(lab)

    def test_same_unit_no_issue(self, db):
        self._insert_lab(db, "glucose", 90.0, "mg/dL", 1)
        self._insert_lab(db, "glucose", 95.0, "mg/dL", 5)

        dq = DataQualityEngine(db)
        new = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=100.0,
            unit="mg/dL",
            date_collected=date(2024, 2, 1),
        )
        assert dq.check_unit_mismatch(new) is None

    def test_different_unit_warns(self, db):
        self._insert_lab(db, "glucose", 90.0, "mg/dL", 1)

        dq = DataQualityEngine(db)
        new = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=5.5,
            unit="mmol/L",
            date_collected=date(2024, 2, 1),
        )
        issue = dq.check_unit_mismatch(new)
        assert issue is not None
        assert issue.issue_type == "unit_mismatch"
        assert "mg/dL" in issue.message.lower() or "mg/dl" in issue.message.lower()

    def test_no_history_no_issue(self, db):
        dq = DataQualityEngine(db)
        new = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=5.5,
            unit="mmol/L",
        )
        assert dq.check_unit_mismatch(new) is None


# ---------------------------------------------------------------------------
# Data Quality Engine — Duplicates
# ---------------------------------------------------------------------------


class TestCheckDuplicate:
    """Detect exact and conflicting duplicates."""

    def test_exact_duplicate(self, db):
        lab1 = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=110.0,
            unit="mg/dL",
            date_collected=date(2024, 3, 15),
        )
        db.insert_observation(lab1)

        dq = DataQualityEngine(db)
        lab2 = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=110.0,
            unit="mg/dL",
            date_collected=date(2024, 3, 15),
        )
        issue = dq.check_duplicate(lab2)
        assert issue is not None
        assert issue.issue_type == "duplicate_exact"
        assert issue.severity == "info"

    def test_conflicting_duplicate(self, db):
        lab1 = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=110.0,
            unit="mg/dL",
            date_collected=date(2024, 3, 15),
        )
        db.insert_observation(lab1)

        dq = DataQualityEngine(db)
        lab2 = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=130.0,
            unit="mg/dL",
            date_collected=date(2024, 3, 15),
        )
        issue = dq.check_duplicate(lab2)
        assert issue is not None
        assert issue.issue_type == "duplicate_conflict"
        assert issue.severity == "error"

    def test_no_duplicate_different_date(self, db):
        lab1 = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=110.0,
            unit="mg/dL",
            date_collected=date(2024, 3, 15),
        )
        db.insert_observation(lab1)

        dq = DataQualityEngine(db)
        lab2 = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=110.0,
            unit="mg/dL",
            date_collected=date(2024, 4, 15),
        )
        assert dq.check_duplicate(lab2) is None


# ---------------------------------------------------------------------------
# Data Quality Engine — Reference Range
# ---------------------------------------------------------------------------


class TestCheckReferenceRange:
    """Missing reference range detection and default suggestion."""

    def test_missing_range_with_default(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
        )
        issue = dq.check_reference_range(lab)
        assert issue is not None
        assert issue.issue_type == "missing_reference_range"
        assert issue.severity == "info"
        assert "70" in issue.suggestion and "100" in issue.suggestion

    def test_missing_range_unknown_test(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Obscure Marker",
            canonical_name="obscure_marker",
            value=5.0,
            unit="U/L",
        )
        issue = dq.check_reference_range(lab)
        assert issue is not None
        assert issue.issue_type == "missing_reference_range"
        assert issue.severity == "warning"
        assert not issue.suggestion  # No default available

    def test_range_present_no_issue(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            reference_low=70.0,
            reference_high=100.0,
        )
        assert dq.check_reference_range(lab) is None

    def test_reference_text_counts_as_present(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            reference_text="70-100 mg/dL",
        )
        assert dq.check_reference_range(lab) is None


# ---------------------------------------------------------------------------
# Data Quality Engine — Batch + Completeness
# ---------------------------------------------------------------------------


class TestBatchAndCompleteness:
    """check_batch and compute_completeness integration."""

    def test_check_batch_returns_all_issues(self, db):
        dq = DataQualityEngine(db)
        labs = [
            LabResult(
                id=uuid.uuid4().hex,
                test_name="Glucose",
                canonical_name="glucose",
                value=95.0,
                unit="mg/dL",
                fasting=None,  # will trigger fasting + missing ref range
            ),
            LabResult(
                id=uuid.uuid4().hex,
                test_name="TSH",
                canonical_name="tsh",
                value=2.1,
                unit="mIU/L",
                # will trigger missing ref range only
            ),
        ]
        issues = dq.check_batch(labs)
        types = {i.issue_type for i in issues}
        assert "missing_fasting_flag" in types
        assert "missing_reference_range" in types

    def test_completeness_perfect(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.1,
            unit="mIU/L",
            date_collected=date(2024, 1, 1),
            reference_low=0.4,
            reference_high=4.0,
        )
        score = dq.compute_completeness([lab])
        assert score == 1.0

    def test_completeness_missing_fields(self, db):
        dq = DataQualityEngine(db)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.1,
            unit="",  # missing unit
            # missing date, missing ref range
        )
        score = dq.compute_completeness([lab])
        assert 0.0 < score < 1.0

    def test_completeness_empty_list(self, db):
        dq = DataQualityEngine(db)
        assert dq.compute_completeness([]) == 0.0

    def test_completeness_fasting_test_penalised(self, db):
        """Fasting tests get an extra check (fasting flag), so missing it lowers score."""
        dq = DataQualityEngine(db)
        # Non-fasting test with all fields
        non_fasting = LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.1,
            unit="mIU/L",
            date_collected=date(2024, 1, 1),
            reference_low=0.4,
            reference_high=4.0,
        )
        # Fasting test with all fields except fasting flag
        fasting = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date(2024, 1, 1),
            reference_low=70.0,
            reference_high=100.0,
            fasting=None,  # missing
        )
        score_non_fasting = dq.compute_completeness([non_fasting])
        score_fasting = dq.compute_completeness([fasting])
        assert score_non_fasting > score_fasting


# ---------------------------------------------------------------------------
# Data Quality Engine — Formatting
# ---------------------------------------------------------------------------


class TestFormatIssues:
    """format_issues output."""

    def test_format_empty(self, db):
        dq = DataQualityEngine(db)
        assert "No data quality issues" in dq.format_issues([])

    def test_format_with_issues(self, db):
        dq = DataQualityEngine(db)
        issues = [
            DataQualityIssue(
                obs_id="1",
                test_name="Glucose",
                canonical_name="glucose",
                issue_type="missing_fasting_flag",
                severity="warning",
                message="Glucose: fasting status not recorded",
                suggestion="Confirm fasting.",
            ),
        ]
        text = dq.format_issues(issues)
        assert "[!]" in text  # warning icon
        assert "Glucose" in text
        assert "Confirm fasting" in text


# ---------------------------------------------------------------------------
# Citation — cite_from_meta
# ---------------------------------------------------------------------------


class TestCiteFromMeta:
    """Static cite_from_meta helper for inline citations."""

    def test_full_meta(self):
        from healthbot.retrieval.citation_manager import CitationManager

        meta = {
            "record_type": "lab_result",
            "date_effective": "2024-01-15",
            "source_doc_id": "blob123abc456",
            "source_page": 2,
        }
        result = CitationManager.cite_from_meta(meta)
        assert "lab_result" in result
        assert "2024-01-15" in result
        assert "doc:blob123a" in result  # first 8 chars
        assert "p.2" in result

    def test_minimal_meta(self):
        from healthbot.retrieval.citation_manager import CitationManager

        meta = {"record_type": "lab_result"}
        result = CitationManager.cite_from_meta(meta)
        assert "lab_result" in result
        assert result.startswith("[")
        assert result.endswith("]")

    def test_empty_meta(self):
        from healthbot.retrieval.citation_manager import CitationManager

        result = CitationManager.cite_from_meta({})
        assert result == "[]"


# ---------------------------------------------------------------------------
# Unit Conversions (expanded coverage)
# ---------------------------------------------------------------------------


class TestUnitConversions:
    """Expanded unit conversion coverage for common lab analytes."""

    def test_glucose_mmol_to_mgdl(self):
        result = convert_unit(5.5, "mmol/L", "mg/dL")
        assert result is not None
        assert 98 < result < 100  # 5.5 * 18.0182 ≈ 99.1

    def test_glucose_mgdl_to_mmol(self):
        result = convert_unit(100.0, "mg/dL", "mmol/L")
        assert result is not None
        assert 5.4 < result < 5.6  # 100 * 0.0555 ≈ 5.55

    def test_creatinine_umol_to_mgdl(self):
        result = convert_unit(88.4, "umol/L", "mg/dL")
        assert result is not None
        assert 0.9 < result < 1.1  # 88.4 * 0.0113 ≈ 1.0

    def test_hemoglobin_gl_to_gdl(self):
        result = convert_unit(145.0, "g/L", "g/dL")
        assert result is not None
        assert result == 14.5  # 145 * 0.1

    def test_hemoglobin_mmol_to_gdl(self):
        result = convert_unit(9.0, "mmol/L", "g/dL")
        assert result is not None
        assert 14.0 < result < 15.0  # 9.0 * 1.611 ≈ 14.5

    def test_b12_pgml_to_pmol(self):
        result = convert_unit(400.0, "pg/mL", "pmol/L")
        assert result is not None
        assert 290 < result < 300  # 400 * 0.738 ≈ 295

    def test_cholesterol_mmol_to_mgdl(self):
        result = convert_unit(5.2, "mmol/L", "mg/dL_chol")
        assert result is not None
        assert 199 < result < 203  # 5.2 * 38.67 ≈ 201

    def test_hematocrit_fraction_to_pct(self):
        result = convert_unit(0.45, "L/L", "%")
        assert result is not None
        assert result == 45.0

    def test_electrolyte_meq_to_mmol(self):
        # Monovalent ions: 1:1 conversion
        result = convert_unit(140.0, "mEq/L", "mmol/L")
        assert result is not None
        assert result == 140.0

    def test_unknown_conversion_returns_none(self):
        assert convert_unit(10.0, "apples", "oranges") is None


# ---------------------------------------------------------------------------
# Age/Sex-Adjusted Reference Ranges
# ---------------------------------------------------------------------------


class TestAdjustedRanges:
    """get_range with sex, age, and ethnicity parameters."""

    def test_female_hemoglobin_lower_range(self):
        from healthbot.reasoning.reference_ranges import get_range

        r = get_range("hemoglobin", sex="female")
        assert r is not None
        assert r["low"] == 12.0  # Female: 12-16 vs male: 13.5-17.5

    def test_elderly_tsh_higher_upper(self):
        from healthbot.reasoning.reference_ranges import get_range

        r = get_range("tsh", age=70)
        assert r is not None
        assert r["high"] == 5.5  # 65+ TSH upper is 5.5 vs standard 4.0

    def test_young_creatinine_lower_range(self):
        from healthbot.reasoning.reference_ranges import get_range

        r = get_range("creatinine", age=10)
        assert r is not None
        assert r["high"] == 0.5  # Pediatric creatinine is much lower

    def test_female_ferritin_lower_upper(self):
        from healthbot.reasoning.reference_ranges import get_range

        r = get_range("ferritin", sex="female")
        assert r is not None
        assert r["high"] == 150.0  # Premenopausal female upper is 150

    def test_unknown_test_returns_none(self):
        from healthbot.reasoning.reference_ranges import get_range

        assert get_range("not_a_real_test") is None

    def test_ethnicity_adjustment_vitamin_d(self):
        from healthbot.reasoning.reference_ranges import get_range

        r = get_range("vitamin_d", ethnicity="african american")
        assert r is not None
        assert r["low"] == 20.0  # Lower threshold for darker skin
