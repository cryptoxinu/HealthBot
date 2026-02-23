"""Tests for lab panel gap detection."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult, TriageLevel
from healthbot.reasoning.panel_gaps import (
    GapReport,
    PanelGapDetector,
)


class TestPanelGaps:
    """Test panel gap detection logic."""

    def _insert_lab(
        self,
        db,
        canonical_name: str,
        value: float = 100.0,
        unit: str = "mg/dL",
        triage: TriageLevel = TriageLevel.NORMAL,
        flag: str = "",
        days_ago: int = 30,
    ) -> None:
        """Helper: insert a single lab result."""
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name=canonical_name,
            canonical_name=canonical_name,
            value=value,
            unit=unit,
            date_collected=date.today() - timedelta(days=days_ago),
            triage_level=triage,
            flag=flag,
        )
        db.insert_observation(lab)

    def test_empty_db_no_gaps(self, db) -> None:
        """Empty database should produce no gaps (no partial panels)."""
        detector = PanelGapDetector(db)
        report = detector.detect()
        assert not report.has_gaps
        assert report.panel_gaps == []
        assert report.conditional_gaps == []

    def test_partial_lipid_panel_detected(self, db) -> None:
        """Having only some lipid panel tests should flag a gap."""
        self._insert_lab(db, "cholesterol_total", value=200.0)
        self._insert_lab(db, "ldl", value=130.0)
        # hdl and triglycerides are missing

        detector = PanelGapDetector(db)
        report = detector.detect()
        assert report.has_gaps

        lipid_gaps = [g for g in report.panel_gaps if g.panel_name == "lipid_panel"]
        assert len(lipid_gaps) == 1
        gap = lipid_gaps[0]
        assert "cholesterol_total" in gap.present
        assert "ldl" in gap.present
        assert "hdl" in gap.missing
        assert "triglycerides" in gap.missing

    def test_complete_panel_no_gap(self, db) -> None:
        """A fully completed lipid panel should not be flagged."""
        self._insert_lab(db, "cholesterol_total", value=200.0)
        self._insert_lab(db, "ldl", value=130.0)
        self._insert_lab(db, "hdl", value=55.0)
        self._insert_lab(db, "triglycerides", value=150.0)

        detector = PanelGapDetector(db)
        existing = detector._get_existing_tests()
        panel_gaps = detector.detect_panel_gaps(existing)

        lipid_gaps = [g for g in panel_gaps if g.panel_name == "lipid_panel"]
        assert len(lipid_gaps) == 0

    def test_low_ferritin_triggers_iron_studies(self, db) -> None:
        """Low ferritin should recommend iron study companions."""
        self._insert_lab(
            db, "ferritin", value=8.0, unit="ng/mL",
            triage=TriageLevel.WATCH, flag="L",
        )

        detector = PanelGapDetector(db)
        report = detector.detect()

        ferritin_conds = [
            c for c in report.conditional_gaps
            if c.rule.trigger_test == "ferritin"
        ]
        assert len(ferritin_conds) == 1
        cg = ferritin_conds[0]
        # All recommended companions should be missing since only ferritin exists
        assert "iron" in cg.missing_tests
        assert "tibc" in cg.missing_tests
        assert "transferrin" in cg.missing_tests
        assert "hemoglobin" in cg.missing_tests

    def test_elevated_a1c_triggers_metabolic(self, db) -> None:
        """Elevated HbA1c should recommend metabolic/kidney companions."""
        self._insert_lab(
            db, "hba1c", value=7.2, unit="%",
            triage=TriageLevel.URGENT, flag="H",
        )

        detector = PanelGapDetector(db)
        report = detector.detect()

        a1c_conds = [
            c for c in report.conditional_gaps
            if c.rule.trigger_test == "hba1c"
        ]
        assert len(a1c_conds) == 1
        cg = a1c_conds[0]
        assert "glucose" in cg.missing_tests
        assert "creatinine" in cg.missing_tests
        assert "egfr" in cg.missing_tests
        assert "albumin" in cg.missing_tests

    def test_abnormal_tsh_triggers_thyroid(self, db) -> None:
        """Abnormal TSH should recommend free_t3 and free_t4."""
        self._insert_lab(
            db, "tsh", value=0.2, unit="mIU/L",
            triage=TriageLevel.WATCH, flag="L",
        )

        detector = PanelGapDetector(db)
        report = detector.detect()

        tsh_conds = [
            c for c in report.conditional_gaps
            if c.rule.trigger_test == "tsh"
        ]
        assert len(tsh_conds) == 1
        cg = tsh_conds[0]
        assert "free_t3" in cg.missing_tests
        assert "free_t4" in cg.missing_tests

    def test_format_gaps_output(self, db) -> None:
        """Format output should include expected sections and phrasing."""
        # Create a partial lipid panel
        self._insert_lab(db, "cholesterol_total", value=200.0)
        self._insert_lab(db, "ldl", value=130.0)

        # Create a flagged ferritin
        self._insert_lab(
            db, "ferritin", value=8.0, unit="ng/mL",
            triage=TriageLevel.WATCH, flag="L",
        )

        detector = PanelGapDetector(db)
        report = detector.detect()
        output = detector.format_gaps(report)

        assert "LAB PANEL GAP ANALYSIS" in output
        assert "INCOMPLETE PANELS" in output
        assert "Lipid Panel" in output
        assert "COMPANION TEST RECOMMENDATIONS" in output
        assert "consider discussing" in output.lower()

    def test_no_gaps_message(self, db) -> None:
        """No gaps should produce a clean 'all clear' message."""
        detector = PanelGapDetector(db)
        report = GapReport()
        output = detector.format_gaps(report)
        assert "No lab panel gaps detected" in output

    def test_normal_result_does_not_trigger_conditional(self, db) -> None:
        """A normal (non-flagged) result should not trigger conditional rules."""
        self._insert_lab(
            db, "ferritin", value=80.0, unit="ng/mL",
            triage=TriageLevel.NORMAL, flag="",
        )

        detector = PanelGapDetector(db)
        report = detector.detect()

        ferritin_conds = [
            c for c in report.conditional_gaps
            if c.rule.trigger_test == "ferritin"
        ]
        assert len(ferritin_conds) == 0

    def test_conditional_no_missing_when_companions_exist(self, db) -> None:
        """If all companion tests already exist, no conditional gap is flagged."""
        # Abnormal TSH
        self._insert_lab(
            db, "tsh", value=0.2, unit="mIU/L",
            triage=TriageLevel.WATCH, flag="L",
        )
        # But companions already present
        self._insert_lab(db, "free_t3", value=3.0, unit="pg/mL")
        self._insert_lab(db, "free_t4", value=1.2, unit="ng/dL")

        detector = PanelGapDetector(db)
        report = detector.detect()

        tsh_conds = [
            c for c in report.conditional_gaps
            if c.rule.trigger_test == "tsh"
        ]
        assert len(tsh_conds) == 0
