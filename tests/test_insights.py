"""Tests for insight engine."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import LabResult, TriageLevel
from healthbot.reasoning.insights import InsightEngine
from healthbot.reasoning.trends import TrendAnalyzer
from healthbot.reasoning.triage import TriageEngine


class TestInsights:
    """Test WHOOP-style domain scoring."""

    def test_dashboard_runs(self, db):
        """Dashboard generation should not crash even with no data."""
        triage = TriageEngine()
        trends = TrendAnalyzer(db)
        engine = InsightEngine(db, triage, trends)
        dashboard = engine.generate_dashboard()
        assert "HEALTH DASHBOARD" in dashboard
        assert "NOTABLE TRENDS" in dashboard

    def test_domain_scores_with_data(self, db):
        """Domain scores should reflect triage levels."""
        # Insert a normal glucose
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=90.0,
            unit="mg/dL",
            reference_low=70,
            reference_high=100,
            date_collected=date.today(),
            triage_level=TriageLevel.NORMAL,
        )
        db.insert_observation(lab)

        triage = TriageEngine()
        trends = TrendAnalyzer(db)
        engine = InsightEngine(db, triage, trends)
        scores = engine.compute_domain_scores()

        # Metabolic domain should have data
        metabolic = next((s for s in scores if s.domain == "metabolic"), None)
        assert metabolic is not None
        assert metabolic.tests_found >= 1
