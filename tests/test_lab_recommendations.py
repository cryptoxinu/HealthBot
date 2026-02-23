"""Tests for condition-based lab recommendations."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult, Medication
from healthbot.reasoning.lab_recommendations import (
    format_recommendations,
    recommend_labs,
)


class TestRecommendLabs:
    def test_empty_db_no_recommendations(self, db) -> None:
        """No conditions = no recommendations."""
        recs = recommend_labs(db, user_id=0)
        assert recs == []

    def test_medication_triggers_monitoring(self, db) -> None:
        """Statin medication should recommend ALT/AST/CK monitoring."""
        med = Medication(
            id=uuid.uuid4().hex, name="Atorvastatin",
            dose="20mg", frequency="daily", status="active",
        )
        db.insert_medication(med)

        recs = recommend_labs(db, user_id=0)
        canonical_names = [r.canonical_name for r in recs]
        assert "alt" in canonical_names
        assert "ast" in canonical_names

    def test_metformin_recommends_b12(self, db) -> None:
        """Metformin should recommend B12 monitoring."""
        med = Medication(
            id=uuid.uuid4().hex, name="Metformin",
            dose="1000mg", frequency="twice daily", status="active",
        )
        db.insert_medication(med)

        recs = recommend_labs(db, user_id=0)
        canonical_names = [r.canonical_name for r in recs]
        assert "vitamin_b12" in canonical_names

    def test_recent_test_not_flagged(self, db) -> None:
        """A test done recently should NOT be recommended."""
        med = Medication(
            id=uuid.uuid4().hex, name="Atorvastatin",
            dose="20mg", frequency="daily", status="active",
        )
        db.insert_medication(med)

        # Insert a recent ALT result (today)
        lab = LabResult(
            id=uuid.uuid4().hex, test_name="ALT",
            canonical_name="alt", value=25,
            unit="U/L", flag="",
            date_collected=date.today(),
        )
        db.insert_observation(lab)

        recs = recommend_labs(db, user_id=0)
        canonical_names = [r.canonical_name for r in recs]
        # ALT should NOT be recommended (just tested)
        assert "alt" not in canonical_names

    def test_old_test_is_flagged(self, db) -> None:
        """A test done long ago should be recommended."""
        med = Medication(
            id=uuid.uuid4().hex, name="Atorvastatin",
            dose="20mg", frequency="daily", status="active",
        )
        db.insert_medication(med)

        # Insert an old ALT result (1 year ago)
        lab = LabResult(
            id=uuid.uuid4().hex, test_name="ALT",
            canonical_name="alt", value=25,
            unit="U/L", flag="",
            date_collected=date.today() - timedelta(days=365),
        )
        db.insert_observation(lab)

        recs = recommend_labs(db, user_id=0)
        canonical_names = [r.canonical_name for r in recs]
        assert "alt" in canonical_names

    def test_dedup_across_sources(self, db) -> None:
        """Same test recommended by both med and condition should appear once."""
        med = Medication(
            id=uuid.uuid4().hex, name="Metformin",
            dose="500mg", frequency="daily", status="active",
        )
        db.insert_medication(med)

        recs = recommend_labs(db, user_id=0)
        # Count occurrences of each canonical name
        from collections import Counter
        counts = Counter(r.canonical_name for r in recs)
        assert all(c == 1 for c in counts.values())


class TestFormatRecommendations:
    def test_empty_list(self) -> None:
        output = format_recommendations([])
        assert "up to date" in output

    def test_with_recommendations(self) -> None:
        from healthbot.reasoning.lab_recommendations import LabRecommendation

        recs = [
            LabRecommendation(
                test_name="ALT", canonical_name="alt",
                reason="Due for statin (every 6mo, last 2025-01-01)",
                frequency_months=6, last_tested="2025-01-01",
                months_since=12, source="medication",
            ),
            LabRecommendation(
                test_name="CK", canonical_name="creatine_kinase",
                reason="Recommended for statin (never tested)",
                frequency_months=12, last_tested="",
                months_since=-1, source="medication",
            ),
        ]
        output = format_recommendations(recs)
        assert "RECOMMENDED LAB TESTS" in output
        assert "Never tested" in output
        assert "Overdue" in output
        assert "ALT" in output
        assert "CK" in output
