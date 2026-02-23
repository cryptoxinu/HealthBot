"""Tests for scalability improvements (Phase 6)."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.reasoning.correlate import CorrelationEngine
from healthbot.reasoning.hypothesis_generator import HypothesisGenerator


class TestQueryBounds:
    def test_hypothesis_generator_limits_canonical_names(self):
        """_get_latest_values query should include LIMIT 500."""
        db = MagicMock()
        db.conn.execute.return_value.fetchall.return_value = []
        gen = HypothesisGenerator(db)
        gen._get_latest_values(user_id=1)

        call_args = db.conn.execute.call_args[0]
        sql = call_args[0]
        assert "LIMIT 500" in sql

    def test_correlation_auto_discover_limits_lab_names(self):
        """auto_discover query should include LIMIT 100."""
        db = MagicMock()
        db.conn.execute.return_value.fetchall.return_value = []
        engine = CorrelationEngine(db)
        engine.auto_discover(user_id=1)

        call_args = db.conn.execute.call_args[0]
        sql = call_args[0]
        assert "LIMIT 100" in sql

    def test_intelligence_auditor_uses_canonical_name_filter(self):
        """_check_condition_test_gaps should use canonical_name filter."""
        from healthbot.reasoning.intelligence_auditor import (
            IntelligenceAuditor,
        )

        db = MagicMock()
        db.get_ltm_by_category.return_value = [
            {"fact": "hypothyroidism"},
        ]
        db.query_observations.return_value = []

        auditor = IntelligenceAuditor(db)
        auditor._check_condition_test_gaps(user_id=1)

        # Should be called with canonical_name= param
        for call in db.query_observations.call_args_list:
            assert "canonical_name" in call.kwargs or (
                len(call.args) >= 2
            )
