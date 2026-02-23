"""Tests for health goal tracking (Phase S3)."""
from __future__ import annotations

import uuid
from datetime import date

from healthbot.data.models import LabResult, WhoopDaily
from healthbot.reasoning.goals import GoalTracker, format_goals


class TestGoalCRUD:
    """Test creating, reading, and removing goals."""

    def test_add_goal(self, db) -> None:
        """Adding a goal should return an ID."""
        tracker = GoalTracker(db)
        goal_id = tracker.add_goal(1, "ldl", 100.0, "below", "LDL")
        assert goal_id

    def test_get_goals(self, db) -> None:
        """Goals should be retrievable."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "ldl", 100.0, "below", "LDL")
        tracker.add_goal(1, "vitamin_d", 40.0, "above", "Vitamin D")
        goals = tracker.get_goals(1)
        assert len(goals) == 2
        names = {g.metric for g in goals}
        assert "ldl" in names
        assert "vitamin_d" in names

    def test_remove_goal(self, db) -> None:
        """Removing a goal should work."""
        tracker = GoalTracker(db)
        gid = tracker.add_goal(1, "ldl", 100.0, "below", "LDL")
        goals = tracker.get_goals(1)
        assert len(goals) == 1
        tracker.remove_goal(gid)
        goals = tracker.get_goals(1)
        assert len(goals) == 0

    def test_no_goals(self, db) -> None:
        """No goals returns empty."""
        tracker = GoalTracker(db)
        assert tracker.get_goals(1) == []


class TestGoalProgress:
    """Test goal progress tracking."""

    def test_goal_achieved_below(self, db) -> None:
        """LDL below target should show achieved."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "ldl", 100.0, "below", "LDL")

        # Insert lab result below target
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=85.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        progress = tracker.check_progress(1)
        assert len(progress) == 1
        assert progress[0].status == "achieved"
        assert progress[0].pct_progress == 100.0

    def test_goal_achieved_above(self, db) -> None:
        """Vitamin D above target should show achieved."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "vitamin_d", 40.0, "above", "Vitamin D")

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Vitamin D",
            canonical_name="vitamin_d",
            value=55.0,
            unit="ng/mL",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        progress = tracker.check_progress(1)
        assert len(progress) == 1
        assert progress[0].status == "achieved"

    def test_goal_not_achieved(self, db) -> None:
        """LDL above target should not be achieved."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "ldl", 100.0, "below", "LDL")

        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="LDL",
            canonical_name="ldl",
            value=130.0,
            unit="mg/dL",
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        progress = tracker.check_progress(1)
        assert len(progress) == 1
        assert progress[0].status in ("on_track", "off_track")
        assert progress[0].pct_progress < 100

    def test_no_data_returns_no_data_status(self, db) -> None:
        """Goal with no matching data should show no_data."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "ldl", 100.0, "below", "LDL")

        progress = tracker.check_progress(1)
        assert len(progress) == 1
        assert progress[0].status == "no_data"

    def test_wearable_goal(self, db) -> None:
        """Goal for a wearable metric (HRV above 50)."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "hrv", 50.0, "above", "HRV")

        wd = WhoopDaily(
            id=uuid.uuid4().hex, date=date.today(),
            hrv=65.0, rhr=55.0,
        )
        db.insert_wearable_daily(wd, user_id=1)

        progress = tracker.check_progress(1)
        assert len(progress) == 1
        assert progress[0].status == "achieved"
        assert progress[0].current_value == 65.0


class TestGoalAchievements:
    """Test achievement filtering."""

    def test_check_achievements(self, db) -> None:
        """Only achieved goals should be returned."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "ldl", 100.0, "below", "LDL")
        tracker.add_goal(1, "glucose", 100.0, "below", "Glucose")

        # LDL achieved
        lab1 = LabResult(
            id=uuid.uuid4().hex, test_name="LDL",
            canonical_name="ldl", value=85.0,
            date_collected=date.today(),
        )
        db.insert_observation(lab1, user_id=1)

        # Glucose NOT achieved
        lab2 = LabResult(
            id=uuid.uuid4().hex, test_name="Glucose",
            canonical_name="glucose", value=110.0,
            date_collected=date.today(),
        )
        db.insert_observation(lab2, user_id=1)

        achievements = tracker.check_achievements(1)
        assert len(achievements) == 1
        assert achievements[0].goal.metric == "ldl"


class TestFormatGoals:
    """Test formatting."""

    def test_format_empty(self) -> None:
        """No goals should show help text."""
        text = format_goals([])
        assert "No health goals" in text

    def test_format_with_progress(self, db) -> None:
        """Progress should be formatted with bars and messages."""
        tracker = GoalTracker(db)
        tracker.add_goal(1, "ldl", 100.0, "below", "LDL")

        lab = LabResult(
            id=uuid.uuid4().hex, test_name="LDL",
            canonical_name="ldl", value=85.0,
            date_collected=date.today(),
        )
        db.insert_observation(lab, user_id=1)

        progress = tracker.check_progress(1)
        text = format_goals(progress)
        assert "HEALTH GOALS" in text
        assert "LDL" in text
        assert "achieved" in text.lower() or "100%" in text
