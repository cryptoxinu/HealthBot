"""Tests for overdue screening detection."""
from __future__ import annotations

import uuid
from datetime import date, timedelta

from healthbot.data.models import LabResult
from healthbot.reasoning.overdue import OverdueDetector, OverdueItem


class TestCheckOverdue:
    """OverdueDetector.check_overdue()."""

    def test_no_data_returns_empty(self, db):
        detector = OverdueDetector(db)
        assert detector.check_overdue() == []

    def test_recent_test_not_overdue(self, db):
        """Glucose tested 2 months ago (interval=12) → not overdue."""
        recent = date.today() - timedelta(days=60)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=recent,
        )
        db.insert_observation(lab)
        detector = OverdueDetector(db)
        items = detector.check_overdue()
        glucose_items = [i for i in items if i.canonical_name == "glucose"]
        assert len(glucose_items) == 0

    def test_old_test_is_overdue(self, db):
        """Glucose tested 15 months ago → overdue."""
        old = date.today() - timedelta(days=15 * 30)
        lab = LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=old,
        )
        db.insert_observation(lab)
        detector = OverdueDetector(db)
        items = detector.check_overdue()
        glucose_items = [i for i in items if i.canonical_name == "glucose"]
        assert len(glucose_items) == 1
        assert glucose_items[0].days_overdue > 0

    def test_multiple_overdue_sorted_by_days(self, db):
        """Two overdue tests sorted descending by days_overdue."""
        # Glucose: 18 months ago
        db.insert_observation(LabResult(
            id=uuid.uuid4().hex,
            test_name="Glucose",
            canonical_name="glucose",
            value=95.0,
            unit="mg/dL",
            date_collected=date.today() - timedelta(days=18 * 30),
        ))
        # TSH: 15 months ago
        db.insert_observation(LabResult(
            id=uuid.uuid4().hex,
            test_name="TSH",
            canonical_name="tsh",
            value=2.1,
            unit="mIU/L",
            date_collected=date.today() - timedelta(days=15 * 30),
        ))
        detector = OverdueDetector(db)
        items = detector.check_overdue()
        assert len(items) >= 2
        # First item should have more days overdue
        assert items[0].days_overdue >= items[1].days_overdue

    def test_never_tested_not_flagged(self, db):
        """Tests that have never been done are NOT flagged."""
        detector = OverdueDetector(db)
        items = detector.check_overdue()
        # No data at all → nothing overdue
        assert items == []

    def test_hba1c_6_month_interval(self, db):
        """HbA1c has 6-month interval, not 12."""
        old = date.today() - timedelta(days=8 * 30)  # 8 months ago
        db.insert_observation(LabResult(
            id=uuid.uuid4().hex,
            test_name="HbA1c",
            canonical_name="hba1c",
            value=5.7,
            unit="%",
            date_collected=old,
        ))
        detector = OverdueDetector(db)
        items = detector.check_overdue()
        hba1c = [i for i in items if i.canonical_name == "hba1c"]
        assert len(hba1c) == 1  # 8 months > 6 month interval


class TestFormatReminders:
    def test_format_empty(self, db):
        detector = OverdueDetector(db)
        text = detector.format_reminders([])
        assert "up to date" in text.lower()

    def test_format_with_items(self, db):
        detector = OverdueDetector(db)
        items = [
            OverdueItem(
                test_name="Glucose",
                canonical_name="glucose",
                last_date="2023-06-15",
                interval_months=12,
                days_overdue=180,
            ),
        ]
        text = detector.format_reminders(items)
        assert "Glucose" in text
        assert "6 months overdue" in text or "months overdue" in text
        assert "OVERDUE" in text
        assert "pause notifications" in text.lower()
