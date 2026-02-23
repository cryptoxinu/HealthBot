"""Tests for auto-generated weekly/monthly PDF reports."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.export.weekly_pdf_report import PdfReportData, WeeklyPdfReportGenerator


def _mock_db():
    db = MagicMock()
    db.query_observations = MagicMock(return_value=[])
    db.query_wearable_daily = MagicMock(return_value=[])
    db.query_workouts = MagicMock(return_value=[])
    db.get_active_medications = MagicMock(return_value=[])
    return db


class TestPdfReportData:
    def test_defaults(self):
        data = PdfReportData()
        assert data.period == "weekly"
        assert data.lab_items == []
        assert data.wearable_items == []
        assert data.workout_items == []
        assert data.medication_items == []
        assert data.action_items == []
        assert data.goal_items == []
        assert data.dashboard_chart is None


class TestWeeklyPdfReportGenerator:
    def test_generate_weekly_returns_pdf_bytes(self):
        """Weekly report should return valid PDF bytes."""
        db = _mock_db()
        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_weekly(user_id=1)

        assert isinstance(pdf_bytes, bytes)
        assert len(pdf_bytes) > 100
        assert pdf_bytes[:4] == b"%PDF"  # Valid PDF header

    def test_generate_monthly_returns_pdf_bytes(self):
        """Monthly report should return valid PDF bytes."""
        db = _mock_db()
        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_monthly(user_id=1)

        assert isinstance(pdf_bytes, bytes)
        assert pdf_bytes[:4] == b"%PDF"

    def test_empty_data_still_produces_pdf(self):
        """PDF should be generated even with no data."""
        db = _mock_db()
        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_weekly(user_id=1)
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_lab_data(self):
        """Lab data should be included in the report."""
        db = _mock_db()
        db.query_observations = MagicMock(return_value=[
            {
                "date_collected": "2024-01-15",
                "test_name": "LDL",
                "value": "130",
                "unit": "mg/dL",
                "flag": "H",
            },
            {
                "date_collected": "2024-01-15",
                "test_name": "HDL",
                "value": "55",
                "unit": "mg/dL",
                "flag": "",
            },
        ])
        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_weekly(user_id=1)
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_medications(self):
        """Medication data should be included."""
        db = _mock_db()
        db.get_active_medications = MagicMock(return_value=[
            {"name": "Metformin", "dose": "500mg", "frequency": "twice daily"},
        ])
        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_weekly(user_id=1)
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_workouts(self):
        """Workout data should be included."""
        db = _mock_db()
        db.query_workouts = MagicMock(return_value=[
            {
                "sport_type": "running", "_sport_type": "running",
                "_start_date": "2024-01-15",
                "duration_minutes": 30, "calories_burned": 300,
            },
            {
                "sport_type": "yoga", "_sport_type": "yoga",
                "_start_date": "2024-01-16",
                "duration_minutes": 45, "calories_burned": 150,
            },
        ])
        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_weekly(user_id=1)
        assert pdf_bytes[:4] == b"%PDF"

    def test_with_wearable_data(self):
        """Wearable data should be included."""
        db = _mock_db()
        db.query_wearable_daily = MagicMock(return_value=[
            {"hrv": 45, "rhr": 55, "recovery_score": 80, "sleep_score": 75},
            {"hrv": 50, "rhr": 53, "recovery_score": 85, "sleep_score": 80},
        ])
        gen = WeeklyPdfReportGenerator(db)
        pdf_bytes = gen.generate_weekly(user_id=1)
        assert pdf_bytes[:4] == b"%PDF"

    def test_gather_data_populates_dates(self):
        """Gathered data should have correct period and dates."""
        from datetime import date, timedelta

        db = _mock_db()
        gen = WeeklyPdfReportGenerator(db)

        end = date.today()
        start = end - timedelta(days=7)
        data = gen._gather_data(1, "weekly", start, end)

        assert data.period == "weekly"
        assert data.start_date == start.isoformat()
        assert data.end_date == end.isoformat()
        assert data.generated_at  # Non-empty

    def test_render_pdf_with_chart(self):
        """PDF should render even with a chart embedded."""
        db = _mock_db()
        gen = WeeklyPdfReportGenerator(db)

        data = PdfReportData(
            period="weekly",
            start_date="2024-01-08",
            end_date="2024-01-15",
            generated_at="2024-01-15 20:00 UTC",
            lab_items=["2024-01-15: 5 tests, 1 flagged (LDL)"],
            medication_items=["Metformin 500mg twice daily"],
            workout_items=["3 workouts, 2.5h total"],
        )

        pdf_bytes = gen._render_pdf(data)
        assert pdf_bytes[:4] == b"%PDF"
        assert len(pdf_bytes) > 200


class TestSchedulerDelays:
    def test_compute_weekly_first_delay_returns_positive(self):
        """Weekly delay computation should return positive seconds."""
        from healthbot.bot.scheduler import AlertScheduler
        from healthbot.config import Config

        config = Config()
        config.weekly_report_day = "sunday"
        config.weekly_report_time = "20:00"
        km = MagicMock()
        scheduler = AlertScheduler(config, km, chat_id=123)
        delay = scheduler._compute_weekly_first_delay()
        assert delay >= 60.0

    def test_compute_monthly_first_delay_returns_positive(self):
        """Monthly delay computation should return positive seconds."""
        from healthbot.bot.scheduler import AlertScheduler
        from healthbot.config import Config

        config = Config()
        config.monthly_report_day = 15
        config.monthly_report_time = "20:00"
        km = MagicMock()
        scheduler = AlertScheduler(config, km, chat_id=123)
        delay = scheduler._compute_monthly_first_delay()
        assert delay >= 60.0

    def test_disabled_weekly_returns_default(self):
        """Empty weekly_report_day should return 86400."""
        from healthbot.bot.scheduler import AlertScheduler
        from healthbot.config import Config

        config = Config()
        config.weekly_report_day = ""
        km = MagicMock()
        scheduler = AlertScheduler(config, km, chat_id=123)
        delay = scheduler._compute_weekly_first_delay()
        assert delay == 86400.0

    def test_disabled_monthly_returns_default(self):
        """Zero monthly_report_day should return 86400."""
        from healthbot.bot.scheduler import AlertScheduler
        from healthbot.config import Config

        config = Config()
        config.monthly_report_day = 0
        km = MagicMock()
        scheduler = AlertScheduler(config, km, chat_id=123)
        delay = scheduler._compute_monthly_first_delay()
        assert delay == 86400.0
