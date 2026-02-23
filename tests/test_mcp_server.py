"""Tests for MCP server tools.

Covers: tool output format, PII blocking, error handling,
input validation, and Clean DB isolation.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from healthbot.security.phi_firewall import PhiFirewall
from healthbot.skills.base import SkillResult, ToolPolicy

# ── Fixtures ───────────────────────────────────────────────


@pytest.fixture
def phi_firewall() -> PhiFirewall:
    """Real PhiFirewall — deterministic regex, no mocks needed."""
    return PhiFirewall()


@pytest.fixture
def clean_db():
    """Mock CleanDB with controllable return values."""
    db = MagicMock()
    db.get_lab_results.return_value = []
    db.get_medications.return_value = []
    db.get_wearable_data.return_value = []
    db.get_health_summary_markdown.return_value = (
        "# Health Data Summary (Anonymized)"
    )
    db.search.return_value = []
    db.get_hypotheses.return_value = []
    db.get_demographics.return_value = {
        "sex": "M",
        "age": 35,
        "ethnicity": "Caucasian",
    }
    return db


@pytest.fixture
def mock_skill_registry():
    """Stub SkillRegistry for skill-related tool tests."""
    registry = MagicMock()
    registry.list_skills.return_value = [
        {
            "name": "trend_analysis",
            "description": "Detect trends",
            "enabled": True,
        },
        {
            "name": "interaction_check",
            "description": "Check interactions",
            "enabled": False,
        },
    ]
    registry.run_skill.return_value = SkillResult(
        skill_name="trend_analysis",
        summary="Rising TSH trend detected",
        details=["TSH slope +0.3/year"],
        policy=ToolPolicy.HIGH,
    )
    return registry


@pytest.fixture
def server(clean_db, phi_firewall, mock_skill_registry):
    """Create MCP server with mocked skill registry.

    Patches SkillRegistry at the source package so the import
    inside create_server() picks up the mock.
    """
    with (
        patch(
            "healthbot.skills.SkillRegistry",
            return_value=mock_skill_registry,
        ),
        patch("healthbot.skills.builtin.register_builtin_skills"),
    ):
        from healthbot.mcp.server import create_server

        mcp = create_server(clean_db, phi_firewall)
    return mcp


def _call(server, name: str, **kwargs) -> str:
    """Invoke an MCP tool by name, returning the string result.

    FastMCP stores tool functions in its internal registry.
    We look them up and call directly — no transport needed.
    """
    # FastMCP stores tools by name in _tool_manager._tools
    tool_mgr = server._tool_manager
    tool = tool_mgr._tools[name]
    return tool.fn(**kwargs)


# ── get_lab_results ────────────────────────────────────────


class TestGetLabResults:
    """Tests for the get_lab_results MCP tool."""

    def test_get_lab_results_empty(self, server, clean_db):
        """Returns informative message when no labs match."""
        result = _call(server, "get_lab_results")
        assert result == "No lab results found matching the criteria."

    def test_get_lab_results_format(self, server, clean_db):
        """Returns a Markdown table with correct columns."""
        clean_db.get_lab_results.return_value = [
            {
                "date_effective": "2025-01-15",
                "test_name": "Glucose",
                "canonical_name": "glucose",
                "value": "95",
                "unit": "mg/dL",
                "reference_low": 70,
                "reference_high": 100,
                "reference_text": "",
                "flag": "",
            },
        ]
        result = _call(server, "get_lab_results")
        assert "| Date | Test | Value | Unit | Reference | Flag |" in result
        assert "| 2025-01-15 | Glucose |" in result
        assert "| 95 |" in result
        assert "| 70-100 |" in result

    def test_get_lab_results_reference_text_fallback(
        self, server, clean_db,
    ):
        """Uses reference_text when low/high are None."""
        clean_db.get_lab_results.return_value = [
            {
                "date_effective": "2025-01-15",
                "test_name": "Cortisol",
                "canonical_name": "cortisol",
                "value": "12.5",
                "unit": "mcg/dL",
                "reference_low": None,
                "reference_high": None,
                "reference_text": "5.0-25.0",
                "flag": "",
            },
        ]
        result = _call(server, "get_lab_results")
        assert "5.0-25.0" in result

    def test_get_lab_results_high_flag(self, server, clean_db):
        """Passes flag filter through to CleanDB."""
        _call(server, "get_lab_results", flag="H")
        clean_db.get_lab_results.assert_called_once_with(
            test_name=None,
            start_date=None,
            end_date=None,
            flag="H",
            limit=50,
        )

    def test_get_lab_results_limit_clamped(self, server, clean_db):
        """Limit is clamped to [1, 200]."""
        _call(server, "get_lab_results", limit=999)
        _, kwargs = clean_db.get_lab_results.call_args
        assert kwargs["limit"] == 200

        clean_db.get_lab_results.reset_mock()
        _call(server, "get_lab_results", limit=-5)
        _, kwargs = clean_db.get_lab_results.call_args
        assert kwargs["limit"] == 1

    def test_get_lab_results_passes_filters(self, server, clean_db):
        """All filter parameters are forwarded."""
        _call(
            server,
            "get_lab_results",
            test_name="TSH",
            start_date="2024-01-01",
            end_date="2025-01-01",
            flag="L",
            limit=10,
        )
        clean_db.get_lab_results.assert_called_once_with(
            test_name="TSH",
            start_date="2024-01-01",
            end_date="2025-01-01",
            flag="L",
            limit=10,
        )

    def test_get_lab_results_canonical_name_fallback(
        self, server, clean_db,
    ):
        """Falls back to canonical_name when test_name is missing."""
        clean_db.get_lab_results.return_value = [
            {
                "date_effective": "2025-02-01",
                "test_name": "",
                "canonical_name": "hemoglobin_a1c",
                "value": "5.7",
                "unit": "%",
                "reference_low": None,
                "reference_high": None,
                "reference_text": "<5.7",
                "flag": "",
            },
        ]
        result = _call(server, "get_lab_results")
        assert "hemoglobin_a1c" in result


# ── get_medications ────────────────────────────────────────


class TestGetMedications:
    """Tests for the get_medications MCP tool."""

    def test_get_medications_empty(self, server, clean_db):
        result = _call(server, "get_medications")
        assert result == "No medications found matching that status."

    def test_get_medications_format(self, server, clean_db):
        clean_db.get_medications.return_value = [
            {
                "name": "Levothyroxine",
                "dose": "50",
                "unit": "mcg",
                "frequency": "daily",
                "status": "active",
            },
        ]
        result = _call(server, "get_medications")
        assert "| Medication | Dose | Frequency | Status |" in result
        assert "Levothyroxine" in result
        assert "50 mcg" in result
        assert "daily" in result

    def test_get_medications_dose_without_unit(self, server, clean_db):
        """Dose renders alone when unit is empty."""
        clean_db.get_medications.return_value = [
            {
                "name": "Aspirin",
                "dose": "81mg",
                "unit": "",
                "frequency": "daily",
                "status": "active",
            },
        ]
        result = _call(server, "get_medications")
        assert "81mg" in result
        # No trailing space after dose when unit is empty
        assert "81mg  " not in result

    def test_get_medications_invalid_status(self, server, clean_db):
        """Returns error string for invalid status values."""
        result = _call(server, "get_medications", status="invalid")
        assert "Invalid status" in result
        clean_db.get_medications.assert_not_called()

    def test_get_medications_valid_statuses(self, server, clean_db):
        """All three valid statuses are accepted."""
        for status in ("active", "discontinued", "all"):
            clean_db.get_medications.reset_mock()
            _call(server, "get_medications", status=status)
            clean_db.get_medications.assert_called_once_with(
                status=status,
            )


# ── get_wearable_data ──────────────────────────────────────


class TestGetWearableData:
    """Tests for the get_wearable_data MCP tool."""

    def test_get_wearable_data_empty(self, server, clean_db):
        result = _call(server, "get_wearable_data")
        assert "No wearable data" in result

    def test_get_wearable_data_format(self, server, clean_db):
        clean_db.get_wearable_data.return_value = [
            {
                "date": "2025-02-10",
                "hrv": 55.3,
                "rhr": 58.0,
                "sleep_score": 82.0,
                "recovery_score": 71.0,
                "strain": 12.4,
            },
        ]
        result = _call(server, "get_wearable_data")
        assert "| Date | HRV | RHR | Sleep | Recovery | Strain |" in result
        assert "2025-02-10" in result
        assert "55" in result       # HRV rounded
        assert "71%" in result      # recovery with %
        assert "12.4" in result     # strain 1 decimal

    def test_get_wearable_data_none_values(self, server, clean_db):
        """Null metrics render as dash."""
        clean_db.get_wearable_data.return_value = [
            {
                "date": "2025-02-10",
                "hrv": None,
                "rhr": None,
                "sleep_score": None,
                "recovery_score": None,
                "strain": None,
            },
        ]
        result = _call(server, "get_wearable_data")
        # All nulls become dashes
        lines = result.split("\n")
        data_line = lines[2]  # Third line is the data row
        assert data_line.count("- |") >= 4

    def test_get_wearable_data_days_clamped(self, server, clean_db):
        """Days clamped to [1, 365]."""
        _call(server, "get_wearable_data", days=500)
        _, kwargs = clean_db.get_wearable_data.call_args
        assert kwargs["days"] == 365

        clean_db.get_wearable_data.reset_mock()
        _call(server, "get_wearable_data", days=0)
        _, kwargs = clean_db.get_wearable_data.call_args
        assert kwargs["days"] == 1

    def test_get_wearable_data_provider_passed(self, server, clean_db):
        """Provider parameter forwarded to CleanDB."""
        _call(server, "get_wearable_data", provider="oura")
        _, kwargs = clean_db.get_wearable_data.call_args
        assert kwargs["provider"] == "oura"


# ── get_health_summary ─────────────────────────────────────


class TestGetHealthSummary:
    """Tests for the get_health_summary MCP tool."""

    def test_get_health_summary_delegates(self, server, clean_db):
        """Delegates to clean_db.get_health_summary_markdown()."""
        clean_db.get_health_summary_markdown.return_value = (
            "# Health Data Summary (Anonymized)\n\n"
            "## Demographics\n- **Age**: 35"
        )
        result = _call(server, "get_health_summary")
        assert "Health Data Summary" in result
        clean_db.get_health_summary_markdown.assert_called_once()

    def test_get_health_summary_passes_through_safe_response(
        self, server, clean_db,
    ):
        """Clean text passes through _safe_response unchanged."""
        text = "## Demographics\n- Age: 35\n- Sex: M"
        clean_db.get_health_summary_markdown.return_value = text
        assert _call(server, "get_health_summary") == text


# ── search_health_data ─────────────────────────────────────


class TestSearchHealthData:
    """Tests for the search_health_data MCP tool."""

    def test_search_empty(self, server, clean_db):
        result = _call(server, "search_health_data", query="iron")
        assert "No results found" in result

    def test_search_lab_result(self, server, clean_db):
        clean_db.search.return_value = [
            {
                "source": "lab",
                "date": "2025-01-10",
                "test_name": "Iron",
                "value": "85",
                "unit": "mcg/dL",
                "flag": "normal",
            },
        ]
        result = _call(server, "search_health_data", query="iron")
        assert "**Lab**" in result
        assert "Iron" in result
        assert "85" in result

    def test_search_medication_result(self, server, clean_db):
        clean_db.search.return_value = [
            {
                "source": "medication",
                "name": "Metformin",
                "dose": "500mg",
                "frequency": "twice daily",
                "status": "active",
            },
        ]
        result = _call(
            server, "search_health_data", query="metformin",
        )
        assert "**Medication**" in result
        assert "Metformin" in result

    def test_search_hypothesis_result(self, server, clean_db):
        clean_db.search.return_value = [
            {
                "source": "hypothesis",
                "title": "Subclinical hypothyroidism",
                "confidence": 0.75,
            },
        ]
        result = _call(server, "search_health_data", query="thyroid")
        assert "**Hypothesis**" in result
        assert "Subclinical hypothyroidism" in result
        assert "75%" in result

    def test_search_unknown_source_fallback(self, server, clean_db):
        """Unknown source types render as raw string."""
        clean_db.search.return_value = [
            {
                "source": "wearable",
                "data": "HRV=55",
            },
        ]
        result = _call(server, "search_health_data", query="hrv")
        assert "wearable:" in result

    def test_search_limit_clamped(self, server, clean_db):
        """Limit is clamped to [1, 100]."""
        _call(
            server, "search_health_data", query="test", limit=999,
        )
        _, kwargs = clean_db.search.call_args
        assert kwargs["limit"] == 100

    def test_search_lab_no_flag_shows_normal(self, server, clean_db):
        """Empty/falsy flag renders as 'normal'."""
        clean_db.search.return_value = [
            {
                "source": "lab",
                "date": "2025-01-10",
                "test_name": "CRP",
                "value": "0.5",
                "unit": "mg/L",
                "flag": "",
            },
        ]
        result = _call(server, "search_health_data", query="CRP")
        assert "(normal)" in result


# ── get_health_trends ──────────────────────────────────────


class TestGetHealthTrends:
    """Tests for the get_health_trends MCP tool."""

    def test_get_health_trends_lab(self, server, clean_db):
        """Lab-based trend returns a Markdown table."""
        clean_db.get_lab_results.return_value = [
            {
                "date_effective": "2025-01-01",
                "value": "5.2",
                "unit": "%",
                "flag": "",
            },
            {
                "date_effective": "2024-07-01",
                "value": "5.0",
                "unit": "%",
                "flag": "",
            },
        ]
        result = _call(
            server, "get_health_trends", metric="HbA1c",
        )
        assert "HbA1c Trend (2 results)" in result
        assert "| Date | Value | Unit | Flag |" in result

    def test_get_health_trends_wearable(self, server, clean_db):
        """Falls back to wearable data for known wearable metrics."""
        clean_db.get_lab_results.return_value = []  # No lab data
        clean_db.get_wearable_data.return_value = [
            {"date": "2025-02-10", "hrv": 55.3},
            {"date": "2025-02-09", "hrv": 52.1},
        ]
        result = _call(server, "get_health_trends", metric="hrv")
        assert "hrv Trend (2 days)" in result
        assert "55.3" in result

    def test_get_health_trends_wearable_metrics(
        self, server, clean_db,
    ):
        """All mapped wearable metrics are recognized."""
        for metric in (
            "hrv", "rhr", "heart rate", "sleep",
            "recovery", "strain", "spo2",
        ):
            clean_db.get_lab_results.return_value = []
            clean_db.get_wearable_data.return_value = []
            _call(server, "get_health_trends", metric=metric)
            # Should attempt wearable lookup
            clean_db.get_wearable_data.assert_called()
            clean_db.get_wearable_data.reset_mock()

    def test_get_health_trends_no_data(self, server, clean_db):
        """Returns informative message when no trend data exists."""
        clean_db.get_lab_results.return_value = []
        result = _call(
            server, "get_health_trends", metric="Unknown",
        )
        assert "No trend data found" in result

    def test_get_health_trends_days_clamped(self, server, clean_db):
        """Days parameter is clamped to [1, 365] for lab lookup."""
        clean_db.get_lab_results.return_value = []
        _call(server, "get_health_trends", metric="glucose", days=999)
        _, kwargs = clean_db.get_lab_results.call_args
        assert kwargs["limit"] == 365


# ── get_hypotheses ─────────────────────────────────────────


class TestGetHypotheses:
    """Tests for the get_hypotheses MCP tool."""

    def test_get_hypotheses_empty(self, server, clean_db):
        result = _call(server, "get_hypotheses")
        assert result == "No active health hypotheses."

    def test_get_hypotheses_format(self, server, clean_db):
        clean_db.get_hypotheses.return_value = [
            {
                "title": "Subclinical hypothyroidism",
                "confidence": 0.82,
                "evidence_for": '["elevated TSH", "fatigue"]',
                "evidence_against": "[]",
                "missing_tests": '["free T4", "TPO antibodies"]',
            },
        ]
        result = _call(server, "get_hypotheses")
        assert "### Subclinical hypothyroidism (confidence: 82%)" in result
        assert "Evidence for:" in result
        assert "Evidence against:" in result
        assert "Missing tests:" in result

    def test_get_hypotheses_null_confidence(self, server, clean_db):
        """None confidence renders as N/A."""
        clean_db.get_hypotheses.return_value = [
            {
                "title": "Possible iron deficiency",
                "confidence": None,
                "evidence_for": "[]",
                "evidence_against": "[]",
                "missing_tests": "[]",
            },
        ]
        result = _call(server, "get_hypotheses")
        assert "(confidence: N/A)" in result


# ── list_skills / run_skill ────────────────────────────────


class TestSkillTools:
    """Tests for list_skills and run_skill MCP tools."""

    def test_list_skills_format(
        self, server, mock_skill_registry,
    ):
        result = _call(server, "list_skills")
        assert "| Skill | Description | Enabled |" in result
        assert "trend_analysis" in result
        assert "| yes |" in result
        assert "interaction_check" in result
        assert "| no |" in result

    def test_list_skills_empty(
        self, server, mock_skill_registry,
    ):
        mock_skill_registry.list_skills.return_value = []
        result = _call(server, "list_skills")
        assert result == "No skills registered."

    def test_run_skill_found(
        self, server, clean_db, mock_skill_registry,
    ):
        result = _call(
            server, "run_skill", skill_name="trend_analysis",
        )
        assert "## trend_analysis" in result
        assert "Rising TSH trend detected" in result
        assert "TSH slope +0.3/year" in result
        assert "Confidence: high" in result

    def test_run_skill_not_found(
        self, server, clean_db, mock_skill_registry,
    ):
        mock_skill_registry.run_skill.return_value = None
        result = _call(
            server, "run_skill", skill_name="nonexistent",
        )
        assert "not found" in result
        assert "list_skills" in result

    def test_run_skill_builds_context_from_demographics(
        self, server, clean_db, mock_skill_registry,
    ):
        """run_skill reads demographics from CleanDB."""
        _call(server, "run_skill", skill_name="trend_analysis")
        clean_db.get_demographics.assert_called_once()

    def test_run_skill_no_demographics(
        self, server, clean_db, mock_skill_registry,
    ):
        """Handles missing demographics gracefully."""
        clean_db.get_demographics.return_value = None
        _call(server, "run_skill", skill_name="trend_analysis")
        # Should not raise — sex/age/ethnicity are None
        mock_skill_registry.run_skill.assert_called_once()


# ── _safe_response PII blocking ───────────────────────────


class TestSafeResponse:
    """Tests for the _safe_response PII filter."""

    def test_blocks_ssn(self, server, clean_db):
        """SSN pattern in response triggers block."""
        clean_db.get_health_summary_markdown.return_value = (
            "Patient SSN: 123-45-6789"
        )
        result = _call(server, "get_health_summary")
        assert "PII detected" in result
        assert "123-45-6789" not in result

    def test_blocks_phone_number(self, server, clean_db):
        """US phone number in response triggers block."""
        clean_db.get_health_summary_markdown.return_value = (
            "Contact: (555) 123-4567"
        )
        result = _call(server, "get_health_summary")
        assert "PII detected" in result
        assert "(555) 123-4567" not in result

    def test_blocks_email(self, server, clean_db):
        """Email address in response triggers block."""
        clean_db.get_health_summary_markdown.return_value = (
            "Email: john.doe@example.com"
        )
        result = _call(server, "get_health_summary")
        assert "PII detected" in result
        assert "john.doe@example.com" not in result

    def test_blocks_labeled_name(self, server, clean_db):
        """Labeled patient name triggers block."""
        clean_db.get_health_summary_markdown.return_value = (
            "Patient: John Smith"
        )
        result = _call(server, "get_health_summary")
        assert "PII detected" in result
        assert "John Smith" not in result

    def test_blocks_dob(self, server, clean_db):
        """DOB pattern triggers block."""
        clean_db.get_health_summary_markdown.return_value = (
            "DOB: 03/15/1990"
        )
        result = _call(server, "get_health_summary")
        assert "PII detected" in result

    def test_passes_clean_text(self, server, clean_db):
        """Clean medical text passes through unchanged."""
        text = "Glucose: 95 mg/dL (normal range 70-100)"
        clean_db.get_health_summary_markdown.return_value = text
        result = _call(server, "get_health_summary")
        assert result == text

    def test_passes_lab_table(self, server, clean_db):
        """Standard lab table format passes through."""
        clean_db.get_lab_results.return_value = [
            {
                "date_effective": "2025-01-15",
                "test_name": "TSH",
                "canonical_name": "tsh",
                "value": "2.5",
                "unit": "mIU/L",
                "reference_low": 0.4,
                "reference_high": 4.0,
                "reference_text": "",
                "flag": "",
            },
        ]
        result = _call(server, "get_lab_results")
        assert "TSH" in result
        assert "PII detected" not in result

    def test_blocks_mrn(self, server, clean_db):
        """Medical record number triggers block."""
        clean_db.get_health_summary_markdown.return_value = (
            "MRN: 12345678"
        )
        result = _call(server, "get_health_summary")
        assert "PII detected" in result

    @patch("healthbot.security.pii_alert.PiiAlertService")
    def test_pii_alert_recorded_on_block(
        self, mock_alert_cls, server, clean_db,
    ):
        """PII block records an alert via PiiAlertService."""
        mock_service = MagicMock()
        mock_alert_cls.get_instance.return_value = mock_service

        clean_db.get_health_summary_markdown.return_value = (
            "Patient: Jane Doe"
        )
        _call(server, "get_health_summary")
        mock_service.record.assert_called_once_with(
            category="PHI_in_response",
            destination="mcp",
        )

    @patch("healthbot.security.pii_alert.PiiAlertService")
    def test_pii_alert_failure_does_not_raise(
        self, mock_alert_cls, server, clean_db,
    ):
        """PiiAlertService failure is silently caught."""
        mock_alert_cls.get_instance.side_effect = RuntimeError("boom")
        clean_db.get_health_summary_markdown.return_value = (
            "Patient: Jane Doe"
        )
        # Should not raise despite PiiAlertService failure
        result = _call(server, "get_health_summary")
        assert "PII detected" in result

    def test_blocks_pii_in_lab_results(self, server, clean_db):
        """PII embedded in lab result data is caught."""
        clean_db.get_lab_results.return_value = [
            {
                "date_effective": "2025-01-15",
                "test_name": "Patient: John Smith glucose",
                "canonical_name": "",
                "value": "95",
                "unit": "mg/dL",
                "reference_low": 70,
                "reference_high": 100,
                "reference_text": "",
                "flag": "",
            },
        ]
        result = _call(server, "get_lab_results")
        assert "PII detected" in result

    def test_blocks_pii_in_search_results(self, server, clean_db):
        """PII in search results is caught."""
        clean_db.search.return_value = [
            {
                "source": "lab",
                "date": "2025-01-10",
                "test_name": "Patient: Jane Doe test",
                "value": "12",
                "unit": "g/dL",
                "flag": "",
            },
        ]
        result = _call(
            server, "search_health_data", query="test",
        )
        assert "PII detected" in result

    def test_blocks_pii_in_hypotheses(self, server, clean_db):
        """PII in hypothesis text is caught."""
        clean_db.get_hypotheses.return_value = [
            {
                "title": "Patient: John Smith has condition",
                "confidence": 0.5,
                "evidence_for": "[]",
                "evidence_against": "[]",
                "missing_tests": "[]",
            },
        ]
        result = _call(server, "get_hypotheses")
        assert "PII detected" in result

    def test_blocks_pii_in_medications(self, server, clean_db):
        """PII in medication data is caught."""
        clean_db.get_medications.return_value = [
            {
                "name": "Patient: John Smith medication",
                "dose": "50",
                "unit": "mg",
                "frequency": "daily",
                "status": "active",
            },
        ]
        result = _call(server, "get_medications")
        assert "PII detected" in result


# ── Error handling: CleanDB unavailable ────────────────────


class TestErrorHandling:
    """Tests for resilience when CleanDB raises errors."""

    def test_lab_results_db_error(self, server, clean_db):
        """Exception from CleanDB propagates (no silent swallow)."""
        clean_db.get_lab_results.side_effect = Exception(
            "database is locked",
        )
        with pytest.raises(Exception, match="database is locked"):
            _call(server, "get_lab_results")

    def test_medications_db_error(self, server, clean_db):
        clean_db.get_medications.side_effect = Exception("db closed")
        with pytest.raises(Exception, match="db closed"):
            _call(server, "get_medications", status="active")

    def test_wearable_db_error(self, server, clean_db):
        clean_db.get_wearable_data.side_effect = Exception(
            "disk full",
        )
        with pytest.raises(Exception, match="disk full"):
            _call(server, "get_wearable_data")

    def test_summary_db_error(self, server, clean_db):
        clean_db.get_health_summary_markdown.side_effect = Exception(
            "corrupt db",
        )
        with pytest.raises(Exception, match="corrupt db"):
            _call(server, "get_health_summary")

    def test_search_db_error(self, server, clean_db):
        clean_db.search.side_effect = Exception("no such table")
        with pytest.raises(Exception, match="no such table"):
            _call(
                server, "search_health_data", query="test",
            )

    def test_hypotheses_db_error(self, server, clean_db):
        clean_db.get_hypotheses.side_effect = Exception("read error")
        with pytest.raises(Exception, match="read error"):
            _call(server, "get_hypotheses")


# ── Clean DB isolation ─────────────────────────────────────


class TestCleanDBIsolation:
    """Verify tools only interact with CleanDB (never raw vault)."""

    def test_get_lab_results_calls_clean_db(
        self, server, clean_db,
    ):
        _call(server, "get_lab_results")
        clean_db.get_lab_results.assert_called_once()

    def test_get_medications_calls_clean_db(
        self, server, clean_db,
    ):
        _call(server, "get_medications")
        clean_db.get_medications.assert_called_once()

    def test_get_wearable_data_calls_clean_db(
        self, server, clean_db,
    ):
        _call(server, "get_wearable_data")
        clean_db.get_wearable_data.assert_called_once()

    def test_get_health_summary_calls_clean_db(
        self, server, clean_db,
    ):
        _call(server, "get_health_summary")
        clean_db.get_health_summary_markdown.assert_called_once()

    def test_search_calls_clean_db(self, server, clean_db):
        _call(server, "search_health_data", query="glucose")
        clean_db.search.assert_called_once()

    def test_get_hypotheses_calls_clean_db(
        self, server, clean_db,
    ):
        _call(server, "get_hypotheses")
        clean_db.get_hypotheses.assert_called_once()

    def test_run_skill_passes_clean_db_in_context(
        self, server, clean_db, mock_skill_registry,
    ):
        """Skills receive CleanDB in HealthContext, not raw vault."""
        _call(server, "run_skill", skill_name="trend_analysis")
        call_args = mock_skill_registry.run_skill.call_args
        ctx = call_args[0][1]  # Second positional arg is ctx
        assert ctx.db is clean_db

    def test_no_raw_vault_methods_called(self, server, clean_db):
        """No HealthDB-specific methods are called by tools.

        CleanDB exposes only anonymized data accessors. The server
        must never call raw vault methods like _encrypt or
        get_identity_fields.
        """
        # Run every tool to exercise all code paths
        _call(server, "get_lab_results")
        _call(server, "get_medications")
        _call(server, "get_wearable_data")
        _call(server, "get_health_summary")
        _call(server, "search_health_data", query="test")
        _call(server, "get_hypotheses")
        _call(server, "get_health_trends", metric="glucose")

        # Collect all attribute names accessed on the mock
        accessed = {
            str(c) for c in clean_db.method_calls
        }
        raw_vault_methods = {
            "_encrypt", "_decrypt", "get_identity_fields",
            "upsert_identity_field", "get_raw_observations",
        }
        for method_name in raw_vault_methods:
            assert not any(method_name in a for a in accessed), (
                f"Raw vault method {method_name!r} was called"
            )


# ── Server creation ────────────────────────────────────────


class TestServerCreation:
    """Tests for create_server configuration."""

    def test_server_has_all_tools(self, server):
        """All expected tools are registered."""
        tool_mgr = server._tool_manager
        expected = {
            "get_lab_results",
            "get_medications",
            "get_wearable_data",
            "get_health_summary",
            "search_health_data",
            "get_health_trends",
            "get_hypotheses",
            "list_skills",
            "run_skill",
        }
        registered = set(tool_mgr._tools.keys())
        assert expected.issubset(registered), (
            f"Missing tools: {expected - registered}"
        )

    def test_server_name(self, server):
        """Server name is 'healthbot'."""
        assert server.name == "healthbot"

    def test_server_has_privacy_instructions(self, server):
        """Privacy protocol is included in server instructions."""
        instructions = server.instructions
        assert "PRIVACY PROTOCOL" in instructions
        assert "Do NOT save" in instructions
        assert "ephemeral" in instructions


# ── Multi-row output ───────────────────────────────────────


class TestMultiRowOutput:
    """Tests for tools rendering multiple data rows."""

    def test_lab_results_multiple_rows(self, server, clean_db):
        clean_db.get_lab_results.return_value = [
            {
                "date_effective": "2025-02-01",
                "test_name": "TSH",
                "canonical_name": "tsh",
                "value": "3.1",
                "unit": "mIU/L",
                "reference_low": 0.4,
                "reference_high": 4.0,
                "reference_text": "",
                "flag": "",
            },
            {
                "date_effective": "2025-01-01",
                "test_name": "TSH",
                "canonical_name": "tsh",
                "value": "4.5",
                "unit": "mIU/L",
                "reference_low": 0.4,
                "reference_high": 4.0,
                "reference_text": "",
                "flag": "H",
            },
        ]
        result = _call(server, "get_lab_results")
        lines = result.strip().split("\n")
        # Header + separator + 2 data rows = 4 lines
        assert len(lines) == 4
        assert "3.1" in lines[2]
        assert "4.5" in lines[3]
        assert "H" in lines[3]

    def test_medications_multiple_rows(self, server, clean_db):
        clean_db.get_medications.return_value = [
            {
                "name": "Levothyroxine",
                "dose": "50",
                "unit": "mcg",
                "frequency": "daily",
                "status": "active",
            },
            {
                "name": "Vitamin D",
                "dose": "5000",
                "unit": "IU",
                "frequency": "daily",
                "status": "active",
            },
        ]
        result = _call(server, "get_medications")
        assert "Levothyroxine" in result
        assert "Vitamin D" in result
        lines = result.strip().split("\n")
        assert len(lines) == 4  # header + sep + 2 rows

    def test_hypotheses_multiple_rows(self, server, clean_db):
        clean_db.get_hypotheses.return_value = [
            {
                "title": "Iron deficiency",
                "confidence": 0.9,
                "evidence_for": '["low ferritin"]',
                "evidence_against": "[]",
                "missing_tests": '["TIBC"]',
            },
            {
                "title": "B12 deficiency",
                "confidence": 0.6,
                "evidence_for": '["fatigue"]',
                "evidence_against": '["normal MCV"]',
                "missing_tests": '["methylmalonic acid"]',
            },
        ]
        result = _call(server, "get_hypotheses")
        assert "Iron deficiency" in result
        assert "B12 deficiency" in result
        assert "90%" in result
        assert "60%" in result
