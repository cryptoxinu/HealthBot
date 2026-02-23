"""Tests for reasoning/doctor_templates.py — condition-specific templates."""
from __future__ import annotations

import pytest

from healthbot.reasoning.doctor_templates import (
    TEMPLATE_REGISTRY,
    DoctorTemplateEngine,
)


@pytest.fixture
def engine(config, key_manager, db) -> DoctorTemplateEngine:
    db.run_migrations()
    return DoctorTemplateEngine(db)


class TestTemplateRegistry:
    def test_has_required_templates(self):
        required = {"pots", "thyroid", "prediabetes", "cardiovascular", "anemia", "inflammation"}
        assert required.issubset(set(TEMPLATE_REGISTRY.keys()))

    def test_all_templates_have_required_fields(self):
        for key, tmpl in TEMPLATE_REGISTRY.items():
            assert "title" in tmpl, f"{key} missing title"
            assert "relevant_labs" in tmpl, f"{key} missing relevant_labs"
            assert "discussion_points" in tmpl, f"{key} missing discussion_points"
            assert len(tmpl["relevant_labs"]) >= 2, f"{key} needs at least 2 labs"
            assert len(tmpl["discussion_points"]) >= 3, f"{key} needs at least 3 points"


class TestListTemplates:
    def test_returns_all_templates(self, engine):
        templates = engine.list_templates()
        assert len(templates) == len(TEMPLATE_REGISTRY)
        keys = [k for k, _ in templates]
        assert "pots" in keys
        assert "thyroid" in keys

    def test_returns_tuples(self, engine):
        templates = engine.list_templates()
        for item in templates:
            assert isinstance(item, tuple)
            assert len(item) == 2


class TestGenerate:
    def test_unknown_template(self, engine):
        result = engine.generate("nonexistent", user_id=123)
        assert "Unknown template" in result
        assert "pots" in result  # Should list available templates

    def test_pots_template_structure(self, engine):
        result = engine.generate("pots", user_id=123)
        assert "POTS" in result
        assert "YOUR LAB VALUES" in result
        assert "DISCUSSION POINTS" in result
        assert "SUGGESTED QUESTIONS" in result

    def test_thyroid_template(self, engine):
        result = engine.generate("thyroid", user_id=123)
        assert "Thyroid" in result
        assert "TSH" in result.upper() or "tsh" in result.lower()

    def test_prediabetes_template(self, engine):
        result = engine.generate("prediabetes", user_id=123)
        assert "Pre-diabetes" in result or "Metabolic" in result

    def test_cardiovascular_template(self, engine):
        result = engine.generate("cardiovascular", user_id=123)
        assert "Cardiovascular" in result

    def test_anemia_template(self, engine):
        result = engine.generate("anemia", user_id=123)
        assert "Anemia" in result

    def test_inflammation_template(self, engine):
        result = engine.generate("inflammation", user_id=123)
        assert "Inflammation" in result

    def test_no_data_message(self, engine):
        result = engine.generate("pots", user_id=123)
        assert "No data" in result or "No lab data" in result

class TestHandlerRegistration:
    def test_template_handler_exists(self):
        from healthbot.bot.handlers import Handlers

        assert hasattr(Handlers, "template")

    def test_help_mentions_template(self):
        import inspect

        from healthbot.bot.handlers_session import SessionHandlers

        src = inspect.getsource(SessionHandlers.help_cmd)
        assert "/template" in src
