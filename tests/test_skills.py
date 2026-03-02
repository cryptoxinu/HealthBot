"""Tests for the OpenClaw-inspired skill system."""
from __future__ import annotations

from unittest.mock import MagicMock

from healthbot.skills.base import (
    HealthContext,
    SkillRegistry,
    SkillResult,
    ToolPolicy,
)


class DummySkill:
    """Minimal skill for testing."""

    name = "dummy"
    description = "A test skill."

    def __init__(self, relevant: bool = True) -> None:
        self._relevant = relevant
        self.run_count = 0

    def is_relevant(self, ctx: HealthContext) -> bool:
        return self._relevant

    def run(self, ctx: HealthContext) -> SkillResult:
        self.run_count += 1
        return SkillResult(
            skill_name=self.name,
            summary="Dummy ran.",
            details=["detail1"],
            policy=ToolPolicy.MEDIUM,
            changed=True,
        )


class FailingSkill:
    """Skill that raises on run."""

    name = "failing"
    description = "Always fails."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return True

    def run(self, ctx: HealthContext) -> SkillResult:
        raise RuntimeError("boom")


class TestSkillRegistry:

    def test_register_and_list(self):
        reg = SkillRegistry()
        reg.register(DummySkill())
        skills = reg.list_skills()
        assert len(skills) == 1
        assert skills[0]["name"] == "dummy"
        assert skills[0]["enabled"] is True

    def test_enable_disable(self):
        reg = SkillRegistry()
        reg.register(DummySkill())
        reg.disable("dummy")
        skills = reg.list_skills()
        assert skills[0]["enabled"] is False
        reg.enable("dummy")
        assert reg.list_skills()[0]["enabled"] is True

    def test_unregister(self):
        reg = SkillRegistry()
        reg.register(DummySkill())
        reg.unregister("dummy")
        assert reg.list_skills() == []

    def test_run_skill(self):
        reg = SkillRegistry()
        skill = DummySkill()
        reg.register(skill)
        ctx = HealthContext()
        result = reg.run_skill("dummy", ctx)
        assert result is not None
        assert result.summary == "Dummy ran."
        assert skill.run_count == 1

    def test_run_nonexistent_skill(self):
        reg = SkillRegistry()
        result = reg.run_skill("nonexistent", HealthContext())
        assert result is None

    def test_run_disabled_skill(self):
        reg = SkillRegistry()
        reg.register(DummySkill())
        reg.disable("dummy")
        result = reg.run_skill("dummy", HealthContext())
        assert result is not None
        assert "disabled" in result.summary

    def test_run_relevant(self):
        reg = SkillRegistry()
        relevant = DummySkill(relevant=True)
        irrelevant = DummySkill(relevant=False)
        irrelevant.name = "irrelevant"
        reg.register(relevant)
        reg.register(irrelevant)

        results = reg.run_relevant(HealthContext())
        assert len(results) == 1
        assert results[0].skill_name == "dummy"

    def test_failing_skill_handled(self):
        reg = SkillRegistry()
        reg.register(FailingSkill())
        result = reg.run_skill("failing", HealthContext())
        assert result is not None
        assert "failed" in result.summary

    def test_failing_skill_in_run_relevant(self):
        reg = SkillRegistry()
        reg.register(FailingSkill())
        # Should not raise; failing skills now append an error SkillResult (L76)
        results = reg.run_relevant(HealthContext())
        assert len(results) == 1
        assert "error" in results[0].summary.lower() or "failed" in results[0].summary.lower()

    def test_get_skill(self):
        reg = SkillRegistry()
        skill = DummySkill()
        reg.register(skill)
        assert reg.get("dummy") is skill
        assert reg.get("nonexistent") is None


class TestToolPolicy:

    def test_policy_values(self):
        assert ToolPolicy.HIGH.value == "high"
        assert ToolPolicy.NEEDS_RESEARCH.value == "needs_research"


class TestHealthContext:

    def test_default_context(self):
        ctx = HealthContext()
        assert ctx.user_id == 0
        assert ctx.sex is None
        assert ctx.db is None

    def test_context_with_values(self):
        db = MagicMock()
        ctx = HealthContext(user_id=123, sex="male", age=30, db=db)
        assert ctx.user_id == 123
        assert ctx.sex == "male"
        assert ctx.age == 30
        assert ctx.db is db


class TestSkillResult:

    def test_default_result(self):
        r = SkillResult(skill_name="test", summary="ok")
        assert r.policy == ToolPolicy.MEDIUM
        assert r.changed is False
        assert r.details == []


class TestBuiltinSkillRegistration:

    def test_register_builtin_skills(self):
        from healthbot.skills.builtin import register_builtin_skills

        reg = SkillRegistry()
        register_builtin_skills(reg)
        skills = reg.list_skills()
        names = {s["name"] for s in skills}
        assert "trend_analysis" in names
        assert "interaction_check" in names
        assert "panel_gaps" in names
        assert "hypothesis_generator" in names
        assert "overdue_screenings" in names
        assert "intelligence_audit" in names
        assert "family_risk" in names
        assert "wearable_trends" in names
        assert "derived_markers" in names
        assert "lab_alerts" in names
        assert "pathway_analysis" in names
        assert "pharmacogenomics" in names
        assert len(skills) == 12

    def test_all_skills_have_name_and_description(self):
        from healthbot.skills.builtin import register_builtin_skills

        reg = SkillRegistry()
        register_builtin_skills(reg)
        for info in reg.list_skills():
            assert info["name"]
            assert info["description"]
            assert len(info["description"]) > 10

    def test_all_skills_report_relevant_with_db(self):
        """All built-in skills should be relevant when DB is available."""
        from healthbot.skills.builtin import register_builtin_skills

        reg = SkillRegistry()
        register_builtin_skills(reg)
        ctx = HealthContext(db=MagicMock())
        for info in reg.list_skills():
            skill = reg.get(info["name"])
            assert skill.is_relevant(ctx)

    def test_all_skills_not_relevant_without_db(self):
        """All built-in skills should be irrelevant without DB."""
        from healthbot.skills.builtin import register_builtin_skills

        reg = SkillRegistry()
        register_builtin_skills(reg)
        ctx = HealthContext(db=None)
        for info in reg.list_skills():
            skill = reg.get(info["name"])
            assert not skill.is_relevant(ctx)
