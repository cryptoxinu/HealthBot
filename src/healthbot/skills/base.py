"""Skill system base: Protocol, context, result, registry, and policy.

Inspired by OpenClaw's skill architecture. Each skill is a thin adapter
over an existing reasoning module — no logic rewrite needed.
"""
from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger("healthbot")


class ToolPolicy(enum.Enum):
    """Confidence policy for skill outputs."""

    HIGH = "high"           # Directly actionable (e.g., lab triage)
    MEDIUM = "medium"       # Informational (e.g., trend analysis)
    LOW = "low"             # Speculative (e.g., hypothesis)
    NEEDS_RESEARCH = "needs_research"  # Auto-queue for PubMed


@dataclass
class HealthContext:
    """Context passed to skills for execution."""

    user_id: int = 0
    sex: str | None = None
    age: int | None = None
    ethnicity: str | None = None
    db: object | None = None  # HealthDB instance


@dataclass
class SkillResult:
    """Standardized output from a skill execution."""

    skill_name: str
    summary: str
    details: list[str] = field(default_factory=list)
    policy: ToolPolicy = ToolPolicy.MEDIUM
    changed: bool = False   # True if result differs from last run


@runtime_checkable
class Skill(Protocol):
    """Protocol for a HealthBot skill."""

    name: str
    description: str

    def run(self, ctx: HealthContext) -> SkillResult:
        """Execute the skill and return a result."""
        ...

    def is_relevant(self, ctx: HealthContext) -> bool:
        """Check if this skill is relevant given the current context."""
        ...


class SkillRegistry:
    """Registry of available skills with discovery and execution."""

    def __init__(self) -> None:
        self._skills: dict[str, Skill] = {}
        self._enabled: set[str] = set()

    def register(self, skill: Skill) -> None:
        """Register a skill. Enabled by default."""
        self._skills[skill.name] = skill
        self._enabled.add(skill.name)

    def unregister(self, name: str) -> None:
        """Remove a skill from the registry."""
        self._skills.pop(name, None)
        self._enabled.discard(name)

    def enable(self, name: str) -> None:
        """Enable a registered skill."""
        if name in self._skills:
            self._enabled.add(name)

    def disable(self, name: str) -> None:
        """Disable a skill without removing it."""
        self._enabled.discard(name)

    def list_skills(self) -> list[dict[str, Any]]:
        """List all registered skills with status."""
        return [
            {
                "name": s.name,
                "description": s.description,
                "enabled": s.name in self._enabled,
            }
            for s in self._skills.values()
        ]

    def get(self, name: str) -> Skill | None:
        """Get a skill by name."""
        return self._skills.get(name)

    def run_skill(self, name: str, ctx: HealthContext) -> SkillResult | None:
        """Run a specific skill by name."""
        skill = self._skills.get(name)
        if skill is None:
            return None
        if name not in self._enabled:
            return SkillResult(
                skill_name=name,
                summary=f"Skill '{name}' is disabled.",
            )
        try:
            return skill.run(ctx)
        except Exception as e:
            logger.warning("Skill '%s' failed: %s", name, e)
            return SkillResult(
                skill_name=name,
                summary=f"Skill failed: {type(e).__name__}",
            )

    def run_relevant(self, ctx: HealthContext) -> list[SkillResult]:
        """Run all enabled skills that are relevant to the context."""
        results: list[SkillResult] = []
        for name in sorted(self._enabled):
            skill = self._skills.get(name)
            if skill is None:
                continue
            try:
                if skill.is_relevant(ctx):
                    result = skill.run(ctx)
                    results.append(result)
            except Exception as e:
                logger.warning("Skill '%s' error: %s", name, e, exc_info=True)
                results.append(SkillResult(
                    skill_name=name,
                    summary=f"Skill error: {type(e).__name__}: {e}",
                ))
        return results
