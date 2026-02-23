"""OpenClaw-inspired skill system for HealthBot.

Each reasoning engine is wrapped as a Skill with a standardized interface.
The SkillRegistry manages discovery, execution, and MCP integration.
"""
from healthbot.skills.base import (
    HealthContext,
    Skill,
    SkillRegistry,
    SkillResult,
    ToolPolicy,
)

__all__ = [
    "HealthContext",
    "Skill",
    "SkillRegistry",
    "SkillResult",
    "ToolPolicy",
]
