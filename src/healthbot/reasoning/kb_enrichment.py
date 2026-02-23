"""Proactive KB enrichment from analysis findings.

Auto-feeds significant findings from reasoning engines into the
knowledge base with dedup and TTL-based cleanup.
"""
from __future__ import annotations

import logging

from healthbot.data.db import HealthDB
from healthbot.research.knowledge_base import KnowledgeBase

logger = logging.getLogger("healthbot")

_SOURCE = "auto_analysis"


class KBEnrichmentEngine:
    """Store significant analysis findings in the knowledge base."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db
        self._kb = KnowledgeBase(db)

    def store_trend_finding(
        self, trend_text: str, user_id: int,
    ) -> str | None:
        """Store a significant trend finding if not already present."""
        topic = f"trend:{trend_text.split(':')[0].strip()}"
        if self._kb.find_similar(topic, trend_text, _SOURCE):
            return None
        return self._kb.store_finding(
            topic=topic,
            finding=trend_text,
            source=_SOURCE,
            relevance_score=0.6,
        )

    def store_interaction_finding(
        self, interaction_text: str, user_id: int,
    ) -> str | None:
        """Store a drug-lab interaction finding if not already present."""
        topic = f"interaction:{interaction_text.split(':')[0].strip()}"
        if self._kb.find_similar(topic, interaction_text, _SOURCE):
            return None
        return self._kb.store_finding(
            topic=topic,
            finding=interaction_text,
            source=_SOURCE,
            relevance_score=0.7,
        )

    def store_gap_finding(
        self, gap_text: str, user_id: int,
    ) -> str | None:
        """Store a gap/missing test finding if not already present."""
        topic = f"gap:{gap_text.split(':')[0].strip()}"
        if self._kb.find_similar(topic, gap_text, _SOURCE):
            return None
        return self._kb.store_finding(
            topic=topic,
            finding=gap_text,
            source=_SOURCE,
            relevance_score=0.5,
        )

    def cleanup_stale(self, max_age_days: int = 90) -> int:
        """Remove auto-generated entries older than max_age_days.

        Never removes user_confirmed or claude_research entries.
        """
        return self._kb.delete_stale(_SOURCE, max_age_days)
