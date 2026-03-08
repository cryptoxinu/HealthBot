"""ResearchQueryPacket — sanitized query for external research.

Every outbound research query must go through this.
PHI is hard-blocked (not sanitized-and-sent).
"""
from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


@dataclass
class ResearchQueryPacket:
    """A sanitized research query safe for external use."""

    query: str
    query_hash: str
    context: str  # Sanitized context (no PHI)
    created_at: str
    blocked: bool = False
    block_reason: str = ""


def _build_demographic_context(demographics: dict | None) -> str:
    """Build anonymized demographic context for research queries.

    Uses age RANGE (decade) to avoid re-identification. Never includes
    name, DOB, SSN, or other identifiers.
    """
    if not demographics:
        return ""
    parts: list[str] = []
    age = demographics.get("age")
    if age:
        decade = (age // 10) * 10
        parts.append(f"age {decade}s")
    sex = demographics.get("sex")
    if sex:
        parts.append(sex.lower())
    ethnicity = demographics.get("ethnicity")
    if ethnicity:
        parts.append(ethnicity.lower())
    bmi = demographics.get("bmi")
    if bmi:
        if bmi < 18.5:
            parts.append("underweight")
        elif bmi < 25:
            parts.append("normal weight")
        elif bmi < 30:
            parts.append("overweight")
        else:
            parts.append("obese")
    if parts:
        return f"Patient context: {', '.join(parts)}"
    return ""


def build_research_packet(
    raw_query: str,
    context: str = "",
    firewall: PhiFirewall | None = None,
    demographics: dict | None = None,
    heuristic_name_check: Callable[[str], list[str]] | None = None,
) -> ResearchQueryPacket:
    """Build a research packet. Hard-blocks if PHI detected.

    Does NOT sanitize-and-send. If PHI is found, the packet is
    marked as blocked and must not be sent.

    If demographics provided, appends anonymized patient context
    (age decade, sex, ethnicity, BMI category — no identifiers).
    """
    if firewall is None:
        logger.warning(
            "build_research_packet called without shared firewall "
            "— identity patterns may be missing"
        )
    fw = firewall or PhiFirewall()
    now = datetime.now(UTC).isoformat()
    query_hash = hashlib.sha256(raw_query.encode()).hexdigest()[:16]

    # Hard-block on PHI in query
    if fw.contains_phi(raw_query):
        try:
            from healthbot.security.pii_alert import PiiAlertService
            svc = PiiAlertService.get_instance()
            svc.record(category="PHI_in_query", destination="research")
        except Exception:
            pass
        return ResearchQueryPacket(
            query="",
            query_hash=query_hash,
            context="",
            created_at=now,
            blocked=True,
            block_reason="PHI detected in query. Research blocked.",
        )

    # Heuristic name check (when NER unavailable — catches unlabeled names)
    if heuristic_name_check:
        suspects = heuristic_name_check(raw_query)
        if suspects:
            try:
                from healthbot.security.pii_alert import PiiAlertService
                svc = PiiAlertService.get_instance()
                svc.record(category="heuristic_name_in_query", destination="research")
            except Exception:
                pass
            return ResearchQueryPacket(
                query="",
                query_hash=query_hash,
                context="",
                created_at=now,
                blocked=True,
                block_reason=(
                    f"Suspected person name(s) in query: "
                    f"{', '.join(suspects[:3])}. Research blocked."
                ),
            )

    # Add anonymized demographic context (not PHI — decade, sex, BMI category)
    demo_ctx = _build_demographic_context(demographics)
    full_context = context
    if demo_ctx:
        full_context = f"{demo_ctx}\n{context}" if context else demo_ctx

    # Hard-block on PHI in context
    if full_context and fw.contains_phi(full_context):
        try:
            from healthbot.security.pii_alert import PiiAlertService
            svc = PiiAlertService.get_instance()
            svc.record(category="PHI_in_context", destination="research")
        except Exception:
            pass
        return ResearchQueryPacket(
            query="",
            query_hash=query_hash,
            context="",
            created_at=now,
            blocked=True,
            block_reason="PHI detected in context. Research blocked.",
        )

    return ResearchQueryPacket(
        query=raw_query,
        query_hash=query_hash,
        context=full_context,
        created_at=now,
    )
