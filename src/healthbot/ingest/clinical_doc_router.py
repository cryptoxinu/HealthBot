"""Smart clinical document routing via Claude CLI.

Sends non-lab document text to Claude CLI with classification instructions.
Claude classifies each data point and emits structured blocks that are routed
to the appropriate database tables (observations, medications, conditions,
extensible health records, analysis rules, etc.).

Replaces the Ollama-based clinical extraction path for richer, more
structured data routing.
"""
from __future__ import annotations

import json
import logging
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date

logger = logging.getLogger("healthbot")


def _parse_date(raw: str) -> date | None:
    """Parse a date string (YYYY-MM-DD) to a date object, or return None."""
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except (ValueError, TypeError):
        return None

# Block patterns for routing prompt response parsing
_ROUTE_BLOCK_PATTERN = re.compile(
    r"(OBSERVATION|CONDITION|MEDICATION|PROVIDER|GOAL|MEMORY|HEALTH_DATA|ANALYSIS_RULE):\s*(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})",
)

_ROUTING_SYSTEM = """\
You are a medical data classifier. Read this clinical document and extract \
every piece of structured medical data. For each item, emit the appropriate \
structured block:

For test scores/measurements (PHQ-9, GAD-7, vitals, etc.):
  OBSERVATION: {"test": "PHQ-9", "value": "14", "unit": "score", \
"date": "2024-01-15", "flag": "moderate", "reference": "0-27"}

For diagnoses or conditions:
  CONDITION: {"name": "...", "status": "confirmed|suspected", "evidence": "..."}

For medications (prescribed, started, stopped, changed):
  MEDICATION: {"name": "...", "dose": "...", "status": "active|stopped", \
"date": "..."}

For provider/doctor information:
  PROVIDER: {"name": "...", "specialty": "...", "role": "..."}

For goals or treatment plans:
  GOAL: {"description": "...", "target_date": "...", "status": "active"}

For demographic updates (height, weight, etc.):
  MEMORY: {"key": "...", "value": "...", "category": "demographic"}

For any other medical data that doesn't fit the above:
  HEALTH_DATA: {"type": "...", "label": "...", "value": "...", "date": "...", \
"details": {}}

For analysis patterns you notice that should be monitored going forward:
  ANALYSIS_RULE: {"name": "...", "scope": "...", \
"rule": "...", "priority": "high|medium|low"}

Extract EVERYTHING. Do not summarize -- emit a block for each discrete data point.
"""


@dataclass
class RouteResult:
    """Result from routing a clinical document."""

    observations: int = 0
    conditions: int = 0
    medications: int = 0
    providers: int = 0
    goals: int = 0
    memories: int = 0
    health_data: int = 0
    analysis_rules: int = 0
    errors: list[str] = field(default_factory=list)
    routing_error: str = ""

    @property
    def total(self) -> int:
        return (self.observations + self.conditions + self.medications
                + self.providers + self.goals + self.memories
                + self.health_data + self.analysis_rules)


class ClinicalDocRouter:
    """Route non-lab clinical documents via Claude CLI.

    Sends redacted document text to Claude CLI with instructions to
    classify each data point and emit structured blocks for routing
    to the appropriate database tables.
    """

    def __init__(
        self,
        claude_client: object,
        db: object,
        clean_db: object | None,
        phi_firewall: object,
        on_progress: Callable[[str], None] | None = None,
    ) -> None:
        self._claude = claude_client
        self._db = db
        self._clean_db = clean_db
        self._fw = phi_firewall
        self._on_progress = on_progress

    def route_document(
        self, text: str, user_id: int, doc_id: str,
        health_summary_excerpt: str = "",
    ) -> RouteResult:
        """Send text to Claude CLI, parse response blocks, store each.

        Returns a RouteResult with counts per block type.
        """
        result = RouteResult()

        self._progress("Analyzing document with Claude...")

        # Build prompt
        prompt_parts = []
        if health_summary_excerpt:
            prompt_parts.append(
                f"Patient context (for cross-referencing):\n{health_summary_excerpt}\n\n"
            )
        prompt_parts.append(f"Document text:\n{text}")
        prompt = "".join(prompt_parts)

        # Send to Claude CLI
        try:
            response = self._claude.send(
                prompt=prompt, system=_ROUTING_SYSTEM,
            )
        except Exception as e:
            result.routing_error = str(e)
            logger.warning("Claude CLI routing failed: %s", e)
            return result

        if not response or len(response.strip()) < 5:
            result.routing_error = "Empty response from Claude CLI"
            return result

        # Parse blocks
        blocks = self._parse_blocks(response)
        self._progress(f"Extracted {len(blocks)} data points")

        # Route each block
        for block_type, data in blocks:
            try:
                self._route_one(block_type, data, user_id, result)
            except Exception as e:
                result.errors.append(f"{block_type}: {e}")
                logger.warning("Failed to route %s block: %s", block_type, e)

        self._progress(
            f"Stored {result.total} records"
            + (f", {result.analysis_rules} analysis rules defined"
               if result.analysis_rules else "")
        )
        return result

    def _parse_blocks(self, response: str) -> list[tuple[str, dict]]:
        """Parse structured blocks from Claude CLI response."""
        blocks: list[tuple[str, dict]] = []
        for match in _ROUTE_BLOCK_PATTERN.finditer(response):
            block_type = match.group(1)
            try:
                data = json.loads(match.group(2))
                blocks.append((block_type, data))
            except (json.JSONDecodeError, ValueError):
                continue
        return blocks

    def _route_one(
        self, block_type: str, data: dict, user_id: int,
        result: RouteResult,
    ) -> None:
        """Route a single parsed block to the correct storage."""
        # PII check on block content
        text_check = json.dumps(data, ensure_ascii=False)
        if self._fw and self._fw.contains_phi(text_check):
            logger.warning("Blocked %s block with PII during routing", block_type)
            return

        if block_type == "OBSERVATION":
            self._route_observation(data, user_id)
            result.observations += 1

        elif block_type == "CONDITION":
            self._route_condition(data, user_id)
            result.conditions += 1

        elif block_type == "MEDICATION":
            self._route_medication(data, user_id)
            result.medications += 1

        elif block_type == "PROVIDER":
            self._route_provider(data, user_id)
            result.providers += 1

        elif block_type == "GOAL":
            self._route_goal(data, user_id)
            result.goals += 1

        elif block_type == "MEMORY":
            self._route_memory(data, user_id)
            result.memories += 1

        elif block_type == "HEALTH_DATA":
            self._route_health_data(data, user_id)
            result.health_data += 1

        elif block_type == "ANALYSIS_RULE":
            self._route_analysis_rule(data)
            result.analysis_rules += 1

    def _route_observation(self, data: dict, user_id: int) -> None:
        """Store as an observation (lab result / vital sign / score)."""
        if not self._db:
            return
        from healthbot.data.models import LabResult
        obs = LabResult(
            id=uuid.uuid4().hex,
            test_name=data.get("test", ""),
            value=data.get("value", ""),
            unit=data.get("unit", ""),
            reference_text=data.get("reference", ""),
            flag=data.get("flag", ""),
            date_collected=_parse_date(data.get("date", "")),
        )
        self._db.insert_observation(obs, user_id=user_id)

    def _route_condition(self, data: dict, user_id: int) -> None:
        """Store condition as a hypothesis."""
        if not self._db:
            return
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker
        tracker = HypothesisTracker(self._db)
        status = data.get("status", "suspected")
        confidence = 0.8 if status == "confirmed" else 0.5
        tracker.upsert_hypothesis(user_id, {
            "title": data.get("name", ""),
            "confidence": confidence,
            "evidence_for": [data.get("evidence", "")] if data.get("evidence") else [],
            "evidence_against": [],
            "missing_tests": [],
        })

    def _route_medication(self, data: dict, user_id: int) -> None:
        """Store medication record."""
        if not self._db:
            return
        from healthbot.data.models import Medication
        med = Medication(
            id=uuid.uuid4().hex,
            name=data.get("name", ""),
            dose=data.get("dose", ""),
            status=data.get("status", "active"),
            start_date=_parse_date(data.get("date", "")),
        )
        self._db.insert_medication(med, user_id=user_id)

    def _route_provider(self, data: dict, user_id: int) -> None:
        """Store provider information."""
        if not self._db:
            return
        try:
            self._db.insert_provider(
                user_id=user_id,
                data={
                    "name": data.get("name", ""),
                    "specialty": data.get("specialty", ""),
                    "role": data.get("role", ""),
                },
            )
        except Exception as e:
            logger.debug("Provider insert failed: %s", e)

    def _route_goal(self, data: dict, user_id: int) -> None:
        """Store health goal."""
        if not self._db:
            return
        try:
            self._db.insert_health_goal(
                user_id=user_id,
                goal_data={
                    "description": data.get("description", ""),
                    "target_date": data.get("target_date", ""),
                    "status": data.get("status", "active"),
                },
            )
        except Exception as e:
            logger.debug("Goal insert failed: %s", e)

    def _route_memory(self, data: dict, user_id: int) -> None:
        """Store demographic/memory data in LTM."""
        if not self._db:
            return
        key = data.get("key", "")
        value = data.get("value", "")
        category = data.get("category", "demographic")
        if key and value:
            fact_text = f"{key}: {value}"
            self._db.insert_ltm(user_id, category, fact_text, source="document_routing")

    def _route_health_data(self, data: dict, user_id: int) -> None:
        """Store in extensible health records table."""
        if not self._db:
            return
        data_type = data.get("type", "other")
        label = data.get("label", "")
        if not label:
            return
        self._db.insert_health_record_ext(
            user_id=user_id,
            data_type=data_type,
            label=label,
            data_dict={
                "value": data.get("value", ""),
                "unit": data.get("unit", ""),
                "date": data.get("date", ""),
                "source": "document_routing",
                "details": data.get("details", {}),
                "tags": data.get("tags", []),
                "label": label,
            },
        )

    def _route_analysis_rule(self, data: dict) -> None:
        """Store analysis rule in clean DB."""
        if not self._clean_db:
            return
        name = data.get("name", "")
        rule = data.get("rule", "")
        if not name or not rule:
            return
        self._clean_db.upsert_analysis_rule(
            name=name,
            scope=data.get("scope", ""),
            rule=rule,
            priority=data.get("priority", "medium"),
        )

    def _progress(self, msg: str) -> None:
        if self._on_progress:
            try:
                self._on_progress(msg)
            except Exception:
                pass
