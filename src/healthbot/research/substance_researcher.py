"""Deep substance research engine.

Orchestrates comprehensive research on a substance using Claude CLI
(WebSearch + WebFetch) and PubMed, then stores structured results
in the substance_knowledge table.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger("healthbot")

# Research prompt template for Claude CLI
_RESEARCH_PROMPT = """\
Research the substance "{name}" comprehensively. Provide structured data in \
EXACTLY this JSON format (no other text, just the JSON):

{{
  "aliases": ["list of common names, brand names, abbreviations"],
  "mechanism_of_action": "detailed mechanism of action",
  "half_life": "pharmacokinetic half-life with units",
  "dosing_protocols": "common dosing ranges and protocols",
  "side_effects": ["list of known side effects"],
  "contraindications": ["list of contraindications"],
  "cyp_interactions": {{"CYP_enzyme": "role (substrate/inhibitor/inducer)"}},
  "pathway_effects": {{"biological_pathway": "effect (increase/decrease/modulate)"}},
  "clinical_evidence_summary": "summary of clinical trial evidence and quality",
  "drug_interactions": ["list of known drug interactions with brief descriptions"],
  "research_sources": ["PMID numbers or key reference citations"]
}}

Be thorough and evidence-based. Include CYP-450 enzyme data if known. \
For supplements/nootropics/peptides, include mechanism and safety data. \
Only return the JSON object, no explanatory text.
"""


@dataclass
class SubstanceProfile:
    """Structured substance research profile."""

    name: str
    aliases: list[str] = field(default_factory=list)
    mechanism_of_action: str = ""
    half_life: str = ""
    dosing_protocols: str = ""
    side_effects: list[str] = field(default_factory=list)
    contraindications: list[str] = field(default_factory=list)
    cyp_interactions: dict[str, str] = field(default_factory=dict)
    pathway_effects: dict[str, str] = field(default_factory=dict)
    clinical_evidence_summary: str = ""
    drug_interactions: list[str] = field(default_factory=list)
    research_sources: list[str] = field(default_factory=list)
    quality_score: float = 0.0

    def to_dict(self) -> dict:
        """Convert to dict for storage."""
        return {
            "aliases": self.aliases,
            "mechanism_of_action": self.mechanism_of_action,
            "half_life": self.half_life,
            "dosing_protocols": self.dosing_protocols,
            "side_effects": self.side_effects,
            "contraindications": self.contraindications,
            "cyp_interactions": self.cyp_interactions,
            "pathway_effects": self.pathway_effects,
            "clinical_evidence_summary": self.clinical_evidence_summary,
            "drug_interactions": self.drug_interactions,
            "research_sources": self.research_sources,
        }


@dataclass
class ResearchProgress:
    """Progress tracking for substance research."""

    stage: str = "starting"
    pct: int = 0
    message: str = ""


class SubstanceResearcher:
    """Orchestrates deep substance research."""

    def __init__(self, config: object, firewall: object) -> None:
        self._config = config
        self._fw = firewall

    def research(
        self,
        name: str,
        db: object | None = None,
        user_id: int = 0,
        on_progress: object | None = None,
    ) -> SubstanceProfile:
        """Run comprehensive research on a substance.

        Steps:
        1. Check existing profile (skip if quality > 0.7 and recent)
        2. Claude CLI research (WebSearch + WebFetch)
        3. PubMed queries (peer-reviewed evidence)
        4. Synthesize into SubstanceProfile
        5. Store in substance_knowledge table
        6. Update interaction KB overlay

        Returns SubstanceProfile with results.
        """
        self._progress(on_progress, "starting", 0, f"Researching {name}...")

        # Step 1: Check existing
        existing = self._check_existing(db, user_id, name)
        if existing and existing.get("quality_score", 0) > 0.7:
            updated = existing.get("updated_at", "")
            if updated and self._is_recent(updated, days=30):
                self._progress(on_progress, "cached", 100, "Using cached profile")
                return self._profile_from_data(name, existing.get("data", {}))

        # Step 2: Claude CLI research
        self._progress(on_progress, "web_search", 20, "Searching web sources...")
        cli_result = self._claude_cli_research(name)

        # Step 3: PubMed
        self._progress(on_progress, "pubmed", 50, "Querying PubMed...")
        pubmed_results = self._pubmed_search(name)

        # Step 4: Synthesize
        self._progress(on_progress, "synthesis", 70, "Synthesizing results...")
        profile = self._synthesize(name, cli_result, pubmed_results)

        # Step 5: Store
        self._progress(on_progress, "storing", 85, "Storing profile...")
        if db:
            self._store_profile(db, user_id, name, profile)

        # Step 6: Update KB overlay
        self._progress(on_progress, "kb_update", 95, "Updating interaction KB...")
        kb_additions = self._update_interaction_kb(name, profile)

        self._progress(
            on_progress, "complete", 100,
            f"Research complete. {len(profile.research_sources)} sources, "
            f"{len(profile.cyp_interactions)} CYP enzymes, "
            f"{len(profile.pathway_effects)} pathways, "
            f"{kb_additions} KB additions.",
        )
        return profile

    def _check_existing(
        self, db: object | None, user_id: int, name: str,
    ) -> dict | None:
        """Check for existing substance knowledge profile."""
        if not db:
            return None
        try:
            return db.get_substance_knowledge(user_id, name.lower())
        except Exception:
            return None

    def _is_recent(self, timestamp: str, days: int = 30) -> bool:
        """Check if a timestamp is within N days."""
        try:
            dt = datetime.fromisoformat(timestamp)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            age = (datetime.now(UTC) - dt).days
            return age < days
        except Exception:
            return False

    def _claude_cli_research(self, name: str) -> dict:
        """Research substance via Claude CLI with WebSearch."""
        try:
            from healthbot.research.claude_cli_client import ClaudeCLIResearchClient
            client = ClaudeCLIResearchClient(self._config, self._fw)
            prompt = _RESEARCH_PROMPT.format(name=name)
            raw = client.research(prompt, context="substance research")

            # Try to parse JSON from response
            return self._extract_json(raw)
        except Exception as e:
            logger.warning("Claude CLI research failed for %s: %s", name, e)
            return {}

    def _pubmed_search(self, name: str) -> list[dict]:
        """Search PubMed for peer-reviewed evidence."""
        try:
            from healthbot.research.pubmed_client import PubMedClient
            client = PubMedClient()
            articles = client.search(
                f"{name} pharmacology mechanism", max_results=5,
            )
            return articles or []
        except Exception as e:
            logger.warning("PubMed search failed for %s: %s", name, e)
            return []

    def _synthesize(
        self,
        name: str,
        cli_data: dict,
        pubmed_results: list[dict],
    ) -> SubstanceProfile:
        """Combine research sources into a SubstanceProfile."""
        profile = SubstanceProfile(name=name.lower())

        # From Claude CLI research
        if cli_data:
            profile.aliases = cli_data.get("aliases", [])
            profile.mechanism_of_action = cli_data.get("mechanism_of_action", "")
            profile.half_life = cli_data.get("half_life", "")
            profile.dosing_protocols = cli_data.get("dosing_protocols", "")
            profile.side_effects = cli_data.get("side_effects", [])
            profile.contraindications = cli_data.get("contraindications", [])
            profile.cyp_interactions = cli_data.get("cyp_interactions", {})
            profile.pathway_effects = cli_data.get("pathway_effects", {})
            profile.clinical_evidence_summary = cli_data.get(
                "clinical_evidence_summary", "",
            )
            profile.drug_interactions = cli_data.get("drug_interactions", [])
            profile.research_sources = cli_data.get("research_sources", [])

        # Add PubMed PMIDs
        for article in pubmed_results:
            pmid = article.get("pmid", "")
            if pmid and f"PMID:{pmid}" not in profile.research_sources:
                profile.research_sources.append(f"PMID:{pmid}")

        # Calculate quality score
        profile.quality_score = self._calculate_quality(profile)

        return profile

    def _calculate_quality(self, profile: SubstanceProfile) -> float:
        """Calculate quality score (0.0-1.0) based on completeness."""
        score = 0.0
        if profile.mechanism_of_action:
            score += 0.2
        if profile.half_life:
            score += 0.1
        if profile.cyp_interactions:
            score += 0.15
        if profile.pathway_effects:
            score += 0.15
        if profile.side_effects:
            score += 0.1
        if profile.clinical_evidence_summary:
            score += 0.1
        if profile.research_sources:
            score += min(0.2, len(profile.research_sources) * 0.04)
        return min(1.0, score)

    def _store_profile(
        self, db: object, user_id: int, name: str, profile: SubstanceProfile,
    ) -> None:
        """Store profile in substance_knowledge table."""
        try:
            existing = db.get_substance_knowledge(user_id, name.lower())
            if existing:
                db.update_substance_knowledge(
                    user_id, name.lower(), profile.to_dict(),
                    quality_score=profile.quality_score,
                )
            else:
                db.insert_substance_knowledge(
                    user_id, name.lower(), profile.to_dict(),
                    quality_score=profile.quality_score,
                )
        except Exception as e:
            logger.warning("Failed to store substance profile: %s", e)

    def _update_interaction_kb(
        self, name: str, profile: SubstanceProfile,
    ) -> int:
        """Update interaction KB overlay with discovered data."""
        try:
            from healthbot.reasoning.interaction_kb_updater import (
                InteractionKBUpdater,
            )
            updater = InteractionKBUpdater()
            return updater.propose_and_merge(name, profile)
        except Exception as e:
            logger.debug("KB overlay update skipped: %s", e)
            return 0

    def _extract_json(self, text: str) -> dict:
        """Extract JSON object from Claude CLI response text."""
        # Try direct parse first
        try:
            return json.loads(text)
        except (json.JSONDecodeError, ValueError):
            pass
        # Try to find JSON in response
        import re
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                return json.loads(match.group(0))
            except (json.JSONDecodeError, ValueError):
                pass
        return {}

    def _profile_from_data(self, name: str, data: dict) -> SubstanceProfile:
        """Create a SubstanceProfile from stored data dict."""
        return SubstanceProfile(
            name=name.lower(),
            aliases=data.get("aliases", []),
            mechanism_of_action=data.get("mechanism_of_action", ""),
            half_life=data.get("half_life", ""),
            dosing_protocols=data.get("dosing_protocols", ""),
            side_effects=data.get("side_effects", []),
            contraindications=data.get("contraindications", []),
            cyp_interactions=data.get("cyp_interactions", {}),
            pathway_effects=data.get("pathway_effects", {}),
            clinical_evidence_summary=data.get("clinical_evidence_summary", ""),
            drug_interactions=data.get("drug_interactions", []),
            research_sources=data.get("research_sources", []),
        )

    @staticmethod
    def _progress(
        callback: object | None, stage: str, pct: int, message: str,
    ) -> None:
        if callback and callable(callback):
            callback(ResearchProgress(stage=stage, pct=pct, message=message))
