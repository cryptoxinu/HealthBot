"""Auto-update the interaction KB from deep research.

Writes discovered CYP and pathway data to a user-space overlay file
(~/.healthbot/interactions_custom.json) that gets merged with the base
KB at startup. Never modifies the source-tree interactions.json.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger("healthbot")

_OVERLAY_PATH = Path.home() / ".healthbot" / "interactions_custom.json"


def _load_overlay() -> dict:
    """Load the custom overlay file, or return empty structure."""
    if _OVERLAY_PATH.exists():
        try:
            with open(_OVERLAY_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {
        "cyp_enzyme_profiles": {},
        "pathway_profiles": {},
        "substance_aliases": {},
        "interactions": [],
    }


def _save_overlay(data: dict) -> None:
    """Write the overlay file."""
    _OVERLAY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_OVERLAY_PATH, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


class InteractionKBUpdater:
    """Updates interaction KB overlay from research discoveries."""

    def __init__(self) -> None:
        self._overlay = _load_overlay()

    def propose_and_merge(self, name: str, profile: object) -> int:
        """Generate and merge KB additions from a research profile.

        Returns number of additions made.
        """
        additions = 0
        canonical = name.lower().replace(" ", "_").replace("-", "_")

        # Add CYP profile
        if hasattr(profile, "cyp_interactions") and profile.cyp_interactions:
            if self.add_cyp_profile(canonical, profile.cyp_interactions):
                additions += 1

        # Add pathway profile
        if hasattr(profile, "pathway_effects") and profile.pathway_effects:
            if self.add_pathway_profile(canonical, profile.pathway_effects):
                additions += 1

        # Add substance aliases
        if hasattr(profile, "aliases") and profile.aliases:
            added = self.add_substance_aliases(canonical, profile.aliases)
            additions += added

        if additions:
            _save_overlay(self._overlay)
            self._reload_kb()

        return additions

    def add_cyp_profile(
        self, substance: str, enzymes: dict[str, str],
    ) -> bool:
        """Add or update a CYP enzyme profile in the overlay."""
        existing = self._overlay["cyp_enzyme_profiles"].get(substance, {})
        if existing == enzymes:
            return False
        self._overlay["cyp_enzyme_profiles"][substance] = enzymes
        return True

    def add_pathway_profile(
        self, substance: str, pathways: dict[str, str],
    ) -> bool:
        """Add or update a pathway profile in the overlay."""
        existing = self._overlay["pathway_profiles"].get(substance, {})
        if existing == pathways:
            return False
        self._overlay["pathway_profiles"][substance] = pathways
        return True

    def add_substance_aliases(
        self, canonical: str, aliases: list[str],
    ) -> int:
        """Add substance aliases to the overlay. Returns count added."""
        added = 0
        for alias in aliases:
            key = alias.lower().strip()
            if not key or len(key) < 2:
                continue
            if key not in self._overlay["substance_aliases"]:
                self._overlay["substance_aliases"][key] = canonical
                added += 1
        return added

    def _reload_kb(self) -> None:
        """Reload the KB module to pick up overlay changes."""
        try:
            from healthbot.reasoning import interaction_kb
            # Merge overlay into live KB
            overlay = self._overlay
            for name, enzymes in overlay.get("cyp_enzyme_profiles", {}).items():
                from healthbot.reasoning.interaction_kb import CypProfile
                interaction_kb.CYP_PROFILES[name] = CypProfile(
                    substance=name, enzymes=enzymes,
                )
            for name, pathways in overlay.get("pathway_profiles", {}).items():
                from healthbot.reasoning.interaction_kb import PathwayProfile
                interaction_kb.PATHWAY_PROFILES[name] = PathwayProfile(
                    substance=name, pathways=pathways,
                )
            for alias, canonical in overlay.get("substance_aliases", {}).items():
                interaction_kb.SUBSTANCE_ALIASES[alias] = canonical
        except Exception as e:
            logger.debug("KB overlay reload failed: %s", e)


def load_overlay_into_kb() -> None:
    """Load custom overlay into the KB at startup. Called from interaction_kb.py."""
    overlay = _load_overlay()
    if not any(overlay.values()):
        return
    try:
        from healthbot.reasoning import interaction_kb
        from healthbot.reasoning.interaction_kb import CypProfile, PathwayProfile

        for name, enzymes in overlay.get("cyp_enzyme_profiles", {}).items():
            interaction_kb.CYP_PROFILES[name] = CypProfile(
                substance=name, enzymes=enzymes,
            )
        for name, pathways in overlay.get("pathway_profiles", {}).items():
            interaction_kb.PATHWAY_PROFILES[name] = PathwayProfile(
                substance=name, pathways=pathways,
            )
        for alias, canonical in overlay.get("substance_aliases", {}).items():
            interaction_kb.SUBSTANCE_ALIASES[alias] = canonical
        logger.info(
            "Loaded custom KB overlay: %d CYP, %d pathway, %d aliases",
            len(overlay.get("cyp_enzyme_profiles", {})),
            len(overlay.get("pathway_profiles", {})),
            len(overlay.get("substance_aliases", {})),
        )
    except Exception as e:
        logger.debug("Custom KB overlay load failed: %s", e)
