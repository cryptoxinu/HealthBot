"""Medication and supplement interaction knowledge base.

Static data loaded from data/interactions.json. No PHI. No encryption needed.
All interactions are evidence-based with citations.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Interaction:
    """A known interaction between two substances."""

    substance_a: str
    substance_b: str
    severity: str          # "minor", "moderate", "major", "contraindicated"
    mechanism: str
    recommendation: str
    evidence: str          # "established", "probable", "theoretical"
    citations: tuple[str, ...] = ()
    timing_advice: str = ""  # Specific timing guidance (e.g., "4 hours apart")


@dataclass(frozen=True)
class TimingRule:
    """Standalone timing advice for a substance."""

    substance: str
    advice: str
    reason: str


@dataclass(frozen=True)
class DrugConditionInteraction:
    """A known interaction between a medication and a medical condition."""

    drug: str              # Canonical KB key (e.g., "nsaid")
    condition: str         # Canonical condition key (e.g., "heart_failure")
    severity: str          # "minor", "moderate", "major", "contraindicated"
    mechanism: str
    recommendation: str
    evidence: str          # "established", "probable", "theoretical"
    citation: str = ""


@dataclass(frozen=True)
class DrugLabInteraction:
    """A known drug-to-lab-value interaction."""

    drug: str                # Canonical KB key (e.g., "metformin")
    lab: str                 # Canonical lab name (e.g., "vitamin_b12")
    effect: str              # "decrease", "increase", "alter"
    mechanism: str           # Why it happens
    monitor: str             # What to do
    severity: str            # "major", "moderate", "minor"
    evidence: str            # "established", "probable", "theoretical"
    citation: str = ""


@dataclass(frozen=True)
class CypProfile:
    """CYP-450 enzyme interaction profile for a substance."""

    substance: str
    enzymes: dict[str, str]  # enzyme -> role ("substrate", "inhibitor", "inducer")


@dataclass(frozen=True)
class PathwayProfile:
    """Biological pathway effect profile for a substance."""

    substance: str
    pathways: dict[str, str]  # pathway -> effect ("increase", "decrease", etc.)


# ---------------------------------------------------------------------------
# Load all data from JSON
# ---------------------------------------------------------------------------

_DATA_PATH = Path(__file__).resolve().parent.parent / "data" / "interactions.json"


def _load() -> dict:
    with open(_DATA_PATH) as f:
        return json.load(f)


_data = _load()

TIMING_RULES: tuple[TimingRule, ...] = tuple(
    TimingRule(**r) for r in _data["timing_rules"]
)

SUBSTANCE_ALIASES: dict[str, str] = _data["substance_aliases"]

INTERACTIONS: tuple[Interaction, ...] = tuple(
    Interaction(
        substance_a=i["substance_a"], substance_b=i["substance_b"],
        severity=i["severity"], mechanism=i["mechanism"],
        recommendation=i["recommendation"], evidence=i["evidence"],
        citations=tuple(i.get("citations", ())),
        timing_advice=i.get("timing_advice", ""),
    )
    for i in _data["interactions"]
)

CONDITION_ALIASES: dict[str, str] = _data["condition_aliases"]

DRUG_CONDITION_INTERACTIONS: tuple[DrugConditionInteraction, ...] = tuple(
    DrugConditionInteraction(**d) for d in _data["drug_condition_interactions"]
)

DRUG_LAB_INTERACTIONS: tuple[DrugLabInteraction, ...] = tuple(
    DrugLabInteraction(**d) for d in _data["drug_lab_interactions"]
)

CYP_PROFILES: dict[str, CypProfile] = {
    name: CypProfile(substance=name, enzymes=enzymes)
    for name, enzymes in _data.get("cyp_enzyme_profiles", {}).items()
}

PATHWAY_PROFILES: dict[str, PathwayProfile] = {
    name: PathwayProfile(substance=name, pathways=pathways)
    for name, pathways in _data.get("pathway_profiles", {}).items()
}

del _data  # Free memory after hydration

# Load user-space overlay (custom discoveries from /deep research)
try:
    from healthbot.reasoning.interaction_kb_updater import load_overlay_into_kb
    load_overlay_into_kb()
except Exception:
    pass  # Overlay is optional
