"""CYP-450 enzyme and pathway-level interaction checking.

Deterministic. No LLM. Checks substances against active medications
for CYP enzyme conflicts (inducer vs substrate on same enzyme) and
pathway stacking (multiple substances increasing same pathway).
"""
from __future__ import annotations

from dataclasses import dataclass

from healthbot.reasoning.interaction_kb import (
    CYP_PROFILES,
    PATHWAY_PROFILES,
    SUBSTANCE_ALIASES,
    CypProfile,
    PathwayProfile,
)


@dataclass(frozen=True)
class CypConflict:
    """A CYP-450 enzyme conflict between two substances."""

    substance_a: str
    substance_b: str
    enzyme: str
    role_a: str  # "inducer", "inhibitor", "substrate"
    role_b: str
    severity: str  # "major", "moderate", "minor"
    mechanism: str
    recommendation: str


@dataclass(frozen=True)
class PathwayStack:
    """Multiple substances affecting the same pathway."""

    pathway: str
    substances: tuple[str, ...]
    effects: tuple[str, ...]  # effect per substance
    severity: str
    mechanism: str
    recommendation: str


# Severity mapping for CYP conflicts
_CYP_SEVERITY = {
    ("inducer", "substrate"): "moderate",
    ("substrate", "inducer"): "moderate",
    ("inhibitor", "substrate"): "major",
    ("substrate", "inhibitor"): "major",
    ("inducer", "inhibitor"): "moderate",
    ("inhibitor", "inducer"): "moderate",
}

# Pathways where stacking is particularly dangerous
_HIGH_RISK_PATHWAYS = frozenset({
    "serotonin", "dopamine", "norepinephrine", "GABA",
    "insulin", "GLP-1",
})


def _resolve_substance(name: str) -> str:
    """Resolve a substance name to its canonical KB key."""
    key = name.strip().lower()
    return SUBSTANCE_ALIASES.get(key, key)


def _get_cyp_profile(substance: str) -> CypProfile | None:
    """Get CYP profile for a substance, trying canonical key."""
    canonical = _resolve_substance(substance)
    return CYP_PROFILES.get(canonical)


def _get_pathway_profile(substance: str) -> PathwayProfile | None:
    """Get pathway profile for a substance, trying canonical key."""
    canonical = _resolve_substance(substance)
    return PATHWAY_PROFILES.get(canonical)


class CypInteractionChecker:
    """Check for CYP-450 enzyme conflicts between substances."""

    @staticmethod
    def check_substance_cyp(
        substance: str,
        active_meds: list[str],
    ) -> list[CypConflict]:
        """Check a substance's CYP profile against all active medications.

        Returns list of CypConflict for each enzyme conflict found.
        """
        profile = _get_cyp_profile(substance)
        if not profile or not profile.enzymes:
            return []

        conflicts: list[CypConflict] = []
        sub_canonical = _resolve_substance(substance)

        for med in active_meds:
            med_canonical = _resolve_substance(med)
            if med_canonical == sub_canonical:
                continue

            med_profile = _get_cyp_profile(med)
            if not med_profile or not med_profile.enzymes:
                continue

            # Check each enzyme pair
            for enzyme, role_a in profile.enzymes.items():
                role_b = med_profile.enzymes.get(enzyme)
                if not role_b:
                    continue
                # Same role is not a conflict (both substrates = competition,
                # but typically minor)
                if role_a == role_b == "substrate":
                    continue
                if role_a == role_b:
                    continue

                severity = _CYP_SEVERITY.get((role_a, role_b), "minor")
                mechanism = _build_cyp_mechanism(
                    substance, med, enzyme, role_a, role_b,
                )
                recommendation = _build_cyp_recommendation(
                    substance, med, enzyme, role_a, role_b,
                )
                conflicts.append(CypConflict(
                    substance_a=substance,
                    substance_b=med,
                    enzyme=enzyme,
                    role_a=role_a,
                    role_b=role_b,
                    severity=severity,
                    mechanism=mechanism,
                    recommendation=recommendation,
                ))

        return conflicts


class PathwayInteractionChecker:
    """Check for pathway stacking between substances."""

    @staticmethod
    def check_substance_pathways(
        substance: str,
        active_meds: list[str],
    ) -> list[PathwayStack]:
        """Check a substance's pathway effects against active medications.

        Returns list of PathwayStack for each pathway where multiple
        substances have overlapping effects.
        """
        profile = _get_pathway_profile(substance)
        if not profile or not profile.pathways:
            return []

        # Collect all pathway effects across active meds
        all_effects: dict[str, list[tuple[str, str]]] = {}
        sub_canonical = _resolve_substance(substance)

        for pathway, effect in profile.pathways.items():
            all_effects.setdefault(pathway, []).append((substance, effect))

        for med in active_meds:
            med_canonical = _resolve_substance(med)
            if med_canonical == sub_canonical:
                continue
            med_profile = _get_pathway_profile(med)
            if not med_profile:
                continue
            for pathway, effect in med_profile.pathways.items():
                if pathway in profile.pathways:
                    all_effects.setdefault(pathway, []).append((med, effect))

        stacks: list[PathwayStack] = []
        for pathway, entries in all_effects.items():
            if len(entries) < 2:
                continue

            substances = tuple(e[0] for e in entries)
            effects = tuple(e[1] for e in entries)

            # Check if effects are synergistic (both increase/both decrease)
            _synergistic = ("increase", "agonist", "potentiate", "activate")
            increasing = sum(1 for e in effects if e in _synergistic)
            decreasing = sum(1 for e in effects if e in ("decrease", "inhibit", "suppress"))

            if increasing >= 2 or decreasing >= 2:
                severity = (
                    "major" if pathway in _HIGH_RISK_PATHWAYS
                    else "moderate"
                )
                direction = "increase" if increasing >= 2 else "decrease"
                mechanism = (
                    f"Multiple substances {direction} {pathway}: "
                    + ", ".join(f"{s} ({e})" for s, e in entries)
                )
                recommendation = _build_pathway_recommendation(
                    pathway, substances, direction,
                )
                stacks.append(PathwayStack(
                    pathway=pathway,
                    substances=substances,
                    effects=effects,
                    severity=severity,
                    mechanism=mechanism,
                    recommendation=recommendation,
                ))

        return stacks


def _build_cyp_mechanism(
    sub_a: str, sub_b: str, enzyme: str, role_a: str, role_b: str,
) -> str:
    """Build a human-readable CYP conflict mechanism description."""
    if role_a == "inducer" and role_b == "substrate":
        return (
            f"{sub_a} induces {enzyme}, which metabolizes {sub_b}. "
            f"This may reduce {sub_b} blood levels and efficacy."
        )
    if role_a == "inhibitor" and role_b == "substrate":
        return (
            f"{sub_a} inhibits {enzyme}, which metabolizes {sub_b}. "
            f"This may increase {sub_b} blood levels and risk of side effects."
        )
    if role_a == "substrate" and role_b == "inducer":
        return (
            f"{sub_b} induces {enzyme}, which metabolizes {sub_a}. "
            f"This may reduce {sub_a} blood levels and efficacy."
        )
    if role_a == "substrate" and role_b == "inhibitor":
        return (
            f"{sub_b} inhibits {enzyme}, which metabolizes {sub_a}. "
            f"This may increase {sub_a} blood levels and risk of side effects."
        )
    return (
        f"{sub_a} ({role_a}) and {sub_b} ({role_b}) both interact "
        f"with {enzyme}."
    )


def _build_cyp_recommendation(
    sub_a: str, sub_b: str, enzyme: str, role_a: str, role_b: str,
) -> str:
    """Build a recommendation for a CYP conflict."""
    if "inducer" in (role_a, role_b) and "substrate" in (role_a, role_b):
        substrate = sub_a if role_a == "substrate" else sub_b
        return (
            f"Monitor {substrate} effectiveness. Dose adjustment may be needed. "
            f"Consider alternatives not dependent on {enzyme}."
        )
    if "inhibitor" in (role_a, role_b) and "substrate" in (role_a, role_b):
        substrate = sub_a if role_a == "substrate" else sub_b
        return (
            f"Monitor for {substrate} side effects. Lower dose may be needed. "
            f"Consider alternatives not dependent on {enzyme}."
        )
    return f"Monitor for interaction effects via {enzyme}."


def _build_pathway_recommendation(
    pathway: str,
    substances: tuple[str, ...],
    direction: str,
) -> str:
    """Build a recommendation for pathway stacking."""
    sub_list = ", ".join(substances)
    if pathway == "serotonin" and direction == "increase":
        return (
            f"Serotonin stacking risk with {sub_list}. "
            "Monitor for serotonin syndrome symptoms (agitation, tremor, "
            "hyperthermia). Do not add without medical supervision."
        )
    if pathway == "dopamine" and direction == "increase":
        return (
            f"Dopamine stacking with {sub_list}. "
            "Monitor for overstimulation, insomnia, anxiety. "
            "Consider spacing doses or reducing one substance."
        )
    if pathway in ("insulin", "GLP-1"):
        return (
            f"Multiple substances affecting {pathway} ({sub_list}). "
            "Monitor blood glucose closely for hypoglycemia risk."
        )
    return (
        f"Multiple substances affect {pathway} ({sub_list}). "
        "Monitor for additive/synergistic effects."
    )
