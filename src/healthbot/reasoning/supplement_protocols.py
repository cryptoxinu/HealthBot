"""Evidence-based supplement dosing protocols.

When a deficiency is detected, provides specific dosing guidance:
loading dose, maintenance dose, retest timeline, and interactions.

All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class SupplementProtocol:
    """Evidence-based dosing protocol for a deficiency."""

    deficiency_marker: str        # canonical lab name
    marker_display: str           # human-readable name
    threshold_deficient: float    # below this = deficient
    threshold_insufficient: float  # below this = insufficient
    unit: str                     # lab value unit
    supplement_name: str
    loading_dose: str
    loading_weeks: int
    maintenance_dose: str
    retest_weeks: int
    notes: str
    interactions: list[str]       # drug interactions to warn about
    citation: str


SUPPLEMENT_PROTOCOLS: tuple[SupplementProtocol, ...] = (
    SupplementProtocol(
        deficiency_marker="vitamin_d",
        marker_display="Vitamin D (25-OH)",
        threshold_deficient=20.0,
        threshold_insufficient=30.0,
        unit="ng/mL",
        supplement_name="Vitamin D3 (cholecalciferol)",
        loading_dose="5,000 IU daily",
        loading_weeks=8,
        maintenance_dose="1,000-2,000 IU daily",
        retest_weeks=12,
        notes="Take with a fat-containing meal for best absorption. "
              "Consider adding vitamin K2 (100 mcg MK-7) for calcium metabolism.",
        interactions=["thiazide diuretics (hypercalcemia risk)"],
        citation="Holick MF et al. J Clin Endocrinol Metab. 2011;96(7):1911-1930.",
    ),
    SupplementProtocol(
        deficiency_marker="vitamin_b12",
        marker_display="Vitamin B12",
        threshold_deficient=200.0,
        threshold_insufficient=400.0,
        unit="pg/mL",
        supplement_name="Methylcobalamin (B12)",
        loading_dose="1,000 mcg daily",
        loading_weeks=8,
        maintenance_dose="500-1,000 mcg daily",
        retest_weeks=12,
        notes="Sublingual or oral. If on metformin, ongoing supplementation "
              "recommended. Methylcobalamin preferred over cyanocobalamin.",
        interactions=["metformin (depletes B12, supplement ongoing)"],
        citation="Devalia V et al. Br J Haematol. 2014;166(2):241-249.",
    ),
    SupplementProtocol(
        deficiency_marker="ferritin",
        marker_display="Ferritin",
        threshold_deficient=15.0,
        threshold_insufficient=30.0,
        unit="ng/mL",
        supplement_name="Ferrous bisglycinate (iron)",
        loading_dose="25-50 mg elemental iron every other day",
        loading_weeks=12,
        maintenance_dose="25 mg every other day (if ongoing risk)",
        retest_weeks=12,
        notes="Take on empty stomach with vitamin C (200 mg) for absorption. "
              "Every-other-day dosing improves absorption vs daily. "
              "Avoid with dairy, tea, coffee within 2 hours.",
        interactions=[
            "levothyroxine (separate by 4 hours)",
            "PPIs/antacids (reduce absorption)",
            "tetracycline antibiotics (separate by 2 hours)",
        ],
        citation="Stoffel NU et al. Blood. 2017;130(11):1336-1344.",
    ),
    SupplementProtocol(
        deficiency_marker="folate",
        marker_display="Folate",
        threshold_deficient=3.0,
        threshold_insufficient=5.0,
        unit="ng/mL",
        supplement_name="Methylfolate (5-MTHF)",
        loading_dose="1,000 mcg daily",
        loading_weeks=4,
        maintenance_dose="400-800 mcg daily",
        retest_weeks=8,
        notes="Methylfolate (5-MTHF) preferred over folic acid — bypasses "
              "MTHFR polymorphism. Always co-supplement B12 when treating folate "
              "deficiency to avoid masking B12 deficiency.",
        interactions=["methotrexate (antagonist — do not co-supplement without MD)"],
        citation="Bailey LB et al. Ann N Y Acad Sci. 2015;1352:54-71.",
    ),
    SupplementProtocol(
        deficiency_marker="magnesium",
        marker_display="Magnesium",
        threshold_deficient=1.5,
        threshold_insufficient=1.8,
        unit="mg/dL",
        supplement_name="Magnesium glycinate",
        loading_dose="400 mg daily (split AM/PM)",
        loading_weeks=4,
        maintenance_dose="200-400 mg daily",
        retest_weeks=8,
        notes="Glycinate form preferred for absorption and GI tolerance. "
              "Take evening dose — may improve sleep. "
              "Serum magnesium is a poor marker; RBC magnesium more accurate.",
        interactions=[
            "bisphosphonates (separate by 2 hours)",
            "antibiotics (fluoroquinolones, tetracyclines — separate by 2 hours)",
        ],
        citation="Schwalfenberg GK, Genuis SJ. Scientifica. 2017;2017:4179326.",
    ),
    SupplementProtocol(
        deficiency_marker="zinc",
        marker_display="Zinc",
        threshold_deficient=60.0,
        threshold_insufficient=70.0,
        unit="mcg/dL",
        supplement_name="Zinc picolinate",
        loading_dose="30 mg daily",
        loading_weeks=4,
        maintenance_dose="15 mg daily",
        retest_weeks=8,
        notes="Take with food to reduce nausea. "
              "If supplementing >25 mg zinc daily for >4 weeks, "
              "add copper (1-2 mg) to prevent copper depletion.",
        interactions=[
            "penicillamine (separate by 2 hours)",
            "tetracycline antibiotics (separate by 2 hours)",
        ],
        citation="Maret W, Sandstead HH. J Trace Elem Med Biol. 2006;20(1):3-18.",
    ),
)

# Index by marker for fast lookup
_PROTOCOL_INDEX: dict[str, SupplementProtocol] = {
    p.deficiency_marker: p for p in SUPPLEMENT_PROTOCOLS
}

# Keywords to match active supplements against protocols
_SUPPLEMENT_MATCH_KEYWORDS: dict[str, list[str]] = {
    "vitamin_d": ["vitamin d", "cholecalciferol", "ergocalciferol", "d3"],
    "vitamin_b12": ["b12", "cobalamin", "methylcobalamin", "cyanocobalamin"],
    "ferritin": ["iron", "ferrous", "ferric", "polysaccharide iron"],
    "folate": ["folate", "folic", "methylfolate", "5-mthf"],
    "magnesium": ["magnesium"],
    "zinc": ["zinc"],
}


@dataclass
class SupplementRecommendation:
    """A specific recommendation based on lab values."""

    protocol: SupplementProtocol
    severity: str          # "deficient" or "insufficient"
    current_value: float
    current_date: str
    recommended_dose: str  # loading or maintenance
    duration: str
    retest_in: str
    warnings: list[str]    # drug interaction warnings


class SupplementAdvisor:
    """Generate supplement recommendations from lab data."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def get_recommendations(self, user_id: int) -> list[SupplementRecommendation]:
        """Check latest labs against supplement protocols."""
        recs: list[SupplementRecommendation] = []
        active_meds = self._get_active_med_names(user_id)
        active_supps = self._get_active_supplement_names(user_id)

        for protocol in SUPPLEMENT_PROTOCOLS:
            rec = self._check_protocol(protocol, user_id, active_meds, active_supps)
            if rec:
                recs.append(rec)

        # Sort: deficient before insufficient
        recs.sort(key=lambda r: 0 if r.severity == "deficient" else 1)
        return recs

    def _check_protocol(
        self,
        protocol: SupplementProtocol,
        user_id: int,
        active_meds: list[str],
        active_supps: list[str],
    ) -> SupplementRecommendation | None:
        """Check if a protocol applies based on latest lab value."""
        rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=protocol.deficiency_marker,
            limit=1,
            user_id=user_id,
        )
        if not rows:
            return None

        row = rows[0]
        try:
            value = float(row.get("value", 0))
        except (ValueError, TypeError):
            return None

        dt = row.get("date_collected", "")

        # Check if already supplementing
        match_keywords = _SUPPLEMENT_MATCH_KEYWORDS.get(
            protocol.deficiency_marker, [],
        )
        for s in active_supps:
            s_lower = s.lower()
            if any(kw in s_lower for kw in match_keywords):
                return None

        if value < protocol.threshold_deficient:
            severity = "deficient"
            dose = protocol.loading_dose
            duration = f"{protocol.loading_weeks} weeks, then {protocol.maintenance_dose}"
        elif value < protocol.threshold_insufficient:
            severity = "insufficient"
            dose = protocol.maintenance_dose
            duration = "ongoing"
        else:
            return None

        # Check for drug interactions
        warnings: list[str] = []
        for interaction in protocol.interactions:
            # Extract drug name from interaction string
            drug_part = interaction.split("(")[0].strip().lower()
            for med in active_meds:
                if drug_part in med.lower() or med.lower() in drug_part:
                    warnings.append(interaction)
                    break

        return SupplementRecommendation(
            protocol=protocol,
            severity=severity,
            current_value=value,
            current_date=dt[:10] if dt else "",
            recommended_dose=dose,
            duration=duration,
            retest_in=f"{protocol.retest_weeks} weeks",
            warnings=warnings,
        )

    def _get_active_med_names(self, user_id: int) -> list[str]:
        """Get names of active medications."""
        try:
            meds = self._db.get_active_medications(user_id=user_id)
            return [m.get("name", "") for m in meds if m.get("name")]
        except Exception:
            return []

    def _get_active_supplement_names(self, user_id: int) -> list[str]:
        """Get names of active supplements (medications with supplement-like names)."""
        try:
            meds = self._db.get_active_medications(user_id=user_id)
            supps = []
            supp_keywords = {
                "vitamin", "iron", "ferrous", "folate", "folic",
                "magnesium", "zinc", "calcium", "b12", "d3",
                "methylcobalamin", "methylfolate", "cholecalciferol",
            }
            for m in meds:
                name = m.get("name", "").lower()
                if any(kw in name for kw in supp_keywords):
                    supps.append(m.get("name", ""))
            return supps
        except Exception:
            return []


def format_recommendations(recs: list[SupplementRecommendation]) -> str:
    """Format supplement recommendations for display."""
    if not recs:
        return (
            "No supplement recommendations. "
            "All checked markers are within normal range."
        )

    lines = ["SUPPLEMENT RECOMMENDATIONS", "-" * 30]

    for rec in recs:
        p = rec.protocol
        sev = "DEFICIENT" if rec.severity == "deficient" else "Insufficient"
        lines.append(
            f"\n{p.marker_display}: {rec.current_value} {p.unit} "
            f"({sev}, tested {rec.current_date})"
        )
        lines.append(f"  Supplement: {p.supplement_name}")
        lines.append(f"  Dose: {rec.recommended_dose}")
        lines.append(f"  Duration: {rec.duration}")
        lines.append(f"  Retest: {rec.retest_in}")

        if p.notes:
            lines.append(f"  Notes: {p.notes}")

        if rec.warnings:
            lines.append("  INTERACTIONS:")
            for w in rec.warnings:
                lines.append(f"    ! {w}")

        lines.append(f"  Ref: {p.citation}")

    return "\n".join(lines)
