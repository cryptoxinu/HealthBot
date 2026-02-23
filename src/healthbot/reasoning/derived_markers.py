"""Derived/calculated lab markers.

Pure math with clinical value. Computes markers that labs don't always
report but are clinically meaningful. All logic is deterministic — no LLM.

Follows the pattern from delta.py (takes HealthDB, queries observations,
returns dataclasses).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass
class DerivedMarker:
    """A single derived/calculated marker."""

    name: str
    value: float
    unit: str
    interpretation: str       # "normal", "borderline", "elevated", "high", "low"
    clinical_note: str
    components: dict[str, float] = field(default_factory=dict)  # input values used


@dataclass
class DerivedMarkerReport:
    """All derived markers computed from current labs."""

    markers: list[DerivedMarker] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)  # markers that couldn't be computed


class DerivedMarkerEngine:
    """Compute clinically useful derived markers from raw lab values."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def compute_all(self, user_id: int | None = None) -> DerivedMarkerReport:
        """Compute all possible derived markers from most recent labs."""
        self._current_user_id = user_id
        labs = self._get_latest_labs(user_id)
        if not labs:
            return DerivedMarkerReport()

        report = DerivedMarkerReport()

        calculators = [
            self._homa_ir,
            self._tg_hdl_ratio,
            self._bun_creatinine_ratio,
            self._nlr,
            self._anion_gap,
            self._non_hdl_cholesterol,
            self._apob_apoa1_ratio,
            self._egfr_ckd_epi,
        ]

        for calc in calculators:
            try:
                result = calc(labs)
                if result is not None:
                    report.markers.append(result)
                else:
                    report.missing.append(calc.__name__.lstrip("_"))
            except Exception as e:
                logger.debug("Derived marker %s failed: %s", calc.__name__, e)
                report.missing.append(calc.__name__.lstrip("_"))

        return report

    def _get_latest_labs(self, user_id: int | None = None) -> dict[str, float]:
        """Get most recent value for each canonical lab name."""
        obs = self._db.query_observations(
            record_type="lab_result", limit=200, user_id=user_id,
        )
        if not obs:
            return {}

        latest: dict[str, float] = {}
        seen_dates: dict[str, str] = {}
        for o in obs:
            name = (o.get("canonical_name") or "").lower()
            if not name:
                continue
            val = o.get("value")
            if val is None:
                continue
            try:
                fval = float(val)
            except (ValueError, TypeError):
                continue
            date_eff = o.get("date_effective", "") or o.get(
                "_meta", {},
            ).get("date_effective", "")
            # Keep most recent by date (observations come sorted)
            if name not in latest or date_eff > seen_dates.get(name, ""):
                latest[name] = fval
                seen_dates[name] = date_eff

        return latest

    def _homa_ir(self, labs: dict[str, float]) -> DerivedMarker | None:
        """HOMA-IR = (fasting glucose mg/dL * fasting insulin uIU/mL) / 405.

        Assesses insulin resistance. <1.0 optimal, 1.0-1.9 early IR,
        2.0-2.9 significant IR, >=3.0 severe IR.
        """
        glucose = labs.get("glucose")
        insulin = labs.get("insulin")
        if glucose is None or insulin is None:
            return None
        if insulin <= 0 or glucose <= 0:
            return None

        homa = (glucose * insulin) / 405.0
        homa = round(homa, 2)

        if homa < 1.0:
            interp, note = "normal", "Optimal insulin sensitivity"
        elif homa < 2.0:
            interp = "borderline"
            note = "Early insulin resistance — consider lifestyle modifications"
        elif homa < 3.0:
            interp = "elevated"
            note = "Significant insulin resistance — monitor glucose trends closely"
        else:
            interp = "high"
            note = "Severe insulin resistance — evaluate for metabolic syndrome"

        return DerivedMarker(
            name="HOMA-IR",
            value=homa,
            unit="index",
            interpretation=interp,
            clinical_note=note,
            components={"glucose": glucose, "insulin": insulin},
        )

    def _tg_hdl_ratio(self, labs: dict[str, float]) -> DerivedMarker | None:
        """TG/HDL ratio — surrogate for insulin resistance and small dense LDL.

        <2.0 ideal, 2.0-3.0 borderline, >3.0 elevated atherogenic risk.
        """
        tg = labs.get("triglycerides")
        hdl = labs.get("hdl")
        if tg is None or hdl is None or hdl <= 0:
            return None

        ratio = round(tg / hdl, 2)

        if ratio < 2.0:
            interp = "normal"
            note = "Low atherogenic risk, likely large buoyant LDL pattern"
        elif ratio < 3.0:
            interp = "borderline"
            note = "Moderate risk — consider advanced lipid testing"
        else:
            interp = "elevated"
            note = "Elevated atherogenic risk — likely small dense LDL (Pattern B)"

        return DerivedMarker(
            name="TG/HDL Ratio",
            value=ratio,
            unit="ratio",
            interpretation=interp,
            clinical_note=note,
            components={"triglycerides": tg, "hdl": hdl},
        )

    def _bun_creatinine_ratio(self, labs: dict[str, float]) -> DerivedMarker | None:
        """BUN/Creatinine ratio — differentiates prerenal from intrinsic renal causes.

        Normal 10-20:1, >20 suggests prerenal (dehydration, GI bleed, high protein),
        <10 suggests liver disease or malnutrition.
        """
        bun = labs.get("bun")
        creat = labs.get("creatinine")
        if bun is None or creat is None or creat <= 0:
            return None

        ratio = round(bun / creat, 1)

        if ratio < 10:
            interp = "low"
            note = "Low BUN/Cr — consider liver disease or low protein intake"
        elif ratio <= 20:
            interp, note = "normal", "Normal BUN/Creatinine ratio"
        else:
            interp = "elevated"
            note = "Elevated BUN/Cr — consider dehydration, GI bleed, or high protein"

        return DerivedMarker(
            name="BUN/Creatinine Ratio",
            value=ratio,
            unit="ratio",
            interpretation=interp,
            clinical_note=note,
            components={"bun": bun, "creatinine": creat},
        )

    def _nlr(self, labs: dict[str, float]) -> DerivedMarker | None:
        """Neutrophil-to-Lymphocyte Ratio — marker of systemic inflammation.

        <3.0 normal, 3.0-6.0 mild stress/inflammation, >6.0 significant.
        """
        neut = labs.get("neutrophils") or labs.get("neutrophils_abs")
        lymph = labs.get("lymphocytes") or labs.get("lymphocytes_abs")
        if neut is None or lymph is None or lymph <= 0:
            return None

        ratio = round(neut / lymph, 2)

        if ratio < 3.0:
            interp, note = "normal", "Normal inflammatory balance"
        elif ratio < 6.0:
            interp = "borderline"
            note = "Mild systemic inflammation or physiological stress"
        else:
            interp = "elevated"
            note = "Significant systemic inflammation — evaluate for infection"

        return DerivedMarker(
            name="NLR",
            value=ratio,
            unit="ratio",
            interpretation=interp,
            clinical_note=note,
            components={"neutrophils": neut, "lymphocytes": lymph},
        )

    def _anion_gap(self, labs: dict[str, float]) -> DerivedMarker | None:
        """Anion Gap = Na - (Cl + HCO3).

        Normal 8-12 mEq/L (without K+). >12 suggests metabolic acidosis
        (DKA, lactic acidosis, toxins).
        """
        na = labs.get("sodium")
        cl = labs.get("chloride")
        hco3 = labs.get("carbon_dioxide")
        if na is None or cl is None or hco3 is None:
            return None

        gap = round(na - (cl + hco3), 1)

        if gap < 8:
            interp = "low"
            note = "Low anion gap — consider hypoalbuminemia or lab error"
        elif gap <= 12:
            interp, note = "normal", "Normal anion gap"
        else:
            interp = "elevated"
            note = "Elevated anion gap — evaluate for metabolic acidosis (DKA, lactic acidosis)"

        return DerivedMarker(
            name="Anion Gap",
            value=gap,
            unit="mEq/L",
            interpretation=interp,
            clinical_note=note,
            components={"sodium": na, "chloride": cl, "carbon_dioxide": hco3},
        )

    def _non_hdl_cholesterol(self, labs: dict[str, float]) -> DerivedMarker | None:
        """Non-HDL Cholesterol = Total Cholesterol - HDL.

        Captures all atherogenic particles (LDL + VLDL + IDL + Lp(a)).
        <130 optimal, 130-159 near optimal, 160-189 borderline, >=190 high.
        """
        tc = labs.get("cholesterol_total")
        hdl = labs.get("hdl")
        if tc is None or hdl is None:
            return None

        non_hdl = round(tc - hdl, 1)

        if non_hdl < 130:
            interp = "normal"
            note = "Optimal non-HDL cholesterol — low atherogenic burden"
        elif non_hdl < 160:
            interp = "borderline"
            note = "Near optimal — consider lifestyle optimization"
        elif non_hdl < 190:
            interp = "elevated"
            note = "Borderline high non-HDL — evaluate cardiovascular risk"
        else:
            interp = "high"
            note = "High non-HDL cholesterol — significant atherogenic burden"

        return DerivedMarker(
            name="Non-HDL Cholesterol",
            value=non_hdl,
            unit="mg/dL",
            interpretation=interp,
            clinical_note=note,
            components={"cholesterol_total": tc, "hdl": hdl},
        )

    def _apob_apoa1_ratio(self, labs: dict[str, float]) -> DerivedMarker | None:
        """ApoB/ApoA1 ratio — atherogenic vs. anti-atherogenic particle balance.

        <0.7 low risk, 0.7-0.9 moderate, >0.9 high cardiovascular risk.
        """
        apob = labs.get("apob")
        apoa1 = labs.get("apoa1")
        if apob is None or apoa1 is None or apoa1 <= 0:
            return None

        ratio = round(apob / apoa1, 2)

        if ratio < 0.7:
            interp, note = "normal", "Favorable atherogenic balance"
        elif ratio < 0.9:
            interp, note = "borderline", "Moderate cardiovascular risk from particle imbalance"
        else:
            interp, note = "elevated", "High ApoB/ApoA1 ratio — unfavorable atherogenic balance"

        return DerivedMarker(
            name="ApoB/ApoA1 Ratio",
            value=ratio,
            unit="ratio",
            interpretation=interp,
            clinical_note=note,
            components={"apob": apob, "apoa1": apoa1},
        )

    def _egfr_ckd_epi(self, labs: dict[str, float]) -> DerivedMarker | None:
        """eGFR via CKD-EPI 2021 (race-free equation).

        Uses creatinine, age, and sex. Returns None if demographics unavailable
        (retrieved from DB via user profile).

        2021 equation: eGFR = 142 * min(Scr/k, 1)^a * max(Scr/k, 1)^-1.200
                              * 0.9938^Age (* 1.012 if female)
        """
        creat = labs.get("creatinine")
        if creat is None or creat <= 0:
            return None

        # Need demographics from LTM
        try:
            demo = self._db.get_user_demographics(self._current_user_id or 0)
        except Exception:
            return None
        age = demo.get("age")
        sex = demo.get("sex")
        if age is None or sex is None:
            return None

        is_female = sex.lower() in ("female", "f")
        kappa = 0.7 if is_female else 0.9
        alpha = -0.241 if is_female else -0.302

        scr_k = creat / kappa
        term1 = min(scr_k, 1.0) ** alpha
        term2 = max(scr_k, 1.0) ** -1.200
        egfr = 142 * term1 * term2 * (0.9938 ** age)
        if is_female:
            egfr *= 1.012

        egfr = round(egfr, 1)

        if egfr >= 90:
            interp, note = "normal", "Normal kidney function (G1)"
        elif egfr >= 60:
            interp = "borderline"
            note = "Mildly decreased kidney function (G2) — monitor annually"
        elif egfr >= 45:
            interp = "elevated"
            note = "Mild-to-moderate decrease (G3a) — nephrology referral"
        elif egfr >= 30:
            interp = "elevated"
            note = "Moderate-to-severe decrease (G3b) — nephrology evaluation"
        else:
            interp = "high"
            note = "Severely decreased kidney function (G4-G5) — urgent referral"

        return DerivedMarker(
            name="eGFR (CKD-EPI 2021)",
            value=egfr,
            unit="mL/min/1.73m2",
            interpretation=interp,
            clinical_note=note,
            components={"creatinine": creat, "age": float(age)},
        )

    def format_report(self, report: DerivedMarkerReport) -> str:
        """Format derived markers for display."""
        if not report.markers:
            return "No derived markers could be computed (insufficient lab data)."

        lines = ["DERIVED MARKERS", "-" * 40]

        icons = {
            "normal": "=",
            "borderline": "~",
            "elevated": "!",
            "high": "!!",
            "low": "v",
        }

        for m in report.markers:
            icon = icons.get(m.interpretation, "?")
            lines.append(f"  [{icon}] {m.name}: {m.value} {m.unit} ({m.interpretation})")
            lines.append(f"      {m.clinical_note}")
            components = ", ".join(f"{k}={v}" for k, v in m.components.items())
            lines.append(f"      From: {components}")
            lines.append("")

        if report.missing:
            lines.append(f"  Could not compute: {', '.join(report.missing)}")

        return "\n".join(lines)
