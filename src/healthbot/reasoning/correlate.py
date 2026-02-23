"""Cross-source correlation analysis.

Finds correlations between lab results and wearable metrics.
Uses numpy Pearson correlation. No ML models.

Includes a static knowledge base of clinically meaningful lab-wearable
pairs and generates contextual alert messages when correlations are
detected in the user's data.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import numpy as np

from healthbot.data.db import HealthDB


@dataclass
class Correlation:
    metric_a: str
    metric_b: str
    pearson_r: float
    n_observations: int
    time_window_days: int
    interpretation: str
    p_value: float | None = None


# ---------------------------------------------------------------------------
# Clinical correlation knowledge base
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ClinicalCorrelationRule:
    """A known clinically meaningful lab-wearable correlation."""

    lab_metric: str          # canonical lab name
    wearable_metric: str     # wearable_daily column
    expected_direction: str  # "positive" or "negative"
    clinical_context: str    # Why this matters
    actionable_advice: str   # What the user should consider
    evidence: str            # Citation


CLINICAL_CORRELATION_KB: tuple[ClinicalCorrelationRule, ...] = (
    ClinicalCorrelationRule(
        lab_metric="cortisol",
        wearable_metric="hrv",
        expected_direction="negative",
        clinical_context=(
            "Elevated cortisol suppresses parasympathetic tone, lowering HRV"
        ),
        actionable_advice=(
            "Consider stress management: meditation, breathwork, or adaptogens"
        ),
        evidence="Thayer JF et al. Neurosci Biobehav Rev. 2012;36(2):747-756",
    ),
    ClinicalCorrelationRule(
        lab_metric="cortisol",
        wearable_metric="rhr",
        expected_direction="positive",
        clinical_context=(
            "Chronic stress elevates both cortisol and resting heart rate"
        ),
        actionable_advice=(
            "Persistent elevation suggests chronic stress — prioritize recovery"
        ),
        evidence="Chandola T et al. Eur Heart J. 2010;31(14):1737-1744",
    ),
    ClinicalCorrelationRule(
        lab_metric="cortisol",
        wearable_metric="sleep_score",
        expected_direction="negative",
        clinical_context=(
            "High cortisol disrupts sleep architecture, especially deep sleep"
        ),
        actionable_advice=(
            "Evening cortisol reduction: dim lights, avoid screens, magnesium"
        ),
        evidence="Bush B, Hudson T. Integr Med. 2010;9(6):46-53",
    ),
    ClinicalCorrelationRule(
        lab_metric="tsh",
        wearable_metric="rhr",
        expected_direction="negative",
        clinical_context=(
            "High TSH (hypothyroidism) often lowers RHR; low TSH raises it"
        ),
        actionable_advice=(
            "If TSH is trending, discuss thyroid optimization with your doctor"
        ),
        evidence="Klein I, Danzi S. Circulation. 2007;116(15):1725-1735",
    ),
    ClinicalCorrelationRule(
        lab_metric="tsh",
        wearable_metric="recovery_score",
        expected_direction="negative",
        clinical_context=(
            "Hypothyroidism impairs recovery capacity and exercise tolerance"
        ),
        actionable_advice=(
            "Optimizing TSH to 1-2 mIU/L may improve recovery metrics"
        ),
        evidence="Lankhaar JA et al. Int J Sports Med. 2014;35(9):782-787",
    ),
    ClinicalCorrelationRule(
        lab_metric="glucose",
        wearable_metric="sleep_score",
        expected_direction="negative",
        clinical_context=(
            "Poor sleep impairs glucose regulation; high glucose disrupts sleep"
        ),
        actionable_advice=(
            "Improve sleep hygiene and avoid high-glycemic meals before bed"
        ),
        evidence="Spiegel K et al. Lancet. 1999;354(9188):1435-1439",
    ),
    ClinicalCorrelationRule(
        lab_metric="glucose",
        wearable_metric="hrv",
        expected_direction="negative",
        clinical_context=(
            "Elevated glucose is associated with autonomic dysfunction "
            "reflected in lower HRV"
        ),
        actionable_advice=(
            "Regular exercise and stable blood sugar improve autonomic tone"
        ),
        evidence="Schroeder EB et al. Diabetes Care. 2005;28(3):668-674",
    ),
    ClinicalCorrelationRule(
        lab_metric="hba1c",
        wearable_metric="hrv",
        expected_direction="negative",
        clinical_context=(
            "Higher HbA1c reflects chronic hyperglycemia which damages "
            "autonomic nerves, reducing HRV"
        ),
        actionable_advice=(
            "Improving glycemic control typically restores HRV over months"
        ),
        evidence="Benichou T et al. PLoS One. 2018;13(4):e0195372",
    ),
    ClinicalCorrelationRule(
        lab_metric="ferritin",
        wearable_metric="recovery_score",
        expected_direction="positive",
        clinical_context=(
            "Low ferritin impairs oxygen delivery and exercise recovery"
        ),
        actionable_advice=(
            "If ferritin <30 ng/mL, iron supplementation may boost recovery"
        ),
        evidence="Burden RJ et al. Nutrients. 2015;7(12):10427-10447",
    ),
    ClinicalCorrelationRule(
        lab_metric="ferritin",
        wearable_metric="rhr",
        expected_direction="negative",
        clinical_context=(
            "Iron deficiency increases RHR to compensate for reduced "
            "oxygen-carrying capacity"
        ),
        actionable_advice=(
            "Watch for elevated RHR with declining ferritin — early anemia sign"
        ),
        evidence="Soppi ET. Clin Case Rep. 2018;6(7):1082-1086",
    ),
    ClinicalCorrelationRule(
        lab_metric="vitamin_d",
        wearable_metric="sleep_score",
        expected_direction="positive",
        clinical_context=(
            "Vitamin D deficiency is associated with poor sleep quality "
            "and shorter duration"
        ),
        actionable_advice=(
            "Supplement vitamin D if <30 ng/mL — may improve sleep quality"
        ),
        evidence="Gao Q et al. Nutrients. 2018;10(10):1395",
    ),
    ClinicalCorrelationRule(
        lab_metric="vitamin_d",
        wearable_metric="recovery_score",
        expected_direction="positive",
        clinical_context=(
            "Vitamin D supports immune function and muscle recovery"
        ),
        actionable_advice=(
            "Target 40-60 ng/mL for optimal athletic recovery"
        ),
        evidence="Owens DJ et al. Eur J Sport Sci. 2015;15(7):577-591",
    ),
    ClinicalCorrelationRule(
        lab_metric="crp",
        wearable_metric="hrv",
        expected_direction="negative",
        clinical_context=(
            "Systemic inflammation (elevated CRP) reduces vagal tone "
            "and suppresses HRV"
        ),
        actionable_advice=(
            "Anti-inflammatory strategies: omega-3, turmeric, "
            "stress reduction, sleep"
        ),
        evidence="Haensel A et al. Brain Behav Immun. 2008;22(8):1218-1228",
    ),
    ClinicalCorrelationRule(
        lab_metric="crp",
        wearable_metric="recovery_score",
        expected_direction="negative",
        clinical_context=(
            "Active inflammation impairs recovery and exercise adaptation"
        ),
        actionable_advice=(
            "Reduce training intensity during inflammatory flares"
        ),
        evidence="Kasapis C, Thompson PD. J Am Coll Cardiol. 2005;45(10):1563-1569",
    ),
    ClinicalCorrelationRule(
        lab_metric="testosterone",
        wearable_metric="recovery_score",
        expected_direction="positive",
        clinical_context=(
            "Higher testosterone supports muscle repair and recovery capacity"
        ),
        actionable_advice=(
            "Sleep, resistance training, and zinc support healthy testosterone"
        ),
        evidence="Dattilo M et al. Med Hypotheses. 2011;77(2):220-222",
    ),
    ClinicalCorrelationRule(
        lab_metric="testosterone",
        wearable_metric="sleep_score",
        expected_direction="positive",
        clinical_context=(
            "Sleep deprivation significantly reduces testosterone production"
        ),
        actionable_advice=(
            "7-9 hours of quality sleep is critical for testosterone levels"
        ),
        evidence="Leproult R, Van Cauter E. JAMA. 2011;305(21):2173-2174",
    ),
    ClinicalCorrelationRule(
        lab_metric="magnesium",
        wearable_metric="hrv",
        expected_direction="positive",
        clinical_context=(
            "Magnesium supports parasympathetic nervous system function "
            "and HRV"
        ),
        actionable_advice=(
            "Magnesium glycinate 200-400mg before bed may improve HRV"
        ),
        evidence="Dibaba DT et al. Eur J Clin Nutr. 2017;71(8):1009-1014",
    ),
    ClinicalCorrelationRule(
        lab_metric="magnesium",
        wearable_metric="sleep_score",
        expected_direction="positive",
        clinical_context=(
            "Magnesium deficiency impairs GABA activity, worsening sleep"
        ),
        actionable_advice=(
            "Supplementing magnesium may improve sleep quality and duration"
        ),
        evidence="Abbasi B et al. J Res Med Sci. 2012;17(12):1161-1169",
    ),
)


@dataclass
class CorrelationAlert:
    """An alert generated when a known clinical correlation is found."""

    lab_metric: str
    wearable_metric: str
    pearson_r: float
    clinical_context: str
    actionable_advice: str
    evidence: str
    n_observations: int


class CorrelationEngine:
    """Find correlations between health metrics."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def correlate_lab_wearable(
        self, lab_name: str, wearable_metric: str, days: int = 90,
        user_id: int | None = None,
    ) -> Correlation | None:
        """Compute Pearson correlation between a lab test and wearable metric.

        Aligns by date (daily granularity). Requires at least 5 overlapping points.
        """
        cutoff = (datetime.now(UTC) - timedelta(days=days)).strftime("%Y-%m-%d")

        # Get lab values
        lab_rows = self._db.query_observations(
            record_type="lab_result",
            canonical_name=lab_name,
            start_date=cutoff,
            limit=365,
            user_id=user_id,
        )
        lab_by_date: dict[str, float] = {}
        for row in lab_rows:
            d = row.get("date_collected") or row.get("_meta", {}).get("date_effective", "")
            v = row.get("value")
            if d and v is not None:
                try:
                    lab_by_date[d[:10]] = float(v)
                except (ValueError, TypeError):
                    continue

        # Get wearable values
        wearable_rows = self._db.query_wearable_daily(
            start_date=cutoff, limit=365, user_id=user_id,
        )
        wearable_by_date: dict[str, float] = {}
        for row in wearable_rows:
            d = row.get("_date", "")
            v = row.get(wearable_metric)
            if d and v is not None:
                try:
                    wearable_by_date[d[:10]] = float(v)
                except (ValueError, TypeError):
                    continue

        # Find overlapping dates
        common_dates = sorted(set(lab_by_date.keys()) & set(wearable_by_date.keys()))
        if len(common_dates) < 5:
            return None

        x = np.array([lab_by_date[d] for d in common_dates])
        y = np.array([wearable_by_date[d] for d in common_dates])

        # Pearson correlation
        if np.std(x) == 0 or np.std(y) == 0:
            return None

        r = float(np.corrcoef(x, y)[0, 1])

        # Compute p-value (scipy if available, else None)
        p_value: float | None = None
        try:
            from scipy.stats import pearsonr as _pearsonr

            _, p_value = _pearsonr(x, y)
        except ImportError:
            pass

        # Interpret
        abs_r = abs(r)
        if abs_r >= 0.7:
            strength = "strong"
        elif abs_r >= 0.4:
            strength = "moderate"
        elif abs_r >= 0.2:
            strength = "weak"
        else:
            strength = "negligible"
        direction = "positive" if r > 0 else "negative"
        interpretation = f"{strength} {direction} correlation"

        return Correlation(
            metric_a=lab_name,
            metric_b=wearable_metric,
            pearson_r=round(r, 3),
            n_observations=len(common_dates),
            time_window_days=days,
            interpretation=interpretation,
            p_value=p_value,
        )

    def auto_discover(
        self, days: int = 90, user_id: int | None = None,
    ) -> list[Correlation]:
        """Automatically find interesting correlations.

        Tests lab metrics against wearable metrics.
        Returns correlations with |r| > 0.3.
        """
        wearable_metrics = [
            "hrv", "rhr", "recovery_score", "sleep_score",
            "strain", "sleep_duration_min", "spo2", "skin_temp",
        ]

        # Get lab test names that have data
        sql = (
            "SELECT DISTINCT canonical_name FROM observations "
            "WHERE record_type = 'lab_result' AND canonical_name != ''"
        )
        params: list = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        sql += " LIMIT 100"
        rows = self._db.conn.execute(sql, params).fetchall()
        lab_names = [r["canonical_name"] for r in rows]

        results: list[Correlation] = []
        for lab in lab_names:
            for wm in wearable_metrics:
                corr = self.correlate_lab_wearable(lab, wm, days, user_id=user_id)
                if corr and abs(corr.pearson_r) > 0.3:
                    results.append(corr)

        results.sort(key=lambda c: abs(c.pearson_r), reverse=True)
        return results

    def discover_and_store(
        self,
        user_id: int | None = None,
        days: int = 90,
        min_abs_r: float = 0.5,
        min_n: int = 7,
    ) -> list[Correlation]:
        """Discover statistically significant correlations and store in KB.

        Uses scipy pearsonr for p-value. Only keeps |r| >= min_abs_r
        and p < 0.05. Deduplicates against existing KB entries.

        Returns list of newly stored correlations.
        """
        import logging as _logging

        _logger = _logging.getLogger("healthbot")

        all_corrs = self.auto_discover(days=days, user_id=user_id)

        significant: list[Correlation] = []
        for c in all_corrs:
            if abs(c.pearson_r) < min_abs_r:
                continue
            if c.n_observations < min_n:
                continue
            if c.p_value is not None and c.p_value >= 0.05:
                continue
            significant.append(c)

        if not significant:
            return []

        # Store in KB with dedup
        stored: list[Correlation] = []
        try:
            from healthbot.research.knowledge_base import KnowledgeBase

            kb = KnowledgeBase(self._db)
            for c in significant:
                topic = f"correlation:{c.metric_a}:{c.metric_b}"
                finding = (
                    f"{c.metric_a} <-> {c.metric_b}: r={c.pearson_r:+.3f}, "
                    f"p={c.p_value:.4f}, n={c.n_observations} "
                    f"({c.interpretation})"
                )
                kb_id = kb.store_finding(
                    topic=topic,
                    finding=finding,
                    source="auto_correlation",
                    relevance_score=abs(c.pearson_r),
                )
                if kb_id:
                    stored.append(c)
        except Exception as e:
            _logger.debug("Correlation KB storage failed: %s", e)

        return stored

    def generate_correlation_alerts(
        self, days: int = 90, user_id: int | None = None,
        min_r: float = 0.35,
    ) -> list[CorrelationAlert]:
        """Check user data for known clinically meaningful correlations.

        Only returns alerts where the detected correlation direction
        matches what is expected clinically and |r| >= min_r.
        """
        alerts: list[CorrelationAlert] = []
        for rule in CLINICAL_CORRELATION_KB:
            corr = self.correlate_lab_wearable(
                rule.lab_metric, rule.wearable_metric, days, user_id=user_id,
            )
            if corr is None:
                continue
            if abs(corr.pearson_r) < min_r:
                continue

            # Check direction matches expectation
            detected_direction = "positive" if corr.pearson_r > 0 else "negative"
            if detected_direction != rule.expected_direction:
                continue

            alerts.append(CorrelationAlert(
                lab_metric=rule.lab_metric,
                wearable_metric=rule.wearable_metric,
                pearson_r=corr.pearson_r,
                clinical_context=rule.clinical_context,
                actionable_advice=rule.actionable_advice,
                evidence=rule.evidence,
                n_observations=corr.n_observations,
            ))

        alerts.sort(key=lambda a: abs(a.pearson_r), reverse=True)
        return alerts

    def format_correlations(self, correlations: list[Correlation]) -> str:
        """Format correlation findings for display."""
        if not correlations:
            return "No significant correlations found with available data."

        lines = ["CORRELATIONS (labs <-> wearables)", "-" * 40]
        for c in correlations:
            lines.append(
                f"{c.metric_a} <-> {c.metric_b}: r={c.pearson_r:+.3f} "
                f"({c.interpretation}, n={c.n_observations})"
            )
        lines.append("")
        lines.append("Note: Correlation does not imply causation.")
        return "\n".join(lines)


def format_correlation_alerts(alerts: list[CorrelationAlert]) -> str:
    """Format correlation alerts for display."""
    if not alerts:
        return "No clinically significant lab-wearable correlations detected."

    lines = ["LAB-WEARABLE CORRELATION INSIGHTS", "-" * 40]
    for a in alerts:
        lines.append(
            f"\n{a.lab_metric.upper()} <-> {a.wearable_metric.upper()} "
            f"(r={a.pearson_r:+.3f}, n={a.n_observations})"
        )
        lines.append(f"  Why: {a.clinical_context}")
        lines.append(f"  Action: {a.actionable_advice}")
        lines.append(f"  Ref: {a.evidence}")
    return "\n".join(lines)
