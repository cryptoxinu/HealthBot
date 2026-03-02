"""Built-in skill adapters wrapping existing reasoning modules.

Each skill is a thin adapter — all logic stays in reasoning/.
"""
from __future__ import annotations

from healthbot.skills.base import (
    HealthContext,
    SkillResult,
    ToolPolicy,
)


class TrendAnalysisSkill:
    """Analyze lab result trends over time."""

    name = "trend_analysis"
    description = "Detect rising/falling trends in lab results with slope analysis."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.trends import TrendAnalyzer

        engine = TrendAnalyzer(ctx.db)
        trends = engine.detect_all_trends(user_id=ctx.user_id)
        if not trends:
            return SkillResult(
                skill_name=self.name,
                summary="No significant lab trends detected.",
                policy=ToolPolicy.MEDIUM,
            )
        details = []
        for t in trends[:10]:
            details.append(
                f"{t.canonical_name}: {t.direction} "
                f"({t.pct_change:+.1f}%, {t.data_points} points)"
            )
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(trends)} lab trends detected.",
            details=details,
            policy=ToolPolicy.MEDIUM,
            changed=True,
        )


class InteractionCheckSkill:
    """Check drug-drug, drug-lab, and drug-condition interactions."""

    name = "interaction_check"
    description = "Detect medication interactions (drug-drug, drug-lab, drug-condition)."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.interactions import InteractionChecker

        checker = InteractionChecker(ctx.db)
        drug_drug = checker.check_all(user_id=ctx.user_id)
        drug_lab = checker.check_drug_lab(user_id=ctx.user_id)
        if not drug_drug and not drug_lab:
            return SkillResult(
                skill_name=self.name,
                summary="No medication interactions detected.",
                policy=ToolPolicy.HIGH,
            )
        details = []
        for r in drug_drug[:5]:
            details.append(
                f"{r.med_a_name} + {r.med_b_name}: "
                f"{r.interaction.severity} — {r.interaction.recommendation}"
            )
        for r in drug_lab[:5]:
            details.append(
                f"{r.med_name} affects {r.lab_name}: "
                f"{r.interaction.effect} — {r.interaction.monitor}"
            )
        total = len(drug_drug) + len(drug_lab)
        return SkillResult(
            skill_name=self.name,
            summary=f"{total} interactions found.",
            details=details,
            policy=ToolPolicy.HIGH,
            changed=True,
        )


class PanelGapSkill:
    """Detect incomplete lab panels and conditional gaps."""

    name = "panel_gaps"
    description = "Find missing tests in lab panels and condition-based gaps."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.panel_gaps import PanelGapDetector

        detector = PanelGapDetector(ctx.db)
        report = detector.detect(user_id=ctx.user_id)
        if not report.has_gaps:
            return SkillResult(
                skill_name=self.name,
                summary="All lab panels complete.",
                policy=ToolPolicy.MEDIUM,
            )
        details = []
        for pg in report.panel_gaps[:5]:
            details.append(f"{pg.panel_name}: missing {', '.join(pg.missing)}")
        for cg in report.conditional_gaps[:3]:
            details.append(f"Consider: {', '.join(cg.missing_tests)}")
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(report.panel_gaps)} panel gaps, "
            f"{len(report.conditional_gaps)} conditional gaps.",
            details=details,
            policy=ToolPolicy.MEDIUM,
            changed=True,
        )


class HypothesisSkill:
    """Auto-generate medical hypotheses from lab patterns."""

    name = "hypothesis_generator"
    description = "Detect undiagnosed conditions from lab patterns."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.hypothesis_generator import HypothesisGenerator

        gen = HypothesisGenerator(ctx.db)
        hyps = gen.scan_all(ctx.user_id, sex=ctx.sex, age=ctx.age)
        if not hyps:
            return SkillResult(
                skill_name=self.name,
                summary="No new hypotheses detected.",
                policy=ToolPolicy.LOW,
            )
        details = []
        for h in hyps[:5]:
            evidence = ", ".join(h.evidence_for[:2])
            details.append(
                f"{h.title} ({h.confidence:.0%}, based on {evidence})"
            )
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(hyps)} hypotheses generated.",
            details=details,
            policy=ToolPolicy.NEEDS_RESEARCH,
            changed=True,
        )


class OverdueScreeningSkill:
    """Detect overdue health screenings."""

    name = "overdue_screenings"
    description = "Find overdue lab tests and screenings based on guidelines."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.overdue import OverdueDetector

        detector = OverdueDetector(ctx.db)
        overdue = detector.check_overdue(user_id=ctx.user_id)
        if not overdue:
            return SkillResult(
                skill_name=self.name,
                summary="All screenings up to date.",
                policy=ToolPolicy.MEDIUM,
            )
        details = [
            f"{o.test_name}: {o.days_overdue} days overdue"
            for o in overdue[:10]
        ]
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(overdue)} overdue screenings.",
            details=details,
            policy=ToolPolicy.HIGH,
            changed=True,
        )


class IntelligenceAuditSkill:
    """Self-audit for intelligence gaps."""

    name = "intelligence_audit"
    description = "Identify missing data, untested conditions, and analysis gaps."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.intelligence_auditor import IntelligenceAuditor

        auditor = IntelligenceAuditor(ctx.db)
        demographics = None
        if ctx.sex or ctx.age:
            demographics = {"sex": ctx.sex, "age": ctx.age}
        gaps = auditor.audit(
            user_id=ctx.user_id, demographics=demographics,
        )
        if not gaps:
            return SkillResult(
                skill_name=self.name,
                summary="No intelligence gaps detected.",
                policy=ToolPolicy.MEDIUM,
            )
        details = [g.description for g in gaps[:10]]
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(gaps)} intelligence gaps found.",
            details=details,
            policy=ToolPolicy.MEDIUM,
            changed=True,
        )


class FamilyRiskSkill:
    """Assess genetic and family history risk factors."""

    name = "family_risk"
    description = "Evaluate family history for hereditary condition risks."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        if ctx.db is None:
            return SkillResult(
                skill_name=self.name,
                summary="No database available for family risk analysis.",
                policy=ToolPolicy.MEDIUM,
            )

        from healthbot.reasoning.family_risk import (
            FamilyRiskEngine,
            parse_family_history,
        )

        facts = ctx.db.get_ltm_by_category(ctx.user_id, "family_history")
        fact_texts = [f.get("fact", "") for f in facts if f.get("fact")]
        if not fact_texts:
            return SkillResult(
                skill_name=self.name,
                summary="No family history data available.",
                policy=ToolPolicy.MEDIUM,
            )
        conditions = parse_family_history(fact_texts)
        if not conditions:
            return SkillResult(
                skill_name=self.name,
                summary="No actionable family risk factors found.",
                policy=ToolPolicy.MEDIUM,
            )
        engine = FamilyRiskEngine()
        implications = engine.get_all_screening_implications(conditions)
        details = [
            f"{c.condition} ({c.relationship.replace('_', ' ')})"
            for c in conditions[:5]
        ]
        for imp in implications[:5]:
            details.append(f"Screen: {imp}")
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(conditions)} family risk factors.",
            details=details,
            policy=ToolPolicy.HIGH,
            changed=True,
        )


class WearableTrendSkill:
    """Analyze wearable data trends (HRV, sleep, recovery)."""

    name = "wearable_trends"
    description = "Detect trends and anomalies in wearable metrics."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer

        engine = WearableTrendAnalyzer(ctx.db)
        trends = engine.detect_all_trends(days=14, user_id=ctx.user_id)
        anomalies = engine.detect_anomalies(days=1, user_id=ctx.user_id)
        if not trends and not anomalies:
            return SkillResult(
                skill_name=self.name,
                summary="No significant wearable trends.",
                policy=ToolPolicy.MEDIUM,
            )
        details = []
        for t in trends[:5]:
            details.append(
                f"{t.display_name}: {t.direction} "
                f"({t.pct_change:+.1f}%)"
            )
        for a in anomalies[:3]:
            details.append(f"Anomaly: {a.display_name} — {a.message}")
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(trends)} trends, {len(anomalies)} anomalies.",
            details=details,
            policy=ToolPolicy.MEDIUM,
            changed=bool(trends or anomalies),
        )


class DerivedMarkersSkill:
    """Compute derived lab markers (HOMA-IR, TG/HDL, eGFR, etc.)."""

    name = "derived_markers"
    description = "Calculate clinically useful derived markers from raw lab values."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.derived_markers import DerivedMarkerEngine

        engine = DerivedMarkerEngine(ctx.db)
        report = engine.compute_all(user_id=ctx.user_id)
        if not report.markers:
            return SkillResult(
                skill_name=self.name,
                summary="No derived markers computable.",
                policy=ToolPolicy.MEDIUM,
            )
        details = [
            f"{m.name}: {m.value} {m.unit} ({m.interpretation})"
            for m in report.markers
        ]
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(report.markers)} derived markers computed.",
            details=details,
            policy=ToolPolicy.HIGH,
            changed=True,
        )


class LabAlertsSkill:
    """Scan for clinically significant lab alerts."""

    name = "lab_alerts"
    description = "Check for critical values, rapid changes, and threshold crossings."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.lab_alerts import LabAlertEngine

        engine = LabAlertEngine(ctx.db)
        report = engine.scan(user_id=ctx.user_id, sex=ctx.sex, age=ctx.age)
        if not report.has_alerts:
            return SkillResult(
                skill_name=self.name,
                summary="No lab alerts.",
                policy=ToolPolicy.MEDIUM,
            )
        details = [
            f"[{a.severity.upper()}] {a.message}" for a in report.alerts[:10]
        ]
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(report.alerts)} alerts ({report.critical_count} critical).",
            details=details,
            policy=ToolPolicy.HIGH,
            changed=True,
        )


class PathwayAnalysisSkill:
    """Analyze genetic variants by biological pathway."""

    name = "pathway_analysis"
    description = "Group genetic risk findings into pathways with cumulative scoring."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.pathway_analysis import PathwayAnalysisEngine

        engine = PathwayAnalysisEngine(ctx.db)
        reports = engine.analyze(user_id=ctx.user_id)
        active = [r for r in reports if r.risk_snps_found > 0]
        if not active:
            return SkillResult(
                skill_name=self.name,
                summary="No pathway impacts detected.",
                policy=ToolPolicy.MEDIUM,
            )
        details = [
            f"{r.pathway_name}: {r.impact_score}/10 ({r.risk_snps_found} variants)"
            for r in active
        ]
        return SkillResult(
            skill_name=self.name,
            summary=f"{len(active)} pathways with findings.",
            details=details,
            policy=ToolPolicy.HIGH,
            changed=True,
        )


class PharmacogenomicsSkill:
    """Pharmacogenomics metabolizer profile."""

    name = "pharmacogenomics"
    description = "Classify CYP enzyme metabolizer status and flag drug interactions."

    def is_relevant(self, ctx: HealthContext) -> bool:
        return ctx.db is not None

    def run(self, ctx: HealthContext) -> SkillResult:
        from healthbot.reasoning.pharmacogenomics import PharmacogenomicsEngine

        engine = PharmacogenomicsEngine(ctx.db)
        report = engine.profile(user_id=ctx.user_id)
        if not report.actionable_count:
            return SkillResult(
                skill_name=self.name,
                summary="No actionable pharmacogenomics findings.",
                policy=ToolPolicy.MEDIUM,
            )
        details = [
            f"{ep.enzyme}: {ep.status.replace('_', ' ')}"
            for ep in report.enzyme_profiles
            if ep.status != "normal"
        ]
        if report.drug_flags:
            details.extend(
                f"DRUG FLAG: {df.drug_name} ({df.recommendation})"
                for df in report.drug_flags
            )
        return SkillResult(
            skill_name=self.name,
            summary=f"{report.actionable_count} actionable findings.",
            details=details,
            policy=ToolPolicy.HIGH,
            changed=True,
        )


def register_builtin_skills(registry) -> None:
    """Register all built-in skills with the registry."""
    for skill_cls in [
        TrendAnalysisSkill,
        InteractionCheckSkill,
        PanelGapSkill,
        HypothesisSkill,
        OverdueScreeningSkill,
        IntelligenceAuditSkill,
        FamilyRiskSkill,
        WearableTrendSkill,
        DerivedMarkersSkill,
        LabAlertsSkill,
        PathwayAnalysisSkill,
        PharmacogenomicsSkill,
    ]:
        registry.register(skill_cls())
