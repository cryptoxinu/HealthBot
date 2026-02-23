"""Comorbidity cross-analysis engine.

Detects clinically significant interactions between co-existing conditions.
For example: hypothyroidism + hypercholesterolemia → thyroid directly affects
lipid metabolism, so optimizing TSH may improve cholesterol without statins.

All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class ComorbidityInteraction:
    """A clinically significant interaction between two conditions."""

    condition_a: str
    condition_b: str
    interaction_type: str   # "causal", "bidirectional", "shared_mechanism"
    description: str
    clinical_implication: str
    evidence: str
    priority: str           # "high", "medium", "low"


COMORBIDITY_KB: tuple[ComorbidityInteraction, ...] = (
    ComorbidityInteraction(
        "hypothyroidism", "hyperlipidemia",
        "causal",
        "Hypothyroidism directly impairs LDL receptor activity, raising LDL "
        "cholesterol and triglycerides.",
        "Optimize TSH (target 0.5-2.5 mIU/L) before initiating or escalating "
        "statin therapy. Lipid levels may normalize with adequate thyroid replacement.",
        "Duntas LH. Thyroid. 2002;12(4):287-293.",
        "high",
    ),
    ComorbidityInteraction(
        "diabetes", "hypertension",
        "bidirectional",
        "Insulin resistance promotes sodium retention and sympathetic activation. "
        "Hypertension accelerates diabetic nephropathy and retinopathy.",
        "Target BP <130/80 in diabetics. ACE inhibitors/ARBs preferred "
        "(renoprotective). Monitor renal function closely.",
        "de Boer IH et al. Diabetes Care. 2017;40(Suppl 1):S99-S110.",
        "high",
    ),
    ComorbidityInteraction(
        "chronic kidney disease", "anemia",
        "causal",
        "CKD reduces erythropoietin production, causing anemia of chronic disease. "
        "Iron metabolism is also impaired.",
        "Monitor hemoglobin, ferritin, and TSAT regularly. Consider EPO-stimulating "
        "agents if Hgb <10. IV iron may be needed if oral absorption is impaired.",
        "KDIGO Anemia Work Group. Kidney Int Suppl. 2012;2(4):279-335.",
        "high",
    ),
    ComorbidityInteraction(
        "iron deficiency", "hypothyroidism",
        "bidirectional",
        "Iron deficiency impairs thyroid peroxidase activity, worsening thyroid "
        "function. Hypothyroidism reduces iron absorption.",
        "Treat iron deficiency concurrently with thyroid replacement. "
        "Separate iron and levothyroxine by 4+ hours.",
        "Hess SY et al. J Nutr. 2002;132(7):1809S-1813S.",
        "high",
    ),
    ComorbidityInteraction(
        "diabetes", "liver disease",
        "bidirectional",
        "Type 2 diabetes is a major driver of NAFLD/NASH. Liver disease impairs "
        "glucose metabolism and insulin clearance.",
        "Screen diabetics for NAFLD (ALT, ultrasound). Avoid hepatotoxic agents. "
        "Pioglitazone or GLP-1 agonists may benefit both conditions.",
        "Targher G et al. N Engl J Med. 2021;385(17):1603-1612.",
        "high",
    ),
    ComorbidityInteraction(
        "obesity", "hypertension",
        "causal",
        "Adipose tissue promotes RAAS activation, sympathetic hyperactivity, "
        "and sodium retention, directly raising blood pressure.",
        "Weight loss of 5-10% can reduce SBP by 5-20 mmHg. "
        "Prioritize weight management alongside antihypertensives.",
        "Hall JE et al. Circ Res. 2015;116(6):991-1006.",
        "high",
    ),
    ComorbidityInteraction(
        "diabetes", "chronic kidney disease",
        "causal",
        "Diabetic nephropathy is the leading cause of CKD. Hyperglycemia damages "
        "glomerular capillaries and tubules.",
        "Target HbA1c <7% (individualized). SGLT2 inhibitors provide renal "
        "protection beyond glucose control. Monitor eGFR and albumin/creatinine ratio.",
        "KDIGO Diabetes Work Group. Kidney Int. 2020;98(4S):S1-S115.",
        "high",
    ),
    ComorbidityInteraction(
        "hypothyroidism", "depression",
        "causal",
        "Thyroid hormones regulate serotonin and norepinephrine metabolism. "
        "Even subclinical hypothyroidism increases depression risk 2-3x.",
        "Check TSH before starting antidepressants. Thyroid optimization may "
        "resolve depressive symptoms without psychotropic medication.",
        "Hage MP, Azar ST. J Thyroid Res. 2012;2012:590648.",
        "medium",
    ),
    ComorbidityInteraction(
        "vitamin d deficiency", "osteoporosis",
        "causal",
        "Vitamin D is essential for calcium absorption. Deficiency causes "
        "secondary hyperparathyroidism, accelerating bone loss.",
        "Replete vitamin D (target >30 ng/mL) before initiating bisphosphonates. "
        "Ensure adequate calcium intake (1000-1200 mg/day).",
        "Holick MF. N Engl J Med. 2007;357(3):266-281.",
        "high",
    ),
    ComorbidityInteraction(
        "gout", "chronic kidney disease",
        "bidirectional",
        "CKD reduces uric acid excretion, worsening gout. Urate crystal "
        "deposition can also damage kidneys.",
        "Dose-adjust allopurinol for eGFR. Febuxostat may be preferred in CKD. "
        "Avoid NSAIDs. Colchicine dose reduction required.",
        "Vargas-Santos AB, Neogi T. Expert Rev Clin Immunol. 2017;13(6):593-604.",
        "medium",
    ),
    ComorbidityInteraction(
        "hypertension", "chronic kidney disease",
        "bidirectional",
        "Hypertension damages renal vasculature. CKD impairs sodium excretion "
        "and RAAS regulation, worsening hypertension.",
        "Target BP <130/80. ACE/ARBs first-line (unless hyperkalemia). "
        "Monitor potassium and creatinine closely with RAAS blockade.",
        "Cheung AK et al. Kidney Int. 2021;99(3):559-569.",
        "high",
    ),
    ComorbidityInteraction(
        "insulin resistance", "polycystic ovary syndrome",
        "bidirectional",
        "Insulin resistance drives ovarian androgen production in PCOS. "
        "PCOS-associated hyperandrogenism worsens metabolic syndrome.",
        "Metformin or inositol may improve both metabolic and reproductive outcomes. "
        "Screen PCOS patients for glucose intolerance, lipids, and metabolic syndrome.",
        "Diamanti-Kandarakis E. Endocr Rev. 2012;33(6):981-1030.",
        "high",
    ),
    ComorbidityInteraction(
        "anemia", "heart failure",
        "bidirectional",
        "Anemia increases cardiac workload. Heart failure causes renal hypoperfusion "
        "and inflammatory cytokine release, worsening anemia.",
        "Iron repletion (IV preferred) improves exercise capacity and quality of life "
        "in heart failure even without anemia. Target ferritin >100.",
        "Anker SD et al. N Engl J Med. 2009;361(25):2436-2448.",
        "high",
    ),
    ComorbidityInteraction(
        "sleep apnea", "hypertension",
        "causal",
        "Obstructive sleep apnea causes intermittent hypoxia and sympathetic surges, "
        "driving resistant hypertension.",
        "Screen hypertension patients (especially resistant) for OSA. "
        "CPAP therapy can reduce BP by 2-10 mmHg.",
        "Pedrosa RP et al. Chest. 2011;140(1):62-67.",
        "high",
    ),
    ComorbidityInteraction(
        "diabetes", "depression",
        "bidirectional",
        "Depression increases cortisol and inflammatory cytokines, worsening "
        "insulin resistance. Poorly controlled diabetes increases depression risk.",
        "Screen diabetics for depression annually. SSRIs are generally safe. "
        "Integrated treatment improves outcomes for both conditions.",
        "Holt RIG et al. Lancet Diabetes Endocrinol. 2014;2(9):740-753.",
        "medium",
    ),
    ComorbidityInteraction(
        "vitamin b12 deficiency", "neuropathy",
        "causal",
        "B12 is essential for myelin synthesis. Deficiency causes peripheral "
        "neuropathy that can mimic diabetic neuropathy.",
        "Check B12 in all neuropathy patients, especially those on metformin. "
        "Early repletion can reverse neurological damage; delayed treatment may not.",
        "Devalia V et al. Br J Haematol. 2014;166(2):241-249.",
        "high",
    ),
    ComorbidityInteraction(
        "hypothyroidism", "anemia",
        "causal",
        "Thyroid hormones stimulate erythropoiesis and iron absorption. "
        "Hypothyroidism can cause normocytic or macrocytic anemia.",
        "Investigate thyroid function in unexplained anemia. "
        "Anemia may resolve with thyroid replacement alone.",
        "Szczepanek-Parulska E et al. Pol Arch Intern Med. 2017;127(5):352-360.",
        "medium",
    ),
    ComorbidityInteraction(
        "magnesium deficiency", "hypertension",
        "causal",
        "Magnesium regulates vascular tone and endothelial function. "
        "Deficiency promotes vasoconstriction and elevated BP.",
        "Supplementation (400-600 mg/day) may reduce SBP by 2-5 mmHg. "
        "Check RBC magnesium (serum is unreliable).",
        "Zhang X et al. Hypertension. 2016;68(2):324-333.",
        "medium",
    ),
)

# Build condition alias map for flexible matching
_CONDITION_ALIASES: dict[str, str] = {
    "type 2 diabetes": "diabetes",
    "type 1 diabetes": "diabetes",
    "diabetes mellitus": "diabetes",
    "t2dm": "diabetes",
    "dm2": "diabetes",
    "prediabetes": "insulin resistance",
    "high cholesterol": "hyperlipidemia",
    "dyslipidemia": "hyperlipidemia",
    "high blood pressure": "hypertension",
    "hashimoto": "hypothyroidism",
    "hashimoto's": "hypothyroidism",
    "graves": "hyperthyroidism",
    "ckd": "chronic kidney disease",
    "kidney disease": "chronic kidney disease",
    "fatty liver": "liver disease",
    "nafld": "liver disease",
    "nash": "liver disease",
    "hepatitis": "liver disease",
    "iron deficiency anemia": "iron deficiency",
    "low iron": "iron deficiency",
    "low vitamin d": "vitamin d deficiency",
    "vitamin d insufficiency": "vitamin d deficiency",
    "low b12": "vitamin b12 deficiency",
    "b12 deficiency": "vitamin b12 deficiency",
    "low magnesium": "magnesium deficiency",
    "hyperuricemia": "gout",
    "pcos": "polycystic ovary syndrome",
    "osa": "sleep apnea",
    "obstructive sleep apnea": "sleep apnea",
    "chf": "heart failure",
    "congestive heart failure": "heart failure",
    "osteopenia": "osteoporosis",
}


def _normalize_condition(text: str) -> str:
    """Normalize a condition string to a KB key."""
    lower = text.lower().strip()
    return _CONDITION_ALIASES.get(lower, lower)


@dataclass
class ComorbidityFinding:
    """A detected comorbidity interaction for a specific patient."""

    interaction: ComorbidityInteraction
    matched_a: str   # actual condition text from patient
    matched_b: str   # actual condition text from patient


class ComorbidityAnalyzer:
    """Detect comorbidity interactions from patient conditions."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def analyze(self, user_id: int) -> list[ComorbidityFinding]:
        """Find comorbidity interactions for a user."""
        conditions = self._extract_conditions(user_id)
        if len(conditions) < 2:
            return []

        # Normalize to KB keys
        normalized: dict[str, str] = {}  # kb_key -> original text
        for cond in conditions:
            key = _normalize_condition(cond)
            if key not in normalized:
                normalized[key] = cond

        findings: list[ComorbidityFinding] = []
        seen: set[tuple[str, str]] = set()

        for interaction in COMORBIDITY_KB:
            a = interaction.condition_a
            b = interaction.condition_b
            if a in normalized and b in normalized:
                pair = (min(a, b), max(a, b))
                if pair not in seen:
                    seen.add(pair)
                    findings.append(ComorbidityFinding(
                        interaction=interaction,
                        matched_a=normalized[a],
                        matched_b=normalized[b],
                    ))

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        findings.sort(
            key=lambda f: priority_order.get(f.interaction.priority, 3),
        )
        return findings

    def _extract_conditions(self, user_id: int) -> list[str]:
        """Extract all known conditions from LTM and hypotheses."""
        conditions: list[str] = []

        # From LTM condition facts
        try:
            facts = self._db.get_ltm_by_category(user_id, "condition")
            for f in facts:
                text = f.get("fact", "")
                if text:
                    conditions.append(text)
        except Exception:
            pass

        # From active hypotheses (high confidence only)
        try:
            hyps = self._db.get_active_hypotheses(user_id)
            for h in hyps:
                conf = h.get("confidence", h.get("_confidence", 0))
                if conf >= 0.6:
                    title = h.get("title", "")
                    if title:
                        conditions.append(title)
        except Exception:
            pass

        # From supplement-detectable deficiencies
        try:
            self._check_lab_deficiencies(user_id, conditions)
        except Exception:
            pass

        return conditions

    def _check_lab_deficiencies(
        self, user_id: int, conditions: list[str],
    ) -> None:
        """Detect deficiencies from lab values to add as conditions."""
        deficiency_markers = {
            "vitamin_d": ("vitamin d deficiency", 20.0),
            "vitamin_b12": ("vitamin b12 deficiency", 200.0),
            "ferritin": ("iron deficiency", 15.0),
            "magnesium": ("magnesium deficiency", 1.5),
        }
        for marker, (condition_name, threshold) in deficiency_markers.items():
            rows = self._db.query_observations(
                record_type="lab_result",
                canonical_name=marker,
                limit=1,
                user_id=user_id,
            )
            if rows:
                try:
                    val = float(rows[0].get("value", 0))
                    if val < threshold:
                        conditions.append(condition_name)
                except (ValueError, TypeError):
                    pass


def format_comorbidities(findings: list[ComorbidityFinding]) -> str:
    """Format comorbidity findings for display."""
    if not findings:
        return "No significant comorbidity interactions detected."

    lines = ["COMORBIDITY ANALYSIS", "-" * 30]

    for f in findings:
        i = f.interaction
        type_label = {
            "causal": "->",
            "bidirectional": "<->",
            "shared_mechanism": "~",
        }.get(i.interaction_type, "?")

        lines.append(
            f"\n{i.condition_a.title()} {type_label} {i.condition_b.title()} "
            f"[{i.priority}]"
        )
        lines.append(f"  {i.description}")
        lines.append(f"  Action: {i.clinical_implication}")
        lines.append(f"  Ref: {i.evidence}")

    return "\n".join(lines)
