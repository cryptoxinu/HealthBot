"""Data-display section builders for AI health data export.

Each function takes (db, user_id, report) and returns a markdown string.
Analysis/reasoning builders are in ai_export_analysis.py.
"""
from __future__ import annotations

import logging

from healthbot.llm.anonymizer import Anonymizer

logger = logging.getLogger("healthbot")


def build_demographics(db, user_id: int, report) -> str:
    demo = db.get_user_demographics(user_id)
    if not demo or not any(demo.values()):
        return "No demographic data available."

    lines: list[str] = []
    age = demo.get("age")
    if age is not None:
        lines.append(f"- **Age**: {age}")
    if demo.get("dob"):
        report.add(1, "dob", "Date of birth excluded", "stripped")
    if sex := demo.get("sex"):
        lines.append(f"- **Sex**: {sex}")
    if ethnicity := demo.get("ethnicity"):
        lines.append(f"- **Ethnicity**: {ethnicity}")
    if (height_m := demo.get("height_m")) is not None:
        inches = height_m * 39.3701
        feet = int(inches // 12)
        remaining = int(round(inches % 12))
        lines.append(f"- **Height**: {feet}'{remaining}\" ({height_m:.2f} m)")
    if (weight_kg := demo.get("weight_kg")) is not None:
        lbs = weight_kg * 2.20462
        lines.append(f"- **Weight**: {lbs:.0f} lbs ({weight_kg:.1f} kg)")
    if (bmi := demo.get("bmi")) is not None:
        lines.append(f"- **BMI**: {bmi:.1f}")

    return "\n".join(lines) if lines else "No demographic data available."


def build_labs(db, user_id: int, report) -> str:
    labs = db.query_observations(record_type="lab_result", user_id=user_id)
    if not labs:
        return "No lab results on file."

    stripped = sum(1 for rec in labs if rec.get("ordering_provider"))
    if stripped:
        report.add(
            1, "lab_provider",
            f"Stripped ordering_provider from {stripped} records", "stripped",
        )

    has_lab = any(rec.get("lab_name") for rec in labs)
    if has_lab:
        lines = [
            "| Date | Test | Value | Unit | Reference | Flag | Lab |",
            "|------|------|-------|------|-----------|------|-----|",
        ]
    else:
        lines = [
            "| Date | Test | Value | Unit | Reference | Flag |",
            "|------|------|-------|------|-----------|------|",
        ]
    for lab in labs:
        meta = lab.get("_meta", {})
        d = meta.get("date_effective", "")
        name = lab.get("test_name") or lab.get("canonical_name") or "Unknown"
        val = lab.get("value", "")
        unit = lab.get("unit", "")
        ref_lo = lab.get("reference_low")
        ref_hi = lab.get("reference_high")
        if ref_lo is not None and ref_hi is not None:
            ref = f"{ref_lo}-{ref_hi}"
        else:
            ref = lab.get("reference_text", "")
        flag = lab.get("flag", "")
        if has_lab:
            lab_brand = lab.get("lab_name", "")
            lines.append(
                f"| {d} | {name} | {val} | {unit} | {ref} | {flag} | {lab_brand} |"
            )
        else:
            lines.append(f"| {d} | {name} | {val} | {unit} | {ref} | {flag} |")

    return "\n".join(lines)


def build_medications(db, user_id: int, report) -> str:
    meds = db.get_active_medications(user_id=user_id)
    if not meds:
        return "No active medications."

    stripped = sum(1 for m in meds if m.get("prescriber"))
    if stripped:
        report.add(
            1, "prescriber",
            f"Stripped prescriber from {stripped} medications", "stripped",
        )

    lines = [
        "| Medication | Dose | Frequency |",
        "|------------|------|-----------|",
    ]
    for med in meds:
        name = med.get("name", "Unknown")
        dose = med.get("dose", "")
        if med.get("unit"):
            dose = f"{dose} {med['unit']}"
        freq = med.get("frequency", "")
        lines.append(f"| {name} | {dose} | {freq} |")

    return "\n".join(lines)


def build_discovered_correlations(db, user_id: int, report) -> str:
    try:
        rows = db.conn.execute(
            "SELECT topic, finding, created_at FROM knowledge_base "
            "WHERE source = 'auto_correlation' "
            "ORDER BY relevance_score DESC LIMIT 20",
        ).fetchall()
    except Exception:
        return "No discovered correlations available."

    if not rows:
        return "No statistically significant lab-wearable correlations discovered yet."

    lines: list[str] = []
    for row in rows:
        lines.append(f"- {row['finding']}")
    return "\n".join(lines)


def build_hypotheses(db, user_id: int, report) -> str:
    hyps = db.get_active_hypotheses(user_id)
    if not hyps:
        return "No active health hypotheses."

    lines: list[str] = []
    for h in hyps:
        title = h.get("title", "Unknown")
        conf = h.get("_confidence", 0)
        lines.append(f"### {title} (confidence: {conf:.0%})")
        evidence_fields = [
            ("Evidence for", "evidence_for"),
            ("Evidence against", "evidence_against"),
            ("Missing tests", "missing_tests"),
        ]
        for label, key in evidence_fields:
            items = h.get(key, [])
            if items:
                lines.append(f"**{label}:**")
                lines.extend(f"- {e}" for e in items)
        lines.append("")
    return "\n".join(lines)


def build_health_context(db, user_id: int, report, anon: Anonymizer) -> str:
    facts = db.get_ltm_by_user(user_id)
    if not facts:
        return "No health context stored."

    non_demo = [f for f in facts if f.get("_category", "").lower() != "demographic"]
    if not non_demo:
        return "No non-demographic health context."

    lines: list[str] = []
    anon_count = 0
    for fact in non_demo:
        text = fact.get("fact", "")
        if not text:
            continue
        cleaned, had_phi = anon.anonymize(text)
        if had_phi:
            anon_count += 1
        cat = fact.get("_category", "")
        prefix = f"[{cat}] " if cat else ""
        lines.append(f"- {prefix}{cleaned}")

    if anon_count:
        report.add(
            1, "health_context",
            f"Anonymized {anon_count} health context entries", "redacted",
        )
    return "\n".join(lines) if lines else "No non-demographic health context."


def build_journal(db, user_id: int, report, anon: Anonymizer) -> str:
    entries = db.query_journal(user_id, limit=20)
    if not entries:
        return "No medical journal entries."

    lines: list[str] = []
    anon_count = 0
    for entry in entries:
        content = entry.get("content", "")
        if not content:
            continue
        cleaned, had_phi = anon.anonymize(content)
        if had_phi:
            anon_count += 1
        ts = (entry.get("_timestamp") or "")[:10]
        cat = entry.get("_category", "")
        label = f"[{ts}]" if ts else ""
        if cat:
            label += f" ({cat})"
        lines.append(f"- {label} {cleaned}")

    if anon_count:
        report.add(1, "journal", f"Anonymized {anon_count} journal entries", "redacted")
    return "\n".join(lines) if lines else "No medical journal entries."


def build_genetics(db, user_id: int, report) -> str:
    try:
        count = db.get_genetic_variant_count(user_id)
    except Exception:
        return "No genetic data on file."

    if count == 0:
        return "No genetic data on file."

    lines: list[str] = [f"Genetic variants on file: {count}"]
    try:
        from healthbot.reasoning.genetic_risk import GeneticRiskEngine
        engine = GeneticRiskEngine(db)
        findings = engine.scan_variants(user_id)
        if findings:
            lines.append("")
            lines.append("### Risk Findings")
            for f in findings:
                lines.append(f"\n**{f.gene} — {f.condition}**")
                lines.append(f"Genotype: {f.user_genotype} ({f.risk_level})")
                for note in f.clinical_notes:
                    lines.append(f"- {note}")
                if f.affected_labs:
                    lines.append(
                        f"Labs to monitor: {', '.join(f.affected_labs)}",
                    )
        else:
            lines.append("No clinically significant risk variants detected.")
    except Exception as e:
        logger.warning("Genetic risk scan failed in export: %s", e)
        lines.append("Risk analysis unavailable.")
    return "\n".join(lines)
