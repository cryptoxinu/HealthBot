"""Clean DB reporting mixin — summary markdown generation methods."""
from __future__ import annotations

from datetime import UTC, datetime


class ReportingMixin:
    """Mixin providing health summary and section builder methods for CleanDB."""

    def get_health_summary_markdown(self) -> str:
        """Generate a comprehensive health summary as Markdown."""
        sections = self.get_health_summary_sections()
        # Join all full-detail sections (skip summaries, they're for compact mode)
        order = [
            "header", "demographics", "labs", "medications",
            "wearable_detail", "workouts", "hypotheses", "health_context",
            "genetics", "goals", "med_reminders", "providers",
            "appointments", "health_records_ext", "analysis_rules",
            "user_memory",
        ]
        parts = [sections[k] for k in order if sections.get(k)]
        return "\n".join(parts)

    def get_health_summary_sections(self) -> dict[str, str]:
        """Return health summary as named sections for query-aware selection.

        Keys returned (empty string if no data):
          header, demographics, labs, labs_summary, medications,
          wearable_detail, wearable_summary, hypotheses,
          health_context, user_memory
        """
        sections: dict[str, str] = {}

        # Header
        sections["header"] = (
            "# Health Data Summary (Anonymized)\n\n"
            f"> Generated: {datetime.now(UTC).strftime('%Y-%m-%d')}\n"
            "> All PII has been stripped. This data is safe for AI analysis.\n"
        )

        # Demographics
        sections["demographics"] = self._build_demographics_section()

        # Labs (full + summary)
        labs = self._get_latest_per_test(limit=200)
        sections["labs"] = self._build_labs_section(labs)
        sections["labs_summary"] = self._build_labs_summary(labs)

        # Medications
        sections["medications"] = self._build_medications_section()

        # Wearable data (full detail + compact summary)
        detail_parts: list[str] = []
        summary_parts: list[str] = []
        for prov, label in [("whoop", "WHOOP"), ("oura", "Oura")]:
            all_data = self.get_wearable_data(days=365, provider=prov)
            if not all_data:
                continue
            detail_parts.append(self._build_wearable_detail(all_data, label))
            summary_parts.append(self._build_wearable_summary(all_data, label))
        sections["wearable_detail"] = "\n".join(detail_parts)
        sections["wearable_summary"] = "\n".join(summary_parts)

        # Workouts
        sections["workouts"] = self._build_workouts_section()

        # Hypotheses
        sections["hypotheses"] = self._build_hypotheses_section()

        # Health context
        sections["health_context"] = self._build_health_context_section()

        # Genetics
        sections["genetics"] = self._build_genetics_section()

        # Goals
        sections["goals"] = self._build_goals_section()

        # Med reminders
        sections["med_reminders"] = self._build_med_reminders_section()

        # Providers
        sections["providers"] = self._build_providers_section()

        # Appointments
        sections["appointments"] = self._build_appointments_section()

        # Extended health records
        sections["health_records_ext"] = self._build_health_records_ext_section()

        # Analysis rules
        sections["analysis_rules"] = self._build_analysis_rules_section()

        # User memory
        sections["user_memory"] = self._build_user_memory_section()

        return sections

    # ── Section builders for get_health_summary_sections() ────

    def _build_demographics_section(self) -> str:
        demo = self.get_demographics()
        if not demo:
            return ""
        parts: list[str] = ["## Demographics\n"]
        if demo.get("age"):
            parts.append(f"- **Age**: {demo['age']}")
        if demo.get("sex"):
            parts.append(f"- **Sex**: {demo['sex']}")
        if demo.get("ethnicity"):
            parts.append(f"- **Ethnicity**: {demo['ethnicity']}")
        if demo.get("height_m"):
            inches = demo["height_m"] * 39.3701
            feet = int(inches // 12)
            rem = int(round(inches % 12))
            parts.append(f"- **Height**: {feet}'{rem}\" ({demo['height_m']:.2f} m)")
        if demo.get("weight_kg"):
            lbs = demo["weight_kg"] * 2.20462
            parts.append(f"- **Weight**: {lbs:.0f} lbs ({demo['weight_kg']:.1f} kg)")
        if demo.get("bmi"):
            parts.append(f"- **BMI**: {demo['bmi']:.1f}")
        parts.append("")
        return "\n".join(parts)

    def _build_labs_section(self, labs: list[dict]) -> str:
        if not labs:
            return ""
        parts: list[str] = ["## Recent Lab Results\n"]
        has_lab = any(lab.get("source_lab") for lab in labs)
        if has_lab:
            parts.append("| Date | Test | Value | Unit | Reference | Flag | Lab |")
            parts.append("|------|------|-------|------|-----------|------|-----|")
        else:
            parts.append("| Date | Test | Value | Unit | Reference | Flag |")
            parts.append("|------|------|-------|------|-----------|------|")
        for lab in labs:
            ref = ""
            if lab.get("reference_low") is not None and lab.get("reference_high") is not None:
                ref = f"{lab['reference_low']}-{lab['reference_high']}"
            elif lab.get("reference_text"):
                ref = lab["reference_text"]
            row = (
                f"| {lab.get('date_effective', '')} "
                f"| {lab.get('test_name') or lab.get('canonical_name', '')} "
                f"| {lab.get('value', '')} "
                f"| {lab.get('unit', '')} "
                f"| {ref} "
                f"| {lab.get('flag', '')} "
            )
            if has_lab:
                row += f"| {lab.get('source_lab', '')} |"
            else:
                row += "|"
            parts.append(row)
        parts.append("")
        return "\n".join(parts)

    def _build_labs_summary(self, labs: list[dict]) -> str:
        """Compact labs: flagged results + total count."""
        if not labs:
            return ""
        flagged = [
            lab for lab in labs
            if lab.get("flag") and lab["flag"].upper() not in ("", "NORMAL")
        ]
        parts: list[str] = [f"## Lab Results ({len(labs)} tests on file)\n"]
        if flagged:
            parts.append("Flagged results:")
            for lab in flagged[:15]:
                name = lab.get("test_name") or lab.get("canonical_name", "")
                parts.append(
                    f"- {name}: {lab.get('value', '')} {lab.get('unit', '')} "
                    f"[{lab.get('flag', '')}] ({lab.get('date_effective', '')})"
                )
        else:
            parts.append("No flagged results.")
        parts.append("")
        return "\n".join(parts)

    def _build_medications_section(self) -> str:
        meds = self.get_medications()
        if not meds:
            return ""
        parts: list[str] = [
            "## Active Medications\n",
            "| Medication | Dose | Frequency |",
            "|------------|------|-----------|",
        ]
        for med in meds:
            dose = med.get("dose", "")
            if med.get("unit"):
                dose = f"{dose} {med['unit']}"
            parts.append(f"| {med.get('name', '')} | {dose} | {med.get('frequency', '')} |")
        parts.append("")
        return "\n".join(parts)

    @staticmethod
    def _build_wearable_detail(all_data: list[dict], label: str) -> str:
        """Full wearable section: monthly averages + 14-day daily detail."""
        from collections import defaultdict

        dates = [d.get("date", "") for d in all_data if d.get("date")]
        first_date = min(dates) if dates else "?"
        last_date = max(dates) if dates else "?"
        parts: list[str] = [
            f"## {label} Data \u2014 {len(all_data)} records"
            f" ({first_date} to {last_date})\n",
        ]

        # Monthly averages
        months: dict[str, list[dict]] = defaultdict(list)
        for day in all_data:
            date_str = day.get("date", "")
            if date_str and len(date_str) >= 7:
                months[date_str[:7]].append(day)

        def _month_avg(rows: list[dict], field: str) -> str:
            vals = [d[field] for d in rows if d.get(field) is not None]
            return f"{sum(vals) / len(vals):.0f}" if vals else "-"

        if months:
            parts.append("### Monthly Averages\n")
            parts.append(
                "| Month | Days | Avg HRV | Avg RHR"
                " | Avg Sleep | Avg Recovery | Avg Strain |"
            )
            parts.append(
                "|-------|------|---------|--------"
                "|-----------|--------------|------------|"
            )
            for month_key in sorted(months.keys()):
                m = months[month_key]
                parts.append(
                    f"| {month_key} | {len(m)}"
                    f" | {_month_avg(m, 'hrv')}"
                    f" | {_month_avg(m, 'rhr')}"
                    f" | {_month_avg(m, 'sleep_score')}"
                    f" | {_month_avg(m, 'recovery_score')}"
                    f" | {_month_avg(m, 'strain')} |"
                )
            parts.append("")

        # Recent 14 days daily detail
        recent = all_data[:14] if len(all_data) >= 14 else all_data
        parts.append("### Recent Daily Detail\n")
        parts.append(
            "| Date | HRV | RHR | Sleep | Recovery"
            " | Strain | Deep | REM | SpO2 | Workout |"
        )
        parts.append(
            "|------|-----|-----|-------|----------"
            "|--------|------|-----|------|---------|"
        )
        for day in recent:
            hrv = f"{day['hrv']:.0f}" if day.get("hrv") is not None else "-"
            rhr = f"{day['rhr']:.0f}" if day.get("rhr") is not None else "-"
            slp = f"{day['sleep_score']:.0f}" if day.get("sleep_score") is not None else "-"
            rec_val = day.get("recovery_score")
            rec = f"{rec_val:.0f}%" if rec_val is not None else "-"
            strain = f"{day['strain']:.1f}" if day.get("strain") is not None else "-"
            deep = f"{day['deep_min']}m" if day.get("deep_min") is not None else "-"
            rem_val = f"{day['rem_min']}m" if day.get("rem_min") is not None else "-"
            spo2 = f"{day['spo2']:.0f}%" if day.get("spo2") is not None else "-"
            wk = day.get("workout_sport_name") or "-"
            parts.append(
                f"| {day.get('date', '')} | {hrv} | {rhr} | {slp}"
                f" | {rec} | {strain} | {deep} | {rem_val} | {spo2} | {wk} |"
            )
        parts.append("")
        return "\n".join(parts)

    @staticmethod
    def _build_wearable_summary(all_data: list[dict], label: str) -> str:
        """Compact wearable: 14-day averages in one line per provider."""
        recent = all_data[:14] if len(all_data) >= 14 else all_data
        if not recent:
            return ""

        def _avg(field: str) -> str:
            vals = [d[field] for d in recent if d.get(field) is not None]
            return f"{sum(vals) / len(vals):.0f}" if vals else "-"

        return (
            f"{label} ({len(recent)}d avg): "
            f"HRV {_avg('hrv')} | RHR {_avg('rhr')} | "
            f"Recovery {_avg('recovery_score')}% | "
            f"Sleep {_avg('sleep_score')} | "
            f"Strain {_avg('strain')}"
        )

    def _build_hypotheses_section(self) -> str:
        hyps = self.get_hypotheses()
        if not hyps:
            return ""
        parts: list[str] = ["## Health Hypotheses\n"]
        for h in hyps:
            parts.append(f"### {h['title']} (confidence: {h['confidence']:.0%})")
            parts.append(f"- Evidence for: {h.get('evidence_for', '[]')}")
            parts.append(f"- Evidence against: {h.get('evidence_against', '[]')}")
            parts.append(f"- Missing tests: {h.get('missing_tests', '[]')}")
            parts.append("")
        return "\n".join(parts)

    def _build_health_context_section(self) -> str:
        context = self.get_health_context()
        if not context:
            return ""
        parts: list[str] = ["## Health Context\n"]
        for ctx in context:
            prefix = f"[{ctx['category']}] " if ctx.get("category") else ""
            parts.append(f"- {prefix}{ctx['fact']}")
        parts.append("")
        return "\n".join(parts)

    def _build_workouts_section(self) -> str:
        workouts = self.get_workouts(limit=50)
        if not workouts:
            return ""
        parts: list[str] = [
            "## Recent Workouts\n",
            "| Date | Sport | Duration | Calories | Avg HR | Distance |",
            "|------|-------|----------|----------|--------|----------|",
        ]
        for w in workouts:
            dur = f"{w['duration_minutes']:.0f}m" if w.get("duration_minutes") else "-"
            cal = f"{w['calories_burned']:.0f}" if w.get("calories_burned") else "-"
            hr = f"{w['avg_heart_rate']:.0f}" if w.get("avg_heart_rate") else "-"
            dist = f"{w['distance_km']:.1f}km" if w.get("distance_km") else "-"
            parts.append(
                f"| {w.get('start_date', '')[:10]} "
                f"| {w.get('sport_type', '')} "
                f"| {dur} | {cal} | {hr} | {dist} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_genetics_section(self) -> str:
        variants = self.get_genetic_variants(limit=200)
        if not variants:
            return ""
        parts: list[str] = [
            "## Genetic Variants\n",
            "| rsID | Genotype | Phenotype |",
            "|------|----------|-----------|",
        ]
        for v in variants:
            parts.append(
                f"| {v.get('rsid', '')} "
                f"| {v.get('genotype', '')} "
                f"| {v.get('phenotype', '')} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_goals_section(self) -> str:
        goals = self.get_health_goals()
        if not goals:
            return ""
        parts: list[str] = ["## Health Goals\n"]
        for g in goals:
            parts.append(f"- {g['goal_text']}")
        parts.append("")
        return "\n".join(parts)

    def _build_med_reminders_section(self) -> str:
        reminders = self.get_med_reminders()
        if not reminders:
            return ""
        parts: list[str] = [
            "## Medication Reminders\n",
            "| Time | Medication | Notes |",
            "|------|------------|-------|",
        ]
        for r in reminders:
            parts.append(
                f"| {r.get('time', '')} "
                f"| {r.get('med_name', '')} "
                f"| {r.get('notes', '')} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_providers_section(self) -> str:
        providers = self.get_providers()
        if not providers:
            return ""
        parts: list[str] = ["## Healthcare Providers\n"]
        for p in providers:
            line = f"- **{p.get('specialty', 'Unknown')}**"
            if p.get("notes"):
                line += f" \u2014 {p['notes']}"
            parts.append(line)
        parts.append("")
        return "\n".join(parts)

    def _build_appointments_section(self) -> str:
        appts = self.get_appointments(limit=20)
        if not appts:
            return ""
        parts: list[str] = [
            "## Appointments\n",
            "| Date | Status | Reason |",
            "|------|--------|--------|",
        ]
        for a in appts:
            parts.append(
                f"| {a.get('appt_date', '')} "
                f"| {a.get('status', '')} "
                f"| {a.get('reason', '')} |"
            )
        parts.append("")
        return "\n".join(parts)

    def _build_health_records_ext_section(self) -> str:
        try:
            records = self.get_health_records_ext()
        except Exception:
            return ""
        if not records:
            return ""
        by_type: dict[str, list[dict]] = {}
        for r in records:
            by_type.setdefault(r.get("data_type", "other"), []).append(r)
        parts: list[str] = ["## Additional Health Records\n"]
        for dtype in sorted(by_type.keys()):
            parts.append(f"### {dtype.replace('_', ' ').title()}")
            for r in by_type[dtype]:
                line = f"- {r.get('label', '')}"
                if r.get("value"):
                    line += f": {r['value']}"
                if r.get("unit"):
                    line += f" {r['unit']}"
                if r.get("date_effective"):
                    line += f" ({r['date_effective']})"
                parts.append(line)
        parts.append("")
        return "\n".join(parts)

    def _build_analysis_rules_section(self) -> str:
        try:
            rules = self.get_active_analysis_rules()
        except Exception:
            return ""
        if not rules:
            return ""
        parts: list[str] = ["## Active Analysis Rules\n"]
        for r in rules:
            priority = r.get("priority", "medium").upper()
            parts.append(f"- [{priority}] **{r.get('name', '')}** (scope: {r.get('scope', '')})")
            parts.append(f"  {r.get('rule', '')}")
        parts.append("")
        return "\n".join(parts)

    def _build_user_memory_section(self) -> str:
        try:
            memories = self.get_user_memory()
        except Exception:
            return ""  # Table may not exist in old DBs
        if not memories:
            return ""
        parts: list[str] = ["## User Memory\n"]
        by_cat: dict[str, list[dict]] = {}
        for mem in memories:
            by_cat.setdefault(mem.get("category", "general"), []).append(mem)
        for cat in sorted(by_cat.keys()):
            parts.append(f"### {cat.replace('_', ' ').title()}")
            for mem in by_cat[cat]:
                conf = mem.get("confidence", 1.0)
                marker = "" if conf >= 0.9 else f" (~{conf:.0%} confidence)"
                parts.append(f"- {mem['key']}: {mem['value']}{marker}")
        parts.append("")
        return "\n".join(parts)
