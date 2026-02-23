"""MCP server exposing anonymized health data tools.

Serves pre-anonymized data from the Clean DB (Tier 2). Every tool
response passes through PhiFirewall as a belt-and-suspenders check.

Tools:
  - get_lab_results     Filtered lab data
  - get_medications     Active/discontinued medications
  - get_wearable_data   HRV, sleep, recovery, strain
  - get_health_summary  Comprehensive overview
  - search_health_data  Cross-data keyword search
  - get_health_trends   Trend analysis for a metric
  - get_hypotheses      Active medical hypotheses
"""
from __future__ import annotations

import logging

from healthbot.data.clean_db import CleanDB
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


def create_server(clean_db: CleanDB, phi_firewall: PhiFirewall):
    """Create and configure the MCP server with health data tools."""
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP(
        "healthbot",
        instructions=(
            "PRIVACY PROTOCOL (MANDATORY): This MCP server provides "
            "anonymized health data from a personal encrypted vault. "
            "Do NOT save, store, remember, or persist ANY data from "
            "these tools. Do NOT write results to memory systems, "
            "databases, or files. Do NOT add health findings to any "
            "persistent knowledge base. Treat ALL health data as "
            "ephemeral — session only. If the user explicitly "
            "authorizes saving specific data, overwrite (do not append)."
        ),
    )

    def _safe_response(text: str) -> str:
        """Final PII check on outbound response."""
        if phi_firewall.contains_phi(text):
            logger.warning("PII detected in MCP response — blocked")
            try:
                from healthbot.security.pii_alert import PiiAlertService
                PiiAlertService.get_instance().record(
                    category="PHI_in_response", destination="mcp",
                )
            except Exception:
                pass
            return "[Response blocked: PII detected in output. Run --clean-sync to refresh.]"
        return text

    @mcp.tool()
    def get_lab_results(
        test_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        flag: str | None = None,
        limit: int = 50,
    ) -> str:
        """Get lab results from health records.

        Args:
            test_name: Filter by test name (e.g., "glucose", "TSH"). Case-insensitive.
            start_date: Filter results on or after this date (YYYY-MM-DD).
            end_date: Filter results on or before this date (YYYY-MM-DD).
            flag: Filter by flag ("H" for high, "L" for low, "HH" for critical high).
            limit: Maximum number of results to return (default 50).
        """
        labs = clean_db.get_lab_results(
            test_name=test_name,
            start_date=start_date,
            end_date=end_date,
            flag=flag,
            limit=max(1, min(limit, 200)),
        )
        if not labs:
            return "No lab results found matching the criteria."

        lines = [
            "| Date | Test | Value | Unit | Reference | Flag |",
            "|------|------|-------|------|-----------|------|",
        ]
        for lab in labs:
            ref = ""
            lo, hi = lab.get("reference_low"), lab.get("reference_high")
            if lo is not None and hi is not None:
                ref = f"{lo}-{hi}"
            elif lab.get("reference_text"):
                ref = lab["reference_text"]
            name = lab.get("test_name") or lab.get("canonical_name", "")
            lines.append(
                f"| {lab.get('date_effective', '')} | {name} "
                f"| {lab.get('value', '')} | {lab.get('unit', '')} "
                f"| {ref} | {lab.get('flag', '')} |"
            )
        return _safe_response("\n".join(lines))

    @mcp.tool()
    def get_medications(status: str = "active") -> str:
        """Get medication records.

        Args:
            status: Filter by status ("active", "discontinued", or "all").
        """
        if status not in ("active", "discontinued", "all"):
            return "Invalid status. Use 'active', 'discontinued', or 'all'."
        meds = clean_db.get_medications(status=status)
        if not meds:
            return "No medications found matching that status."

        lines = [
            "| Medication | Dose | Frequency | Status |",
            "|------------|------|-----------|--------|",
        ]
        for med in meds:
            dose = med.get("dose", "")
            if med.get("unit"):
                dose = f"{dose} {med['unit']}"
            lines.append(
                f"| {med.get('name', '')} | {dose} "
                f"| {med.get('frequency', '')} | {med.get('status', '')} |"
            )
        return _safe_response("\n".join(lines))

    @mcp.tool()
    def get_wearable_data(
        days: int = 7,
        provider: str = "whoop",
    ) -> str:
        """Get wearable device data (HRV, sleep, recovery, strain).

        Args:
            days: Number of days of data to return (default 7, max 365).
            provider: Wearable provider ("whoop" or "oura").
        """
        data = clean_db.get_wearable_data(
            days=max(1, min(days, 365)), provider=provider,
        )
        if not data:
            return "No wearable data found for the requested period."

        lines = [
            "| Date | HRV | RHR | Sleep | Recovery | Strain |",
            "|------|-----|-----|-------|----------|--------|",
        ]
        for day in data:
            hrv = f"{day['hrv']:.0f}" if day.get("hrv") is not None else "-"
            rhr = f"{day['rhr']:.0f}" if day.get("rhr") is not None else "-"
            slp = f"{day['sleep_score']:.0f}" if day.get("sleep_score") is not None else "-"
            rec_val = day.get("recovery_score")
            rec = f"{rec_val:.0f}%" if rec_val is not None else "-"
            strain = f"{day['strain']:.1f}" if day.get("strain") is not None else "-"
            lines.append(
                f"| {day.get('date', '')} | {hrv} | {rhr} "
                f"| {slp} | {rec} | {strain} |"
            )
        return _safe_response("\n".join(lines))

    @mcp.tool()
    def get_health_summary() -> str:
        """Get a comprehensive health summary including demographics,
        recent labs, active medications, wearable trends, and hypotheses."""
        return _safe_response(clean_db.get_health_summary_markdown())

    @mcp.tool()
    def search_health_data(query: str, limit: int = 20) -> str:
        """Search across all health data by keyword.

        Args:
            query: Search query (e.g., "iron", "sleep quality", "thyroid").
            limit: Maximum results (default 20).
        """
        results = clean_db.search(query, limit=max(1, min(limit, 100)))
        if not results:
            return "No results found matching the search query."

        lines: list[str] = []
        for r in results:
            source = r.get("source", "")
            if source == "lab":
                name = r.get("test_name", "")
                lines.append(
                    f"- **Lab** [{r.get('date', '')}]: {name} = "
                    f"{r.get('value', '')} {r.get('unit', '')} "
                    f"({r.get('flag', '') or 'normal'})"
                )
            elif source == "medication":
                lines.append(
                    f"- **Medication**: {r.get('name', '')} "
                    f"{r.get('dose', '')} {r.get('frequency', '')} "
                    f"({r.get('status', '')})"
                )
            elif source == "hypothesis":
                lines.append(
                    f"- **Hypothesis**: {r.get('title', '')} "
                    f"(confidence: {r.get('confidence', 0):.0%})"
                )
            elif source == "workout":
                dur = r.get("duration_minutes")
                dur_str = f"{dur:.0f}min" if dur else ""
                lines.append(
                    f"- **Workout** [{r.get('date', '')}]: "
                    f"{r.get('sport_type', '')} {dur_str}"
                )
            elif source == "genetic":
                lines.append(
                    f"- **Genetic**: {r.get('rsid', '')} "
                    f"{r.get('genotype', '')} — {r.get('phenotype', '')}"
                )
            elif source == "goal":
                lines.append(
                    f"- **Goal**: {r.get('goal_text', '')}"
                )
            else:
                lines.append(f"- {source}: {r}")

        return _safe_response("\n".join(lines))

    @mcp.tool()
    def get_health_trends(metric: str, days: int = 90) -> str:
        """Get trend analysis for a specific health metric over time.

        Args:
            metric: The metric to trend (e.g., "glucose", "HRV", "TSH").
            days: Number of days to analyze (default 90).
        """
        # Try lab results first
        labs = clean_db.get_lab_results(
            test_name=metric, limit=max(1, min(days, 365)),
        )
        if labs:
            lines = [
                f"## {metric} Trend ({len(labs)} results)\n",
                "| Date | Value | Unit | Flag |",
                "|------|-------|------|------|",
            ]
            for lab in labs:
                lines.append(
                    f"| {lab.get('date_effective', '')} "
                    f"| {lab.get('value', '')} "
                    f"| {lab.get('unit', '')} "
                    f"| {lab.get('flag', '')} |"
                )
            return _safe_response("\n".join(lines))

        # Try wearable metrics
        metric_lower = metric.lower()
        wearable_map = {
            "hrv": "hrv", "rhr": "rhr", "heart rate": "rhr",
            "sleep": "sleep_score", "recovery": "recovery_score",
            "strain": "strain", "spo2": "spo2",
        }
        wearable_field = wearable_map.get(metric_lower)
        if wearable_field:
            data = clean_db.get_wearable_data(days=max(1, min(days, 365)))
            if data:
                lines = [
                    f"## {metric} Trend ({len(data)} days)\n",
                    f"| Date | {metric} |",
                    "|------|--------|",
                ]
                for day in data:
                    val = day.get(wearable_field)
                    val_str = f"{val:.1f}" if val is not None else "-"
                    lines.append(f"| {day.get('date', '')} | {val_str} |")
                return _safe_response("\n".join(lines))

        return "No trend data found for the requested metric."

    @mcp.tool()
    def get_hypotheses() -> str:
        """Get active medical hypotheses with evidence and confidence."""
        hyps = clean_db.get_hypotheses()
        if not hyps:
            return "No active health hypotheses."

        lines: list[str] = []
        for h in hyps:
            title = h.get("title", "Untitled")
            conf = h.get("confidence")
            conf_str = f"{conf:.0%}" if conf is not None else "N/A"
            lines.append(f"### {title} (confidence: {conf_str})")
            lines.append(f"- Evidence for: {h.get('evidence_for', '[]')}")
            lines.append(f"- Evidence against: {h.get('evidence_against', '[]')}")
            lines.append(f"- Missing tests: {h.get('missing_tests', '[]')}")
            lines.append("")
        return _safe_response("\n".join(lines))

    # ── Skill system tools ─────────────────────────────────────────

    from healthbot.skills import HealthContext, SkillRegistry
    from healthbot.skills.builtin import register_builtin_skills

    _skill_registry = SkillRegistry()
    register_builtin_skills(_skill_registry)

    @mcp.tool()
    def list_skills() -> str:
        """List all available HealthBot skills with descriptions.

        Returns skill names, descriptions, and enabled status.
        """
        skills = _skill_registry.list_skills()
        if not skills:
            return "No skills registered."
        lines = [
            "| Skill | Description | Enabled |",
            "|-------|-------------|---------|",
        ]
        for s in skills:
            status = "yes" if s["enabled"] else "no"
            lines.append(f"| {s['name']} | {s['description']} | {status} |")
        return "\n".join(lines)

    @mcp.tool()
    def run_skill(skill_name: str) -> str:
        """Run a specific HealthBot skill by name.

        Args:
            skill_name: Name of the skill to run (use list_skills to see available skills).
        """
        # Build context from clean DB demographics
        demo = clean_db.get_demographics()
        ctx = HealthContext(
            user_id=0,
            sex=demo.get("sex") if demo else None,
            age=demo.get("age") if demo else None,
            ethnicity=demo.get("ethnicity") if demo else None,
            db=clean_db,
        )
        result = _skill_registry.run_skill(skill_name, ctx)
        if result is None:
            return f"Skill '{skill_name}' not found. Use list_skills to see available skills."
        lines = [f"## {result.skill_name}", f"**{result.summary}**", ""]
        if result.details:
            for d in result.details:
                lines.append(f"- {d}")
        lines.append(f"\nConfidence: {result.policy.value}")
        return _safe_response("\n".join(lines))

    return mcp
