"""AI-ready anonymized health data export.

Three-layer PII validation pipeline:
  Layer 1: Assembly-time stripping (PII fields excluded by design)
  Layer 2: Regex + NER scan on assembled Markdown (PhiFirewall + GLiNER)
  Layer 3: Ollama LLM scan (optional, graceful fallback)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from healthbot.export.ai_export_analysis import (
    build_delta,
    build_intelligence_gaps,
    build_interactions,
    build_panel_gaps,
    build_therapeutic_response,
    build_trends,
    build_wearables,
)
from healthbot.export.ai_export_sections import (
    build_demographics,
    build_discovered_correlations,
    build_genetics,
    build_health_context,
    build_hypotheses,
    build_journal,
    build_labs,
    build_medications,
)
from healthbot.llm.anonymizer import Anonymizer
from healthbot.llm.ollama_client import OllamaClient
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")


@dataclass
class ValidationFinding:
    """A single PII finding from any validation layer."""

    layer: int  # 1, 2, or 3
    category: str
    description: str
    action: str  # "stripped", "redacted", "blocked"


@dataclass
class ValidationReport:
    """Results from the 3-layer validation pipeline."""

    findings: list[ValidationFinding] = field(default_factory=list)
    layer1_passed: bool = True
    layer2_passed: bool = True
    layer3_passed: bool | None = None  # None = skipped
    warnings: list[str] = field(default_factory=list)

    def add(self, layer: int, category: str, desc: str, action: str) -> None:
        self.findings.append(ValidationFinding(layer, category, desc, action))

    def summary(self) -> str:
        lines = ["Validation Report", "=" * 40]
        l1 = "PASS" if self.layer1_passed else "FAIL"
        lines.append(f"Layer 1 (Assembly-time stripping): {l1}")
        lines.append(f"Layer 2 (Regex + NER scan): {'PASS' if self.layer2_passed else 'FAIL'}")
        if self.layer3_passed is None:
            lines.append("Layer 3 (LLM scan): SKIPPED")
        else:
            lines.append(f"Layer 3 (LLM scan): {'PASS' if self.layer3_passed else 'FAIL'}")

        if self.findings:
            lines.append(f"\nFindings ({len(self.findings)}):")
            for f in self.findings:
                lines.append(f"  L{f.layer} [{f.category}] {f.description} -> {f.action}")

        if self.warnings:
            lines.append("\nWarnings:")
            for w in self.warnings:
                lines.append(f"  {w}")

        stripped = sum(1 for f in self.findings if f.action == "stripped")
        redacted = sum(1 for f in self.findings if f.action == "redacted")
        if stripped or redacted:
            lines.append(f"\n{stripped} fields stripped, {redacted} items redacted")

        return "\n".join(lines)


@dataclass
class AiExportResult:
    """Final export result."""

    markdown: str
    validation: ValidationReport
    file_path: Path | None = None


class AiExporter:
    """Export all health data as anonymized, AI-ready Markdown."""

    def __init__(
        self,
        db: object,
        anonymizer: Anonymizer,
        phi_firewall: PhiFirewall,
        ollama: OllamaClient | None = None,
        key_manager: object | None = None,
    ) -> None:
        self._db = db
        self._anon = anonymizer
        self._fw = phi_firewall
        self._ollama = ollama
        self._km = key_manager

    def export(self, user_id: int) -> AiExportResult:
        report = ValidationReport()
        sections = self._assemble_sections(user_id, report)
        markdown = self._render_markdown(sections)
        markdown = self._validate_layer2(markdown, report)
        markdown = self._validate_layer3(markdown, report)
        return AiExportResult(markdown=markdown, validation=report)

    def export_to_file(self, user_id: int, exports_dir: Path) -> AiExportResult:
        result = self.export(user_id)
        exports_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        if self._km:
            import os

            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            path = exports_dir / f"health_export_ai_{ts}.enc"
            key = self._km.get_key()
            nonce = os.urandom(12)
            aad = f"ai_export.{ts}".encode()
            ct = AESGCM(key).encrypt(
                nonce, result.markdown.encode("utf-8"), aad,
            )
            path.write_bytes(nonce + ct)
        else:
            path = exports_dir / f"health_export_ai_{ts}.md"
            path.write_text(result.markdown, encoding="utf-8")

        result.file_path = path
        self._cleanup_old_exports(exports_dir)
        return result

    @staticmethod
    def _cleanup_old_exports(exports_dir: Path, keep: int = 3) -> None:
        """Remove old AI export files, keeping only the most recent."""
        exports = sorted(
            list(exports_dir.glob("health_export_ai_*.md"))
            + list(exports_dir.glob("health_export_ai_*.enc")),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for old in exports[keep:]:
            try:
                old.unlink()
            except OSError:
                pass

    # ── Layer 1: Assembly ────────────────────────────────

    def _assemble_sections(self, user_id: int, report: ValidationReport) -> dict[str, str]:
        db = self._db
        return {
            "demographics": build_demographics(db, user_id, report),
            "labs": build_labs(db, user_id, report),
            "medications": build_medications(db, user_id, report),
            "trends": build_trends(db, user_id, report),
            "delta": build_delta(db, user_id, report),
            "interactions": build_interactions(db, user_id, report),
            "therapeutic_response": build_therapeutic_response(db, user_id, report),
            "wearables": build_wearables(db, user_id, report),
            "discovered_correlations": build_discovered_correlations(db, user_id, report),
            "hypotheses": build_hypotheses(db, user_id, report),
            "intelligence_gaps": build_intelligence_gaps(db, user_id, report),
            "panel_gaps": build_panel_gaps(db, user_id, report),
            "genetics": build_genetics(db, user_id, report),
            "health_context": build_health_context(db, user_id, report, self._anon),
            "journal": build_journal(db, user_id, report, self._anon),
            "schema_evolution": self._section_schema_evolution(),
        }

    def _section_schema_evolution(self) -> str:
        """Schema evolution history — what tables Claude created autonomously."""
        try:
            from healthbot.data.clean_db import CleanDB
            km = self._km
            if not km:
                return ""
            config = getattr(self._db, "_config", None)
            if not config:
                return ""
            clean_path = getattr(config, "clean_db_path", None)
            if not clean_path or not clean_path.exists():
                return ""
            clean = CleanDB(clean_path, phi_firewall=self._fw)
            clean.open(clean_key=km.get_clean_key())
            try:
                events = clean.get_schema_evolution_log(limit=20)
            finally:
                clean.close()
        except Exception:
            return ""

        if not events:
            return ""

        lines = ["## Schema Evolution History\n"]
        for ev in events:
            status_icon = "OK" if ev["status"] == "success" else "FAILED"
            lines.append(f"### {ev['data_type']} [{status_icon}] — {ev['created_at']}")
            lines.append(f"**Reason**: {ev['reason']}")
            lines.append(f"**Changes**: {ev['changes_summary']}")
            lines.append(f"**Files**: {', '.join(ev['files_modified'])}")
            if ev["ddl_executed"]:
                lines.append("**DDL**:")
                for ddl in ev["ddl_executed"]:
                    lines.append(f"```sql\n{ddl}\n```")
            if ev["error_message"]:
                lines.append(f"**Error**: {ev['error_message']}")
            lines.append("")
        return "\n".join(lines)

    def _render_markdown(self, sections: dict[str, str]) -> str:
        today = date.today().isoformat()
        parts = [
            "# Health Data Export (AI-Ready, Anonymized)",
            "",
            f"> Generated: {today}",
            "> This export has been processed through a 3-layer PII validation pipeline.",
            "> All personally identifiable information has been stripped or redacted.",
            "> Lab collection dates are preserved for clinical timeline analysis.",
            "",
            "## Demographics", "", sections["demographics"], "",
            "## Lab Results", "", sections["labs"], "",
            "## Active Medications", "", sections["medications"], "",
            "## Lab Trends", "", sections["trends"], "",
            "## What Changed Since Last Panel", "", sections["delta"], "",
            "## Drug-Lab Interactions", "", sections["interactions"], "",
            "## Medication-Lab Correlations", "", sections["therapeutic_response"], "",
            "## Wearable Data", "", sections["wearables"], "",
            "## Discovered Correlations", "", sections["discovered_correlations"], "",
            "## Health Hypotheses", "", sections["hypotheses"], "",
            "## Intelligence Gaps", "", sections["intelligence_gaps"], "",
            "## Panel Gaps", "", sections["panel_gaps"], "",
            "## Genetic Risk Profile", "", sections["genetics"], "",
            "## Health Context", "", sections["health_context"], "",
            "## Medical Journal (Recent)", "", sections["journal"], "",
        ]
        # Append schema evolution section if present
        if sections.get("schema_evolution"):
            parts.extend([sections["schema_evolution"], ""])
        parts.extend([
            "## Instructions for AI",
            "",
            "You are reviewing an anonymized health data export. Key guidelines:",
            "- All PII has been removed. Do not ask for identifying information.",
            "- Lab dates are real collection dates. Use them for trend analysis.",
            "- Exact age, height, weight, and BMI are provided. Date of birth is excluded.",
            "- Lab Trends show significant changes. Spot developing conditions.",
            "- What Changed Since Last Panel shows the delta between two most recent panels.",
            "- Medication-Lab Correlations show temporal effects of starting medications.",
            "- Drug-Lab Interactions flag medication effects on labs. Prioritize flagged.",
            "- Intelligence Gaps list missing tests and unfollowed abnormals. Resolve HIGH first.",
            "- Panel Gaps show incomplete panels. Recommend completing for full diagnosis.",
            "- Cross-reference wearable trends and anomalies with lab results for correlations.",
            "- Cross-reference genetic risk findings with lab values and trends.",
            "- Review hypotheses and update confidence based on all available evidence.",
            "- When new data changes the picture, say so directly.",
            "- Be direct and specific in medical analysis.",
        ])
        return "\n".join(parts)

    # ── Layer 2: Regex + NER scan ────────────────────────

    def _validate_layer2(self, markdown: str, report: ValidationReport) -> str:
        from healthbot.llm.anonymize_pipeline import AnonymizePipeline

        pipeline = AnonymizePipeline(
            self._anon, max_passes=2, fallback="redact_all",
        )
        result = pipeline.process(markdown)

        if result.had_phi:
            report.add(2, "phi_detected", "PII found in assembled Markdown", "redacted")

        for event in result.audit_trail:
            report.add(
                2, event.category,
                f"Layer {event.layer}: confidence={event.confidence:.2f}",
                "redacted",
            )

        report.layer2_passed = result.redaction_score >= 0.6
        return result.text

    # ── Layer 3: Ollama LLM scan ─────────────────────────

    def _validate_layer3(self, markdown: str, report: ValidationReport) -> str:
        from healthbot.llm.anonymizer_llm import _LLM_PII_PROMPT, _parse_llm_response

        if not self._ollama:
            report.layer3_passed = None
            report.warnings.append("Layer 3 skipped: no Ollama client provided")
            return markdown

        if not self._ollama.is_available():
            report.layer3_passed = None
            report.warnings.append("Layer 3 skipped: Ollama not available")
            return markdown

        try:
            response = self._ollama.send(
                prompt=f"Scan this text for PII:\n\n{markdown}",
                system=_LLM_PII_PROMPT,
            )
            result = _parse_llm_response(response)

            if result.get("found"):
                report.layer3_passed = False
                for item in result.get("items", []):
                    text = item.get("text", "")
                    pii_type = item.get("type", "unknown")
                    report.add(3, f"llm_{pii_type}", f"LLM detected: '{text}'", "redacted")
                    if text and text in markdown:
                        markdown = markdown.replace(text, f"[REDACTED-llm_{pii_type}]")
            else:
                report.layer3_passed = True

        except Exception as e:
            report.layer3_passed = None
            report.warnings.append(f"Layer 3 error (non-fatal): {e}")
            logger.warning("Layer 3 LLM PII scan failed: %s", e)

        return markdown
