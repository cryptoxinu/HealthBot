"""In-memory PDF generation for doctor packets.

Uses fpdf2. PDF bytes NEVER touch disk as plaintext.
All generation happens in memory via ``pdf.output()`` which returns bytes.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from fpdf import FPDF

# Regex to match internal vault IDs: hex UUIDs with or without dashes,
# and _id fields like ``_id: <value>``.
_INTERNAL_ID_RE = re.compile(
    r"\b[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}\b"
    r"|_id\s*[:=]\s*\S+",
    re.IGNORECASE,
)


def _strip_internal_ids(text: str) -> str:
    """Remove internal vault IDs (UUIDs, _id fields) from doctor-facing text."""
    return _INTERNAL_ID_RE.sub("", text).strip()


@dataclass
class PrepData:
    """All data needed to generate a doctor visit preparation packet."""

    generated_date: str = ""
    urgent_items: list[dict] = field(default_factory=list)
    # Each: {level, name, value, unit, date, citation}
    trends: list[dict] = field(default_factory=list)
    # Each: {test_name, direction, pct_change, first_val, last_val, dates}
    medications: list[dict] = field(default_factory=list)
    # Each: {name, dose, frequency}
    overdue_items: list[dict] = field(default_factory=list)
    # Each: {test_name, last_date, months_overdue}
    panel_gaps: list[dict] = field(default_factory=list)
    # Each: {panel_name, missing_tests, reason}
    questions: list[str] = field(default_factory=list)
    trend_tables: list[dict] = field(default_factory=list)
    # Each: {test_name, values: list of (date, value, unit)}
    citations: list[dict] = field(default_factory=list)
    # Each: {record_id, source, page, section, date}


class DoctorPacketPdf:
    """Generates a doctor visit preparation packet as in-memory PDF bytes."""

    _TITLE = "Doctor Visit Preparation Packet"
    _FOOTER_NOTE = (
        "Personal Health Summary - Generated from encrypted vault data."
    )

    def generate(self, data: PrepData) -> bytes:
        """Build the PDF in memory and return raw bytes. Never writes to disk."""
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=20)

        # Try to load a Unicode-capable font so non-Latin characters render.
        self._setup_unicode_font(pdf)

        self._add_summary_page(pdf, data)
        self._add_appendix(pdf, data)

        return bytes(pdf.output())

    @staticmethod
    def _setup_unicode_font(pdf: FPDF) -> None:
        """Register a Unicode font if available, else fall back to built-in."""
        # fpdf2 ships with DejaVuSans; try common system paths too.
        candidates = [
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
            Path("/Library/Fonts/Arial Unicode.ttf"),
        ]
        for ttf in candidates:
            if ttf.exists():
                try:
                    pdf.add_font("DejaVu", "", str(ttf), uni=True)
                    pdf.add_font("DejaVu", "B", str(ttf), uni=True)
                    pdf.add_font("DejaVu", "I", str(ttf), uni=True)
                    return  # Success — caller can use "DejaVu" as family
                except Exception:
                    continue
        # Fallback: stick with Helvetica (latin-1 only, but always available)

    # ------------------------------------------------------------------
    # Page 1+: Summary
    # ------------------------------------------------------------------

    def _add_summary_page(self, pdf: FPDF, data: PrepData) -> None:
        pdf.add_page()

        # Title
        pdf.set_font("Helvetica", "B", 18)
        pdf.cell(0, 12, self._TITLE, new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(2)

        # Date generated
        gen_date = data.generated_date or datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 6, f"Generated: {gen_date}", new_x="LMARGIN", new_y="NEXT", align="C")
        pdf.ln(6)

        # Section 1: Items Requiring Attention
        self._section_header(pdf, "1. Items Requiring Attention")
        if data.urgent_items:
            for item in data.urgent_items:
                level = item.get("level", "")
                name = item.get("name", "")
                value = item.get("value", "")
                unit = item.get("unit", "")
                dt = item.get("date", "")
                cite = item.get("citation", "")
                line = f"[{level}] {name}: {value} {unit}"
                if dt:
                    line += f"  ({dt})"
                if cite:
                    line += f"  [ref: {cite}]"
                self._body_text(pdf, line)
        else:
            self._body_text(pdf, "No urgent items identified.")
        pdf.ln(3)

        # Section 2: Notable Trends
        self._section_header(pdf, "2. Notable Trends")
        if data.trends:
            for t in data.trends:
                name = t.get("test_name", "")
                direction = t.get("direction", "")
                pct = t.get("pct_change", "")
                first_v = t.get("first_val", "")
                last_v = t.get("last_val", "")
                line = f"{name}: {direction} {pct}% ({first_v} -> {last_v})"
                self._body_text(pdf, line)
        else:
            self._body_text(pdf, "No notable trends detected.")
        pdf.ln(3)

        # Section 3: Active Medications
        self._section_header(pdf, "3. Active Medications")
        if data.medications:
            for med in data.medications:
                name = med.get("name", "")
                dose = med.get("dose", "")
                freq = med.get("frequency", "")
                self._body_text(pdf, f"{name} {dose} - {freq}")
        else:
            self._body_text(pdf, "No active medications recorded.")
        pdf.ln(3)

        # Section 4: Overdue Screenings
        self._section_header(pdf, "4. Overdue Screenings")
        if data.overdue_items:
            for od in data.overdue_items:
                name = od.get("test_name", "")
                last = od.get("last_date", "unknown")
                months = od.get("months_overdue", "?")
                self._body_text(pdf, f"{name} - last: {last}, overdue by {months} months")
        else:
            self._body_text(pdf, "All screenings appear up to date.")
        pdf.ln(3)

        # Section 5: Panel Gap Suggestions
        self._section_header(pdf, "5. Panel Gap Suggestions")
        if data.panel_gaps:
            for gap in data.panel_gaps:
                panel = gap.get("panel_name", "")
                missing = gap.get("missing_tests", "")
                reason = gap.get("reason", "")
                self._body_text(pdf, f"{panel}: missing {missing}")
                if reason:
                    self._body_text(pdf, f"  Context: {reason}")
        else:
            self._body_text(pdf, "No panel gaps identified.")
        pdf.ln(3)

        # Section 6: Questions for Your Doctor
        self._section_header(pdf, "6. Questions for Your Doctor")
        if data.questions:
            for i, q in enumerate(data.questions, 1):
                self._body_text(pdf, f"{i}. {q}")
        else:
            self._body_text(pdf, "No questions queued.")
        pdf.ln(6)

        # Footer note
        pdf.set_font("Helvetica", "I", 8)
        pdf.multi_cell(0, 4, self._FOOTER_NOTE, new_x="LMARGIN", new_y="NEXT")

    # ------------------------------------------------------------------
    # Appendix: Trend tables + Citations
    # ------------------------------------------------------------------

    def _add_appendix(self, pdf: FPDF, data: PrepData) -> None:
        has_trends = bool(data.trend_tables)
        has_citations = bool(data.citations)
        if not has_trends and not has_citations:
            return

        pdf.add_page()
        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 10, "Appendix", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(4)

        # Trend tables
        if has_trends:
            self._section_header(pdf, "A. Trend Data Tables")
            for table in data.trend_tables:
                test_name = table.get("test_name", "")
                values = table.get("values", [])
                pdf.set_font("Helvetica", "B", 10)
                pdf.cell(0, 7, test_name, new_x="LMARGIN", new_y="NEXT")

                # Table header
                pdf.set_font("Helvetica", "B", 9)
                col_w_date = 50
                col_w_val = 40
                col_w_unit = 40
                pdf.cell(col_w_date, 6, "Date", border=1)
                pdf.cell(col_w_val, 6, "Value", border=1)
                pdf.cell(col_w_unit, 6, "Unit", border=1, new_x="LMARGIN", new_y="NEXT")

                # Table rows
                pdf.set_font("Helvetica", "", 9)
                for row in values:
                    if isinstance(row, (list, tuple)) and len(row) >= 3:
                        r_date, r_val, r_unit = str(row[0]), str(row[1]), str(row[2])
                    elif isinstance(row, dict):
                        r_date = str(row.get("date", ""))
                        r_val = str(row.get("value", ""))
                        r_unit = str(row.get("unit", ""))
                    else:
                        continue
                    # Page break check: add new page if near bottom
                    if pdf.get_y() > pdf.h - 30:
                        pdf.add_page()
                    # Truncate long cell text with ellipsis indicator
                    r_date = self._truncate_cell(r_date, col_w_date, pdf)
                    r_val = self._truncate_cell(r_val, col_w_val, pdf)
                    r_unit = self._truncate_cell(r_unit, col_w_unit, pdf)
                    pdf.cell(col_w_date, 6, r_date, border=1)
                    pdf.cell(col_w_val, 6, r_val, border=1)
                    pdf.cell(col_w_unit, 6, r_unit, border=1, new_x="LMARGIN", new_y="NEXT")
                pdf.ln(4)

        # Citations (internal vault IDs stripped for doctor-facing output)
        if has_citations:
            self._section_header(pdf, "B. Source Citations")
            for cite in data.citations:
                rec_id = _strip_internal_ids(cite.get("record_id", ""))
                source = _strip_internal_ids(cite.get("source", ""))
                page = cite.get("page", "")
                section = _strip_internal_ids(cite.get("section", ""))
                dt = cite.get("date", "")
                parts: list[str] = []
                if rec_id:
                    parts.append(f"[{rec_id}]")
                if source:
                    parts.append(source)
                if page:
                    parts.append(f"p.{page}")
                if section:
                    parts.append(f"sec: {section}")
                if dt:
                    parts.append(f"({dt})")
                if parts:
                    self._body_text(pdf, " ".join(parts))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _section_header(self, pdf: FPDF, text: str) -> None:
        pdf.set_font("Helvetica", "B", 12)
        pdf.cell(0, 8, text, new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

    def _body_text(self, pdf: FPDF, text: str) -> None:
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, text, new_x="LMARGIN", new_y="NEXT")

    @staticmethod
    def _truncate_cell(text: str, col_width: float, pdf: FPDF) -> str:
        """Truncate text with ellipsis if it exceeds the cell width."""
        if pdf.get_string_width(text) <= col_width - 2:
            return text
        # Progressively shorten until it fits with ellipsis
        for end in range(len(text) - 1, 0, -1):
            candidate = text[:end] + "\u2026"
            if pdf.get_string_width(candidate) <= col_width - 2:
                return candidate
        return "\u2026"
