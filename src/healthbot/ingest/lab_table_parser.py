"""Structural table extraction from lab report PDFs.

Uses PyMuPDF's ``find_tables()`` to detect table structures, identifies
column headers, and maps cells directly to :class:`LabResult` objects —
bypassing the fragile text-to-regex pipeline.  Falls back to content-based
column inference when headers aren't recognisable, and produces markdown
for Ollama when direct mapping isn't possible.
"""
from __future__ import annotations

import logging
import re
import uuid
from difflib import SequenceMatcher

from healthbot.data.models import LabResult
from healthbot.normalize.lab_normalizer import normalize_test_name

logger = logging.getLogger("healthbot")

# ---------------------------------------------------------------------------
# Column header synonyms for fuzzy matching
# ---------------------------------------------------------------------------
_COLUMN_SYNONYMS: dict[str, list[str]] = {
    "test_name": [
        "tests", "test", "test name", "component", "analyte",
        "test ordered", "order name", "test description", "assay",
    ],
    "value": [
        "result", "value", "results", "your result", "your value",
        "result value", "observed",
    ],
    "unit": [
        "units", "unit", "uom", "unit of measure",
    ],
    "reference": [
        "reference interval", "reference range", "ref range",
        "standard range", "normal range", "range", "ref interval",
        "expected range", "expected values",
    ],
    "flag": [
        "flag", "flags", "status", "abnormal", "abnormal flag",
        "result flag", "alert",
    ],
    "lab": [
        "lab", "laboratory", "performing lab", "lab site",
    ],
}

_MATCH_THRESHOLD = 0.75

# Local copies of simple regexes (avoids circular import with lab_pdf_parser)
_HAS_LETTER = re.compile(r"[a-zA-Z]")

# Patterns for content-based column inference
_NUMERIC_RE = re.compile(r"^[<>=]*\s*\d+\.?\d*$")
_REF_RANGE_RE = re.compile(r"\d+\.?\d*\s*[-–]\s*\d+\.?\d*")
_KNOWN_UNITS = {
    "mg/dl", "g/dl", "mmol/l", "miu/l", "uiu/ml", "ng/ml", "pg/ml",
    "u/l", "fl", "pg", "%", "mm/hr", "meq/l", "ug/dl", "umol/l",
    "iu/l", "x10e3/ul", "x10e6/ul", "m/ul", "k/ul", "x10(3)/ul",
    "x10(6)/ul", "g/l", "mg/l", "ng/dl", "sec", "seconds",
    "ratio", "index", "iu/ml", "copies/ml", "cells/ul", "10*3/ul",
    "10*6/ul", "mill/ul", "thou/ul", "fmol/l",
}
_FLAG_VALUES = {"h", "l", "hh", "ll", "high", "low", "*", "a", ""}

# Reference range pattern for direct parsing (duplicated to avoid import)
_REF_RANGE = re.compile(
    r"([<>=]*\s*[\d.]+)\s*[-–]\s*([<>=]*\s*[\d.]+)",
)
_REF_UPPER = re.compile(r"[<≤]\s*([\d.]+)")
_REF_LOWER = re.compile(r"[>≥]\s*([\d.]+)")


# ---------------------------------------------------------------------------
# Column identification
# ---------------------------------------------------------------------------

def _fuzzy_match(cell_text: str, synonyms: list[str]) -> bool:
    """Check if *cell_text* matches any synonym via containment or similarity."""
    for syn in synonyms:
        if syn in cell_text or cell_text.startswith(syn):
            return True
        if SequenceMatcher(None, cell_text, syn).ratio() >= _MATCH_THRESHOLD:
            return True
    return False


def identify_columns(header_row: list[str | None]) -> dict[str, int] | None:
    """Map header cells to column roles (test_name, value, unit, etc.).

    Returns ``None`` if both ``test_name`` and ``value`` cannot be identified.
    """
    mapping: dict[str, int] = {}
    assigned_roles: set[str] = set()

    for i, cell in enumerate(header_row):
        if cell is None:
            continue
        cell_lower = str(cell).strip().lower()
        if not cell_lower:
            continue
        for role, synonyms in _COLUMN_SYNONYMS.items():
            if role in assigned_roles:
                continue
            if _fuzzy_match(cell_lower, synonyms):
                mapping[role] = i
                assigned_roles.add(role)
                break

    if "test_name" not in mapping or "value" not in mapping:
        return None
    return mapping


def infer_columns_from_content(
    rows: list[list[str | None]],
    sample_size: int = 8,
) -> dict[str, int] | None:
    """Infer column roles from cell content patterns when headers are absent.

    Examines the first *sample_size* rows to build a statistical profile
    of each column, then assigns roles based on dominant content type.
    """
    if not rows:
        return None

    n_cols = max(len(r) for r in rows)
    if n_cols < 2:
        return None

    sample = rows[:sample_size]
    n_rows = len(sample)
    if n_rows < 2:
        return None

    # Build per-column stats
    col_numeric: list[int] = [0] * n_cols
    col_unit: list[int] = [0] * n_cols
    col_ref: list[int] = [0] * n_cols
    col_flag: list[int] = [0] * n_cols
    col_text_len: list[float] = [0.0] * n_cols
    col_has_letters: list[int] = [0] * n_cols
    col_unique: list[set[str]] = [set() for _ in range(n_cols)]

    for row in sample:
        for col_idx in range(min(len(row), n_cols)):
            cell = row[col_idx]
            if cell is None:
                col_flag[col_idx] += 1
                continue
            val = str(cell).strip()
            if not val:
                col_flag[col_idx] += 1
                continue

            col_unique[col_idx].add(val.lower())
            col_text_len[col_idx] += len(val)

            if _HAS_LETTER.search(val):
                col_has_letters[col_idx] += 1

            val_lower = val.lower()
            if _NUMERIC_RE.match(val):
                col_numeric[col_idx] += 1
            if val_lower in _KNOWN_UNITS:
                col_unit[col_idx] += 1
            if _REF_RANGE_RE.search(val):
                col_ref[col_idx] += 1
            if val_lower in _FLAG_VALUES:
                col_flag[col_idx] += 1

    mapping: dict[str, int] = {}
    used_cols: set[int] = set()

    # Value column: highest proportion of numeric cells (>= 70%)
    best_val_col, best_val_pct = -1, 0.0
    for c in range(n_cols):
        pct = col_numeric[c] / n_rows if n_rows else 0
        if pct > best_val_pct and pct >= 0.7:
            best_val_col, best_val_pct = c, pct
    if best_val_col >= 0:
        mapping["value"] = best_val_col
        used_cols.add(best_val_col)

    # Unit column: highest proportion of known-unit cells (>= 50%)
    best_unit_col, best_unit_pct = -1, 0.0
    for c in range(n_cols):
        if c in used_cols:
            continue
        pct = col_unit[c] / n_rows if n_rows else 0
        if pct > best_unit_pct and pct >= 0.5:
            best_unit_col, best_unit_pct = c, pct
    if best_unit_col >= 0:
        mapping["unit"] = best_unit_col
        used_cols.add(best_unit_col)

    # Reference column: highest proportion of dash-range cells (>= 40%)
    best_ref_col, best_ref_pct = -1, 0.0
    for c in range(n_cols):
        if c in used_cols:
            continue
        pct = col_ref[c] / n_rows if n_rows else 0
        if pct > best_ref_pct and pct >= 0.4:
            best_ref_col, best_ref_pct = c, pct
    if best_ref_col >= 0:
        mapping["reference"] = best_ref_col
        used_cols.add(best_ref_col)

    # Flag column: high flag proportion, few unique values
    best_flag_col, best_flag_pct = -1, 0.0
    for c in range(n_cols):
        if c in used_cols:
            continue
        pct = col_flag[c] / n_rows if n_rows else 0
        if pct > best_flag_pct and pct >= 0.3 and len(col_unique[c]) <= 6:
            best_flag_col, best_flag_pct = c, pct
    if best_flag_col >= 0:
        mapping["flag"] = best_flag_col
        used_cols.add(best_flag_col)

    # Test name column: longest avg text with letters among remaining
    best_name_col = -1
    best_avg_len = 0.0
    for c in range(n_cols):
        if c in used_cols:
            continue
        avg_len = col_text_len[c] / n_rows if n_rows else 0
        if col_has_letters[c] >= n_rows * 0.5 and avg_len > best_avg_len:
            best_name_col, best_avg_len = c, avg_len
    if best_name_col >= 0:
        mapping["test_name"] = best_name_col
        used_cols.add(best_name_col)

    if "test_name" not in mapping or "value" not in mapping:
        return None
    return mapping


# ---------------------------------------------------------------------------
# Reference range parsing (local copy to avoid circular import)
# ---------------------------------------------------------------------------

def _parse_ref_range(text: str) -> tuple[float | None, float | None]:
    """Parse reference range text into (low, high)."""
    m = _REF_RANGE.search(text)
    if m:
        try:
            low = float(re.sub(r"[<>=\s]", "", m.group(1)))
            high = float(re.sub(r"[<>=\s]", "", m.group(2)))
            return low, high
        except ValueError:
            pass
    m = _REF_UPPER.search(text)
    if m:
        try:
            return None, float(m.group(1))
        except ValueError:
            pass
    m = _REF_LOWER.search(text)
    if m:
        try:
            return float(m.group(1)), None
        except ValueError:
            pass
    return None, None


def _normalize_flag(flag: str) -> str:
    """Normalize flag to single-letter H/L format."""
    if not flag:
        return ""
    f = flag.strip().upper()
    if f in ("LOW", "L"):
        return "L"
    if f in ("HIGH", "H"):
        return "H"
    return f


# ---------------------------------------------------------------------------
# Direct table -> LabResult conversion
# ---------------------------------------------------------------------------

def parse_table_direct(
    rows: list[list[str | None]],
    col_map: dict[str, int],
    page_num: int,
    blob_id: str = "",
    header_row_idx: int = 0,
) -> list[LabResult]:
    """Convert table data rows to LabResult objects using a column mapping.

    Skips the header row at *header_row_idx* and any rows where the test
    name is invalid or the value is non-numeric.  Confidence is set to
    0.95 (structural table data).
    """
    # Lazy import to avoid circular dependency
    from healthbot.ingest.lab_pdf_parser import (
        _BAD_TEST_NAMES,
        _COMMA_TEST_NAMES,
    )

    results: list[LabResult] = []
    seen_canonical: set[str] = set()

    def _cell(row: list, field: str) -> str:
        idx = col_map.get(field)
        if idx is None or idx >= len(row):
            return ""
        val = row[idx]
        return str(val).strip() if val is not None else ""

    for row_idx, row in enumerate(rows):
        if row_idx <= header_row_idx:
            continue

        test_name = _cell(row, "test_name")
        value_str = _cell(row, "value")

        if not test_name or not value_str:
            continue

        # Blocklist checks
        if not _HAS_LETTER.search(test_name):
            continue
        if _BAD_TEST_NAMES.search(test_name):
            if test_name.strip().lower() not in _COMMA_TEST_NAMES:
                continue

        # Parse numeric value
        value_str = value_str.replace(",", "")
        try:
            value: float | str = float(value_str)
        except ValueError:
            continue  # Skip non-numeric (e.g., "Non-Reactive")

        unit = _cell(row, "unit")
        flag = _normalize_flag(_cell(row, "flag"))

        ref_text = _cell(row, "reference")
        ref_low, ref_high = _parse_ref_range(ref_text)

        canonical = normalize_test_name(test_name)
        if canonical in seen_canonical:
            continue
        seen_canonical.add(canonical)

        results.append(LabResult(
            id=uuid.uuid4().hex,
            test_name=test_name,
            canonical_name=canonical,
            value=value,
            unit=unit,
            reference_low=ref_low,
            reference_high=ref_high,
            reference_text=ref_text,
            flag=flag,
            source_blob_id=blob_id,
            source_page=page_num,
            confidence=0.95,
        ))

    return results


# ---------------------------------------------------------------------------
# Multi-strategy table extraction
# ---------------------------------------------------------------------------

def extract_tables_multi_strategy(
    data: bytes,
) -> list[tuple[list[list[str | None]], int]]:
    """Extract tables from PDF using multiple PyMuPDF detection strategies.

    Returns a list of ``(rows, page_number)`` tuples.  Tries default
    line-based detection first, then text-based detection as a fallback
    for PDFs without visible grid lines.
    """
    try:
        import fitz  # noqa: F811
    except ImportError:
        return []

    doc = fitz.open(stream=data, filetype="pdf")
    all_tables: list[tuple[list[list[str | None]], int]] = []
    seen: set[tuple[int, int, str]] = set()

    for page in doc:
        page_num = page.number + 1
        found_on_page = False

        # Strategy 1: default (line-based)
        try:
            tf = page.find_tables()
            for table in tf.tables:
                rows = table.extract()
                if rows and len(rows) >= 2:
                    first_cell = str(rows[0][0]) if rows[0] else ""
                    key = (page_num, len(rows), first_cell)
                    if key not in seen:
                        seen.add(key)
                        all_tables.append((rows, page_num))
                        found_on_page = True
        except Exception as exc:
            logger.debug("find_tables default p%d: %s", page_num, exc)

        if found_on_page:
            continue

        # Strategy 2: text-based detection (no grid lines needed)
        try:
            tf = page.find_tables(strategy="text")
            for table in tf.tables:
                rows = table.extract()
                if rows and len(rows) >= 2:
                    first_cell = str(rows[0][0]) if rows[0] else ""
                    key = (page_num, len(rows), first_cell)
                    if key not in seen:
                        seen.add(key)
                        all_tables.append((rows, page_num))
        except Exception as exc:
            logger.debug("find_tables text p%d: %s", page_num, exc)

    page_count = doc.page_count
    doc.close()

    if all_tables:
        total_rows = sum(len(r) for r, _ in all_tables)
        logger.info(
            "Table extraction: %d tables, %d total rows from %d pages",
            len(all_tables), total_rows, page_count,
        )
    return all_tables


def tables_to_markdown(data: bytes) -> str:
    """Convert all PDF tables to markdown for LLM consumption.

    Uses PyMuPDF's built-in ``to_markdown()``.  Each table is prefixed
    with its page number for context.
    """
    try:
        import fitz  # noqa: F811
    except ImportError:
        return ""

    doc = fitz.open(stream=data, filetype="pdf")
    md_parts: list[str] = []

    for page in doc:
        page_num = page.number + 1
        try:
            tf = page.find_tables()
            for table in tf.tables:
                if table.row_count < 2:
                    continue
                md = table.to_markdown()
                if md and md.strip():
                    md_parts.append(f"--- Page {page_num} ---\n{md}")
        except Exception:
            pass

    doc.close()
    return "\n\n".join(md_parts)
