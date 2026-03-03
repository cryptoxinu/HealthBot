"""Core lab PDF parser — entry points and main class.

Composes all mixins into the LabPdfParser class. Provides parse_bytes()
and extract_text_and_tables() as the main entry points.
"""
from __future__ import annotations

import logging
from datetime import date

from healthbot.data.models import LabResult
from healthbot.ingest.lab_pdf_parser.helpers import (
    HelpersMixin,
    _replace_result,
    _values_match,
)
from healthbot.ingest.lab_pdf_parser.merge_engine import MergeEngineMixin
from healthbot.ingest.lab_pdf_parser.ollama_parser import OllamaParserMixin
from healthbot.ingest.lab_pdf_parser.regex_parser import RegexParserMixin
from healthbot.ingest.lab_pdf_parser.text_extraction import TextExtractionMixin
from healthbot.security.pdf_safety import PdfSafety

logger = logging.getLogger("healthbot")


class LabPdfParser(
    HelpersMixin,
    TextExtractionMixin,
    RegexParserMixin,
    OllamaParserMixin,
    MergeEngineMixin,
):
    """Extract structured lab results from PDF reports.

    Dual extraction: regex (fast, Quest/LabCorp patterns) + Ollama LLM
    (single call, fast model). Both always run; results merged for accuracy.
    """

    def __init__(self, pdf_safety: PdfSafety, config: object | None = None) -> None:
        self._safety = pdf_safety
        self._config = config
        self._ollama_collection_date: date | None = None

    def extract_text_and_tables(self, data: bytes) -> tuple[str, str]:
        """Extract raw text and markdown tables from PDF without parsing.

        Returns (full_text, markdown_text). Used by Claude extraction
        to get content without running the full regex/Ollama pipeline.
        """
        _, markdown_text = self._parse_tables_direct(data, "")
        full_text = self._extract_text(data)
        return full_text or "", markdown_text

    def parse_bytes(
        self, data: bytes, blob_id: str = "",
        demographics: dict | None = None,
        on_progress: object | None = None,
    ) -> tuple[list[LabResult], str]:
        """Parse a lab PDF from bytes and return (results, extracted_text).

        Uses a multi-layer extraction pipeline:

        1. **Direct table parsing** — PyMuPDF ``find_tables()`` with column
           header detection maps cells directly to LabResult (confidence 0.95).
        2. **Regex** — four patterns for Quest/LabCorp text formats.
        3. **Ollama LLM** — medical model parses the text (or markdown table).
        4. **Three-way merge** — table > Ollama > regex, deduped by canonical name.
        """
        def _progress(msg: str) -> None:
            if on_progress:
                try:
                    on_progress(msg)
                except Exception:
                    pass

        self._safety.validate_bytes(data)

        # Stage 1: direct table extraction (highest confidence)
        _progress("Reading PDF tables...")
        table_results, markdown_text = self._parse_tables_direct(data, blob_id)

        # Stage 2: text extraction (for dates, regex, Ollama fallback)
        _progress("Extracting text from PDF...")
        full_text = self._extract_text(data)

        if not full_text and not table_results:
            return [], full_text or ""
        if not full_text:
            full_text = ""

        logger.debug(
            "PDF text extracted (%d chars). First 300: %s",
            len(full_text), full_text[:300],
        )

        # Stage 3: metadata (dates, lab name) from text
        collection_date = self._extract_date(
            full_text or markdown_text, demographics,
        )
        lab_name = self._extract_lab_name(full_text or markdown_text)

        # Stage 4: regex parsing (deterministic fallback)
        _progress("Parsing with regex patterns...")
        pages = full_text.split("\f") if full_text else []
        regex_results = self._regex_parse_all_pages(pages) if pages else []

        # Stage 5: Ollama parsing — send markdown table if available
        _progress("Running local AI analysis (may take a moment)...")
        self._ollama_collection_date = None
        ollama_input = markdown_text if markdown_text else full_text
        ollama_pages = ollama_input.split("\f") if ollama_input else []
        ollama_results = self._ollama_parse_pages(ollama_pages, blob_id)

        # Prefer Ollama-extracted collection date
        if self._ollama_collection_date:
            if self._validate_date(self._ollama_collection_date, demographics):
                collection_date = self._ollama_collection_date
                logger.info(
                    "Using Ollama-extracted collection date: %s",
                    collection_date,
                )

        # Stage 5.5: cross-validate and re-run Ollama on conflicts
        _progress("Cross-validating extraction results...")
        conflicts = self._find_conflicts(
            table_results, ollama_results, regex_results,
        )
        ollama_conflicts = {
            name for name, info in conflicts.items()
            if not info["consensus"]
            and "ollama" in info["sources"]
            and info["deterministic_value"] is not None
        }
        if ollama_conflicts and ollama_results:
            rerun = self._rerun_ollama_conflicts(
                ollama_pages, blob_id, ollama_conflicts,
            )
            for name in ollama_conflicts:
                det_val = conflicts[name]["deterministic_value"]
                orig_val = conflicts[name]["sources"]["ollama"]
                rerun_r = rerun.get(name)

                if rerun_r and _values_match(rerun_r.value, det_val):
                    # Ollama self-corrected → use corrected value
                    _replace_result(ollama_results, name, rerun_r)
                    conflicts[name]["consensus"] = True
                    conflicts[name]["conflict_note"] = None
                    logger.info(
                        "Ollama self-corrected %s: %s -> %s",
                        name, orig_val, rerun_r.value,
                    )
                elif rerun_r and _values_match(rerun_r.value, orig_val):
                    # Ollama consistent but disagrees → genuine ambiguity
                    conflicts[name]["conflict_note"] = (
                        f"Ollama consistently reads {orig_val}, "
                        f"but table/regex reads {det_val}"
                    )
                    logger.warning(
                        "Persistent conflict for %s: Ollama=%s vs "
                        "deterministic=%s",
                        name, orig_val, det_val,
                    )
                elif rerun_r is None:
                    # Ollama failed to re-parse → drop it, use deterministic
                    ollama_results = [
                        r for r in ollama_results
                        if r.canonical_name != name
                    ]
                    conflicts[name]["conflict_note"] = (
                        f"Ollama failed to re-parse {name}, "
                        f"using deterministic"
                    )
                    logger.warning(
                        "Ollama re-run failed for %s, "
                        "using deterministic=%s",
                        name, det_val,
                    )
                else:
                    # Ollama gave third value → unreliable, drop it
                    ollama_results = [
                        r for r in ollama_results
                        if r.canonical_name != name
                    ]
                    conflicts[name]["conflict_note"] = (
                        f"Ollama unreliable for {name} (3 different "
                        f"values), using deterministic"
                    )
                    logger.warning(
                        "Ollama unreliable for %s: run1=%s, run2=%s, "
                        "deterministic=%s",
                        name, orig_val, rerun_r.value, det_val,
                    )

        # Stage 6: three-way merge (with cross-validation confidence)
        if table_results:
            results = self._merge_three_way(
                table_results, ollama_results, regex_results, conflicts,
            )
            logger.info(
                "Table: %d, Ollama: %d, regex: %d, merged: %d results",
                len(table_results), len(ollama_results),
                len(regex_results), len(results),
            )
        elif ollama_results:
            results = self._merge_three_way(
                [], ollama_results, regex_results, conflicts,
            )
            logger.info(
                "Ollama: %d, regex: %d, merged: %d results",
                len(ollama_results), len(regex_results), len(results),
            )
        else:
            # Regex-only: still apply any conflict adjustments
            results = self._merge_three_way(
                [], [], regex_results, conflicts,
            )
            logger.info("Regex-only: %d results", len(results))

        # Stage 7: image extraction (fills gaps missed by text extraction)
        try:
            image_results = self._extract_from_images(data, blob_id)
        except Exception as exc:
            logger.debug("Image extraction failed: %s", exc)
            image_results = []
        if image_results:
            existing_names = {r.canonical_name for r in results}
            new_image = [
                r for r in image_results
                if r.canonical_name not in existing_names
            ]
            results.extend(new_image)
            logger.info(
                "Image extraction: %d total, %d new (not in text)",
                len(image_results), len(new_image),
            )

        # Self-validation warning
        combined_len = len(full_text) + len(markdown_text)
        if combined_len > 500 and len(results) < 3:
            logger.warning(
                "EXTRACTION WARNING: only %d results from %d chars of text. "
                "First 500 chars:\n%s",
                len(results), combined_len,
                (full_text or markdown_text)[:500],
            )

        # Stamp metadata on all results
        for r in results:
            r.date_collected = collection_date
            r.lab_name = lab_name
            r.source_blob_id = blob_id

        return results, full_text or markdown_text

    def _parse_tables_direct(
        self, data: bytes, blob_id: str,
    ) -> tuple[list[LabResult], str]:
        """Extract lab results directly from PDF table structure.

        Returns ``(results, markdown_text)``.  *results* are high-confidence
        LabResult objects mapped from table cells.  *markdown_text* is a
        markdown rendering of all tables for Ollama consumption.
        """
        try:
            from healthbot.ingest.lab_table_parser import (
                extract_tables_multi_strategy,
                identify_columns,
                infer_columns_from_content,
                parse_table_direct,
                tables_to_markdown,
            )
        except ImportError:
            return [], ""

        all_results: list[LabResult] = []

        try:
            all_tables = extract_tables_multi_strategy(data)
        except Exception as exc:
            logger.debug("Table extraction failed: %s", exc)
            all_tables = []

        for rows, page_num in all_tables:
            if not rows or len(rows) < 2:
                continue

            # Try identifying columns from header row
            col_map = identify_columns(rows[0])
            header_idx = 0

            # Header might be row 1 (row 0 is a section title)
            if col_map is None and len(rows) >= 3:
                col_map = identify_columns(rows[1])
                if col_map is not None:
                    header_idx = 1

            # Fallback: infer columns from cell content
            if col_map is None:
                col_map = infer_columns_from_content(rows[1:])
                if col_map is not None:
                    header_idx = 0  # no header row to skip

            if col_map is None:
                logger.debug(
                    "Page %d: table with %d rows — columns not identified",
                    page_num, len(rows),
                )
                continue

            logger.info(
                "Page %d: table columns identified: %s",
                page_num, col_map,
            )
            table_results = parse_table_direct(
                rows, col_map, page_num, blob_id, header_row_idx=header_idx,
            )
            all_results.extend(table_results)

        # Generate markdown for Ollama
        try:
            markdown_text = tables_to_markdown(data)
        except Exception:
            markdown_text = ""

        if all_results:
            logger.info(
                "Direct table parsing: %d results extracted", len(all_results),
            )

        return all_results, markdown_text

    def _extract_from_images(
        self, data: bytes, blob_id: str,
    ) -> list[LabResult]:
        """Extract lab results from embedded images in the PDF.

        Runs image extraction → OCR → optional chart vision analysis.
        Parses OCR text through existing regex patterns.
        Image results get confidence capped at 0.50.
        """
        try:
            from healthbot.ingest.image_extractor import (
                analyze_chart_with_vision,
                extract_images_from_pdf,
                ocr_images,
            )
        except ImportError:
            return []

        images = extract_images_from_pdf(data)
        if not images:
            return []

        all_results: list[LabResult] = []

        # OCR pass
        ocr_results = ocr_images(images)
        for ocr_r in ocr_results:
            page_results = self._parse_result_lines(ocr_r.text, ocr_r.page_number)
            for r in page_results:
                r.confidence = 0.50  # lower than all text methods
            all_results.extend(page_results)

        # Vision pass (chart analysis) — required per plan
        try:
            ollama_url = "http://localhost:11434"
            ollama_timeout = 120
            if self._config:
                ollama_url = getattr(self._config, "ollama_url", ollama_url)
                ollama_timeout = getattr(self._config, "ollama_timeout", ollama_timeout)

            vision_results = analyze_chart_with_vision(
                images, ollama_url=ollama_url, timeout=ollama_timeout,
            )
            for vis_r in vision_results:
                page_results = self._parse_result_lines(vis_r.text, vis_r.page_number)
                for r in page_results:
                    r.confidence = 0.50
                all_results.extend(page_results)
        except Exception as e:
            logger.debug("Chart vision analysis skipped: %s", e)

        # Deduplicate within image results by canonical name
        seen: set[str] = set()
        deduped: list[LabResult] = []
        for r in all_results:
            if r.canonical_name not in seen:
                deduped.append(r)
                seen.add(r.canonical_name)

        return deduped
