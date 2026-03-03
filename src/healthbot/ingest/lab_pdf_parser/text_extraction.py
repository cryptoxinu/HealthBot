"""PDF text extraction strategies.

Multi-strategy text extraction: PyMuPDF tables, PyMuPDF text, pymupdf4llm,
pdfminer, and OCR fallback. Selects the strategy that finds the most lab
result lines.
"""
from __future__ import annotations

import io
import logging
import re
import sys

from pdfminer.high_level import extract_text  # noqa: F401 — re-exported for patching

from healthbot.ingest.lab_pdf_parser.regex_parser import _RESULT_PATTERNS

logger = logging.getLogger("healthbot")


def _get_extract_text():
    """Retrieve extract_text through the package module to support patching.

    Tests patch ``healthbot.ingest.lab_pdf_parser.extract_text`` at the
    package level.  This helper resolves through the package namespace at
    call time so that patches applied to the ``__init__`` module are
    respected.
    """
    pkg = sys.modules.get("healthbot.ingest.lab_pdf_parser")
    if pkg is not None and hasattr(pkg, "extract_text"):
        return pkg.extract_text
    return extract_text


class TextExtractionMixin:
    """Mixin providing PDF text extraction strategies."""

    def _extract_text(self, data: bytes) -> str:
        """Extract text from PDF using multiple strategies.

        Priority order (short-circuits when >= 3 lab lines found):
        1. PyMuPDF table extraction (structural — best for lab tables)
        2. PyMuPDF get_text (visual reading order)
        3. pdfminer default
        4. OCR fallback
        """
        best_text = ""
        best_count = 0

        def _score(text: str) -> int:
            """Count unique lines matching ANY result pattern."""
            matched_starts: set[int] = set()
            for pattern in _RESULT_PATTERNS:
                for m in pattern.finditer(text):
                    matched_starts.add(m.start())
            return len(matched_starts)

        # Strategy 1: PyMuPDF TABLE extraction — reads actual table structure
        try:
            from healthbot.ingest.lab_table_parser import (
                extract_tables_multi_strategy,
            )
            all_tables = extract_tables_multi_strategy(data)
            if all_tables:
                lines: list[str] = []
                for rows, _pn in all_tables:
                    for row in rows:
                        cells = [str(c).strip() for c in row if c is not None]
                        cells = [c for c in cells if c]
                        if cells:
                            lines.append("  ".join(cells))
                table_text = self._normalize_text("\n".join(lines))
                count = _score(table_text)
                logger.info(
                    "PDF strategy pymupdf-tables: %d lab lines in %d chars",
                    count, len(table_text),
                )
                if count >= 3:
                    return table_text
                if count > best_count:
                    best_text, best_count = table_text, count
        except ImportError:
            pass
        except Exception as e:
            logger.debug("PyMuPDF table extraction failed: %s", e)

        # Strategy 2: PyMuPDF text (visual reading order)
        try:
            import fitz
            doc = fitz.open(stream=data, filetype="pdf")
            pages = [page.get_text(sort=True) for page in doc]
            doc.close()
            text = self._normalize_text("\n\f\n".join(pages))
            if text.strip():
                count = _score(text)
                logger.info(
                    "PDF strategy pymupdf-text: %d lab lines in %d chars",
                    count, len(text),
                )
                if count >= 3:
                    return text
                if count > best_count:
                    best_text, best_count = text, count
        except ImportError:
            pass
        except Exception as e:
            logger.debug("PyMuPDF text failed: %s", e)

        # Strategy 2b: pymupdf4llm layout-aware extraction
        try:
            import pymupdf4llm
            md_text = pymupdf4llm.to_markdown(
                fitz.open(stream=data, filetype="pdf"),
            )
            if md_text:
                text = self._normalize_text(md_text)
                count = _score(text)
                logger.info(
                    "PDF strategy pymupdf4llm: %d lab lines in %d chars",
                    count, len(text),
                )
                if count >= 3:
                    return text
                if count > best_count:
                    best_text, best_count = text, count
        except ImportError:
            pass
        except Exception as e:
            logger.debug("pymupdf4llm extraction failed: %s", e)

        # Strategy 3: pdfminer default
        try:
            _extract_fn = _get_extract_text()
            text = self._normalize_text(_extract_fn(io.BytesIO(data)))
            if text.strip():
                count = _score(text)
                logger.info(
                    "PDF strategy pdfminer: %d lab lines in %d chars",
                    count, len(text),
                )
                if count >= 3:
                    return text
                if count > best_count:
                    best_text, best_count = text, count
        except Exception as e:
            logger.debug("pdfminer failed: %s", e)

        # Strategy 4: OCR fallback
        if best_count < 3:
            try:
                from healthbot.ingest.ocr_fallback import ocr_pdf_bytes
                ocr_text = ocr_pdf_bytes(data)
                if ocr_text:
                    ocr_text = self._normalize_text(ocr_text)
                    count = _score(ocr_text)
                    logger.info(
                        "PDF strategy OCR: %d lab lines in %d chars",
                        count, len(ocr_text),
                    )
                    if count > best_count:
                        best_text, best_count = ocr_text, count
            except Exception as e:
                logger.debug("OCR failed: %s", e)

        logger.info(
            "PDF extraction: best strategy found %d lab lines", best_count,
        )
        return best_text

    @staticmethod
    def _normalize_text(text: str) -> str:
        """Normalize text extracted from PDFs.

        Fixes common pdfminer issues: non-breaking spaces, stray control
        characters, and inconsistent whitespace that break regex patterns.
        """
        # Replace non-breaking spaces and other Unicode spaces with regular space
        text = text.replace("\u00a0", " ")
        text = text.replace("\u2007", " ")  # figure space
        text = text.replace("\u202f", " ")  # narrow no-break space
        # Normalize dashes: em-dash and figure dash to en-dash (our regex uses [-–])
        text = text.replace("\u2014", "\u2013")  # em-dash → en-dash
        text = text.replace("\u2012", "\u2013")  # figure dash → en-dash
        # Strip control characters (except newline, tab, form feed)
        text = re.sub(r"[^\x09\x0a\x0c\x20-\x7e\u00a0-\uffff]", "", text)
        # Normalize runs of spaces on each line (but preserve line structure)
        lines = text.split("\n")
        cleaned = []
        for line in lines:
            # Repair broken words: pdfminer often inserts 1-2 extra spaces
            # mid-word (e.g. "Specime  n" → "Specimen", "Patie  nt" →
            # "Patient").  Only rejoin when the right fragment is 1-2
            # chars — fragments that short are never standalone words.
            # Only rejoin when preceded by 3+ lowercase chars (mid-word
            # context) to avoid merging separate short column values.
            line = re.sub(
                r"(?<=[a-z]{3}) {1,2}(?=[a-z]{1,2}(?:[^a-zA-Z]|$))",
                "", line,
            )
            # Collapse runs of 3+ spaces to 2 (preserves Pattern 3 column detection)
            line = re.sub(r" {3,}", "  ", line)
            cleaned.append(line.rstrip())
        return "\n".join(cleaned)
