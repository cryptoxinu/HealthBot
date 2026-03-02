"""PDF safety validation.

Checks file size, page count, magic bytes, and scans for
JavaScript/Launch actions before allowing parsing.
"""
from __future__ import annotations

import io
import re

from healthbot.config import Config


class PdfSafetyError(Exception):
    """Raised when a PDF fails safety validation."""


class PdfSafety:
    """Validates PDFs before allowing parsing."""

    # Dangerous PDF action patterns
    _JS_PATTERNS = [
        re.compile(rb"/JavaScript\b", re.IGNORECASE),
        re.compile(rb"/JS\b"),
        re.compile(rb"/Launch\b", re.IGNORECASE),
        re.compile(rb"/OpenAction\b", re.IGNORECASE),
        re.compile(rb"/AA\b"),  # Additional actions
    ]

    # Encrypted PDF indicator
    _ENCRYPT_PATTERN = re.compile(rb"/Encrypt\b")

    def __init__(self, config: Config) -> None:
        self._max_size = config.max_pdf_size_bytes
        self._max_pages = config.max_pdf_pages

    def validate_bytes(self, data: bytes) -> None:
        """Run all safety checks on raw PDF bytes. Raises PdfSafetyError on failure."""
        self._check_size(data)
        self._check_magic(data)
        self._check_encrypted(data)
        self._check_dangerous_actions(data)
        self._check_page_count(data)

    def _check_size(self, data: bytes) -> None:
        """Check file size against limit."""
        if len(data) > self._max_size:
            raise PdfSafetyError(
                f"PDF too large: {len(data)} bytes (max {self._max_size})"
            )

    def _check_magic(self, data: bytes) -> None:
        """Check PDF magic bytes (%PDF-)."""
        if not data[:5] == b"%PDF-":
            raise PdfSafetyError("Not a valid PDF file (missing %PDF- header)")

    def _check_encrypted(self, data: bytes) -> None:
        """Reject encrypted PDFs.

        Only checks the PDF trailer/xref area (last 4KB) where the
        /Encrypt dictionary is referenced in the PDF structure, avoiding
        false positives when body text happens to contain the string.
        """
        # PDF trailers with /Encrypt appear near the end of the file.
        # Check the last 4096 bytes (trailer + xref area) to avoid
        # matching on body text content.
        trailer = data[-4096:] if len(data) > 4096 else data
        if self._ENCRYPT_PATTERN.search(trailer):
            raise PdfSafetyError(
                "Encrypted PDFs are not supported. Please provide an unencrypted version."
            )

    def _check_dangerous_actions(self, data: bytes) -> None:
        """Reject PDFs with JavaScript or Launch actions."""
        for pattern in self._JS_PATTERNS:
            match = pattern.search(data)
            if match:
                raise PdfSafetyError(
                    "PDF contains potentially dangerous action: "
                    f"{match.group().decode(errors='replace')}"
                )

    def _check_page_count(self, data: bytes) -> None:
        """Check page count using pdfminer (lightweight scan)."""
        try:
            from pdfminer.pdfpage import PDFPage

            pages = list(PDFPage.get_pages(io.BytesIO(data), maxpages=self._max_pages + 1))
            if len(pages) > self._max_pages:
                raise PdfSafetyError(
                    f"PDF has too many pages: >{self._max_pages} (max {self._max_pages})"
                )
        except PdfSafetyError:
            raise
        except Exception as e:
            raise PdfSafetyError(f"Could not parse PDF structure: {e}") from e
