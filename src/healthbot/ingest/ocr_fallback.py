"""OCR fallback for image-based PDFs.

Uses pytesseract for PDFs where pdfminer extracts little or no text.
Converts PDF pages to images via pdf2image/Pillow, then runs OCR.
"""
from __future__ import annotations

import io
import logging

from PIL import Image

logger = logging.getLogger("healthbot")

# Minimum text length from pdfminer before we try OCR
MIN_TEXT_LENGTH = 50

# Max pages to OCR (prevents memory exhaustion on large PDFs)
MAX_OCR_PAGES = 30


def needs_ocr(pdfminer_text: str) -> bool:
    """Check if extracted text is too short/empty, suggesting image-based PDF."""
    stripped = pdfminer_text.strip()
    return len(stripped) < MIN_TEXT_LENGTH


def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    """Run OCR on PDF bytes. Returns extracted text.

    Uses pytesseract. Requires tesseract to be installed:
        brew install tesseract

    Falls back gracefully if pytesseract is not available.
    """
    try:
        import pytesseract
    except ImportError:
        logger.warning("pytesseract not installed. OCR unavailable.")
        return ""

    pages = _pdf_to_images(pdf_bytes)
    if not pages:
        return ""

    texts: list[str] = []
    for i, img in enumerate(pages):
        try:
            text = pytesseract.image_to_string(img, lang="eng")
            texts.append(text)
        except Exception as e:
            logger.warning("OCR failed on page %d: %s", i + 1, e)
            continue

    return "\n\f\n".join(texts)


def ocr_pdf_page(pdf_bytes: bytes, page_number: int, dpi: int = 300) -> str:
    """OCR a single PDF page at specified DPI for targeted re-extraction.

    Used by the feedback loop when Claude flags cut-off or missing data
    on a specific page. Higher DPI (300-400) catches small text.

    Args:
        pdf_bytes: Raw PDF bytes.
        page_number: 1-indexed page number.
        dpi: Render resolution (default 300, use 400 for small text).

    Returns:
        Extracted text from that page, or empty string on failure.
    """
    try:
        import pytesseract
    except ImportError:
        logger.warning("pytesseract not installed. Page OCR unavailable.")
        return ""

    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        if page_number < 1 or page_number > len(doc):
            doc.close()
            return ""
        page = doc[page_number - 1]
        pix = page.get_pixmap(dpi=dpi)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        doc.close()

        text = pytesseract.image_to_string(img, lang="eng")
        logger.info(
            "Page %d OCR at %d DPI: %d chars extracted",
            page_number, dpi, len(text),
        )
        return text
    except ImportError:
        logger.warning("PyMuPDF not available for page-specific OCR.")
        return ""
    except Exception as e:
        logger.warning("Page %d OCR failed: %s", page_number, e)
        return ""


def _pdf_to_images(pdf_bytes: bytes) -> list[Image.Image]:
    """Convert PDF bytes to list of PIL Images.

    Tries pdf2image first (poppler-based), falls back to
    a simple fitz/PyMuPDF approach if available.
    """
    # Try pdf2image (requires poppler: brew install poppler)
    try:
        from pdf2image import convert_from_bytes
        images = convert_from_bytes(pdf_bytes, dpi=200, fmt="png")
        return images[:MAX_OCR_PAGES]
    except ImportError:
        pass
    except Exception as e:
        logger.warning("pdf2image failed: %s", e)

    # Try PyMuPDF (fitz)
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        images = []
        for page in doc[:MAX_OCR_PAGES]:
            pix = page.get_pixmap(dpi=200)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            images.append(img)
        doc.close()
        return images
    except ImportError:
        pass
    except Exception as e:
        logger.warning("PyMuPDF failed: %s", e)

    logger.warning(
        "No PDF-to-image converter available. "
        "Install pdf2image (brew install poppler) or PyMuPDF."
    )
    return []
