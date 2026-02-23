"""Tests for OCR fallback module."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from healthbot.ingest.ocr_fallback import MIN_TEXT_LENGTH, needs_ocr, ocr_pdf_bytes


class TestNeedsOcr:
    def test_empty_text_needs_ocr(self):
        assert needs_ocr("") is True

    def test_whitespace_only_needs_ocr(self):
        assert needs_ocr("   \n\t  ") is True

    def test_short_text_needs_ocr(self):
        assert needs_ocr("short") is True

    def test_adequate_text_no_ocr(self):
        text = "a" * MIN_TEXT_LENGTH
        assert needs_ocr(text) is False

    def test_long_text_no_ocr(self):
        text = "Glucose 108 mg/dL Reference Range: 70-100 Normal " * 5
        assert needs_ocr(text) is False

    def test_threshold_boundary(self):
        assert needs_ocr("a" * (MIN_TEXT_LENGTH - 1)) is True
        assert needs_ocr("a" * MIN_TEXT_LENGTH) is False


class TestOcrPdfBytes:
    def test_no_pytesseract_returns_empty(self):
        with patch.dict("sys.modules", {"pytesseract": None}):
            result = ocr_pdf_bytes(b"fake pdf")
            assert result == ""

    def test_no_images_returns_empty(self):
        mock_tess = MagicMock()
        with patch.dict("sys.modules", {"pytesseract": mock_tess}):
            with patch("healthbot.ingest.ocr_fallback._pdf_to_images", return_value=[]):
                result = ocr_pdf_bytes(b"fake pdf")
                assert result == ""

    def test_ocr_success(self):
        mock_tess = MagicMock()
        mock_tess.image_to_string.return_value = "Extracted text"
        mock_img = MagicMock()
        with patch.dict("sys.modules", {"pytesseract": mock_tess}):
            with patch("healthbot.ingest.ocr_fallback._pdf_to_images", return_value=[mock_img]):
                result = ocr_pdf_bytes(b"fake pdf")
                assert "Extracted text" in result
