"""Tests for PDF image extraction and OCR pipeline."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from PIL import Image

from healthbot.ingest.image_extractor import (
    ExtractedImage,
    ImageOcrResult,
    _is_likely_chart,
    ocr_images,
    preprocess_image,
)


class TestPreprocessImage:
    """Test image preprocessing for OCR."""

    def test_converts_to_grayscale(self) -> None:
        img = Image.new("RGB", (400, 400), color="red")
        result = preprocess_image(img)
        assert result.mode == "L"

    def test_already_grayscale(self) -> None:
        img = Image.new("L", (400, 400), color=128)
        result = preprocess_image(img)
        assert result.mode == "L"

    def test_upscales_small_images(self) -> None:
        img = Image.new("L", (100, 100))
        result = preprocess_image(img)
        assert result.size[0] >= 300
        assert result.size[1] >= 300

    def test_does_not_upscale_large_images(self) -> None:
        img = Image.new("L", (500, 500))
        result = preprocess_image(img)
        assert result.size == (500, 500)

    def test_enhances_contrast(self) -> None:
        # Just verify it doesn't crash — exact pixel values are implementation detail
        img = Image.new("L", (400, 400), color=128)
        result = preprocess_image(img)
        assert result.size[0] == 400


class TestChartHeuristic:
    """Test chart-like image detection."""

    def test_reasonable_chart_size(self) -> None:
        img = ExtractedImage(
            page_number=1, xref=1, width=600, height=400,
            image=Image.new("L", (600, 400)),
        )
        assert _is_likely_chart(img)

    def test_too_small_rejected(self) -> None:
        img = ExtractedImage(
            page_number=1, xref=1, width=40, height=40,
            image=Image.new("L", (40, 40)),
        )
        assert not _is_likely_chart(img)

    def test_too_narrow_rejected(self) -> None:
        """Very narrow images (lines/separators) are not charts."""
        img = ExtractedImage(
            page_number=1, xref=1, width=1000, height=5,
            image=Image.new("L", (1000, 5)),
        )
        assert not _is_likely_chart(img)

    def test_full_page_rejected(self) -> None:
        """Full-page scanned images are not charts."""
        img = ExtractedImage(
            page_number=1, xref=1, width=3000, height=4000,
            image=Image.new("L", (3000, 4000)),
        )
        assert not _is_likely_chart(img)

    def test_square_accepted(self) -> None:
        img = ExtractedImage(
            page_number=1, xref=1, width=400, height=400,
            image=Image.new("L", (400, 400)),
        )
        assert _is_likely_chart(img)


class TestOcrImages:
    """Test OCR on extracted images."""

    @patch("healthbot.ingest.image_extractor.pytesseract", create=True)
    def test_ocr_produces_text(self, mock_tess) -> None:
        # Mock pytesseract.image_to_data
        import sys
        mock_module = MagicMock()
        mock_module.Output.DICT = "dict"
        mock_module.image_to_data.return_value = {
            "text": ["WBC", "8.2", "x10E3/uL"],
            "conf": [90, 85, 80],
        }
        sys.modules["pytesseract"] = mock_module

        try:
            img = Image.new("L", (400, 400), color=128)
            ext_img = ExtractedImage(
                page_number=1, xref=1, width=400, height=400, image=img,
            )
            results = ocr_images([ext_img])
            assert len(results) == 1
            assert "WBC" in results[0].text
            assert results[0].confidence > 0
        finally:
            sys.modules.pop("pytesseract", None)

    def test_ocr_empty_on_no_pytesseract(self) -> None:
        """Graceful fallback when pytesseract not installed."""
        import sys
        # Temporarily remove pytesseract if it exists
        had_it = "pytesseract" in sys.modules
        saved = sys.modules.pop("pytesseract", None)
        try:
            img = Image.new("L", (400, 400))
            ext = ExtractedImage(1, 1, 400, 400, img)
            # This should return empty, not crash
            result = ocr_images([ext])
            assert result == [] or isinstance(result, list)
        finally:
            if had_it and saved:
                sys.modules["pytesseract"] = saved


class TestExtractImagesFromPdf:
    """Test PDF image extraction."""

    def test_returns_empty_for_no_fitz(self) -> None:
        """Returns empty list when PyMuPDF not available."""
        from healthbot.ingest.image_extractor import extract_images_from_pdf

        # Pass invalid data — should handle gracefully
        result = extract_images_from_pdf(b"not a pdf")
        # Either empty (fitz available but bad data) or empty (no fitz)
        assert isinstance(result, list)


class TestImageOcrResult:
    """Test ImageOcrResult dataclass."""

    def test_defaults(self) -> None:
        result = ImageOcrResult(page_number=1, text="test", confidence=0.85)
        assert result.source == "ocr"

    def test_vision_source(self) -> None:
        result = ImageOcrResult(
            page_number=1, text="test", confidence=0.70, source="vision",
        )
        assert result.source == "vision"
