"""Extract and OCR images embedded in PDF lab reports.

Handles mixed text+image PDFs where lab results or reference ranges
are rendered as images (scanned pages, bar charts, tables-as-images).

Pipeline:
1. Extract embedded images from PDF pages via PyMuPDF
2. Filter out tiny (logos) and huge images
3. Preprocess: grayscale, contrast enhance, sharpen
4. OCR with pytesseract (per-word confidence)
5. Optionally analyze chart-like images with Ollama vision model

All processing is local. No data leaves the machine.
"""
from __future__ import annotations

import io
import logging
from dataclasses import dataclass

from PIL import Image, ImageEnhance, ImageFilter

logger = logging.getLogger("healthbot")

# Size filters
MIN_IMAGE_SIZE = 50  # pixels — skip logos/icons
MAX_IMAGE_SIZE = 5000  # pixels — skip unreasonably large images
MAX_PAGES = 30  # Max pages to process
MIN_OCR_CONFIDENCE = 30  # pytesseract word confidence threshold (0-100)


@dataclass
class ExtractedImage:
    """An image extracted from a PDF page."""

    page_number: int
    xref: int
    width: int
    height: int
    image: Image.Image


@dataclass
class ImageOcrResult:
    """OCR result from a single image."""

    page_number: int
    text: str
    confidence: float  # 0.0 - 1.0
    source: str = "ocr"  # "ocr" or "vision"


def extract_images_from_pdf(pdf_bytes: bytes) -> list[ExtractedImage]:
    """Extract embedded images from PDF pages.

    Uses PyMuPDF to enumerate images per page, then extracts them
    as PIL Image objects. Filters out tiny and oversized images.
    """
    try:
        import fitz
    except ImportError:
        logger.info("PyMuPDF not available — image extraction skipped")
        return []

    images: list[ExtractedImage] = []

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        logger.warning("Failed to open PDF for image extraction: %s", e)
        return []

    try:
        for page_idx, page in enumerate(doc[:MAX_PAGES]):
            page_num = page_idx + 1
            try:
                image_list = page.get_images(full=True)
            except Exception as e:
                logger.debug("Failed to get images from page %d: %s", page_num, e)
                continue

            for img_info in image_list:
                xref = img_info[0]
                try:
                    base_image = doc.extract_image(xref)
                    if not base_image or "image" not in base_image:
                        continue

                    img_bytes = base_image["image"]
                    pil_img = Image.open(io.BytesIO(img_bytes))
                    w, h = pil_img.size

                    # Filter by size
                    if w < MIN_IMAGE_SIZE or h < MIN_IMAGE_SIZE:
                        continue
                    if w > MAX_IMAGE_SIZE or h > MAX_IMAGE_SIZE:
                        continue

                    images.append(ExtractedImage(
                        page_number=page_num,
                        xref=xref,
                        width=w,
                        height=h,
                        image=pil_img,
                    ))
                except Exception as e:
                    logger.debug(
                        "Failed to extract image xref=%d from page %d: %s",
                        xref, page_num, e,
                    )
    finally:
        page_count = doc.page_count
        doc.close()

    logger.info(
        "Image extraction: %d images from %d pages",
        len(images), min(page_count, MAX_PAGES),
    )
    return images


def preprocess_image(img: Image.Image) -> Image.Image:
    """Preprocess image for better OCR accuracy.

    - Convert to grayscale
    - Enhance contrast (1.5x)
    - Sharpen
    - Upscale if too small (< 300px on shortest side)
    """
    # Grayscale
    if img.mode != "L":
        img = img.convert("L")

    # Contrast enhancement
    enhancer = ImageEnhance.Contrast(img)
    img = enhancer.enhance(1.5)

    # Sharpen
    img = img.filter(ImageFilter.SHARPEN)

    # Upscale small images
    w, h = img.size
    min_dim = min(w, h)
    if min_dim < 300:
        scale = 300 / min_dim
        new_w = int(w * scale)
        new_h = int(h * scale)
        img = img.resize((new_w, new_h), Image.LANCZOS)

    return img


def ocr_images(images: list[ExtractedImage]) -> list[ImageOcrResult]:
    """Run OCR on extracted images.

    Uses pytesseract with per-word confidence scoring.
    Words below MIN_OCR_CONFIDENCE are filtered out.
    Returns text and average confidence per image.
    """
    try:
        import pytesseract
    except ImportError:
        logger.info("pytesseract not available — image OCR skipped")
        return []

    results: list[ImageOcrResult] = []

    for ext_img in images:
        try:
            processed = preprocess_image(ext_img.image)
            data = pytesseract.image_to_data(
                processed, lang="eng", output_type=pytesseract.Output.DICT,
            )

            # Filter words by confidence
            words = []
            confidences = []
            for i, word in enumerate(data["text"]):
                word = word.strip()
                if not word:
                    continue
                conf = int(data["conf"][i])
                if conf >= MIN_OCR_CONFIDENCE:
                    words.append(word)
                    confidences.append(conf)

            if not words:
                continue

            text = " ".join(words)
            avg_conf = sum(confidences) / len(confidences) / 100.0  # normalize to 0-1

            results.append(ImageOcrResult(
                page_number=ext_img.page_number,
                text=text,
                confidence=avg_conf,
            ))
        except Exception as e:
            logger.debug(
                "OCR failed on image (page %d): %s", ext_img.page_number, e,
            )

    logger.info("Image OCR: %d images produced text", len(results))
    return results


def _is_likely_chart(img: ExtractedImage) -> bool:
    """Heuristic to detect chart-like images.

    Charts are typically:
    - Reasonably sized (not tiny icons or full-page scans)
    - Not too narrow (not borders/lines)
    - Wider than tall, or roughly square
    """
    w, h = img.width, img.height

    # Too small for a meaningful chart
    if w < 150 or h < 100:
        return False

    # Too narrow — likely a line or separator
    aspect = w / h if h > 0 else 0
    if aspect > 10 or aspect < 0.1:
        return False

    # Full-page images are not charts (they're scanned pages)
    if w > 2500 and h > 3000:
        return False

    # Reasonable chart size: between 150x100 and 2500x2000
    return True


def analyze_chart_with_vision(
    images: list[ExtractedImage],
    ollama_url: str = "http://localhost:11434",
    timeout: int = 120,
) -> list[ImageOcrResult]:
    """Analyze chart-like images with Ollama vision model.

    Sends chart candidates to gemma3:27b for structured data extraction
    (test names, values, units, reference ranges).

    Returns results with confidence 0.70 (vision-based).
    Warns if Ollama is unavailable and chart-like images were detected.
    """
    import base64

    import httpx

    from healthbot.config import MODEL_PRESETS
    from healthbot.llm.ollama_client import _validate_ollama_url

    _validate_ollama_url(ollama_url)

    vision_model = MODEL_PRESETS.get("vision", ("gemma3:27b",))[0]

    # Find chart candidates
    chart_images = [img for img in images if _is_likely_chart(img)]
    if not chart_images:
        return []

    # Check Ollama availability
    try:
        resp = httpx.get(
            f"{ollama_url}/api/tags",
            timeout=5,
        )
        if resp.status_code != 200:
            logger.warning(
                "Ollama unavailable — %d chart-like image(s) detected but "
                "cannot be analyzed. Chart data may be missing.",
                len(chart_images),
            )
            return []

        available_models = [
            m["name"] for m in resp.json().get("models", [])
        ]
        # Check if vision model is available (handle tag variants)
        vision_available = any(
            vision_model.split(":")[0] in m for m in available_models
        )
        if not vision_available:
            logger.warning(
                "Vision model %s not available — %d chart-like image(s) "
                "cannot be analyzed. Pull it with: ollama pull %s",
                vision_model, len(chart_images), vision_model,
            )
            return []
    except Exception:
        logger.warning(
            "Ollama not reachable — %d chart-like image(s) detected but "
            "cannot be analyzed. Chart data may be missing.",
            len(chart_images),
        )
        return []

    results: list[ImageOcrResult] = []

    prompt = (
        "This image is from a medical lab report. Extract any test results "
        "visible in this chart or table image.\n\n"
        "For each result, provide:\n"
        "- Test name\n"
        "- Value\n"
        "- Unit (if visible)\n"
        "- Reference range (if visible)\n\n"
        "Format as structured text, one result per line:\n"
        "TestName: Value Unit (ref: Low-High)\n\n"
        "If no lab data is visible, respond with 'No lab data found.'"
    )

    for chart_img in chart_images:
        try:
            # Convert to bytes for Ollama
            buf = io.BytesIO()
            chart_img.image.save(buf, format="PNG")
            img_b64 = base64.b64encode(buf.getvalue()).decode()

            resp = httpx.post(
                f"{ollama_url}/api/generate",
                json={
                    "model": vision_model,
                    "prompt": prompt,
                    "images": [img_b64],
                    "stream": False,
                },
                timeout=timeout,
            )

            if resp.status_code == 200:
                text = resp.json().get("response", "").strip()
                if text and "no lab data" not in text.lower():
                    results.append(ImageOcrResult(
                        page_number=chart_img.page_number,
                        text=text,
                        confidence=0.70,
                        source="vision",
                    ))
        except Exception as e:
            logger.warning(
                "Vision analysis failed for image on page %d: %s",
                chart_img.page_number, e,
            )

    logger.info(
        "Chart analysis: %d/%d chart images produced results",
        len(results), len(chart_images),
    )
    return results
