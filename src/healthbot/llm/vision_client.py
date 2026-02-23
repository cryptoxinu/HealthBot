"""Two-stage photo analysis via Ollama multimodal models.

Stage 1 (Describe): gemma3:27b describes the image objectively.
Stage 2 (Interpret): med42 provides health-relevant interpretation.

This two-stage design prevents the vision model from making
unsupported medical claims. The medical model never sees the image
directly — only a text description.

All processing is local via Ollama. No PHI leaves the machine.
"""
from __future__ import annotations

import base64
import logging

import httpx

from healthbot.config import MODEL_PRESETS
from healthbot.llm.ollama_client import _validate_ollama_url

logger = logging.getLogger("healthbot")

DEFAULT_URL = "http://localhost:11434"
DEFAULT_TIMEOUT = 180  # Vision models can be slow

_DESCRIBE_SYSTEM = (
    "You are a visual description assistant. Describe what you see in "
    "the image objectively and in detail. Focus on physical observations: "
    "colors, textures, shapes, sizes, locations on the body if visible. "
    "Provide precise, detailed descriptions."
)

_INTERPRET_SYSTEM = (
    "You are a health advisor reviewing a description of a user's photo. "
    "Based on the visual description and the user's health context, provide "
    "relevant observations and clinical interpretation. Note: you have NOT "
    "seen the image yourself — you are working from a text description only. "
    "Give direct, evidence-based analysis."
)


_JPEG_MAGIC = b"\xff\xd8\xff"
_PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
_DEFAULT_MAX_IMAGE_SIZE = 10 * 1024 * 1024  # 10 MB


class VisionClient:
    """Two-stage photo analysis: describe (vision model) -> interpret (medical model).

    Both stages run locally via Ollama. No data leaves the machine.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_URL,
        timeout: int = DEFAULT_TIMEOUT,
        max_image_size: int = _DEFAULT_MAX_IMAGE_SIZE,
    ) -> None:
        _validate_ollama_url(base_url)
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_image_size = max_image_size

    def analyze_photo(
        self,
        image_bytes: bytes,
        user_context: str = "",
    ) -> str:
        """Analyze a photo in two stages.

        Args:
            image_bytes: Raw image bytes (JPEG/PNG).
            user_context: Optional health context for interpretation.

        Returns:
            Combined description + interpretation text.
        """
        # Validate image format and size
        if len(image_bytes) > self._max_image_size:
            actual = len(image_bytes) // (1024 * 1024)
            limit = self._max_image_size // (1024 * 1024)
            return f"Error: Image too large ({actual}MB). Max is {limit}MB."
        if not (image_bytes[:3] == _JPEG_MAGIC or image_bytes[:8] == _PNG_MAGIC):
            return "Error: Unsupported image format. Please send a JPEG or PNG photo."

        # Stage 1: Describe the image objectively
        description = self._describe(image_bytes)
        if description.startswith("Error:"):
            return description

        # Stage 2: Medical interpretation of the description
        interpretation = self._interpret(description, user_context)

        return (
            f"**What I see:**\n{description}\n\n"
            f"**Health context:**\n{interpretation}"
        )

    def _describe(self, image_bytes: bytes) -> str:
        """Stage 1: Send image to vision model for objective description."""
        vision_model = MODEL_PRESETS["vision"][0]
        b64_image = base64.b64encode(image_bytes).decode("utf-8")

        payload = {
            "model": vision_model,
            "messages": [
                {"role": "system", "content": _DESCRIBE_SYSTEM},
                {
                    "role": "user",
                    "content": "Please describe this image in detail.",
                    "images": [b64_image],
                },
            ],
            "stream": False,
        }

        try:
            with httpx.Client(timeout=self._timeout) as client:
                resp = client.post(
                    f"{self._base_url}/api/chat", json=payload
                )
                resp.raise_for_status()
                content = resp.json().get("message", {}).get("content", "")
                if not content:
                    return "Error: Vision model returned empty response."
                return content
        except httpx.ConnectError:
            return "Error: Ollama is not running. Start it with: ollama serve"
        except httpx.TimeoutException:
            return "Error: Image analysis timed out. Try a smaller image."
        except httpx.HTTPStatusError as e:
            logger.error("Vision describe HTTP error: %s", e)
            return "Error: Vision model returned an error."
        except Exception as e:
            logger.error("Vision describe error: %s", e)
            return "Error: Could not analyze the image."

    def _interpret(self, description: str, user_context: str) -> str:
        """Stage 2: Send description to medical model for interpretation."""
        medical_model = MODEL_PRESETS["medical"][0]

        prompt_parts = [
            "A user sent a photo. Here is an objective description of what's visible:\n",
            description,
        ]
        if user_context:
            prompt_parts.append(f"\n\nUser's health context:\n{user_context}")
        prompt_parts.append(
            "\n\nBased on this description, what health-relevant observations "
            "can you share? Remember to recommend professional evaluation."
        )
        prompt = "\n".join(prompt_parts)

        payload = {
            "model": medical_model,
            "messages": [
                {"role": "system", "content": _INTERPRET_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
        }

        try:
            with httpx.Client(timeout=self._timeout) as client:
                resp = client.post(
                    f"{self._base_url}/api/chat", json=payload
                )
                resp.raise_for_status()
                content = resp.json().get("message", {}).get("content", "")
                if not content:
                    return "Could not generate interpretation."
                return content
        except Exception as e:
            logger.error("Vision interpret error: %s", e)
            return "Could not generate a medical interpretation at this time."

    def is_available(self) -> bool:
        """Check if the vision model is available in Ollama."""
        vision_model = MODEL_PRESETS["vision"][0]
        try:
            with httpx.Client(timeout=5) as client:
                resp = client.get(f"{self._base_url}/api/tags")
                if resp.status_code != 200:
                    return False
                models = [
                    m.get("name", "") for m in resp.json().get("models", [])
                ]
                base = vision_model.split(":")[0]
                return any(base in m for m in models)
        except Exception:
            return False
