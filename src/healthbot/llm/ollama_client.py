"""Ollama local LLM client via HTTP REST API.

Handles ALL health data conversations locally. PHI never leaves the machine.
No anonymization needed — Ollama runs on localhost.

Uses httpx sync client to match ClaudeClient's synchronous send() interface.
Thread-safe: a lock protects send() + set_model() from concurrent access.
"""
from __future__ import annotations

import logging
import subprocess
import threading
import time

import httpx

logger = logging.getLogger("healthbot")

# Default model — user can override via config/app.json
DEFAULT_MODEL = "llama3.3:70b-instruct-q4_K_M"
DEFAULT_URL = "http://localhost:11434"
DEFAULT_TIMEOUT = 120

_ALLOWED_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})


def _validate_ollama_url(url: str) -> None:
    """Ensure Ollama URL points to localhost only.

    Prevents SSRF — Ollama handles raw health data and must never
    be redirected to a remote server.
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    hostname = (parsed.hostname or "").lower()
    if hostname not in _ALLOWED_HOSTS:
        raise ValueError(
            f"Ollama URL must point to localhost, got: {hostname!r}"
        )


class OllamaUnavailableError(Exception):
    """Raised when Ollama server is not reachable."""


class OllamaClient:
    """Local LLM client via Ollama REST API.

    All health data stays on-machine. No anonymization required.
    """

    # Module-level process tracker (one Ollama per bot instance)
    _ollama_process: subprocess.Popen | None = None

    def __init__(
        self,
        model: str = DEFAULT_MODEL,
        base_url: str = DEFAULT_URL,
        timeout: int = DEFAULT_TIMEOUT,
        retry_count: int = 2,
        retry_backoff: float = 1.0,
    ) -> None:
        _validate_ollama_url(base_url)
        self._model = model
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._retry_count = retry_count
        self._retry_backoff = retry_backoff
        self._lock = threading.Lock()

    def send(self, prompt: str, system: str = "", model: str | None = None) -> str:
        """Send a prompt to Ollama and return the text response.

        Args:
            prompt: The user prompt with health context included.
            system: System instructions (persona, citation rules, etc.).
            model: Override the default model for this call. Preferred over
                   set_model() for per-request model selection.

        Returns:
            Ollama's text response.

        Note: No anonymization needed — Ollama is local-only.
        Thread-safe: protected by self._lock.
        """
        with self._lock:
            active_model = model or self._model
            messages = []
            if system:
                messages.append({"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})

            payload = {
                "model": active_model,
                "messages": messages,
                "stream": False,
                "keep_alive": "5m",
            }

            for attempt in range(1 + self._retry_count):
                try:
                    t0 = time.time()
                    with httpx.Client(timeout=self._timeout) as client:
                        resp = client.post(
                            f"{self._base_url}/api/chat",
                            json=payload,
                        )
                        resp.raise_for_status()
                    elapsed = time.time() - t0
                    logger.info("Ollama inference: %.1fs (model=%s)", elapsed, active_model)

                    data = resp.json()
                    content = data.get("message", {}).get("content", "")
                    if not content:
                        logger.warning("Ollama returned empty response")
                        return "I couldn't generate a response. Try rephrasing."
                    return content
                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    if attempt < self._retry_count:
                        wait = self._retry_backoff * (2 ** attempt)
                        logger.warning(
                            "Ollama attempt %d/%d failed (%s), retrying in %.1fs",
                            attempt + 1, self._retry_count + 1,
                            type(e).__name__, wait,
                        )
                        time.sleep(wait)
                        continue
                    if isinstance(e, httpx.ConnectError):
                        logger.error("Ollama not reachable at %s", self._base_url)
                        return (
                            "Local AI (Ollama) is not running. "
                            "Start it with: ollama serve"
                        )
                    logger.warning("Ollama timed out after %ds", self._timeout)
                    return "I'm taking too long to respond. Try a simpler question."
                except httpx.HTTPStatusError as e:
                    logger.error("Ollama HTTP error: %s", e)
                    return "Local AI returned an error. Check Ollama logs."
                except Exception as e:
                    logger.error("Ollama unexpected error: %s", e)
                    return "I had trouble processing that with the local AI."
            # Should not reach here, but safety fallback
            return "I had trouble processing that with the local AI."

    # Minimum model size (bytes) for reliable PII detection.
    # Models under 2 GB are too small for nuanced anonymization.
    _MIN_MODEL_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB

    def is_available(self, model: str | None = None) -> bool:
        """Check if Ollama server is running and a suitable model is loaded.

        Validates that the model is at least 2 GB — smaller models produce
        unreliable PII detection and silently degrade Layer 3 anonymization.

        Args:
            model: Specific model to check. Defaults to the active model.
        """
        target = model or self._model
        try:
            with httpx.Client(timeout=5) as client:
                resp = client.get(f"{self._base_url}/api/tags")
                if resp.status_code != 200:
                    return False
                data = resp.json()
                base_name = target.split(":")[0]
                for m in data.get("models", []):
                    name = m.get("name", "")
                    if name == target or name.split(":")[0] == base_name:
                        size_bytes = m.get("size", 0)
                        if size_bytes < self._MIN_MODEL_SIZE_BYTES:
                            size_gb = size_bytes / (1024 ** 3)
                            logger.warning(
                                "Ollama model %s too small (%.1f GB) for "
                                "reliable PII detection — need >= 2 GB",
                                name, size_gb,
                            )
                            return False
                        return True
                return False
        except Exception:
            return False

    def ensure_running(self) -> bool:
        """Start Ollama if not running. Returns True if available after attempt."""
        if self.is_available():
            return True
        # Try to start ollama serve
        import shutil

        ollama_path = shutil.which("ollama")
        if not ollama_path:
            logger.info("Ollama binary not found — cannot auto-start")
            return False
        try:
            logger.info("Auto-starting Ollama server...")
            OllamaClient._ollama_process = subprocess.Popen(
                [ollama_path, "serve"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            # Wait for server to become available
            for _ in range(10):
                time.sleep(1)
                if self.is_available():
                    logger.info("Ollama auto-started successfully")
                    return True
            logger.warning("Ollama started but model not available after 10s")
            return False
        except Exception as e:
            logger.warning("Failed to auto-start Ollama: %s", e)
            return False

    @property
    def model(self) -> str:
        return self._model

    def set_model(self, model: str) -> None:
        """Switch the active model.

        Both ConversationManager and MemoryStore share this client,
        so changing _model here affects all downstream callers.
        Thread-safe: protected by self._lock.
        """
        with self._lock:
            self._model = model

    def unload_model(self, model: str | None = None) -> None:
        """Tell Ollama to unload the model from GPU memory.

        Uses keep_alive=0 on the /api/generate endpoint which triggers
        immediate model eviction without generating any tokens.
        """
        target = model or self._model
        try:
            with httpx.Client(timeout=5) as client:
                client.post(
                    f"{self._base_url}/api/generate",
                    json={"model": target, "keep_alive": 0},
                )
            logger.info("Ollama model unloaded: %s", target)
        except Exception as e:
            logger.debug("Ollama model unload failed (may not be loaded): %s", e)

    @staticmethod
    def stop_server() -> None:
        """Terminate the Ollama server process if we started it."""
        proc = OllamaClient._ollama_process
        if proc is None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        OllamaClient._ollama_process = None
        logger.info("Ollama server stopped")

    @staticmethod
    def safe_unload_on_lock(config) -> None:
        """Unload Ollama model and stop server during vault lock."""
        try:
            ollama = OllamaClient(
                model=getattr(config, "ollama_model", ""),
                base_url=getattr(config, "ollama_url", "http://localhost:11434"),
                timeout=5,
            )
            ollama.unload_model()
        except Exception:
            pass
        OllamaClient.stop_server()

    def list_local_models(self) -> list[str]:
        """Return names of all models available in Ollama."""
        try:
            with httpx.Client(timeout=5) as client:
                resp = client.get(f"{self._base_url}/api/tags")
                if resp.status_code != 200:
                    return []
                data = resp.json()
                return [m.get("name", "") for m in data.get("models", [])]
        except Exception:
            return []
