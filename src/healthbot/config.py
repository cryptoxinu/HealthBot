"""Application configuration.

All paths, constants, and settings. No secrets stored here.
Secrets come from macOS Keychain or the vault at runtime.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger("healthbot")

# Module-level lock for thread-safe app.json writes
_app_config_lock = threading.Lock()

# Available model presets — name -> (ollama_tag, description)
MODEL_PRESETS: dict[str, tuple[str, str]] = {
    "general": ("llama3.3:70b-instruct-q4_K_M", "General purpose, strong reasoning"),
    "fast": ("qwen3:14b", "Lightweight, fast inference, 128K context"),
    "medical": ("thewindmom/llama3-med42-70b", "Llama 3 + medical fine-tune, MedQA 79.1%"),
    "embedding": ("nomic-embed-text:latest", "768-dim embeddings for RAG and semantic search"),
    "vision": ("gemma3:27b", "Multimodal model for photo analysis"),
}


@dataclass
class Config:
    """Central configuration. All paths derived from vault_home."""

    vault_home: Path = field(default_factory=lambda: Path.home() / ".healthbot")


    # Security
    session_timeout_seconds: int = 1800  # 30 minutes
    argon2_time_cost: int = 3
    argon2_memory_cost: int = 65536  # 64 MB
    argon2_parallelism: int = 4
    argon2_hash_len: int = 32  # 256-bit key
    argon2_salt_len: int = 16

    # PDF safety
    max_pdf_size_bytes: int = 50 * 1024 * 1024  # 50 MB
    max_pdf_pages: int = 500

    # Telegram
    allowed_user_ids: list[int] = field(default_factory=list)
    rate_limit_per_minute: int = 20
    telegram_local_api_port: int = 8082  # local Bot API server (large file support)

    # Search
    tfidf_max_features: int = 10000
    search_top_k: int = 10

    # WHOOP OAuth
    whoop_auth_url: str = "https://api.prod.whoop.com/oauth/oauth2/auth"
    whoop_token_url: str = "https://api.prod.whoop.com/oauth/oauth2/token"
    whoop_api_base: str = "https://api.prod.whoop.com/developer/v2"

    # Oura Ring OAuth
    oura_auth_url: str = "https://cloud.ouraring.com/oauth/authorize"
    oura_token_url: str = "https://api.ouraring.com/oauth/token"
    oura_api_base: str = "https://api.ouraring.com"

    # LLM
    llm_enabled: bool = True  # False = command-only mode (no LLM calls)

    # Ollama (optional — used for Layer 3 PII anonymization and lab PDF parsing)
    ollama_model: str = "qwen3:14b"
    ollama_url: str = "http://localhost:11434"
    ollama_timeout: int = 120
    max_image_size_bytes: int = 10 * 1024 * 1024  # 10 MB
    context_max_chars: int = 12000  # ~3000 tokens

    # Memory
    consolidation_interval_seconds: int = 7200  # 2 hours
    stm_cleanup_days: int = 30

    # Backup retention
    backup_daily_retention: int = 7   # Keep last 7 daily backups
    backup_weekly_retention: int = 4  # Keep 4 weekly backups (oldest daily per week)

    # Digest
    digest_time: str = "08:00"  # HH:MM local time, or "" to disable
    digest_interval: int = 86400  # 24 hours

    # Auto AI export (opt-in)
    auto_ai_export: bool = False  # Enable via app.json
    auto_ai_export_interval: int = 86400  # Default: daily (seconds)

    # Apple Health (Health Auto Export → iCloud Drive)
    apple_health_export_path: str = ""  # iCloud Drive path, or "" to disable

    # Auto PDF reports
    weekly_report_day: str = ""  # e.g. "sunday", or "" to disable
    weekly_report_time: str = "20:00"  # HH:MM local time
    monthly_report_day: int = 0  # 1-28, or 0 to disable
    monthly_report_time: str = "20:00"  # HH:MM local time

    # NLU
    state_timeout_seconds: int = 300  # 5 minutes


    # Research (Claude CLI — sanitized queries only, no PHI)
    claude_cli_path: str | None = None  # Auto-detected if None
    claude_cli_timeout: int = 180  # Higher for web research
    pubmed_enabled: bool = True
    pubmed_max_results: int = 5
    pubmed_base_url: str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

    # Derived paths
    @property
    def db_path(self) -> Path:
        return self.vault_home / "db" / "health.db"

    @property
    def blobs_dir(self) -> Path:
        return self.vault_home / "vault"

    @property
    def vectors_dir(self) -> Path:
        return self.vault_home / "index"

    @property
    def backups_dir(self) -> Path:
        return self.vault_home / "backups"

    @property
    def log_dir(self) -> Path:
        return self.vault_home / "logs"

    @property
    def exports_dir(self) -> Path:
        return self.vault_home / "exports"

    @property
    def incoming_dir(self) -> Path:
        return self.vault_home / "incoming"

    @property
    def clean_db_path(self) -> Path:
        return self.vault_home / "db" / "clean.db"

    @property
    def claude_dir(self) -> Path:
        return self.vault_home / "claude"

    @property
    def manifest_path(self) -> Path:
        return self.vault_home / "manifest.json"

    @property
    def app_config_path(self) -> Path:
        return self.vault_home / "config" / "app.json"

    def ensure_dirs(self) -> None:
        """Create all vault directories if they don't exist."""
        for d in [
            self.vault_home,
            self.vault_home / "db",
            self.blobs_dir,
            self.vectors_dir,
            self.backups_dir,
            self.log_dir,
            self.exports_dir,
            self.incoming_dir,
            self.claude_dir,
            self.vault_home / "config",
        ]:
            d.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _validate_type(value: object, expected_type: type, field_name: str) -> bool:
        """Check that a config value matches the expected type."""
        if not isinstance(value, expected_type):
            logger.warning(
                "Config field '%s': expected %s, got %s — ignoring",
                field_name, expected_type.__name__, type(value).__name__,
            )
            return False
        return True

    def load_app_config(self) -> None:
        """Load allowed_user_ids, limits, and Ollama settings from app.json."""
        if self.app_config_path.exists():
            data = json.loads(self.app_config_path.read_text())
            v = data.get("allowed_user_ids", [])
            if self._validate_type(v, list, "allowed_user_ids"):
                self.allowed_user_ids = v
            v = data.get("rate_limit_per_minute", self.rate_limit_per_minute)
            if self._validate_type(v, int, "rate_limit_per_minute"):
                self.rate_limit_per_minute = v
            v = data.get("ollama_url", self.ollama_url)
            if self._validate_type(v, str, "ollama_url"):
                self.ollama_url = v
            v = data.get("ollama_model", self.ollama_model)
            if self._validate_type(v, str, "ollama_model"):
                self.ollama_model = v
            v = data.get("ollama_timeout", self.ollama_timeout)
            if self._validate_type(v, int, "ollama_timeout"):
                self.ollama_timeout = v
            v = data.get("digest_time", self.digest_time)
            if self._validate_type(v, str, "digest_time"):
                self.digest_time = v
            v = data.get("auto_ai_export", self.auto_ai_export)
            if self._validate_type(v, bool, "auto_ai_export"):
                self.auto_ai_export = v
            v = data.get("auto_ai_export_interval", self.auto_ai_export_interval)
            if self._validate_type(v, int, "auto_ai_export_interval"):
                self.auto_ai_export_interval = v
            v = data.get(
                "apple_health_export_path", self.apple_health_export_path,
            )
            if self._validate_type(v, str, "apple_health_export_path"):
                self.apple_health_export_path = v
            v = data.get(
                "weekly_report_day", self.weekly_report_day,
            )
            if self._validate_type(v, str, "weekly_report_day"):
                self.weekly_report_day = v
            v = data.get(
                "weekly_report_time", self.weekly_report_time,
            )
            if self._validate_type(v, str, "weekly_report_time"):
                self.weekly_report_time = v
            v = data.get(
                "monthly_report_day", self.monthly_report_day,
            )
            if self._validate_type(v, int, "monthly_report_day"):
                self.monthly_report_day = v
            v = data.get(
                "monthly_report_time", self.monthly_report_time,
            )
            if self._validate_type(v, str, "monthly_report_time"):
                self.monthly_report_time = v
            self._wearable_state = data.get("wearable_state", {})
            self._privacy_mode = data.get("privacy_mode", "strict")
            self._send_redacted_pdf = data.get("send_redacted_pdf", False)
        else:
            self._wearable_state = {}
            self._privacy_mode = "strict"
            self._send_redacted_pdf = False

        # Load research client settings
        rc_path = self.vault_home / "config" / "research_clients.json"
        if rc_path.exists():
            rc = json.loads(rc_path.read_text())
            self.claude_cli_path = rc.get("claude_cli_path", self.claude_cli_path)
            self.claude_cli_timeout = rc.get("claude_cli_timeout", self.claude_cli_timeout)
            self.pubmed_enabled = rc.get("pubmed_enabled", self.pubmed_enabled)
            self.pubmed_max_results = rc.get("pubmed_max_results", self.pubmed_max_results)

    def get_wearable_state(self, name: str) -> dict:
        """Get connection state for a wearable device."""
        if not hasattr(self, "_wearable_state"):
            self._wearable_state = {}
        return self._wearable_state.get(name, {
            "ever_connected": False,
            "last_connected": None,
        })

    def set_wearable_connected(self, name: str, connected: bool) -> None:
        """Record that a wearable was connected."""
        if not hasattr(self, "_wearable_state"):
            self._wearable_state = {}
        from datetime import date as _date
        state = self._wearable_state.get(name, {})
        if connected:
            state["ever_connected"] = True
            state["last_connected"] = _date.today().isoformat()
        self._wearable_state[name] = state
        self._save_wearable_state()

    def was_wearable_ever_connected(self, name: str) -> bool:
        """Check if a wearable was ever connected."""
        return self.get_wearable_state(name).get("ever_connected", False)

    @property
    def privacy_mode(self) -> str:
        """Get PDF extraction privacy mode ('relaxed' or 'strict')."""
        return getattr(self, "_privacy_mode", "strict")

    def set_privacy_mode(self, mode: str) -> None:
        """Set PDF extraction privacy mode and persist to app.json."""
        self._privacy_mode = mode
        self._save_app_setting("privacy_mode", mode)

    @property
    def send_redacted_pdf(self) -> bool:
        """Whether to send redacted PDF back after ingestion (default: off)."""
        return getattr(self, "_send_redacted_pdf", False)

    def set_send_redacted_pdf(self, enabled: bool) -> None:
        """Set redacted PDF send-back preference and persist to app.json."""
        self._send_redacted_pdf = enabled
        self._save_app_setting("send_redacted_pdf", enabled)

    def _save_app_setting(self, key: str, value: object) -> None:
        """Persist a single setting to app.json (thread-safe)."""
        with _app_config_lock:
            config_dir = self.vault_home / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            data = {}
            if self.app_config_path.exists():
                data = json.loads(self.app_config_path.read_text())
            data[key] = value
            self.app_config_path.write_text(json.dumps(data, indent=2))

    def _save_wearable_state(self) -> None:
        """Persist wearable state to app.json."""
        self._save_app_setting("wearable_state", self._wearable_state)
