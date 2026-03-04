"""Knowledge export — reads 6 knowledge stores, builds portable JSON.

Supports two modes:
- plain: PII-stripped via PhiFirewall.redact(), safe to share
- encrypted: full fidelity, password-protected via EncryptedExport
"""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.llm.anonymizer import Anonymizer
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")

_EXPORT_FORMAT = "healthbot_knowledge_export"
_EXPORT_VERSION = 1


class KnowledgeExporter:
    """Export accumulated knowledge from 6 stores as JSON."""

    def __init__(
        self,
        db: HealthDB,
        config: Config,
        key_manager: KeyManager,
        phi_firewall: PhiFirewall,
    ) -> None:
        self._db = db
        self._config = config
        self._km = key_manager
        self._fw = phi_firewall

    def export_all(
        self,
        user_id: int,
        mode: str = "plain",
        password: str | None = None,
    ) -> tuple[bytes, dict[str, int]]:
        """Export all knowledge stores.

        Args:
            user_id: Telegram user ID.
            mode: "plain" (PII-stripped) or "encrypted" (full, password-protected).
            password: Required when mode is "encrypted".

        Returns:
            (file_bytes, counts) — raw JSON bytes or encrypted bytes, plus
            per-store record counts.
        """
        if mode == "encrypted" and not password:
            raise ValueError("Encrypted export requires a password")

        stores = {
            "ltm_facts": self._export_ltm(user_id),
            "hypotheses": self._export_hypotheses(user_id),
            "medical_journal": self._export_journal(user_id),
            "claude_insights": self._export_claude_insights(),
            "knowledge_base": self._export_knowledge_base(),
            "external_evidence": self._export_external_evidence(),
        }

        counts = {k: len(v) for k, v in stores.items()}

        if mode == "plain":
            stores = self._redact_stores(stores)

        envelope = {
            "format": _EXPORT_FORMAT,
            "version": _EXPORT_VERSION,
            "exported_at": datetime.now(UTC).isoformat(),
            "mode": mode,
            "stores": stores,
        }

        json_bytes = json.dumps(envelope, indent=2, ensure_ascii=False).encode("utf-8")

        if mode == "encrypted" and password:
            from healthbot.export.encrypted_export import EncryptedExport

            enc = EncryptedExport(self._config)
            return enc.encrypt_for_sharing(json_bytes, password), counts

        return json_bytes, counts

    # ── Per-store exporters ──────────────────────────────────────

    def _export_ltm(self, user_id: int) -> list[dict]:
        """Export long-term memory facts."""
        rows = self._db.get_ltm_by_user(user_id)
        return [
            {
                "category": r.get("category", r.get("_category", "")),
                "fact": r.get("fact", ""),
                "source": r.get("_source", r.get("source", "")),
                "updated_at": r.get("_updated_at", r.get("updated_at", "")),
            }
            for r in rows
        ]

    def _export_hypotheses(self, user_id: int) -> list[dict]:
        """Export all hypotheses (active + ruled out)."""
        rows = self._db.get_all_hypotheses(user_id)
        return [
            {
                "title": r.get("title", ""),
                "status": r.get("_status", r.get("status", "")),
                "confidence": r.get("confidence", r.get("_confidence", 0)),
                "evidence_for": r.get("evidence_for", []),
                "evidence_against": r.get("evidence_against", []),
                "missing_tests": r.get("missing_tests", []),
                "notes": r.get("notes", ""),
            }
            for r in rows
        ]

    def _export_journal(self, user_id: int) -> list[dict]:
        """Export medical journal entries."""
        rows = self._db.query_journal(user_id, limit=10000)
        return [
            {
                "speaker": r.get("speaker", ""),
                "content": r.get("content", ""),
                "category": r.get("_category", r.get("category", "")),
                "timestamp": r.get("_timestamp", r.get("timestamp", "")),
                "source": r.get("_source", r.get("source", "")),
            }
            for r in rows
        ]

    def _export_claude_insights(self) -> list[dict]:
        """Export Claude's memory (memory.enc)."""
        claude_dir = self._config.vault_home / "claude"
        enc_path = claude_dir / "memory.enc"
        if not enc_path.exists():
            return []

        try:
            key = self._km.get_key()
            blob = enc_path.read_bytes()
            if len(blob) < 28:
                return []
            nonce = blob[:12]
            ct = blob[12:]
            plaintext = AESGCM(key).decrypt(
                nonce, ct, b"relaxed.memory",
            )
            data = json.loads(plaintext.decode("utf-8"))
            if not isinstance(data, list):
                return []
            # Each item should have at minimum a fact/key
            return [
                {
                    "fact": item.get("fact", item.get("key", str(item))),
                    "category": item.get("category", ""),
                    "timestamp": item.get("timestamp", ""),
                }
                for item in data
            ]
        except Exception as e:
            logger.warning("Failed to read Claude insights: %s", e)
            return []

    def _export_knowledge_base(self) -> list[dict]:
        """Export knowledge base (raw SQL + decrypt loop)."""
        try:
            rows = self._db.conn.execute(
                """SELECT id, topic, source, relevance_score,
                          user_confirmed, category, created_at, encrypted_data
                   FROM knowledge_base
                   ORDER BY created_at DESC""",
            ).fetchall()

            results = []
            for row in rows:
                kb_id = row["id"]
                aad = f"knowledge_base.encrypted_data.{kb_id}"
                try:
                    data = self._db._decrypt(row["encrypted_data"], aad)
                except Exception:
                    data = {}

                results.append({
                    "topic": data.get("topic", row["topic"]),
                    "finding": data.get("finding", ""),
                    "source": data.get("source", row["source"]),
                    "relevance_score": row["relevance_score"],
                    "user_confirmed": bool(row["user_confirmed"]),
                    "category": row["category"],
                })
            return results
        except Exception as e:
            logger.warning("Failed to export knowledge base: %s", e)
            return []

    def _export_external_evidence(self) -> list[dict]:
        """Export external evidence store."""
        from healthbot.research.external_evidence_store import ExternalEvidenceStore

        store = ExternalEvidenceStore(self._db)
        entries = store.list_evidence(limit=10000)
        results = []
        for entry in entries:
            detail = store.get_evidence_detail(entry["evidence_id"])
            if detail:
                results.append({
                    "source": detail.get("_source", entry.get("source", "")),
                    "query": detail.get("prompt_sanitized", ""),
                    "result": detail.get("result_json", detail.get("text", "")),
                    "created_at": detail.get("_created_at", ""),
                    "condition_related": bool(entry.get("expired") is False),
                })
            else:
                results.append({
                    "source": entry.get("source", ""),
                    "query": entry.get("query", ""),
                    "result": entry.get("summary", ""),
                    "created_at": entry.get("created_at", ""),
                    "condition_related": False,
                })
        return results

    # ── PII redaction ────────────────────────────────────────────

    def _redact_stores(self, stores: dict[str, list[dict]]) -> dict[str, list[dict]]:
        """Redact PII from all text fields in every store."""
        redacted = {}
        for store_name, records in stores.items():
            redacted[store_name] = [
                self._redact_record(record) for record in records
            ]
        return redacted

    @staticmethod
    def _redact_names(text: str) -> str:
        """Replace heuristic-detected names with [NAME].

        Catches unlabeled person names that PhiFirewall.redact() misses
        (it only handles labeled PII like SSN, MRN, DOB, etc.).
        """
        names = Anonymizer._heuristic_name_scan(text)
        if not names:
            return text
        result = text
        for name in sorted(names, key=len, reverse=True):
            result = result.replace(name, "[NAME]")
        return result

    def _redact_record(self, record: dict) -> dict:
        """Redact all string fields in a single record (recursive)."""
        out = {}
        for key, value in record.items():
            if isinstance(value, str):
                out[key] = self._redact_names(self._fw.redact(value))
            elif isinstance(value, dict):
                out[key] = self._redact_record(value)
            elif isinstance(value, list):
                out[key] = [self._redact_list_item(item) for item in value]
            else:
                out[key] = value
        return out

    def _redact_list_item(self, item):
        """Redact a single list item (str or nested dict)."""
        if isinstance(item, str):
            return self._redact_names(self._fw.redact(item))
        if isinstance(item, dict):
            return self._redact_record(item)
        return item
