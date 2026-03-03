"""Knowledge import — validates, PII-checks, deduplicates, inserts.

Handles both plain JSON and encrypted (.enc) exports.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall

logger = logging.getLogger("healthbot")

_EXPORT_FORMAT = "healthbot_knowledge_export"


@dataclass
class ImportReport:
    """Summary of a knowledge import operation."""

    ltm_facts: int = 0
    hypotheses: int = 0
    medical_journal: int = 0
    claude_insights: int = 0
    knowledge_base: int = 0
    external_evidence: int = 0
    duplicates_skipped: int = 0
    pii_redacted: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total_imported(self) -> int:
        return (
            self.ltm_facts + self.hypotheses + self.medical_journal
            + self.claude_insights + self.knowledge_base + self.external_evidence
        )

    def summary(self) -> str:
        parts = []
        if self.ltm_facts:
            parts.append(f"{self.ltm_facts} LTM facts")
        if self.hypotheses:
            parts.append(f"{self.hypotheses} hypotheses")
        if self.medical_journal:
            parts.append(f"{self.medical_journal} journal entries")
        if self.claude_insights:
            parts.append(f"{self.claude_insights} Claude insights")
        if self.knowledge_base:
            parts.append(f"{self.knowledge_base} KB entries")
        if self.external_evidence:
            parts.append(f"{self.external_evidence} evidence entries")

        msg = f"Imported: {', '.join(parts)}." if parts else "No new records imported."
        if self.duplicates_skipped:
            msg += f"\nSkipped {self.duplicates_skipped} duplicates."
        if self.pii_redacted:
            msg += f"\nRedacted PII in {self.pii_redacted} records."
        if self.errors:
            msg += f"\n{len(self.errors)} error(s) encountered."
        return msg


class KnowledgeImporter:
    """Import knowledge from a previously exported JSON payload."""

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

    def import_bytes(
        self,
        data: bytes,
        user_id: int,
        password: str | None = None,
    ) -> ImportReport:
        """Import from raw bytes (JSON or encrypted).

        Args:
            data: Raw file bytes (.json or .enc).
            user_id: Target Telegram user ID.
            password: Required for encrypted exports.

        Returns:
            ImportReport with counts and any errors.
        """
        report = ImportReport()

        # Decrypt if password provided
        if password:
            try:
                from healthbot.export.encrypted_export import EncryptedExport
                data = EncryptedExport.decrypt_export(data, password)
            except Exception as e:
                report.errors.append(f"Decryption failed: {e}")
                return report

        # Parse JSON
        try:
            payload = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            report.errors.append(f"Invalid JSON: {e}")
            return report

        # Validate format
        if payload.get("format") != _EXPORT_FORMAT:
            report.errors.append(
                f"Unknown format: {payload.get('format')}. "
                f"Expected: {_EXPORT_FORMAT}"
            )
            return report

        stores = payload.get("stores", {})

        # Import each store
        self._import_ltm(stores.get("ltm_facts", []), user_id, report)
        self._import_hypotheses(stores.get("hypotheses", []), user_id, report)
        self._import_journal(stores.get("medical_journal", []), user_id, report)
        self._import_claude_insights(stores.get("claude_insights", []), report)
        self._import_knowledge_base(stores.get("knowledge_base", []), report)
        self._import_external_evidence(stores.get("external_evidence", []), report)

        return report

    # ── Per-store importers ──────────────────────────────────────

    def _import_ltm(
        self, records: list[dict], user_id: int, report: ImportReport,
    ) -> None:
        """Import LTM facts with exact (category, fact) dedup."""
        existing = self._db.get_ltm_by_user(user_id)
        existing_set = {
            (r.get("category", r.get("_category", "")), r.get("fact", ""))
            for r in existing
        }

        for record in records:
            try:
                fact = self._pii_check(record.get("fact", ""), report)
                category = record.get("category", "")
                if (category, fact) in existing_set:
                    report.duplicates_skipped += 1
                    continue
                self._db.insert_ltm(
                    user_id, category, fact,
                    source=record.get("source", "knowledge_import"),
                )
                report.ltm_facts += 1
                existing_set.add((category, fact))
            except Exception as e:
                report.errors.append(f"LTM import error: {e}")

    def _import_hypotheses(
        self, records: list[dict], user_id: int, report: ImportReport,
    ) -> None:
        """Import hypotheses using HypothesisTracker.upsert_hypothesis() for dedup."""
        from healthbot.reasoning.hypothesis_tracker import HypothesisTracker

        tracker = HypothesisTracker(self._db)

        for record in records:
            try:
                title = self._pii_check(record.get("title", ""), report)
                if not title:
                    continue
                incoming = {
                    "title": title,
                    "status": record.get("status", "active"),
                    "confidence": record.get("confidence", 0),
                    "evidence_for": [
                        self._pii_check(e, report)
                        for e in record.get("evidence_for", [])
                        if isinstance(e, str)
                    ],
                    "evidence_against": [
                        self._pii_check(e, report)
                        for e in record.get("evidence_against", [])
                        if isinstance(e, str)
                    ],
                    "missing_tests": record.get("missing_tests", []),
                    "notes": self._pii_check(record.get("notes", ""), report),
                }
                tracker.upsert_hypothesis(user_id, incoming)
                report.hypotheses += 1
            except Exception as e:
                report.errors.append(f"Hypothesis import error: {e}")

    def _import_journal(
        self, records: list[dict], user_id: int, report: ImportReport,
    ) -> None:
        """Import journal entries with (timestamp, speaker) dedup."""
        existing = self._db.query_journal(user_id, limit=10000)
        existing_set = {
            (r.get("_timestamp", r.get("timestamp", "")), r.get("speaker", ""))
            for r in existing
        }

        for record in records:
            try:
                ts = record.get("timestamp", "")
                speaker = record.get("speaker", "")
                if (ts, speaker) in existing_set:
                    report.duplicates_skipped += 1
                    continue
                content = self._pii_check(record.get("content", ""), report)
                self._db.insert_journal_entry(
                    user_id, speaker, content,
                    category=record.get("category", ""),
                    source=record.get("source", "knowledge_import"),
                )
                report.medical_journal += 1
                existing_set.add((ts, speaker))
            except Exception as e:
                report.errors.append(f"Journal import error: {e}")

    def _import_claude_insights(
        self, records: list[dict], report: ImportReport,
    ) -> None:
        """Import Claude insights into memory.enc.

        Exact match on `fact` text against existing _memory list.
        """
        from healthbot.llm.claude_context import ensure_claude_dir

        claude_dir = ensure_claude_dir(self._config.vault_home)
        existing_memory = self._load_claude_memory(claude_dir)
        existing_facts = {item.get("fact", "") for item in existing_memory}

        added = 0
        for record in records:
            try:
                fact = self._pii_check(record.get("fact", ""), report)
                if not fact or fact in existing_facts:
                    if fact in existing_facts:
                        report.duplicates_skipped += 1
                    continue
                existing_memory.append({
                    "fact": fact,
                    "category": record.get("category", ""),
                    "timestamp": record.get("timestamp", ""),
                })
                existing_facts.add(fact)
                added += 1
            except Exception as e:
                report.errors.append(f"Claude insight import error: {e}")

        if added:
            self._save_claude_memory(claude_dir, existing_memory)
            report.claude_insights = added

    def _import_knowledge_base(
        self, records: list[dict], report: ImportReport,
    ) -> None:
        """Import KB entries with find_similar dedup."""
        from healthbot.research.knowledge_base import KnowledgeBase

        kb = KnowledgeBase(self._db)

        for record in records:
            try:
                topic = self._pii_check(record.get("topic", ""), report)
                finding = self._pii_check(record.get("finding", ""), report)
                source = record.get("source", "knowledge_import")

                if kb.find_similar(topic, finding, source, threshold=0.80):
                    report.duplicates_skipped += 1
                    continue

                kb.store_finding(
                    topic, finding, source,
                    relevance_score=record.get("relevance_score", 0.5),
                    user_confirmed=record.get("user_confirmed", False),
                )
                report.knowledge_base += 1
            except Exception as e:
                report.errors.append(f"KB import error: {e}")

    def _import_external_evidence(
        self, records: list[dict], report: ImportReport,
    ) -> None:
        """Import external evidence with SHA256(query) dedup."""
        from healthbot.research.external_evidence_store import ExternalEvidenceStore

        store = ExternalEvidenceStore(self._db)

        for record in records:
            try:
                query = self._pii_check(record.get("query", ""), report)
                if not query:
                    continue

                # SHA256 dedup via lookup_cached
                if store.lookup_cached(query) is not None:
                    report.duplicates_skipped += 1
                    continue

                store.store(
                    source=record.get("source", "knowledge_import"),
                    query=query,
                    result=record.get("result", ""),
                    condition_related=record.get("condition_related", False),
                )
                report.external_evidence += 1
            except Exception as e:
                report.errors.append(f"Evidence import error: {e}")

    # ── PII belt-and-suspenders ──────────────────────────────────

    def _pii_check(self, text: str, report: ImportReport) -> str:
        """Check text for PII, redact if found. Counts redactions."""
        if not text:
            return text
        if self._fw.contains_phi(text):
            report.pii_redacted += 1
            return self._fw.redact(text)
        return text

    # ── Claude memory helpers ────────────────────────────────────

    def _load_claude_memory(self, claude_dir) -> list[dict]:
        """Load existing Claude memory from encrypted file."""
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

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
            plaintext = AESGCM(key).decrypt(nonce, ct, b"relaxed.memory")
            data = json.loads(plaintext.decode("utf-8"))
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.warning("Failed to load Claude memory for import: %s", e)
            return []

    def _save_claude_memory(self, claude_dir, memory: list[dict]) -> None:
        """Save Claude memory back to encrypted file."""
        import os

        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        try:
            key = self._km.get_key()
            text = json.dumps(memory, indent=2, ensure_ascii=False)
            nonce = os.urandom(12)
            ct = AESGCM(key).encrypt(
                nonce, text.encode("utf-8"), b"relaxed.memory",
            )
            path = claude_dir / "memory.enc"
            path.write_bytes(nonce + ct)
        except Exception as e:
            logger.error("Failed to save Claude memory after import: %s", e)


def is_knowledge_export(data: bytes) -> bool:
    """Quick check if bytes look like a knowledge export JSON."""
    try:
        # Only peek at the first ~200 bytes for format field
        prefix = data[:200].decode("utf-8", errors="ignore")
        return _EXPORT_FORMAT in prefix
    except Exception:
        return False
