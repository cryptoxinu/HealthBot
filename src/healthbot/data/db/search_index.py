"""Full-text search index and migration methods."""
from __future__ import annotations

import logging

logger = logging.getLogger("healthbot")


class SearchIndexMixin:
    """Mixin for search index database operations."""

    def upsert_search_text(
        self, doc_id: str, record_type: str,
        date_effective: str | None, text: str,
        commit: bool = True,
    ) -> None:
        """Update the search index text for a record.

        Encrypts text into ``encrypted_text`` and writes NULL to the
        legacy ``text_for_search`` column to avoid plaintext storage.
        """
        aad = f"search_index.encrypted_text.{doc_id}"
        enc_text = self._encrypt(text, aad)
        try:
            self.conn.execute(
                """INSERT OR REPLACE INTO search_index
                   (doc_id, record_type, date_effective, text_for_search,
                    encrypted_text)
                   VALUES (?, ?, ?, NULL, ?)""",
                (doc_id, record_type, date_effective, enc_text),
            )
        except Exception:
            # Fallback for pre-migration schema without encrypted_text column
            self.conn.execute(
                """INSERT OR REPLACE INTO search_index
                   (doc_id, record_type, date_effective, text_for_search)
                   VALUES (?, ?, ?, ?)""",
                (doc_id, record_type, date_effective, text),
            )
        if commit:
            self.conn.commit()

    def get_all_search_texts(self) -> list[tuple[str, str, str]]:
        """Return (doc_id, record_type, text) for all indexed records.

        Prefers decrypted ``encrypted_text``; falls back to legacy
        ``text_for_search`` for pre-migration rows.
        """
        try:
            rows = self.conn.execute(
                "SELECT doc_id, record_type, text_for_search, encrypted_text"
                " FROM search_index",
            ).fetchall()
        except Exception:
            # Pre-migration: encrypted_text column doesn't exist yet
            rows = self.conn.execute(
                "SELECT doc_id, record_type, text_for_search FROM search_index",
            ).fetchall()
            return [
                (r["doc_id"], r["record_type"], r["text_for_search"] or "")
                for r in rows
            ]

        results: list[tuple[str, str, str]] = []
        for r in rows:
            doc_id = r["doc_id"]
            enc_blob = r["encrypted_text"]
            if enc_blob:
                aad = f"search_index.encrypted_text.{doc_id}"
                try:
                    data = self._decrypt(enc_blob, aad)
                    text = data if isinstance(data, str) else str(data)
                except Exception:
                    text = r["text_for_search"] or ""
            else:
                text = r["text_for_search"] or ""
            results.append((doc_id, r["record_type"], text))
        return results

    def migrate_search_index_encryption(self) -> int:
        """Migrate existing plaintext search_index rows to encrypted_text.

        Call after vault unlock. Returns number of rows migrated.
        """
        migrated = 0
        try:
            rows = self.conn.execute(
                "SELECT doc_id, text_for_search FROM search_index"
                " WHERE text_for_search IS NOT NULL"
                " AND text_for_search != ''"
                " AND (encrypted_text IS NULL)",
            ).fetchall()
        except Exception:
            return 0

        for r in rows:
            doc_id = r["doc_id"]
            text = r["text_for_search"]
            if not text:
                continue
            aad = f"search_index.encrypted_text.{doc_id}"
            enc_text = self._encrypt(text, aad)
            self.conn.execute(
                "UPDATE search_index SET encrypted_text = ?, text_for_search = NULL"
                " WHERE doc_id = ?",
                (enc_text, doc_id),
            )
            migrated += 1
        if migrated:
            self.conn.commit()
            logger.info("Migrated %d search_index rows to encrypted_text", migrated)
        return migrated
