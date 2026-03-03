"""Document storage and retrieval methods."""
from __future__ import annotations

import logging
import uuid
from typing import Any

logger = logging.getLogger("healthbot")


class DocumentsMixin:
    """Mixin for document-related database operations."""

    def insert_document(
        self, doc, user_id: int = 0, commit: bool = True,
    ) -> str:
        """Insert a document record.

        The filename is stored inside meta_encrypted (not in the
        plaintext ``filename`` column) to avoid leaking document
        names on disk.
        """
        doc_id = doc.id or uuid.uuid4().hex
        aad = f"documents.meta_encrypted.{doc_id}"
        # Merge filename into meta blob so it's encrypted
        meta = dict(doc.meta) if doc.meta else {}
        if doc.filename:
            meta["filename"] = doc.filename
        meta_enc = self._encrypt(meta, aad) if meta else None
        try:
            self.conn.execute(
                """INSERT INTO documents (doc_id, source, sha256, received_at,
                   mime_type, size_bytes, page_count, enc_blob_path, filename,
                   meta_encrypted, user_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (doc_id, doc.source, doc.sha256, self._now(),
                 doc.mime_type, doc.size_bytes, doc.page_count,
                 doc.enc_blob_path, '', meta_enc, user_id),
            )
        except Exception:
            # Fallback for pre-migration schema without user_id column
            self.conn.execute(
                """INSERT INTO documents (doc_id, source, sha256, received_at,
                   mime_type, size_bytes, page_count, enc_blob_path, filename,
                   meta_encrypted)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (doc_id, doc.source, doc.sha256, self._now(),
                 doc.mime_type, doc.size_bytes, doc.page_count,
                 doc.enc_blob_path, '', meta_enc),
            )
        if commit:
            self.conn.commit()
        return doc_id

    def document_exists_by_sha256(self, sha256: str) -> dict | None:
        """Check if a document with this SHA256 already exists."""
        row = self.conn.execute(
            "SELECT doc_id, filename, received_at, enc_blob_path"
            " FROM documents WHERE sha256 = ?",
            (sha256,),
        ).fetchone()
        return dict(row) if row else None

    def delete_document(self, doc_id: str) -> None:
        """Delete a document record by ID (e.g., on parse failure)."""
        self.conn.execute("DELETE FROM documents WHERE doc_id = ?", (doc_id,))
        self.conn.commit()

    def get_observation_keys_for_doc(self, source_doc_id: str) -> set[tuple[str, str | None]]:
        """Return (canonical_name, date_effective) pairs for a document's observations."""
        rows = self.conn.execute(
            "SELECT canonical_name, date_effective FROM observations"
            " WHERE source_doc_id = ?",
            (source_doc_id,),
        ).fetchall()
        return {(r["canonical_name"], r["date_effective"]) for r in rows}

    def get_observation_details_for_doc(
        self, source_doc_id: str,
    ) -> dict[tuple[str, str | None], dict]:
        """Return {(canonical_name, date_effective): {obs_id, value, ...}} for a document.

        Used during rescan to detect corrected lab values. The encrypted_data
        is decrypted to extract the original numeric value.
        """
        rows = self.conn.execute(
            "SELECT obs_id, canonical_name, date_effective, encrypted_data"
            " FROM observations WHERE source_doc_id = ?",
            (source_doc_id,),
        ).fetchall()
        result: dict[tuple[str, str | None], dict] = {}
        for r in rows:
            aad = f"observations.encrypted_data.{r['obs_id']}"
            try:
                data = self._decrypt(r["encrypted_data"], aad)
            except Exception as e:
                logger.warning("Decrypt failed for observations row %s: %s", r["obs_id"], e)
                data = {}
            key = (r["canonical_name"], r["date_effective"])
            result[key] = {
                "obs_id": r["obs_id"],
                "value": data.get("value"),
                "reference_low": data.get("reference_low"),
                "reference_high": data.get("reference_high"),
            }
        return result

    def update_observation_value(
        self,
        obs_id: str,
        obs,
        user_id: int = 0,
        age_at_collection: int | None = None,
        commit: bool = True,
    ) -> None:
        """Update an existing observation's encrypted data and set corrected_at.

        Used when a corrected lab report provides a new value for the same
        (canonical_name, date_collected) key.
        """
        from healthbot.data.models import LabResult

        aad = f"observations.encrypted_data.{obs_id}"
        enc_data = self._encrypt(obs, aad)
        triage = "normal"
        flag = ""
        if isinstance(obs, LabResult):
            triage = obs.triage_level.value
            flag = obs.flag
        try:
            self.conn.execute(
                """UPDATE observations SET encrypted_data = ?, triage_level = ?,
                   flag = ?, corrected_at = ? WHERE obs_id = ?""",
                (enc_data, triage, flag, self._now(), obs_id),
            )
        except Exception:
            # corrected_at column may not exist yet (pre-migration)
            self.conn.execute(
                """UPDATE observations SET encrypted_data = ?, triage_level = ?,
                   flag = ? WHERE obs_id = ?""",
                (enc_data, triage, flag, obs_id),
            )
        if commit:
            self.conn.commit()

    def update_document_rescanned(
        self, doc_id: str, commit: bool = True,
    ) -> None:
        """Set last_rescanned timestamp on a document."""
        try:
            self.conn.execute(
                "UPDATE documents SET last_rescanned = ? WHERE doc_id = ?",
                (self._now(), doc_id),
            )
            if commit:
                self.conn.commit()
        except Exception:
            pass  # Column may not exist in pre-migration schema

    def list_documents(self, user_id: int | None = None) -> list[dict]:
        """Return all documents, optionally filtered by user_id."""
        sql = "SELECT * FROM documents"
        params: list[Any] = []
        if user_id is not None:
            sql += " WHERE user_id = ?"
            params.append(user_id)
        sql += " ORDER BY received_at DESC"
        rows = self.conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def get_document_meta(self, doc_id: str) -> dict:
        """Decrypt and return the meta dict for a document."""
        row = self.conn.execute(
            "SELECT meta_encrypted FROM documents WHERE doc_id = ?",
            (doc_id,),
        ).fetchone()
        if not row or not row["meta_encrypted"]:
            return {}
        aad = f"documents.meta_encrypted.{doc_id}"
        try:
            return self._decrypt(row["meta_encrypted"], aad)
        except Exception:
            return {}

    def get_document_filename(self, doc_id: str) -> str:
        """Return the filename for a document.

        Prefers the encrypted ``meta_encrypted.filename`` field; falls
        back to the legacy plaintext ``filename`` column for pre-migration
        rows.
        """
        row = self.conn.execute(
            "SELECT filename, meta_encrypted FROM documents WHERE doc_id = ?",
            (doc_id,),
        ).fetchone()
        if not row:
            return ""
        # Try encrypted meta first
        if row["meta_encrypted"]:
            aad = f"documents.meta_encrypted.{doc_id}"
            try:
                meta = self._decrypt(row["meta_encrypted"], aad)
                if isinstance(meta, dict) and meta.get("filename"):
                    return meta["filename"]
            except Exception:
                pass
        # Fallback to legacy plaintext column
        return row["filename"] or ""

    def migrate_document_filenames(self) -> int:
        """Migrate plaintext filename column into meta_encrypted.

        Call after vault unlock. Returns number of rows migrated.
        """
        rows = self.conn.execute(
            "SELECT doc_id, filename, meta_encrypted FROM documents"
            " WHERE filename IS NOT NULL AND filename != ''",
        ).fetchall()
        migrated = 0
        for r in rows:
            doc_id = r["doc_id"]
            fname = r["filename"]
            aad = f"documents.meta_encrypted.{doc_id}"
            # Decrypt existing meta or start fresh
            meta: dict = {}
            if r["meta_encrypted"]:
                try:
                    meta = self._decrypt(r["meta_encrypted"], aad)
                    if not isinstance(meta, dict):
                        meta = {}
                except Exception:
                    meta = {}
            if meta.get("filename"):
                # Already has filename in encrypted meta — just blank the column
                self.conn.execute(
                    "UPDATE documents SET filename = '' WHERE doc_id = ?",
                    (doc_id,),
                )
                migrated += 1
                continue
            meta["filename"] = fname
            meta_enc = self._encrypt(meta, aad)
            self.conn.execute(
                "UPDATE documents SET filename = '', meta_encrypted = ?"
                " WHERE doc_id = ?",
                (meta_enc, doc_id),
            )
            migrated += 1
        if migrated:
            self.conn.commit()
            logger.info("Migrated %d document filenames to meta_encrypted", migrated)
        return migrated

    def update_document_meta(
        self, doc_id: str, meta: dict, commit: bool = True,
    ) -> None:
        """Encrypt and update the meta dict for a document."""
        aad = f"documents.meta_encrypted.{doc_id}"
        meta_enc = self._encrypt(meta, aad)
        self.conn.execute(
            "UPDATE documents SET meta_encrypted = ? WHERE doc_id = ?",
            (meta_enc, doc_id),
        )
        if commit:
            self.conn.commit()

    def update_document_routing_status(
        self, doc_id: str, *, status: str = "done", error: str = "",
    ) -> None:
        """Update the routing_status and routing_error for a document."""
        try:
            self.conn.execute(
                "UPDATE documents SET routing_status = ?, routing_error = ? WHERE doc_id = ?",
                (status, error, doc_id),
            )
            self.conn.commit()
        except Exception:
            pass  # Columns may not exist in pre-migration schema

    def get_pending_routing_documents(self, user_id: int | None = None) -> list[dict]:
        """Return documents with routing_status = 'pending_routing'."""
        sql = "SELECT * FROM documents WHERE routing_status = 'pending_routing'"
        params: list[Any] = []
        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        sql += " ORDER BY received_at ASC"
        try:
            rows = self.conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []
