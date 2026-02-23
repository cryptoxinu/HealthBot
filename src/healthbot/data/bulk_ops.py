"""Bulk data operations: reset, selective delete, orphan cleanup.

Keeps bulk operations out of db.py. All methods require an open HealthDB
with the vault unlocked (encryption key available).
"""
from __future__ import annotations

import logging

from healthbot.data.db import HealthDB
from healthbot.security.vault import Vault

logger = logging.getLogger("healthbot")

# Map user-facing category names to (table_name, primary_key_column) pairs.
# "memory" is special -- covers both STM and LTM tables.
CATEGORY_TABLE: dict[str, list[tuple[str, str]]] = {
    "labs": [("observations", "obs_id")],
    "medications": [("medications", "med_id")],
    "wearable": [("wearable_daily", "id")],
    "documents": [("documents", "doc_id")],
    "concerns": [("concerns", "concern_id")],
    "evidence": [("external_evidence", "evidence_id")],
    "memory": [("memory_stm", "id"), ("memory_ltm", "id")],
    "hypotheses": [("hypotheses", "id")],
}

# Categories that have rows in search_index (record_type values)
SEARCH_INDEX_RECORD_TYPES: dict[str, list[str]] = {
    "labs": ["lab_result", "vital_sign", "user_event"],
}

# Known vector-store blob names (not tied to documents table)
VECTOR_BLOB_NAMES = {"vec_tfidf", "vocab_tfidf", "dense_search"}


class BulkOps:
    """Bulk delete and reset operations for the health vault."""

    def __init__(
        self,
        db: HealthDB,
        vault: Vault | None = None,
        clean_db: object | None = None,
        config: object | None = None,
    ) -> None:
        self._db = db
        self._vault = vault
        self._clean_db = clean_db
        self._config = config

    def count_all(self) -> dict[str, int]:
        """Return row counts per user-facing category."""
        counts: dict[str, int] = {}
        for category, tables in CATEGORY_TABLE.items():
            total = 0
            for table, _ in tables:
                try:
                    row = self._db.conn.execute(
                        f"SELECT COUNT(*) AS n FROM {table}"  # noqa: S608
                    ).fetchone()
                    total += row["n"] if row else 0
                except Exception:
                    pass  # Table may not exist if migrations haven't run
            counts[category] = total
        # search_index separately
        try:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS n FROM search_index"
            ).fetchone()
            counts["search"] = row["n"] if row else 0
        except Exception:
            counts["search"] = 0
        return counts

    def delete_category(self, category: str) -> int:
        """Delete all rows from a category. Returns total rows deleted.

        Also cleans related search_index entries and orphaned blobs.
        """
        if category == "all":
            result = self.reset_all()
            return sum(result.values())

        tables = CATEGORY_TABLE.get(category)
        if tables is None:
            raise ValueError(
                f"Unknown category: {category!r}. "
                f"Valid: {', '.join(sorted(CATEGORY_TABLE))} or 'all'"
            )

        total_deleted = 0

        # For documents, collect blob paths before deletion
        blob_paths: list[str] = []
        if category == "documents" and self._vault:
            rows = self._db.conn.execute(
                "SELECT enc_blob_path FROM documents WHERE enc_blob_path != ''"
            ).fetchall()
            blob_paths = [r["enc_blob_path"] for r in rows]

        for table, _ in tables:
            cursor = self._db.conn.execute(f"DELETE FROM {table}")  # noqa: S608
            total_deleted += cursor.rowcount

        self._db.conn.commit()

        # Clean search_index for this category
        record_types = SEARCH_INDEX_RECORD_TYPES.get(category)
        if record_types:
            placeholders = ",".join("?" for _ in record_types)
            self._db.conn.execute(
                f"DELETE FROM search_index WHERE record_type IN ({placeholders})",
                record_types,
            )
            self._db.conn.commit()

        # Delete associated blobs
        if blob_paths and self._vault:
            for blob_path in blob_paths:
                try:
                    self._vault.delete_blob(blob_path)
                except Exception as e:
                    logger.warning("Failed to delete blob %s: %s", blob_path, e)

        logger.info("Deleted %d rows from category '%s'", total_deleted, category)
        return total_deleted

    def reset_all(self) -> dict[str, int]:
        """Delete ALL health data from all tables. Returns {category: count}.

        Does NOT delete vault_meta or alert_log (operational tables).
        """
        results: dict[str, int] = {}
        for category in CATEGORY_TABLE:
            results[category] = self.delete_category(category)

        # Clean entire search_index
        try:
            cursor = self._db.conn.execute("DELETE FROM search_index")
            self._db.conn.commit()
            results["search"] = cursor.rowcount
        except Exception:
            results["search"] = 0

        # Clean orphan blobs
        if self._vault:
            results["orphan_blobs"] = self.cleanup_orphan_blobs()

        return results

    def delete_document_cascade(self, doc_id: str) -> dict[str, int]:
        """Delete a single document and all its derived data.

        Cascade chain:
        1. Look up enc_blob_path and meta.redacted_blob_id
        2. Delete observations where source_doc_id matches the blob path
        3. Delete search_index entries for those observations
        4. Delete search_index entry for the clinical note (keyed by doc_id)
        5. Delete the document row
        6. Delete vault blobs (original + redacted)
        7. Delete clean_observations for this document

        Does NOT delete hypotheses/KB/journal (aggregate across documents).

        Returns counts dict of deleted items.
        """
        counts: dict[str, int] = {}

        # 1. Look up blob paths
        row = self._db.conn.execute(
            "SELECT enc_blob_path FROM documents WHERE doc_id = ?",
            (doc_id,),
        ).fetchone()
        if not row:
            return counts

        enc_blob_path = row["enc_blob_path"] or ""
        redacted_blob_id = ""
        try:
            meta = self._db.get_document_meta(doc_id)
            redacted_blob_id = meta.get("redacted_blob_id", "")
        except Exception:
            pass

        # 2. Get observation IDs for this document, then delete
        obs_ids: list[str] = []
        if enc_blob_path:
            obs_rows = self._db.conn.execute(
                "SELECT obs_id FROM observations WHERE source_doc_id = ?",
                (enc_blob_path,),
            ).fetchall()
            obs_ids = [r["obs_id"] for r in obs_rows]

            cursor = self._db.conn.execute(
                "DELETE FROM observations WHERE source_doc_id = ?",
                (enc_blob_path,),
            )
            counts["observations"] = cursor.rowcount

        # 3. Delete search_index entries for those observations
        if obs_ids:
            placeholders = ",".join("?" for _ in obs_ids)
            cursor = self._db.conn.execute(
                f"DELETE FROM search_index WHERE doc_id IN ({placeholders})",  # noqa: S608
                obs_ids,
            )
            counts["search_obs"] = cursor.rowcount

        # 4. Delete search_index entry for the clinical note (keyed by doc_id)
        cursor = self._db.conn.execute(
            "DELETE FROM search_index WHERE doc_id = ?",
            (doc_id,),
        )
        counts["search_clinical"] = cursor.rowcount

        # 5. Delete the document row
        cursor = self._db.conn.execute(
            "DELETE FROM documents WHERE doc_id = ?",
            (doc_id,),
        )
        counts["documents"] = cursor.rowcount

        self._db.conn.commit()

        # 6. Delete vault blobs (original + redacted)
        if self._vault:
            if enc_blob_path:
                try:
                    self._vault.delete_blob(enc_blob_path)
                    counts["vault_blobs"] = counts.get("vault_blobs", 0) + 1
                except Exception as e:
                    logger.warning("Failed to delete blob %s: %s", enc_blob_path, e)
            if redacted_blob_id:
                try:
                    self._vault.delete_blob(redacted_blob_id)
                    counts["vault_blobs"] = counts.get("vault_blobs", 0) + 1
                except Exception as e:
                    logger.warning("Failed to delete redacted blob %s: %s", redacted_blob_id, e)

        # 7. Clean DB: remove observations for this document
        if self._clean_db and enc_blob_path:
            try:
                cursor = self._clean_db.conn.execute(
                    "DELETE FROM clean_observations WHERE source_doc_id = ?",
                    (enc_blob_path,),
                )
                counts["clean_observations"] = cursor.rowcount
                self._clean_db.conn.commit()
            except Exception as e:
                logger.warning("Clean DB cleanup for doc %s: %s", doc_id, e)

        total = sum(counts.values())
        logger.info("Deleted document %s cascade: %s (total %d)", doc_id, counts, total)
        return counts

    def delete_lab_records(self) -> dict[str, int]:
        """Delete lab results, source PDFs, and ALL lab-derived data.

        Wipes every location where lab data or lab-derived insights persist:
        - Raw vault: observations, documents, search_index, hypotheses,
          knowledge_base, medical_journal
        - Vault blobs: original + redacted PDFs, TF-IDF vectors
        - Clean DB: clean_observations, clean_hypotheses,
          clean_health_context, clean_user_memory
        - Claude state: health_data.enc, memory.enc

        Preserves: survey data (LTM), medications, wearable data,
        demographics, conversation STM.

        Returns counts of deleted rows by category.
        """
        counts: dict[str, int] = {}

        # 1. Collect blob paths + redacted blob IDs before deletion
        blob_paths: list[str] = []
        redacted_blob_ids: list[str] = []
        if self._vault:
            try:
                rows = self._db.conn.execute(
                    "SELECT enc_blob_path, doc_id FROM documents "
                    "WHERE source IN ('telegram_pdf', 'mychart') "
                    "AND enc_blob_path != ''"
                ).fetchall()
                blob_paths = [r["enc_blob_path"] for r in rows]
                for r in rows:
                    try:
                        meta = self._db.get_document_meta(r["doc_id"])
                        rbid = meta.get("redacted_blob_id")
                        if rbid:
                            redacted_blob_ids.append(rbid)
                    except Exception:
                        pass
            except Exception:
                pass

        # 2. Delete lab observations only (keep vital_sign, wearable, etc.)
        try:
            cursor = self._db.conn.execute(
                "DELETE FROM observations WHERE record_type = 'lab_result'"
            )
            counts["lab_results"] = cursor.rowcount
        except Exception:
            counts["lab_results"] = 0

        # 3. Delete lab-source documents (keep apple_health, whoop, etc.)
        try:
            cursor = self._db.conn.execute(
                "DELETE FROM documents WHERE source IN ('telegram_pdf', 'mychart')"
            )
            counts["documents"] = cursor.rowcount
        except Exception:
            counts["documents"] = 0

        # 4. Clean search index for lab entries
        try:
            cursor = self._db.conn.execute(
                "DELETE FROM search_index "
                "WHERE record_type IN ('lab_result', 'clinical_note')"
            )
            counts["search_entries"] = cursor.rowcount
        except Exception:
            counts["search_entries"] = 0

        self._db.conn.commit()

        # 5. Delete vault blobs for removed documents
        deleted_blobs = 0
        for blob_path in blob_paths:
            try:
                self._vault.delete_blob(blob_path)
                deleted_blobs += 1
            except Exception as e:
                logger.warning("Failed to delete blob %s: %s", blob_path, e)
        counts["vault_blobs"] = deleted_blobs

        # 6. Delete redacted PDF blobs (stored separately from originals)
        redacted_deleted = 0
        for rbid in redacted_blob_ids:
            try:
                self._vault.delete_blob(rbid)
                redacted_deleted += 1
            except Exception as e:
                logger.warning("Failed to delete redacted blob %s: %s", rbid, e)
        counts["redacted_blobs"] = redacted_deleted

        # 7. Clean DB: remove lab observations (Tier 2 anonymized copies)
        if self._clean_db:
            try:
                cursor = self._clean_db.conn.execute(
                    "DELETE FROM clean_observations WHERE record_type = 'lab_result'"
                )
                counts["clean_observations"] = cursor.rowcount
                self._clean_db.conn.commit()
            except Exception as e:
                logger.warning("Clean DB lab cleanup failed: %s", e)

        # 8. Claude state: delete health data + memory (contain lab summaries)
        if self._config:
            claude_dir = self._config.claude_dir
            for fname in ("health_data.enc", "memory.enc"):
                fpath = claude_dir / fname
                if fpath.exists():
                    try:
                        fpath.unlink()
                        counts[f"claude_{fname}"] = 1
                        logger.info("Deleted Claude state: %s", fname)
                    except Exception as e:
                        logger.warning("Failed to delete %s: %s", fname, e)

        # 9. Raw vault: hypotheses (lab-derived insights Claude uses)
        try:
            cursor = self._db.conn.execute("DELETE FROM hypotheses")
            counts["hypotheses"] = cursor.rowcount
        except Exception:
            counts["hypotheses"] = 0

        # 10. Raw vault: knowledge_base (research findings tied to labs)
        try:
            cursor = self._db.conn.execute("DELETE FROM knowledge_base")
            counts["knowledge_base"] = cursor.rowcount
        except Exception:
            counts["knowledge_base"] = 0

        # 11. Raw vault: medical_journal (lab narrative entries)
        try:
            cursor = self._db.conn.execute("DELETE FROM medical_journal")
            counts["medical_journal"] = cursor.rowcount
        except Exception:
            counts["medical_journal"] = 0

        self._db.conn.commit()

        # 12. Clean DB: wipe all lab-derived insight tables
        if self._clean_db:
            for table in (
                "clean_hypotheses",
                "clean_health_context",
                "clean_user_memory",
            ):
                try:
                    cursor = self._clean_db.conn.execute(
                        f"DELETE FROM {table}"  # noqa: S608
                    )
                    counts[table] = cursor.rowcount
                except Exception as e:
                    logger.warning("Clean DB %s cleanup: %s", table, e)
            try:
                self._clean_db.conn.commit()
            except Exception:
                pass

        # 13. Delete TF-IDF vector blobs (contain lab term indices)
        if self._vault:
            for blob_name in VECTOR_BLOB_NAMES:
                try:
                    self._vault.delete_blob(blob_name)
                    counts[f"vector_{blob_name}"] = 1
                except Exception:
                    pass  # May not exist

        total = sum(counts.values())
        logger.info("Deleted lab records (full wipe): %s (total %d)", counts, total)
        return counts

    def count_lab_records(self) -> dict[str, int]:
        """Count lab results and lab-source documents."""
        counts: dict[str, int] = {}
        try:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS n FROM observations "
                "WHERE record_type = 'lab_result'"
            ).fetchone()
            counts["lab_results"] = row["n"] if row else 0
        except Exception:
            counts["lab_results"] = 0

        try:
            row = self._db.conn.execute(
                "SELECT COUNT(*) AS n FROM documents "
                "WHERE source IN ('telegram_pdf', 'mychart')"
            ).fetchone()
            counts["documents"] = row["n"] if row else 0
        except Exception:
            counts["documents"] = 0

        return counts

    def vacuum(self) -> None:
        """Run VACUUM to reclaim disk space after bulk deletes."""
        self._db.conn.execute("VACUUM")

    def cleanup_orphan_blobs(self) -> int:
        """Delete blob files not referenced by any documents row."""
        if not self._vault:
            return 0

        all_blobs = set(self._vault.list_blobs())
        if not all_blobs:
            return 0

        # Blobs referenced by documents
        try:
            rows = self._db.conn.execute(
                "SELECT enc_blob_path FROM documents WHERE enc_blob_path != ''"
            ).fetchall()
            referenced = {r["enc_blob_path"] for r in rows}
        except Exception:
            referenced = set()

        orphans = all_blobs - referenced - VECTOR_BLOB_NAMES
        for blob_id in orphans:
            try:
                self._vault.delete_blob(blob_id)
            except Exception as e:
                logger.warning("Failed to delete orphan blob %s: %s", blob_id, e)

        if orphans:
            logger.info("Cleaned up %d orphan blobs", len(orphans))
        return len(orphans)
