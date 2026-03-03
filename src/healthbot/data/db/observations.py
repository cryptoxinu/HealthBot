"""Observation (lab results, vitals) CRUD and backfill methods."""
from __future__ import annotations

import logging
import uuid
from typing import Any

from healthbot.data.models import LabResult, VitalSign

logger = logging.getLogger("healthbot")


# Known lab brands for source_lab normalization.  Free-text lab names from
# PDF parsing may contain identifying info ("Dr. Smith Family Practice").
# Only store recognized lab brands; unknown values become empty string.
_KNOWN_LABS: tuple[tuple[str, str], ...] = (
    ("labcorp", "LabCorp"),
    ("quest diagnostics", "Quest Diagnostics"),
    ("quest", "Quest"),
    ("bioreference", "BioReference"),
    ("sonora quest", "Sonora Quest"),
    ("aegis", "Aegis"),
    ("mayo clinic", "Mayo Clinic"),
    ("mayo", "Mayo"),
    ("arup", "ARUP"),
    ("clinical reference laboratory", "Clinical Reference Laboratory"),
    ("crl", "CRL"),
    ("hospital lab", "Hospital Lab"),
    ("kaiser", "Kaiser"),
    ("geisinger", "Geisinger"),
    ("cleveland clinic", "Cleveland Clinic"),
    ("emory", "Emory"),
    ("stanford", "Stanford"),
    ("cedars-sinai", "Cedars-Sinai"),
    ("johns hopkins", "Johns Hopkins"),
    ("intermountain", "Intermountain"),
    ("natera", "Natera"),
    ("exact sciences", "Exact Sciences"),
    ("genomic health", "Genomic Health"),
    ("sonic healthcare", "Sonic Healthcare"),
    ("spectra", "Spectra"),
    ("eurofins", "Eurofins"),
)


def _normalize_source_lab(raw: str) -> str:
    """Normalize to a known lab brand or empty string.

    Prevents potentially identifying free-text (e.g. "Dr. Smith Family
    Practice") from being stored in the plaintext source_lab column.
    """
    if not raw:
        return ""
    lower = raw.strip().lower()
    for pattern, brand in _KNOWN_LABS:
        if pattern in lower:
            return brand
    return ""


class ObservationsMixin:
    """Mixin for observation (lab results, vitals) database operations."""

    def insert_observation(
        self,
        obs: LabResult | VitalSign,
        user_id: int = 0,
        age_at_collection: int | None = None,
        commit: bool = True,
    ) -> str:
        """Insert an observation (lab result, vital sign, etc.)."""
        obs_id = obs.id or uuid.uuid4().hex
        aad = f"observations.encrypted_data.{obs_id}"
        enc_data = self._encrypt(obs, aad)

        record_type = "lab_result"
        canonical_name = ""
        date_eff = None
        triage = "normal"
        flag = ""
        source_doc = ""
        source_page = 0
        source_section = ""

        source_lab = ""

        if isinstance(obs, LabResult):
            canonical_name = obs.canonical_name or obs.test_name.lower()
            date_eff = obs.date_collected.isoformat() if obs.date_collected else None
            triage = obs.triage_level.value
            flag = obs.flag
            source_doc = obs.source_blob_id
            source_page = obs.source_page
            source_section = obs.source_section
            source_lab = _normalize_source_lab(obs.lab_name or "")
        elif isinstance(obs, VitalSign):
            record_type = "vital_sign"
            canonical_name = obs.type
            date_eff = obs.timestamp.isoformat() if obs.timestamp else None
            source_doc = obs.source_blob_id

        try:
            self.conn.execute(
                """INSERT INTO observations (obs_id, record_type, canonical_name,
                   date_effective, triage_level, flag, source_doc_id, source_page,
                   source_section, created_at, encrypted_data, user_id,
                   age_at_collection, source_lab)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (obs_id, record_type, canonical_name, date_eff, triage, flag,
                 source_doc, source_page, source_section, self._now(), enc_data,
                 user_id, age_at_collection, source_lab),
            )
        except Exception:
            # Fallback for pre-migration schema without age_at_collection
            try:
                self.conn.execute(
                    """INSERT INTO observations (obs_id, record_type, canonical_name,
                       date_effective, triage_level, flag, source_doc_id, source_page,
                       source_section, created_at, encrypted_data, user_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (obs_id, record_type, canonical_name, date_eff, triage, flag,
                     source_doc, source_page, source_section, self._now(), enc_data,
                     user_id),
                )
            except Exception:
                # Fallback for pre-migration schema without user_id column
                self.conn.execute(
                    """INSERT INTO observations (obs_id, record_type, canonical_name,
                       date_effective, triage_level, flag, source_doc_id, source_page,
                       source_section, created_at, encrypted_data)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (obs_id, record_type, canonical_name, date_eff, triage, flag,
                     source_doc, source_page, source_section, self._now(), enc_data),
                )
        if commit:
            self.conn.commit()
        return obs_id

    def backfill_source_lab(self) -> int:
        """Populate source_lab from encrypted LabResult data for existing rows.

        Decrypts each observation, extracts lab_name, and stores it in the
        plaintext source_lab column. Returns number of rows updated.
        """
        rows = self.conn.execute(
            "SELECT obs_id, encrypted_data FROM observations "
            "WHERE record_type = 'lab_result' AND "
            "(source_lab IS NULL OR source_lab = '')",
        ).fetchall()
        updated = 0
        for r in rows:
            aad = f"observations.encrypted_data.{r['obs_id']}"
            try:
                data = self._decrypt(r["encrypted_data"], aad)
            except Exception:
                continue
            lab_name = _normalize_source_lab(data.get("lab_name", ""))
            if lab_name:
                self.conn.execute(
                    "UPDATE observations SET source_lab = ? WHERE obs_id = ?",
                    (lab_name, r["obs_id"]),
                )
                updated += 1
        if updated:
            self.conn.commit()
        return updated

    def get_existing_observation_keys(
        self,
        record_type: str = "vital_sign",
        canonical_names: list[str] | None = None,
    ) -> set[tuple[str, str | None]]:
        """Return (canonical_name, date_effective) pairs for dedup checks.

        Loads all existing keys into a set for O(1) lookup during batch imports.
        """
        sql = "SELECT canonical_name, date_effective FROM observations WHERE record_type = ?"
        params: list[Any] = [record_type]
        if canonical_names:
            placeholders = ",".join("?" for _ in canonical_names)
            sql += f" AND canonical_name IN ({placeholders})"
            params.extend(canonical_names)
        rows = self.conn.execute(sql, params).fetchall()
        return {(r["canonical_name"], r["date_effective"]) for r in rows}

    def delete_observation(self, obs_id: str) -> bool:
        """Delete an observation by ID. Returns True if a row was deleted."""
        cursor = self.conn.execute(
            "DELETE FROM observations WHERE obs_id = ?", (obs_id,)
        )
        self.conn.commit()
        return cursor.rowcount > 0

    def stamp_collection_date(self, source_doc_id: str, date_iso: str) -> int:
        """Set date_effective on undated observations for a given document.

        Returns the number of rows updated.
        """
        cursor = self.conn.execute(
            "UPDATE observations SET date_effective = ? "
            "WHERE source_doc_id = ? AND "
            "(date_effective IS NULL OR date_effective = '')",
            (date_iso, source_doc_id),
        )
        self.conn.commit()
        return cursor.rowcount

    def get_observation(self, obs_id: str) -> dict | None:
        """Retrieve and decrypt an observation by ID."""
        row = self.conn.execute(
            "SELECT * FROM observations WHERE obs_id = ?", (obs_id,)
        ).fetchone()
        if not row:
            return None
        aad = f"observations.encrypted_data.{obs_id}"
        data = self._decrypt(row["encrypted_data"], aad)
        data["_meta"] = {
            "record_type": row["record_type"],
            "date_effective": row["date_effective"],
            "triage_level": row["triage_level"],
        }
        return data

    def query_observations(
        self,
        record_type: str | None = None,
        canonical_name: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        triage_level: str | None = None,
        limit: int = 200,
        user_id: int | None = None,
        since: str | None = None,
    ) -> list[dict]:
        """Query observations by plaintext metadata, decrypt matching rows."""
        sql = "SELECT * FROM observations WHERE 1=1"
        params: list[Any] = []

        if user_id is not None:
            sql += " AND user_id = ?"
            params.append(user_id)
        if record_type:
            sql += " AND record_type = ?"
            params.append(record_type)
        if canonical_name:
            sql += " AND canonical_name = ?"
            params.append(canonical_name)
        if start_date:
            sql += " AND date_effective >= ?"
            params.append(start_date)
        if end_date:
            sql += " AND date_effective <= ?"
            params.append(end_date)
        if triage_level:
            sql += " AND triage_level = ?"
            params.append(triage_level)
        if since:
            sql += " AND (created_at > ? OR COALESCE(corrected_at, '') > ?)"
            params.extend([since, since])

        sql += " ORDER BY date_effective DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"observations.encrypted_data.{row['obs_id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
                data["_meta"] = {
                    "obs_id": row["obs_id"],
                    "record_type": row["record_type"],
                    "date_effective": row["date_effective"],
                    "triage_level": row["triage_level"],
                    "source_doc_id": row["source_doc_id"],
                    "source_page": row["source_page"],
                    "source_section": row["source_section"],
                }
                results.append(data)
            except Exception as e:
                logger.warning("Decrypt failed for observations row %s: %s", row["obs_id"], e)
                continue  # Skip corrupted records
        return results
