"""Citation manager for health data provenance.

Every answer must cite its source: PDF blob_id, page, section, date, provider.
"""
from __future__ import annotations

from healthbot.data.db import HealthDB
from healthbot.data.models import Citation


class CitationManager:
    """Build citations for search results and insights."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def cite_observation(self, obs_id: str) -> Citation | None:
        """Build a citation for a single observation."""
        row = self._db.conn.execute(
            "SELECT obs_id, source_doc_id, source_page, source_section, "
            "date_effective, record_type FROM observations WHERE obs_id = ?",
            (obs_id,),
        ).fetchone()
        if not row:
            return None

        # Get document source info
        doc_source = ""
        if row["source_doc_id"]:
            doc_row = self._db.conn.execute(
                "SELECT source FROM documents WHERE doc_id = ?",
                (row["source_doc_id"],),
            ).fetchone()
            if doc_row:
                doc_source = doc_row["source"]

        return Citation(
            record_id=obs_id,
            source_type=row["record_type"],
            source_blob_id=row["source_doc_id"] or "",
            page_number=row["source_page"] or 0,
            section=row["source_section"] or "",
            date_collected=row["date_effective"] or "",
            lab_or_provider=doc_source,
        )

    def cite_search_results(
        self, result_ids: list[str]
    ) -> list[tuple[str, Citation | None]]:
        """Attach citations to a list of record IDs."""
        return [(rid, self.cite_observation(rid)) for rid in result_ids]

    def format_citation(self, citation: Citation) -> str:
        """Format citation for display."""
        return citation.format()

    @staticmethod
    def cite_from_meta(meta: dict) -> str:
        """Build citation string directly from _meta dict.

        Avoids a second DB lookup — uses the metadata already attached
        by query_observations().
        """
        parts = [meta.get("record_type", "")]
        date_eff = meta.get("date_effective", "")
        if date_eff:
            parts.append(date_eff)
        source = meta.get("source_doc_id", "")
        if source:
            parts.append(f"doc:{source[:8]}")
        page = meta.get("source_page", 0)
        if page:
            parts.append(f"p.{page}")
        return f"[{', '.join(p for p in parts if p)}]"
