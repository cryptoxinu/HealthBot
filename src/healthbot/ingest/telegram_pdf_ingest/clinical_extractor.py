"""Ollama/Claude clinical analysis.

Handles clinical document extraction and smart routing for non-lab
documents (doctor's notes, after-visit summaries, etc.).
"""
from __future__ import annotations

import logging

logger = logging.getLogger("healthbot")


class ClinicalExtractorMixin:
    """Mixin providing clinical document extraction capabilities."""

    def _try_clinical_extraction(
        self,
        result: object,
        blob_id: str,
        doc_id: str,
        user_id: int,
        filename: str,
        preextracted_text: str = "",
    ) -> None:
        """Extract medical facts from non-lab documents.

        Strategy:
        1. Try Claude CLI smart routing first (classifies and routes each
           data point to the appropriate table)
        2. If Claude CLI unavailable, mark document as pending_routing
           (no Ollama fallback -- document stays queued for retry)

        Reuses pre-extracted text from the lab parser to avoid double OCR.
        """
        full_text = preextracted_text
        if not full_text or len(full_text.strip()) < 50:
            return

        # Try Claude CLI smart routing
        routed = self._try_claude_routing(result, full_text, user_id, doc_id)
        if routed:
            # Index full document text for search (redact PII before storing)
            try:
                clean_search = full_text[:20000]
                if self._fw:
                    clean_search = self._fw.redact(clean_search)
                self._db.upsert_search_text(
                    doc_id=doc_id,
                    record_type="clinical_note",
                    date_effective=None,
                    text=clean_search,
                )
            except Exception:
                pass
            return

        # Claude CLI unavailable -- mark document as pending
        try:
            self._db.update_document_routing_status(
                doc_id,
                status="pending_routing",
                error="Claude CLI unavailable",
            )
        except Exception:
            pass
        result.warnings.append(
            f"Claude CLI unavailable — '{filename}' queued for processing. "
            "Fix Claude CLI and run /rescan to process queued documents."
        )

    def _try_claude_routing(
        self,
        result: object,
        full_text: str,
        user_id: int,
        doc_id: str,
    ) -> bool:
        """Attempt Claude CLI smart routing. Returns True on success."""
        try:
            from healthbot.llm.claude_client import ClaudeClient
            client = ClaudeClient(timeout=120)
            if not client.is_available():
                logger.info("Claude CLI not available — skipping smart routing")
                return False
        except Exception:
            return False

        clean_db = None
        try:
            from healthbot.ingest.clinical_doc_router import ClinicalDocRouter

            # Build health summary excerpt for cross-referencing
            health_excerpt = self._get_health_summary_excerpt(user_id)

            # Get clean DB for analysis rules
            clean_db = self._get_clean_db()

            router = ClinicalDocRouter(
                claude_client=client,
                db=self._db,
                clean_db=clean_db,
                phi_firewall=self._fw,
                on_progress=self._on_progress,
            )
            route_result = router.route_document(
                text=full_text,
                user_id=user_id,
                doc_id=doc_id,
                health_summary_excerpt=health_excerpt,
            )

            if route_result.routing_error:
                logger.warning(
                    "Claude routing returned error: %s",
                    route_result.routing_error,
                )
                return False

            # Update result with counts
            result.clinical_facts_count = route_result.total
            result.doc_type = "clinical_routed"
            result.clinical_summary = (
                f"Routed: {route_result.observations} observations, "
                f"{route_result.medications} medications, "
                f"{route_result.conditions} conditions, "
                f"{route_result.health_data} extended records"
            )

            # Mark document routing as done
            try:
                self._db.update_document_routing_status(doc_id, status="done")
            except Exception:
                pass

            return True

        except Exception as e:
            logger.warning("Claude routing failed: %s", e)
            return False
        finally:
            if clean_db:
                try:
                    clean_db.close()
                except Exception:
                    pass
