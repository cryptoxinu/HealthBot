"""LLM feedback loop for incomplete or garbled lab data.

When Claude detects cut-off, missing, or garbled lab data in the export,
it emits a DATA_QUALITY structured block. This module handles that signal:

1. Logs the issue to the knowledge base
2. Finds the relevant source PDF in the vault
3. Re-extracts data at higher DPI or with full re-parse
4. Returns new results (deduplicated against existing data)

All re-extracted data goes through the normal ingestion pipeline.
No PII shortcuts — the full anonymization chain still applies.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("healthbot")


class FeedbackLoop:
    """Handle DATA_QUALITY issues from Claude's structured blocks.

    Orchestrates re-extraction from encrypted PDFs when Claude
    flags incomplete or garbled lab data.
    """

    def __init__(self, db: object, vault: object | None = None) -> None:
        self._db = db
        self._vault = vault

    def handle_quality_issue(
        self,
        user_id: int,
        issue_type: str,
        test_name: str,
        details: str,
        page: int | None = None,
    ) -> dict:
        """Orchestrate a data quality re-extraction.

        Args:
            user_id: The user's ID.
            issue_type: One of "cut_off_lab", "missing_ref_range", "garbled_data".
            test_name: The test name Claude flagged (e.g. "CBC", "WBC").
            details: Human-readable description of the issue.
            page: Optional page number for targeted re-OCR.

        Returns:
            dict with keys:
                rescan_attempted (bool): Whether a rescan was attempted.
                rescan_count (int): Number of new results found.
                rescan_results (list[str]): Names of new results.
        """
        result = {
            "rescan_attempted": False,
            "rescan_count": 0,
            "rescan_results": [],
        }

        # Step 1: Log issue to knowledge base
        self._log_to_kb(test_name, issue_type, details)

        # Step 2: Find the relevant source document
        doc_blob_id = self._find_relevant_document(user_id, test_name)
        if not doc_blob_id:
            logger.info(
                "DATA_QUALITY: no source document found for test '%s'",
                test_name,
            )
            return result

        # Step 3: Re-extract from the source PDF
        new_results = self._rescan_document(
            doc_blob_id, user_id, page,
        )
        result["rescan_attempted"] = True
        result["rescan_count"] = len(new_results)
        result["rescan_results"] = new_results

        if new_results:
            logger.info(
                "DATA_QUALITY: re-scan of '%s' found %d new result(s): %s",
                test_name, len(new_results), new_results,
            )
        else:
            logger.info(
                "DATA_QUALITY: re-scan of '%s' found no new results",
                test_name,
            )

        return result

    def _log_to_kb(
        self, test_name: str, issue_type: str, details: str,
    ) -> None:
        """Log the data quality issue to the knowledge base."""
        try:
            from healthbot.research.knowledge_base import KnowledgeBase

            kb = KnowledgeBase(self._db)
            kb.store_finding(
                topic=f"data_quality:{test_name}",
                finding=f"{issue_type}: {details}",
                source="claude_data_quality",
                relevance_score=0.6,
            )
        except Exception as exc:
            logger.debug("Failed to log quality issue to KB: %s", exc)

    def _find_relevant_document(
        self, user_id: int, test_name: str,
    ) -> str | None:
        """Find the source document blob_id for a given test name.

        Searches observations by canonical name to find the source PDF.
        """
        try:
            from healthbot.normalize.lab_normalizer import normalize_test_name

            canonical = normalize_test_name(test_name)
            observations = self._db.query_observations(
                canonical_name=canonical,
                user_id=user_id,
                limit=5,
            )

            # Return the most recent document with a source_doc_id
            for obs in observations:
                meta = obs.get("_meta", {})
                doc_id = meta.get("source_doc_id")
                if doc_id:
                    return doc_id

            # Fallback: try the raw test name as canonical
            if canonical != test_name.lower():
                observations = self._db.query_observations(
                    canonical_name=test_name.lower(),
                    user_id=user_id,
                    limit=5,
                )
                for obs in observations:
                    meta = obs.get("_meta", {})
                    doc_id = meta.get("source_doc_id")
                    if doc_id:
                        return doc_id

        except Exception as exc:
            logger.debug(
                "Failed to find document for test '%s': %s",
                test_name, exc,
            )

        return None

    def _rescan_document(
        self,
        blob_id: str,
        user_id: int,
        page: int | None = None,
    ) -> list[str]:
        """Re-extract data from a source PDF.

        If page is specified, does targeted high-DPI OCR on that page.
        Otherwise, does a full re-parse.

        Returns list of canonical names for newly found results
        (i.e. results not already stored for this document).
        """
        if not self._vault:
            logger.debug("No vault available for rescan")
            return []

        # Get existing results for this document to deduplicate
        try:
            existing_keys = self._db.get_observation_keys_for_doc(blob_id)
            existing_names = {k[0] for k in existing_keys}
        except Exception:
            existing_names = set()

        # Retrieve the encrypted PDF
        try:
            pdf_bytes = self._vault.retrieve_blob(blob_id)
        except Exception as exc:
            logger.warning(
                "Failed to retrieve PDF blob %s for rescan: %s",
                blob_id, exc,
            )
            return []

        new_names: list[str] = []

        if page is not None:
            # Targeted page OCR at 400 DPI
            new_names = self._rescan_page(
                pdf_bytes, page, existing_names,
            )
        else:
            # Full re-parse
            new_names = self._rescan_full(
                pdf_bytes, blob_id, existing_names,
            )

        return new_names

    def _rescan_page(
        self,
        pdf_bytes: bytes,
        page: int,
        existing_names: set[str],
    ) -> list[str]:
        """Re-OCR a single page at 400 DPI and parse results."""
        try:
            from healthbot.ingest.ocr_fallback import ocr_pdf_page

            text = ocr_pdf_page(pdf_bytes, page, dpi=400)
            if not text or len(text.strip()) < 10:
                return []

            return self._parse_text_for_new_results(text, page, existing_names)

        except Exception as exc:
            logger.debug("Page %d re-OCR failed: %s", page, exc)
            return []

    def _rescan_full(
        self,
        pdf_bytes: bytes,
        blob_id: str,
        existing_names: set[str],
    ) -> list[str]:
        """Full re-parse of PDF and return new canonical names."""
        try:
            from healthbot.ingest.lab_pdf_parser import LabPdfParser
            from healthbot.security.pdf_safety import PdfSafety

            parser = LabPdfParser(
                pdf_safety=PdfSafety(),
                config=None,
            )
            results, _text = parser.parse_bytes(pdf_bytes, blob_id=blob_id)

            new_names = []
            for r in results:
                if r.canonical_name not in existing_names:
                    new_names.append(r.canonical_name)

            return new_names

        except Exception as exc:
            logger.debug("Full re-parse failed: %s", exc)
            return []

    def _parse_text_for_new_results(
        self,
        text: str,
        page: int,
        existing_names: set[str],
    ) -> list[str]:
        """Parse OCR text through lab regex and return new canonical names."""
        try:
            from healthbot.ingest.lab_pdf_parser import LabPdfParser
            from healthbot.security.pdf_safety import PdfSafety

            parser = LabPdfParser(
                pdf_safety=PdfSafety(),
                config=None,
            )
            results = parser._parse_result_lines(text, page)

            new_names = []
            for r in results:
                if r.canonical_name not in existing_names:
                    new_names.append(r.canonical_name)

            return new_names

        except Exception as exc:
            logger.debug("Text parsing failed: %s", exc)
            return []
