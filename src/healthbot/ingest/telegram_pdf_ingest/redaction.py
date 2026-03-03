"""PDF redaction engine.

Black-boxes all detected PII in PDF documents using PyMuPDF.
Uses regex + NER + identity profile for comprehensive PII detection.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("healthbot")


class RedactionMixin:
    """Mixin providing PDF redaction capabilities."""

    def _redact_pdf(
        self, pdf_bytes: bytes, user_id: int = 0,
    ) -> tuple[bytes, int]:
        """Black-box all detected PII in the PDF.

        Returns (redacted_pdf_bytes, redaction_count).
        Uses PyMuPDF to physically remove content under black boxes.
        Loads identity profile (if available) for user-specific PII patterns.
        """
        import fitz  # PyMuPDF

        from healthbot.security.phi_firewall import PhiFirewall

        # Use the shared firewall (already has identity patterns from unlock)
        fw = self._fw or PhiFirewall()
        ner = None
        try:
            from healthbot.security.ner_layer import NerLayer
            if NerLayer.is_available():
                ner = NerLayer()
        except Exception:
            pass

        # Load NER known names from identity profile
        if ner and user_id and self._db:
            try:
                from healthbot.security.identity_profile import IdentityProfile
                profile = IdentityProfile(db=self._db)
                known_names = profile.compile_ner_known_names(user_id)
                if known_names:
                    ner.set_known_names(known_names)
                    logger.info(
                        "PDF redaction: %d known names loaded for NER",
                        len(known_names),
                    )
            except Exception as e:
                logger.warning("Could not load identity profile: %s", e)

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        total_redactions = 0

        for page in doc:
            page_text = page.get_text()
            if not page_text:
                continue

            # Collect PII strings to redact
            pii_strings: set[str] = set()

            # Layer 1: Regex + identity patterns (shared firewall has both)
            for m in fw.scan(page_text):
                pii_strings.add(m.text)

            # Layer 2: NER — find person names, orgs, locations, etc.
            if ner:
                for e in ner.detect(page_text):
                    if len(e.text.strip()) > 2:
                        pii_strings.add(e.text)

            if not pii_strings:
                continue

            # Get word-level bounding boxes for precise single-word matching.
            # page.search_for() does substring matching, so "Ali" would match
            # inside "Alkaline" — corrupting lab test names. Word-level matching
            # avoids this by only matching whole words.
            words = page.get_text("words")  # (x0, y0, x1, y1, text, ...)

            for pii_text in pii_strings:
                pii_clean = pii_text.strip()
                if not pii_clean:
                    continue

                if " " in pii_clean or "," in pii_clean:
                    # Multi-word PII: use page search (handles cross-word spans)
                    rects = page.search_for(pii_text)
                    for rect in rects:
                        page.add_redact_annot(rect, fill=(0, 0, 0))
                        total_redactions += 1
                else:
                    # Single-word PII: match against individual PDF words
                    # to avoid substring matches inside longer words
                    pii_lower = pii_clean.lower()
                    for w in words:
                        w_text = w[4].strip().strip(".,;:!?()[]{}\"'")
                        if w_text.lower() == pii_lower:
                            rect = fitz.Rect(w[:4])
                            page.add_redact_annot(rect, fill=(0, 0, 0))
                            total_redactions += 1

            # Apply all redactions on this page (physically removes content)
            # graphics=0: preserve vector graphics (table borders, decorative lines)
            page.apply_redactions(graphics=0)

            # Post-redaction verification: re-extract text and check for survivors
            remaining = page.get_text()
            missed = [
                p for p in pii_strings
                if p.strip() and p.strip().lower() in remaining.lower()
            ]
            if missed:
                for pii_text in missed:
                    for rect in page.search_for(pii_text):
                        page.add_redact_annot(rect, fill=(0, 0, 0))
                        total_redactions += 1
                page.apply_redactions(graphics=0)

        redacted_bytes = doc.tobytes(garbage=3, deflate=True)
        doc.close()
        return redacted_bytes, total_redactions
