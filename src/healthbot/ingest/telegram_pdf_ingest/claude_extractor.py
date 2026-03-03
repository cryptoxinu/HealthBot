"""Claude CLI extraction and PDF read logic.

Handles lab result extraction via Claude CLI, including PDF redaction,
text anonymization fallback, and JSON response parsing.
"""
from __future__ import annotations

import logging

from healthbot.data.models import LabResult

logger = logging.getLogger("healthbot")

_CLAUDE_LAB_SYSTEM = """\
You are a medical lab report parser. Extract ALL lab test results from the \
provided text into a JSON array.

For EACH result, provide an object with:
- test_name: exact name as printed on the report
- value: numeric result for standard tests (number, not string with units); \
for qualitative/molecular tests, the text exactly as printed \
(e.g. "Not Detected", "Positive", "Negative", "Wild Type", "Heterozygous")
- unit: unit of measurement (e.g. "mg/dL", "%", "K/uL") or "" if qualitative
- reference_low: lower bound of reference range (number or null)
- reference_high: upper bound of reference range (number or null)
- reference_text: for qualitative tests, the expected/normal result text \
(e.g. "Not Detected"); for numeric tests, "" or omit
- flag: "H" for high, "L" for low, "A" for abnormal qualitative, "" if normal
- interpretation: for molecular tests with interpretation paragraphs, \
include the full interpretation text; otherwise "" or omit

Include a metadata object as the LAST item with:
- "_type": "metadata"
- "collection_date": "YYYY-MM-DD" or null
- "lab_name": laboratory name or null

IMPORTANT — collection_date:
- Look for "Collected", "Date Collected", "Collection Date", "Specimen \
Collected", "Date Received", "Reported", "Date of Service", or any \
date label at the top of the report.
- Also check headers, footers, and page margins for dates.
- If multiple dates exist, prefer the collection/specimen date over the \
report/printed date.
- Convert any date format to YYYY-MM-DD. Only return null if truly no \
date appears anywhere in the document.

Rules:
- Extract EVERY single result (CBC has ~15-21, CMP ~14, lipid panel ~5)
- Include normal results — do NOT skip them
- Extract molecular/genetic tests (JAK2, CALR, BCR-ABL, Factor V Leiden, \
MTHFR, BRAF, KRAS, EGFR mutations)
- Extract infectious disease screens (HBsAg, HCV antibody, HIV, RPR, \
COVID PCR, influenza, strep)
- Extract other qualitative tests (HLA-B27, urine drug screen, tissue \
transglutaminase)
- [REDACTED-*] tags replaced patient identity info — ignore them
- Black boxes in the PDF replaced patient identity info — ignore them
- Return ONLY a valid JSON array, no other text
- If no lab results found, return: []"""


class ClaudeExtractorMixin:
    """Mixin providing Claude CLI lab extraction capabilities."""

    def _try_claude_extraction(
        self, full_text: str, markdown_text: str, blob_id: str,
        demographics: dict | None = None,
        pdf_bytes: bytes | None = None,
        user_id: int = 0,
    ) -> list[LabResult] | None:
        """Try extracting lab results via Claude CLI on a PII-redacted PDF.

        Primary strategy: redact the PDF itself (black boxes over PII),
        save to a temp file, and let Claude CLI read the actual PDF.
        Claude's multimodal PDF reader handles tables/charts/images
        far better than our text extraction.

        Falls back to text-level anonymization if PDF redaction fails.
        """
        # 1. Check Claude CLI availability
        try:
            from healthbot.llm.claude_client import ClaudeClient
            client = ClaudeClient(timeout=120)
            if not client.is_available():
                logger.info("Claude CLI not available — skipping Claude extraction")
                return None
        except Exception:
            return None

        # 2. Primary: redact PDF and let Claude read it directly
        privacy_mode = self._config.privacy_mode if self._config else "relaxed"
        response = None
        if pdf_bytes:
            response = self._send_redacted_pdf(
                pdf_bytes, client, privacy_mode=privacy_mode,
                user_id=user_id,
            )

        if response:
            logger.info("Claude extraction via PDF redaction got response")
        else:
            logger.info("PDF redaction returned no response, trying text fallback")

        # 3. Fallback: text-level anonymization (if PDF redaction failed)
        if not response:
            combined = ""
            if markdown_text:
                combined += "=== STRUCTURED TABLES ===\n" + markdown_text + "\n\n"
            if full_text:
                combined += "=== FULL TEXT ===\n" + full_text
            combined = combined.strip()
            if not combined or len(combined) < 50:
                return None
            try:
                anon = self._build_anonymizer()
                cleaned = combined
                for attempt in range(1, 4):
                    cleaned, _ = anon.anonymize(cleaned)
                    try:
                        anon.assert_safe(cleaned)
                        logger.info("Text anonymization passed on attempt %d", attempt)
                        break
                    except Exception:
                        if attempt < 3:
                            logger.info("Anonymization attempt %d left residual PII", attempt)
                        else:
                            raise
            except Exception as e:
                logger.warning("Text anonymization failed: %s", e)
                return None

            prompt = (
                "Extract ALL lab results from this redacted medical report:\n\n"
                + cleaned
            )
            try:
                response = client.send(prompt=prompt, system=_CLAUDE_LAB_SYSTEM)
            except Exception as e:
                logger.warning("Claude CLI text extraction failed: %s", e)
                return None

        if not response or len(response.strip()) < 5:
            logger.info("Claude extraction returned empty response")
            return None

        logger.info(
            "Claude raw response (%d chars, first 500): %s",
            len(response), response[:500],
        )

        # 4. Parse JSON response into LabResult objects
        results, metadata = self._parse_claude_response(response, blob_id)
        if not results:
            logger.info("Claude extraction returned no valid results")
            return None

        # 5. Stamp collection date from Claude metadata
        claude_date = metadata.get("collection_date")
        if claude_date:
            validated = self._parser._validate_date(claude_date, demographics)
            if validated:
                logger.info(
                    "Claude metadata date accepted: %s", claude_date,
                )
                for r in results:
                    if not r.date_collected:
                        r.date_collected = claude_date
            else:
                logger.info(
                    "Claude metadata date rejected by validation: %s",
                    claude_date,
                )
        else:
            logger.info(
                "Claude returned no collection_date in metadata",
            )

        # 5b. Regex fallback: if Claude didn't provide a date, extract
        # from the original (un-redacted) text. This catches Claude's
        # non-determinism where it sometimes omits the metadata object.
        undated = sum(1 for r in results if not r.date_collected)
        if undated:
            regex_date = self._parser._extract_date(
                full_text or markdown_text, demographics,
            )
            if regex_date:
                logger.info(
                    "Regex date fallback in Claude path: stamping %d "
                    "undated results with %s", undated, regex_date,
                )
                for r in results:
                    if not r.date_collected:
                        r.date_collected = regex_date

        # 5c. Last resort: focused Claude call for just the date.
        # Send the report header text (first 2000 chars) with a
        # single-purpose prompt. Much cheaper than re-sending the PDF.
        undated = sum(1 for r in results if not r.date_collected)
        if undated:
            header_text = (full_text or markdown_text or "")[:2000]
            if header_text:
                logger.info("Retrying Claude for collection date only...")
                try:
                    date_response = client.send(
                        prompt=(
                            "What is the specimen collection date in "
                            "this lab report header? Reply with ONLY "
                            "the date in YYYY-MM-DD format. If no date "
                            "found, reply NULL.\n\n" + header_text
                        ),
                        system="Extract dates from medical documents.",
                    )
                    if (
                        date_response
                        and date_response.strip().upper() != "NULL"
                    ):
                        retry_str = date_response.strip()[:10]
                        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
                            try:
                                from datetime import datetime
                                retry_date = datetime.strptime(
                                    retry_str, fmt,
                                ).date()
                                if self._parser._validate_date(
                                    retry_date, demographics,
                                ):
                                    logger.info(
                                        "Retry got collection date: %s",
                                        retry_date,
                                    )
                                    for r in results:
                                        if not r.date_collected:
                                            r.date_collected = retry_date
                                    break
                            except ValueError:
                                continue
                except Exception as e:
                    logger.debug("Date retry failed: %s", e)

        # Log final status
        still_undated = sum(1 for r in results if not r.date_collected)
        if still_undated:
            logger.warning(
                "No date found after all attempts: %d of %d results "
                "undated", still_undated, len(results),
            )

        # 6. Stamp lab name from Claude metadata
        claude_lab = metadata.get("lab_name", "")
        if claude_lab:
            for r in results:
                if not r.lab_name:
                    r.lab_name = claude_lab

        logger.info("Claude extraction: %d lab results", len(results))
        return results

    def _send_redacted_pdf(
        self, pdf_bytes: bytes, client: object,
        privacy_mode: str = "relaxed",
        user_id: int = 0,
    ) -> str | None:
        """Redact PII from the PDF, then send to Claude for extraction.

        Privacy modes:
          relaxed -- save redacted PDF to temp file, let Claude read it with
                    vision (better accuracy on tables/charts/images).
          strict  -- extract text from redacted PDF, send text via stdin only.

        Both modes: redact first, verify clean, save inspection copy.
        """
        import os
        import tempfile

        try:
            redacted_bytes, redaction_count = self._redact_pdf(
                pdf_bytes, user_id=user_id,
            )
        except Exception as e:
            logger.warning("PDF redaction failed: %s", e)
            return None

        if not redacted_bytes:
            return None

        logger.info("PDF redaction complete: %d black boxes applied", redaction_count)

        # Stash redacted bytes for encrypted vault storage in ingest()
        self._last_redacted_bytes = redacted_bytes

        # Extract text from redacted PDF (needed for verification + strict fallback)
        import fitz
        try:
            doc = fitz.open(stream=redacted_bytes, filetype="pdf")
            pages_text = []
            for i, page in enumerate(doc):
                text = page.get_text()
                if text.strip():
                    pages_text.append(f"--- Page {i + 1} ---\n{text}")
            doc.close()
        except Exception as e:
            logger.warning("Failed to extract text from redacted PDF: %s", e)
            return None

        clean_text = "\n\n".join(pages_text)

        # Belt-and-suspenders: run full 3-layer anonymizer on extracted text.
        # The PDF redaction (black boxes) is the first pass; this catches
        # anything that slipped through using NER + regex + Ollama LLM.
        from healthbot.llm.anonymizer import AnonymizationError, Anonymizer

        anon = Anonymizer(
            phi_firewall=self._fw,
            use_ner=True,
            ollama_layer=self._get_ollama_layer(),
        )
        cleaned, had_phi = anon.anonymize(clean_text)

        if had_phi:
            logger.info("Post-redaction PII found, re-redacting PDF...")
            try:
                redacted_bytes, extra = self._redact_pdf(
                    redacted_bytes, user_id=user_id,
                )
                redaction_count += extra
                logger.info("Second-pass redaction: %d additional boxes", extra)
                doc = fitz.open(stream=redacted_bytes, filetype="pdf")
                pages_text = []
                for i, page in enumerate(doc):
                    text = page.get_text()
                    if text.strip():
                        pages_text.append(f"--- Page {i + 1} ---\n{text}")
                doc.close()
                clean_text = "\n\n".join(pages_text)
            except Exception as e:
                logger.warning("Second-pass redaction failed: %s", e)

            # Final pass: anonymize whatever remains in the text
            clean_text, _ = anon.anonymize(clean_text)

        try:
            anon.assert_safe(clean_text)
        except Exception:
            logger.warning("assert_safe failed after PDF redaction, applying text-level redaction")
            clean_text, _ = anon.anonymize(clean_text)
            try:
                anon.assert_safe(clean_text)
            except Exception:
                raise AnonymizationError(
                    "PII remains after two anonymization attempts on PDF text"
                ) from None

        from healthbot.llm.claude_client import (
            _CLI_ERROR_RESPONSE,
            _TIMEOUT_RESPONSE,
        )

        # -- Relaxed mode: send redacted PDF file to Claude (vision) --
        # P0: Block image-only PDFs that have no extractable text.
        # In relaxed mode, Claude sees the redacted PDF via vision -- but if
        # redaction found nothing (redaction_count == 0) and extracted text
        # is empty/trivial, the PDF is likely image-only and may contain
        # un-redacted PII in scanned images.
        text_len = len((clean_text or "").strip())
        if privacy_mode == "relaxed" and redaction_count == 0 and text_len < 20:
            logger.warning(
                "Image-only PDF blocked in relaxed mode "
                "(no text extracted, no redactions)",
            )
            return (
                "This PDF appears to be image-only. Text extraction found no "
                "content to redact. Please use OCR or a text-based PDF."
            )

        if privacy_mode == "relaxed":
            tmp_path = None
            try:
                fd, tmp_path = tempfile.mkstemp(
                    suffix=".pdf", prefix="hb_redacted_",
                )
                try:
                    os.write(fd, redacted_bytes)
                finally:
                    os.close(fd)

                logger.info(
                    "Temp redacted PDF: %s (%d bytes)", tmp_path, len(redacted_bytes),
                )

                prompt = (
                    "Read the PDF file at this path using your Read tool:\n"
                    f"  {tmp_path}\n\n"
                    "This is a redacted medical lab report. [REDACTED-*] tags "
                    "and black boxes replaced patient identity info — ignore "
                    "them.\n\n"
                    "Extract ALL lab test results into a JSON array."
                )

                try:
                    response = client.send_with_read(
                        prompt=prompt, system=_CLAUDE_LAB_SYSTEM,
                        read_dirs=[os.path.dirname(tmp_path)],
                    )
                except Exception as e:
                    logger.warning("Claude CLI send_with_read failed: %s", e)
                    response = None

                if response and response not in (
                    _TIMEOUT_RESPONSE, _CLI_ERROR_RESPONSE,
                ):
                    if not response.startswith((
                        "Claude CLI error:", "Claude is rate-limited",
                        "Couldn't reach Claude",
                    )):
                        logger.info("Claude PDF vision extraction succeeded")
                        return response

                logger.info(
                    "PDF vision path failed, falling back to text extraction",
                )
            except Exception as e:
                logger.warning("Temp file / PDF vision path failed: %s", e)
            finally:
                if tmp_path:
                    try:
                        os.unlink(tmp_path)
                        logger.debug("Temp redacted PDF deleted: %s", tmp_path)
                    except OSError:
                        pass

        # -- Strict mode (or relaxed fallback): send extracted text --
        if not clean_text or len(clean_text.strip()) < 50:
            logger.info("Redacted PDF text too short (%d chars)", len(clean_text))
            return None

        logger.info(
            "Text extraction path: %d chars from %d pages",
            len(clean_text), len(pages_text),
        )

        prompt = (
            "Extract ALL lab test results from this redacted lab report "
            "into a JSON array:\n\n" + clean_text
        )

        try:
            response = client.send(prompt=prompt, system=_CLAUDE_LAB_SYSTEM)
        except Exception as e:
            logger.warning("Claude CLI text send failed: %s", e)
            return None

        if response in (_TIMEOUT_RESPONSE, _CLI_ERROR_RESPONSE):
            logger.warning("Claude CLI returned error: %s", response[:100])
            return None
        if response.startswith((
            "Claude CLI error:", "Claude is rate-limited",
            "Couldn't reach Claude",
        )):
            logger.warning("Claude CLI error: %s", response[:200])
            return None

        return response

    def _parse_claude_response(
        self, text: str, blob_id: str,
    ) -> tuple[list[LabResult], dict]:
        """Parse Claude's JSON response into LabResult objects.

        Handles Claude-specific patterns (markdown fences, preamble text)
        before delegating to the shared JSON parser.
        Overrides confidence to 0.92 (Claude is more accurate).
        """
        import re

        # Strip markdown code fences: ```json ... ``` or ``` ... ```
        cleaned = re.sub(r"```(?:json)?\s*\n?", "", text)
        cleaned = cleaned.strip()

        # If response has preamble text with [brackets] before the JSON array,
        # find the actual JSON array by looking for [{ pattern
        array_start = cleaned.find("[{")
        if array_start == -1:
            array_start = cleaned.find("[\n")
        if array_start == -1:
            array_start = cleaned.find("[")
        if array_start > 0:
            cleaned = cleaned[array_start:]

        results, metadata = self._parser._parse_ollama_response(cleaned, blob_id)
        for r in results:
            r.confidence = 0.92
        return results, metadata
