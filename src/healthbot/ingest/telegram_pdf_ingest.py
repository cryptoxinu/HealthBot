"""Telegram PDF ingestion pipeline.

Receives a PDF from Telegram as bytes, validates, encrypts original,
parses lab results, and stores everything in the vault.
Non-lab documents (doctor's notes, after-visit summaries, etc.) are
analyzed by Ollama to extract medical facts into LTM.
PDFs are NEVER saved to disk unencrypted.
"""
from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass, field

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import Document, LabResult
from healthbot.ingest.lab_pdf_parser import LabPdfParser
from healthbot.reasoning.triage import TriageEngine
from healthbot.security.pdf_safety import PdfSafety
from healthbot.security.vault import Vault

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


@dataclass
class IngestResult:
    blob_id: str = ""
    doc_id: str = ""
    lab_results: list[LabResult] = field(default_factory=list)
    triage_summary: str = ""
    quality_summary: str = ""
    clinical_summary: str = ""
    clinical_facts_count: int = 0
    clinical_pii_blocked: int = 0
    doc_type: str = ""
    redacted_blob_id: str = ""
    warnings: list[str] = field(default_factory=list)
    missing_date: bool = False
    success: bool = False
    is_duplicate: bool = False
    is_rescan: bool = False
    rescan_new: int = 0
    rescan_existing: int = 0
    cross_doc_dupes: int = 0
    alerts: list = field(default_factory=list)


class TelegramPdfIngest:
    """Pipeline: Telegram file bytes -> validate -> encrypt -> parse -> store."""

    def __init__(
        self,
        vault: Vault,
        db: HealthDB,
        parser: LabPdfParser,
        pdf_safety: PdfSafety,
        triage: TriageEngine,
        config: Config | None = None,
        on_progress: object | None = None,
        phi_firewall: object | None = None,
    ) -> None:
        self._vault = vault
        self._db = db
        self._parser = parser
        self._safety = pdf_safety
        self._triage = triage
        self._config = config
        self._on_progress = on_progress
        self._fw = phi_firewall
        self._last_redacted_bytes: bytes | None = None

    def _get_ollama_layer(self) -> object | None:
        """Create Ollama anonymization layer if available."""
        try:
            from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer
            from healthbot.llm.ollama_client import OllamaClient

            kwargs: dict = {"retry_count": 0, "timeout": 30}
            if self._config:
                base_url = getattr(self._config, "ollama_url", None)
                if base_url:
                    kwargs["base_url"] = base_url
            ollama = OllamaClient(**kwargs)
            if ollama.is_available():
                return OllamaAnonymizationLayer(ollama)
        except Exception:
            pass
        return None

    def _report(self, msg: str) -> None:
        """Report progress to the caller (thread-safe)."""
        if self._on_progress:
            try:
                self._on_progress(msg)
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Claude CLI extraction (primary path)
    # ------------------------------------------------------------------

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
          relaxed — save redacted PDF to temp file, let Claude read it with
                    vision (better accuracy on tables/charts/images).
          strict  — extract text from redacted PDF, send text via stdin only.

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
        from healthbot.llm.anonymizer import Anonymizer

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

        from healthbot.llm.claude_client import (
            _CLI_ERROR_RESPONSE,
            _TIMEOUT_RESPONSE,
        )

        # ── Relaxed mode: send redacted PDF file to Claude (vision) ──
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

        # ── Strict mode (or relaxed fallback): send extracted text ──
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

    def _build_anonymizer(self) -> object:
        """Create an Anonymizer instance for PII redaction."""
        from healthbot.llm.anonymizer import Anonymizer
        from healthbot.security.phi_firewall import PhiFirewall

        kwargs: dict = {}
        try:
            from healthbot.llm.anonymizer_llm import OllamaAnonymizationLayer
            from healthbot.llm.ollama_client import OllamaClient

            ok: dict = {"retry_count": 0, "timeout": 30}
            if self._config:
                base_url = getattr(self._config, "ollama_url", None)
                if base_url:
                    ok["base_url"] = base_url
            ollama = OllamaClient(**ok)
            if ollama.is_available():
                kwargs["ollama_layer"] = OllamaAnonymizationLayer(ollama)
        except Exception:
            pass
        return Anonymizer(phi_firewall=self._fw or PhiFirewall(), use_ner=True, **kwargs)

    @staticmethod
    def _build_doc_meta(
        redacted_blob_id: str | None, filename: str,
    ) -> dict:
        """Build document meta dict with optional redacted blob reference."""
        if not redacted_blob_id:
            return {}
        from pathlib import PurePosixPath
        stem = PurePosixPath(filename).stem
        return {
            "redacted_blob_id": redacted_blob_id,
            "redacted_filename": f"{stem}_redacted.pdf",
        }

    # ------------------------------------------------------------------
    # Main ingestion pipeline
    # ------------------------------------------------------------------

    def ingest(
        self, pdf_bytes: bytes, filename: str = "unknown.pdf", user_id: int = 0,
    ) -> IngestResult:
        """Full ingestion pipeline. PDF bytes must already be in memory.

        All DB writes are wrapped in a single transaction. On any failure,
        the transaction is rolled back — no orphaned blobs or partial results.
        """
        result = IngestResult()

        # 1. Validate PDF safety
        self._report("Validating PDF safety...")
        try:
            self._safety.validate_bytes(pdf_bytes)
        except Exception as e:
            result.warnings.append(f"PDF rejected: {e}")
            return result

        # Ensure _last_redacted_bytes is always cleared, even on exception
        try:
            return self._ingest_inner(pdf_bytes, filename, user_id, result)
        finally:
            self._last_redacted_bytes = None

    def _ingest_inner(
        self, pdf_bytes: bytes, filename: str, user_id: int,
        result: IngestResult,
    ) -> IngestResult:
        """Core ingestion logic wrapped by ingest() for cleanup safety."""
        # 2. Check for existing document by SHA256
        sha = hashlib.sha256(pdf_bytes).hexdigest()
        existing_details: dict[tuple[str, str | None], dict] = {}
        try:
            existing = self._db.document_exists_by_sha256(sha)
        except RuntimeError:
            existing = None  # DB not open (vault locked) — skip dedup

        if existing:
            # Rescan: re-parse and add only new results
            result.is_rescan = True
            result.doc_id = existing["doc_id"]
            blob_id = existing.get("enc_blob_path", "")
            result.blob_id = blob_id
            existing_details = self._db.get_observation_details_for_doc(blob_id)
        else:
            # 3. Store encrypted original in vault
            self._report("Encrypting PDF (AES-256)...")
            blob_id = uuid.uuid4().hex
            self._vault.store_blob(pdf_bytes, blob_id=blob_id)
            result.blob_id = blob_id

        # 5. Get demographics early — parser needs DOB for date validation
        demographics = {}
        try:
            demographics = self._db.get_user_demographics(user_id)
        except Exception:
            pass

        # 5a. Extract text and tables from PDF
        self._report("Reading PDF...")
        full_text = ""
        markdown_text = ""
        try:
            full_text, markdown_text = self._parser.extract_text_and_tables(
                pdf_bytes,
            )
        except Exception as e:
            logger.warning("Text extraction failed: %s", e)

        # 5b. Try Claude CLI extraction first (redact PII in PDF, send clean text)
        labs: list[LabResult] = []
        if full_text or markdown_text or pdf_bytes:
            self._report("Redacting PII and sending to Claude...")
            try:
                claude_labs = self._try_claude_extraction(
                    full_text, markdown_text, blob_id, demographics,
                    pdf_bytes=pdf_bytes, user_id=user_id,
                )
                if claude_labs:
                    labs = claude_labs
                    self._report(f"Claude extracted {len(labs)} results")
                else:
                    self._report("Claude couldn't parse PDF — using local parser")
            except Exception as e:
                logger.warning("Claude extraction failed: %s", e)
                self._report("Claude extraction error — using local parser")

        # 5c. Fallback to existing pipeline if Claude didn't work
        if not labs:
            self._report("Using local extraction (table + regex)...")
            try:
                labs_fb, full_text_fb = self._parser.parse_bytes(
                    pdf_bytes, blob_id=blob_id, demographics=demographics,
                    on_progress=self._on_progress,
                )
                labs = labs_fb
                if full_text_fb:
                    full_text = full_text_fb
            except Exception as e:
                result.warnings.append(f"Parsing error: {e}")
                if not result.is_rescan:
                    try:
                        self._vault.delete_blob(blob_id)
                    except Exception:
                        pass
                self._last_redacted_bytes = None
                return result

        # 5d. Store redacted PDF in vault (encrypted, separate blob)
        redacted_blob_id = None
        if self._last_redacted_bytes and not result.is_rescan:
            redacted_blob_id = uuid.uuid4().hex
            self._vault.store_blob(
                self._last_redacted_bytes, blob_id=redacted_blob_id,
            )
            logger.info("Redacted PDF stored in vault: %s", redacted_blob_id)
            result.redacted_blob_id = redacted_blob_id
        self._last_redacted_bytes = None  # Free memory

        pre_filter = len(labs)
        labs = self._filter_valid_results(labs)
        if pre_filter > len(labs):
            logger.info(
                "Filter dropped %d of %d results", pre_filter - len(labs), pre_filter,
            )

        # 5d. Stamp metadata from regex extraction (dates, lab name)
        regex_date = self._parser._extract_date(
            full_text or markdown_text, demographics,
        )
        regex_lab = self._parser._extract_lab_name(full_text or markdown_text)
        undated = sum(1 for lab in labs if not lab.date_collected)
        if undated and regex_date:
            logger.info(
                "Regex date fallback: stamping %d undated results with %s",
                undated, regex_date,
            )
        elif undated:
            logger.warning(
                "No date found: %d results have no collection date",
                undated,
            )
        for lab in labs:
            if not lab.date_collected and regex_date:
                lab.date_collected = regex_date
            if not lab.lab_name and regex_lab:
                lab.lab_name = regex_lab
            if not lab.source_blob_id:
                lab.source_blob_id = blob_id

        # Flag if any results still have no date after all extraction
        still_undated = sum(1 for lab in labs if not lab.date_collected)
        if still_undated:
            result.missing_date = True
            logger.warning(
                "MISSING DATE: %d of %d results have no collection date "
                "after Claude + regex extraction", still_undated, len(labs),
            )

        # 5e. Rescan dedup + conflict resolution
        total_parsed = len(labs)
        updated_labs: list[LabResult] = []
        if existing_details:
            new_labs = []
            for lab in labs:
                key = (
                    lab.canonical_name or lab.test_name.lower(),
                    lab.date_collected.isoformat() if lab.date_collected else None,
                )
                if key in existing_details:
                    # Check for corrected value
                    old = existing_details[key]
                    if old.get("value") != lab.value:
                        logger.info(
                            "Corrected lab: %s %s → %s",
                            key[0], old.get("value"), lab.value,
                        )
                        updated_labs.append((old["obs_id"], lab))
                else:
                    new_labs.append(lab)
            result.rescan_existing = len(labs) - len(new_labs) - len(updated_labs)
            result.rescan_new = len(new_labs) + len(updated_labs)
            labs = new_labs

        # 5f. Cross-document dedup (different PDF, same labs)
        # Only for new uploads — rescans are handled above.
        # Only dedup when both sides have a date (without dates we can't
        # confirm they're the same specimen).
        if not result.is_rescan and labs:
            try:
                dated_labs = [l for l in labs if l.date_collected]
                if dated_labs:
                    batch_names = list({
                        l.canonical_name or l.test_name.lower()
                        for l in dated_labs
                    })
                    existing_keys = self._db.get_existing_observation_keys(
                        record_type="lab_result",
                        canonical_names=batch_names,
                    )
                    if existing_keys:
                        pre_dedup = len(labs)
                        labs = [
                            lab for lab in labs
                            if not lab.date_collected  # keep undated (can't dedup)
                            or (
                                lab.canonical_name or lab.test_name.lower(),
                                lab.date_collected.isoformat(),
                            ) not in existing_keys
                        ]
                        dupes = pre_dedup - len(labs)
                        if dupes:
                            result.cross_doc_dupes = dupes
                            logger.info(
                                "Cross-document dedup: skipped %d/%d "
                                "duplicate labs (already in DB from "
                                "another document)",
                                dupes, pre_dedup,
                            )
            except Exception as e:
                logger.debug("Cross-document dedup check failed: %s", e)

        result.lab_results = labs

        # 6a. Classify triage levels (demographic-aware)
        all_labs = labs + [pair[1] for pair in updated_labs]
        if all_labs:
            self._triage.classify_batch(
                all_labs,
                sex=demographics.get("sex"),
                age=demographics.get("age"),
            )

        # 6b. Validate parsed results against demographic expectations
        if labs and demographics:
            labs = self._validate_with_demographics(labs, demographics)

        # ---- BEGIN TRANSACTION ----
        # All DB writes happen inside a single transaction.
        # On failure, everything rolls back — no orphaned records.
        try:
            self._db.conn.execute("BEGIN IMMEDIATE")
        except Exception:
            pass  # Already in a transaction or autocommit

        try:
            # 4. Record document (inside transaction for new uploads)
            if not result.is_rescan:
                doc = Document(
                    id=uuid.uuid4().hex,
                    source="telegram_pdf",
                    sha256=sha,
                    enc_blob_path=blob_id,
                    filename=filename,
                    mime_type="application/pdf",
                    size_bytes=len(pdf_bytes),
                    meta=self._build_doc_meta(
                        redacted_blob_id, filename,
                    ),
                )
                doc_id = self._db.insert_document(
                    doc, user_id=user_id, commit=False,
                )
                result.doc_id = doc_id

            # 7. Store each new result in DB (with age_at_collection)
            if labs:
                self._report(f"Storing {len(labs)} result(s) in encrypted vault...")
            dob = demographics.get("dob")
            for lab in labs:
                lab.source_blob_id = blob_id
                self._db.insert_observation(
                    lab, user_id=user_id,
                    age_at_collection=self._compute_age_at_collection(
                        dob, lab,
                    ),
                    commit=False,
                )
                # lab_name omitted — can contain PII (ordering provider office names)
                search_text = (
                    f"{lab.test_name} {lab.canonical_name} {lab.value} {lab.unit} "
                    f"{lab.reference_text}"
                )
                self._db.upsert_search_text(
                    doc_id=lab.id,
                    record_type="lab_result",
                    date_effective=lab.date_collected.isoformat() if lab.date_collected else None,
                    text=search_text,
                    commit=False,
                )

            # 7b. Update corrected lab values
            for obs_id, lab in updated_labs:
                lab.source_blob_id = blob_id
                self._db.update_observation_value(
                    obs_id, lab, user_id=user_id,
                    age_at_collection=self._compute_age_at_collection(
                        dob, lab,
                    ),
                    commit=False,
                )

            # 10a. Update rescan timestamp
            if result.is_rescan:
                self._db.update_document_rescanned(result.doc_id, commit=False)

            self._db.conn.commit()
        except Exception as e:
            try:
                self._db.conn.rollback()
            except Exception:
                pass
            logger.error("Ingestion transaction failed, rolling back: %s", e)
            # Cleanup blobs if this was a new upload
            if not result.is_rescan:
                try:
                    self._vault.delete_blob(blob_id)
                except Exception:
                    pass
                if redacted_blob_id:
                    try:
                        self._vault.delete_blob(redacted_blob_id)
                    except Exception:
                        pass
            result.warnings.append(f"DB transaction failed: {e}")
            return result
        # ---- END TRANSACTION ----

        # Include updated labs in the result for triage summary
        result.lab_results = labs + [pair[1] for pair in updated_labs]

        # 8. Generate triage summary
        if result.lab_results:
            self._report("Running triage analysis...")
            result.triage_summary = self._triage.get_triage_summary(result.lab_results)

        # 8b. Lab alerts — proactive notifications for clinically significant events
        if result.lab_results:
            try:
                from healthbot.reasoning.lab_alerts import LabAlertEngine
                alert_engine = LabAlertEngine(self._db)
                alert_report = alert_engine.scan(
                    user_id=user_id,
                    sex=demographics.get("sex"),
                    age=demographics.get("age"),
                )
                if alert_report.has_alerts:
                    result.alerts = alert_report.alerts
            except Exception as e:
                logger.debug("Lab alert scan failed during ingest: %s", e)

        # 9. Data quality checks (age/sex-aware)
        if result.lab_results:
            from healthbot.reasoning.data_quality import DataQualityEngine
            dq = DataQualityEngine(self._db)
            issues = dq.check_batch(
                result.lab_results,
                sex=demographics.get("sex"),
                age=demographics.get("age"),
            )
            if issues:
                result.quality_summary = dq.format_issues(issues)

        # 10. Clinical document extraction (only when PDF has no labs at all)
        if total_parsed == 0:
            self._report("No labs found — analyzing as clinical document...")
            self._try_clinical_extraction(
                result, blob_id, result.doc_id, user_id, filename,
                preextracted_text=full_text,
            )

        result.success = True
        return result

    def _try_clinical_extraction(
        self,
        result: IngestResult,
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
           (no Ollama fallback — document stays queued for retry)

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

        # Claude CLI unavailable — mark document as pending
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
        result: IngestResult,
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

    def _get_health_summary_excerpt(self, user_id: int) -> str:
        """Build a brief health summary excerpt for routing context."""
        try:
            clean_db = self._get_clean_db()
            if not clean_db:
                return ""
            try:
                sections = clean_db.get_health_summary_sections()
                parts = []
                for key in ("demographics", "medications", "labs_summary"):
                    if sections.get(key):
                        parts.append(sections[key][:500])
                return "\n".join(parts)[:2000]
            finally:
                clean_db.close()
        except Exception:
            return ""

    def _get_clean_db(self) -> object | None:
        """Try to get a CleanDB connection."""
        try:
            from healthbot.data.clean_db import CleanDB
            if not self._config:
                return None
            path = getattr(self._config, "clean_db_path", None)
            if not path or not path.exists():
                return None
            clean = CleanDB(path, phi_firewall=self._fw)
            # Try to open without key (analysis rules don't need encryption)
            clean.open()
            return clean
        except Exception:
            return None

    def _store_clinical_facts(
        self, facts: list[dict], user_id: int, filename: str,
    ) -> tuple[int, int]:
        """Validate and store extracted clinical facts in LTM.

        Returns (stored_count, pii_blocked_count).

        Dedup/merge logic:
        - PHI firewall check (with PII alert recording)
        - ≥85% similarity → skip or merge (if new is longer)
        - New facts inserted with source="document"
        """
        from difflib import SequenceMatcher

        try:
            existing = self._db.get_ltm_by_user(user_id)
        except Exception:
            existing = []

        count = 0
        blocked = 0
        for fact_obj in facts:
            category = fact_obj.get("category", "")
            fact_text = fact_obj.get("fact", "")
            if not fact_text or len(fact_text.strip()) < 5:
                continue

            # Anonymize fact text through full pipeline (NER + regex + Ollama)
            # before storing. This redacts names/cities instead of blocking
            # the entire fact.
            try:
                from healthbot.llm.anonymizer import Anonymizer, AnonymizationError
                from healthbot.security.phi_firewall import PhiFirewall

                fw = self._fw or PhiFirewall()
                ollama_layer = self._get_ollama_layer()
                anon = Anonymizer(
                    phi_firewall=fw, use_ner=True,
                    ollama_layer=ollama_layer,
                )
                cleaned, had_phi = anon.anonymize(fact_text)
                try:
                    anon.assert_safe(cleaned)
                except AnonymizationError:
                    # Retry once
                    cleaned, _ = anon.anonymize(cleaned)
                    try:
                        anon.assert_safe(cleaned)
                    except AnonymizationError:
                        blocked += 1
                        logger.warning(
                            "Blocked clinical fact with residual PHI "
                            "(category: %s)", category,
                        )
                        try:
                            from healthbot.security.pii_alert import PiiAlertService
                            PiiAlertService.get_instance().record(
                                category="PHI_in_clinical_fact",
                                destination="ltm",
                            )
                        except Exception:
                            pass
                        continue
                fact_text = cleaned
            except Exception:
                blocked += 1
                logger.warning("PHI check failed — blocking fact for safety")
                continue

            # Fuzzy dedup against existing LTM (inline, no MemoryStore dep)
            is_dup = False
            update_id = ""
            new_lower = fact_text.lower().strip()
            for ex in existing:
                ex_text = ex.get("fact", "")
                if not ex_text:
                    continue
                ex_lower = ex_text.lower().strip()
                if new_lower == ex_lower:
                    is_dup = True
                    break
                ratio = SequenceMatcher(None, new_lower, ex_lower).ratio()
                if ratio >= 0.85:
                    is_dup = True
                    if len(fact_text) > len(ex_text):
                        update_id = ex.get("_id", "")
                    break

            if is_dup:
                if update_id:
                    try:
                        self._db.update_ltm(update_id, fact_text, category)
                        logger.info(
                            "Updated LTM (document): %s", fact_text[:50],
                        )
                        count += 1
                    except Exception as e:
                        logger.warning("LTM update failed: %s", e)
                continue

            try:
                self._db.insert_ltm(
                    user_id, category, fact_text, source="document",
                )
                logger.info(
                    "New LTM (document): [%s] %s", category, fact_text[:50],
                )
                count += 1
            except Exception as e:
                logger.warning("LTM insert failed: %s", e)

        return count, blocked

    @staticmethod
    def _compute_age_at_collection(
        dob: str | None, lab: LabResult,
    ) -> int | None:
        """Compute age at the time the lab was collected."""
        if not dob or not lab.date_collected:
            return None
        try:
            from datetime import date as date_type

            if isinstance(dob, str):
                dob_date = date_type.fromisoformat(dob)
            else:
                dob_date = dob
            coll = lab.date_collected
            if isinstance(coll, str):
                coll = date_type.fromisoformat(coll)
            age = (
                coll.year - dob_date.year
                - ((coll.month, coll.day) < (dob_date.month, dob_date.day))
            )
            return age if 0 < age < 150 else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _validate_with_demographics(
        labs: list[LabResult], demographics: dict,
    ) -> list[LabResult]:
        """Flag results that are implausibly far from expected ranges.

        Catches likely parse errors (e.g., decimal point missed →
        glucose 1000 instead of 100).
        """
        from healthbot.reasoning.reference_ranges import get_range

        sex = demographics.get("sex")
        age = demographics.get("age")

        for lab in labs:
            if not isinstance(lab.value, (int, float)):
                continue
            ref = get_range(
                lab.canonical_name or lab.test_name.lower(),
                sex=sex, age=age,
            )
            if not ref:
                continue
            high = ref.get("high")
            if high and lab.value > high * 10:
                lab.confidence *= 0.3
                lab.flag = f"{lab.flag} SUSPECT" if lab.flag else "SUSPECT"
                logger.warning(
                    "Suspect value: %s = %s (>10x high ref %s)",
                    lab.test_name, lab.value, high,
                )
        return labs

    @staticmethod
    def _filter_valid_results(labs: list[LabResult]) -> list[LabResult]:
        """Drop results that are clearly not valid lab tests.

        Safety net after parsing — catches anything the parser's blocklist missed.
        Three acceptance paths:
        1. Numeric — known canonical OR has ref ranges
        2. Inequality string — "<0.5", ">1.0" (clinically valid)
        3. Qualitative string — canonical in QUALITATIVE_TESTS, OR value in
           VALID_QUALITATIVE_VALUES, OR result has reference_text
        """
        from healthbot.normalize.lab_normalizer import (
            QUALITATIVE_TESTS,
            TEST_NAME_MAP,
            VALID_QUALITATIVE_VALUES,
        )

        known_canonical = set(TEST_NAME_MAP.values())

        filtered = []
        for lab in labs:
            name = lab.test_name.strip()
            if len(name) < 2:
                continue

            canonical = lab.canonical_name or ""

            # Path 1: Numeric values
            val = lab.value
            if isinstance(val, (int, float)):
                # Must be known OR have ref ranges
                if (
                    canonical in known_canonical
                    or lab.reference_low is not None
                    or lab.reference_high is not None
                ):
                    filtered.append(lab)
                else:
                    logger.info(
                        "Dropping unrecognized numeric test without ref "
                        "range: %s (canonical: %s)", name, canonical,
                    )
                continue

            if not isinstance(val, str):
                continue

            # Path 2: Inequality-prefixed numeric strings (<0.5, >1.0)
            stripped = val.lstrip("<>≤≥= ")
            try:
                float(stripped)
                if (
                    canonical in known_canonical
                    or lab.reference_low is not None
                    or lab.reference_high is not None
                ):
                    filtered.append(lab)
                else:
                    logger.info(
                        "Dropping unrecognized inequality test without "
                        "ref range: %s (canonical: %s)", name, canonical,
                    )
                continue
            except (ValueError, TypeError):
                pass

            # Path 3a: Trust Claude — high-confidence extraction of string values.
            # Claude reads the PDF with vision (confidence 0.92). If it
            # extracted a string value with high confidence, it's a real
            # result — accept it even if it's not in our hardcoded sets.
            # The hardcoded sets remain as safety nets for the Ollama path
            # (confidence 0.85).
            if lab.confidence >= 0.90 and val.strip():
                filtered.append(lab)
                continue

            # Path 3b: Qualitative string values (hardcoded safety net)
            if (
                canonical in QUALITATIVE_TESTS
                or val.strip().lower() in VALID_QUALITATIVE_VALUES
                or (hasattr(lab, "reference_text") and lab.reference_text)
            ):
                filtered.append(lab)
            else:
                logger.info(
                    "Dropping unrecognized qualitative result: %s = %r "
                    "(canonical: %s)", name, val, canonical,
                )
        return filtered
