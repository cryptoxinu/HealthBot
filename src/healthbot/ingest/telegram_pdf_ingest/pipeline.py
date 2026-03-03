"""Main ingestion pipeline -- entry points and class definition.

Composes all mixins into the TelegramPdfIngest class. Provides ingest()
and _ingest_inner() as the main entry points.
"""
from __future__ import annotations

import hashlib
import logging
import uuid

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.data.models import Document, LabResult
from healthbot.ingest.lab_pdf_parser import LabPdfParser
from healthbot.ingest.telegram_pdf_ingest.claude_extractor import (
    ClaudeExtractorMixin,
)
from healthbot.ingest.telegram_pdf_ingest.clinical_extractor import (
    ClinicalExtractorMixin,
)
from healthbot.ingest.telegram_pdf_ingest.redaction import RedactionMixin
from healthbot.ingest.telegram_pdf_ingest.storage import StorageMixin
from healthbot.ingest.telegram_pdf_ingest.validation import ValidationMixin
from healthbot.reasoning.triage import TriageEngine
from healthbot.security.pdf_safety import PdfSafety
from healthbot.security.vault import Vault

from .models import IngestResult

logger = logging.getLogger("healthbot")


class TelegramPdfIngest(
    ClaudeExtractorMixin,
    RedactionMixin,
    ClinicalExtractorMixin,
    StorageMixin,
    ValidationMixin,
):
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

    # ------------------------------------------------------------------
    # Main ingestion pipeline
    # ------------------------------------------------------------------

    def ingest(
        self, pdf_bytes: bytes, filename: str = "unknown.pdf", user_id: int = 0,
    ) -> IngestResult:
        """Full ingestion pipeline. PDF bytes must already be in memory.

        All DB writes are wrapped in a single transaction. On any failure,
        the transaction is rolled back -- no orphaned blobs or partial results.
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
            existing = None  # DB not open (vault locked) -- skip dedup

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

        # 5. Get demographics early -- parser needs DOB for date validation
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
        # Only for new uploads -- rescans are handled above.
        # Only dedup when both sides have a date (without dates we can't
        # confirm they're the same specimen).
        if not result.is_rescan and labs:
            try:
                dated_labs = [lr for lr in labs if lr.date_collected]
                if dated_labs:
                    batch_names = list({
                        lr.canonical_name or lr.test_name.lower()
                        for lr in dated_labs
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
        # On failure, everything rolls back -- no orphaned records.
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
                # lab_name omitted -- can contain PII (ordering provider office names)
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

        # 8b. Lab alerts -- proactive notifications for clinically significant events
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
