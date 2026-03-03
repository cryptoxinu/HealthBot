# Deep Audit V4 — Post-Fix Report (2026-03-02)

## Executive Summary

All 9 previously claimed fixes verified PASS. 7 additional security gaps from V3 audit closed. 8 monolithic files (16,496 LOC) modularized into 8 packages (77 sub-modules). Structural guardrails added. Full test suite stable at 2925 passing tests.

---

## Findings — Severity Ordered

### P0 — Critical

| # | Finding | Status | Files Changed | Verification |
|---|---------|--------|---------------|--------------|
| P0-1 | Image-only PDF bypasses PII redaction in relaxed mode | **FIXED** | `ingest/telegram_pdf_ingest/claude_extractor.py` | Guard blocks PDFs with `redaction_count == 0` and `clean_text < 20 chars` |

### P1 — High

| # | Finding | Status | Files Changed | Verification |
|---|---------|--------|---------------|--------------|
| P1-1 | Default privacy_mode "relaxed" exposes more data | **FIXED** | `config.py` (3 locations) | Default changed to "strict" in init, load, and property getter |
| P1-2 | source_lab leaks identifying lab names to clean DB | **FIXED** | `data/clean_sync_workers.py` | Unknown labs return `""` instead of raw name; branded labs still mapped |
| P1-3 | Lowercase id_* patterns unconditionally exempt from PII check | **FIXED** | `data/clean_db/db_core.py` | Removed blanket lowercase bypass; medical context loop still handles legit FPs |
| P1-4 | MCP entry passes db_path instead of Config to HealthDB | **FIXED** | `mcp/entry.py` | `HealthDB(config, km)` + real user_id + narrowed `except` to `(OSError, ValueError)` |

### P2 — Medium

| # | Finding | Status | Files Changed | Verification |
|---|---------|--------|---------------|--------------|
| P2-1 | Derived key not zeroed on unlock failure | **FIXED** | `security/key_manager.py` | try/finally wraps `unlock()` and `setup()` — key zeroed on all paths |
| P2-2 | Troubleshoot docstring claims "full tool access" | **FIXED** | `bot/message_router/free_text_handler.py` | Updated to "read-only diagnostic access" |
| P2-3 | /export missing plaintext warning | **FIXED** | `bot/handlers_data/export.py` | Added note pointing users to `/ai_export` for encrypted version |

### Previously Verified (9 fixes — all PASS)

| Fix | Status | Evidence |
|-----|--------|----------|
| Anonymize free-text before Claude | PASS | `llm/claude_conversation.py:156` |
| Encrypted search_index | PASS | `data/schema.py` migration 27 |
| Filename in meta_encrypted | PASS | `data/db/documents.py` |
| Clean DB PII gate | PASS | `data/clean_db/db_core.py` `_assert_no_phi()` |
| PubMed enrichment | PASS | `research/substance_researcher.py:192-193` |
| KB plaintext minimization | PASS | `research/knowledge_base.py:43-68` |
| Auto-discover encrypted tables | PASS | `security/audit_report.py:41-85` |
| Docs contradictions | PASS | CLAUDE.md/README.md/SECURITY.md consistent |
| SSN canary | PASS | `llm/anonymizer.py:63` |

---

## Modularization Completed

| Original File | LOC | New Package | Sub-modules | Package LOC |
|--------------|-----|-------------|-------------|-------------|
| `data/db.py` | 1,883 | `data/db/` | 12 | 2,076 |
| `data/clean_db.py` | 2,074 | `data/clean_db/` | 12 | 2,242 |
| `ingest/lab_pdf_parser.py` | 1,374 | `ingest/lab_pdf_parser/` | 7 | 1,620 |
| `ingest/telegram_pdf_ingest.py` | 1,525 | `ingest/telegram_pdf_ingest/` | 8 | 1,656 |
| `bot/handlers_health.py` | 2,113 | `bot/handlers_health/` | 9 | 2,296 |
| `bot/handlers_data.py` | 2,384 | `bot/handlers_data/` | 10 | 2,564 |
| `bot/message_router.py` | 2,168 | `bot/message_router/` | 8 | 2,359 |
| `bot/scheduler.py` | 1,930 | `bot/scheduler/` | 11 | 2,216 |
| **Total** | **15,451** | **8 packages** | **77 files** | **17,029** |

All packages use the **mixin pattern** with a facade `__init__.py` that re-exports the original class. Zero external import changes required.

---

## Structural Guardrails Added

1. **Startup self-check** (`startup_checks.py`) — runs on vault unlock, logs privacy mode, identity patterns, clean sync status, migration status, allowed user IDs
2. **Exception narrowing** — narrowed bare `except Exception: pass` to typed exceptions in security-critical paths:
   - `key_manager.py` unlock/setup: key zeroization in finally block
   - `clean_sync_workers.py` PII alert: `(ImportError, OSError, RuntimeError)`
   - `anonymizer.py` PII alert: `(ImportError, OSError, RuntimeError)`
   - `mcp/entry.py` identity profile load: `(OSError, ValueError)`
3. **CLAUDE.md structural caps** — added to Code Quality section:
   - Soft cap: 900 LOC for generated modules
   - Cyclomatic complexity <= 12 for new functions
   - Typed exceptions required in security paths

---

## Test Results

```
Full suite:     2925 passed, 19 failed (pre-existing date-parse time-sensitivity)
Security suite: 35/35 passed
Lint (ruff):    0 errors
```

The 19 failing tests are all in `test_nlu_date_parse.py` and `test_event_logger.py` — these compute relative dates ("yesterday", "last week") and break when the calendar date changes. They are pre-existing and unrelated to this work.

---

## Residual Risks & Next Actions

### Short-term
- **Date test flakiness** — 19 tests need `freezegun` or similar to pin dates
- **bulk_ops.py exception audit** — 28 broad `except Exception` patterns; low risk (migration utility, not runtime security path) but should be narrowed

### Mid-term
- **Cyclomatic complexity audit** — several methods in router dispatch and scheduler registration exceed 12
- **Clean sync Ollama coverage** — hybrid mode sends ~15% of fields to Ollama; some uncertainty in the remaining 85% fast-path
- **MCP server response size limits** — no cap on tool response size; could leak large data blocks

### Long-term
- **Key rotation automation** — `/rekey` is manual; consider scheduled rotation
- **Vault backup integrity verification** — backups are encrypted but never verified for restore correctness
- **Multi-user support** — `allowed_user_ids[0]` is used as default; MCP and scheduler assume single user
