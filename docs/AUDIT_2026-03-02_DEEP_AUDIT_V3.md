# HealthBot Deep Audit V3

Date: 2026-03-02
Mode: Read-only audit (no code fixes applied)
Repository: /Users/Server/HealthBot

## Scope and Method

- Reviewed architecture docs: `CLAUDE.md`, `README.md`, and security-relevant code paths in `src/healthbot/**`.
- Performed broad static scans across all Python sources (386 files, ~65k LOC) for security/operational risk patterns.
- Executed full test suite:
  - `pytest -q` -> `2944 passed, 5 xfailed, 30 warnings in 203.73s`.
- Ran targeted runtime repro scripts for each high-impact finding.
- Repro transcript saved at: `/tmp/hb_audit2/repro_v3.txt`.

## Findings (Severity-Ordered)

### P0 - Scanned/image-only PDFs can still be sent to Claude in relaxed mode with zero redaction

Evidence:
- `/Users/Server/HealthBot/src/healthbot/ingest/telegram_pdf_ingest.py:575`-`:579`
  - `_redact_pdf()` skips pages with no extractable text (`if not page_text: continue`).
- `/Users/Server/HealthBot/src/healthbot/ingest/telegram_pdf_ingest.py:445`-`:486`
  - In `privacy_mode == "relaxed"`, the (possibly unredacted) PDF is sent to Claude via `send_with_read(...)` before strict text-length checks.
- `/Users/Server/HealthBot/src/healthbot/ingest/telegram_pdf_ingest.py:501`-`:504`
  - `clean_text` short/empty only blocks strict text mode, not relaxed PDF mode.
- `/Users/Server/HealthBot/src/healthbot/config.py:239`, `:243`, `:283`
  - Default privacy mode is `"relaxed"`.

Runtime repro (saved in `/tmp/hb_audit2/repro_v3.txt`):
- Generated image-only PDF containing visible `"Patient Name: John Smith DOB: ..."`.
- `_redact_pdf()` returned `redaction_count: 0`.
- Relaxed path still called `send_with_read` (`send_with_read_called: True`).

Impact:
- Raw patient identity in scanned/image PDFs can be sent to Claude despite redaction workflow.

---

### P1 - Clean sync can leak provider/name-like lab source strings into clean DB and outbound AI context

Evidence:
- `/Users/Server/HealthBot/src/healthbot/data/clean_sync_workers.py:36`-`:46`
  - `_normalize_lab_brand()` returns raw input for unknown labs (`return raw_name.strip()`).
- `/Users/Server/HealthBot/src/healthbot/data/clean_sync_workers.py:135`
  - `sync_observations()` uses `rec.get("lab_name", "")` (decrypted raw field), not Tier-1 normalized `source_lab` metadata.
- `/Users/Server/HealthBot/src/healthbot/data/clean_db.py:1264`-`:1286`
  - `source_lab` is included in generated markdown sections that feed Claude context.

Runtime repro:
- `_normalize_lab_brand("John Smith Family Clinic")` -> `"John Smith Family Clinic"`.
- `clean.upsert_observation(... source_lab=...)` stored that value successfully.

Impact:
- Name-bearing provider strings can enter clean DB and then outbound prompts.

---

### P1 - Identity-pattern PHI guard has lowercase bypass in clean DB validation

Evidence:
- `/Users/Server/HealthBot/src/healthbot/data/clean_db.py:477`-`:481`
  - `_is_medical_false_positive()` returns `True` for any lowercase-leading `id_*` match, regardless of medical context.
- `/Users/Server/HealthBot/src/healthbot/data/clean_db.py:497`-`:499`
  - `_assert_no_phi()` drops `id_*` matches marked as false positives.

Runtime repro:
- With an `id_*` pattern matching `john smith`:
  - `"john smith"` was stored.
  - `"John Smith"` was blocked.

Impact:
- Case-only differences can bypass identity-aware PHI blocking in clean-store writes.

---

### P1 - MCP identity-aware PHI hardening is effectively disabled in startup path

Evidence:
- `/Users/Server/HealthBot/src/healthbot/mcp/entry.py:75`
  - `HealthDB(config.db_path, km)` passes a path where `HealthDB` expects a `Config` object.
- `/Users/Server/HealthBot/src/healthbot/mcp/entry.py:82`-`:84`
  - Broad exception handler swallows the failure and continues without identity-aware patterns.
- `/Users/Server/HealthBot/src/healthbot/mcp/entry.py:78`
  - Compiles patterns for `user_id=0`, which may not match stored identity profile user IDs.

Runtime repro:
- `HealthDB(Path('/tmp/x.db'), object())` raises `AttributeError: 'PosixPath' object has no attribute 'db_path'`.

Impact:
- MCP server can run without intended identity-based PHI detection strengthening.

---

### P2 - Troubleshoot/debug flow claims full fix access but runs read-only tools

Evidence:
- `/Users/Server/HealthBot/src/healthbot/bot/message_router.py:1056`-`:1057`
  - User-facing docstring says debug has full tool access (edit/run tests/restart).
- `/Users/Server/HealthBot/src/healthbot/research/claude_cli_client.py:31`-`:34`
  - Debug uses `_READ_ONLY_TOOL_FLAGS`.
- `/Users/Server/HealthBot/src/healthbot/research/claude_cli_client.py:165`-`:167`
  - CLI call confirms read-only flags are used.

Impact:
- Operational mismatch: behavior does not match stated capability, causing misleading troubleshooting expectations.

---

### P2 - Key derivation material in `unlock()` is not explicitly zeroized on failure/success

Evidence:
- `/Users/Server/HealthBot/src/healthbot/security/key_manager.py:128`-`:141`
  - `key` derived from passphrase returns on auth failure without explicit zeroization.
- `/Users/Server/HealthBot/src/healthbot/security/key_manager.py:143`-`:146`
  - Success path copies key into `_master_key` but does not zero local derived buffer.

Impact:
- Defense-in-depth gap in secret handling lifecycle (memory remanence risk).

## Status of Previously Flagged Issues

Observed as improved/fixed in current snapshot:
- Full suite now green (`2944 passed`) where previous pass had failures.
- AI export encrypted file transport fix present in handlers/scheduler (`.enc` bytes sent directly).
- Anonymizer canary mismatch appears resolved (valid canary value in current code).
- Data migrations for legacy plaintext helpers are now called from `run_migrations()`.

## Saved Artifacts

- Previous findings snapshot: `/Users/Server/HealthBot/docs/AUDIT_2026-03-02_PREVIOUS_FINDINGS.md`
- Prior latest snapshot: `/Users/Server/HealthBot/docs/AUDIT_2026-03-02_LATEST_FINDINGS.md`
- Prior consolidated snapshot: `/Users/Server/HealthBot/docs/AUDIT_2026-03-02_CONSOLIDATED.md`
- This audit: `/Users/Server/HealthBot/docs/AUDIT_2026-03-02_DEEP_AUDIT_V3.md`
- Repro transcript: `/tmp/hb_audit2/repro_v3.txt`
