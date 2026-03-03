# HealthBot Consolidated Security Audit

Date: 2026-03-02
Scope: Consolidated record of prior audit findings + latest exhaustive pass.
Mode: Read-only audit (no source code modifications performed during audits).

## Executive Summary
This audit found critical gaps between documented guarantees and actual behavior, including plaintext transport where encryption is claimed, incomplete key rotation coverage, and PHI gate weaknesses that allow unlabeled personal names through in important outbound paths.

## Consolidated Findings

### P0 (Critical)

1. AI export labeled as encrypted (`.enc`) is sent to Telegram as plaintext bytes.
- Evidence:
  - `src/healthbot/export/ai_export.py` encrypts file output when key manager exists.
  - `src/healthbot/bot/handlers_data.py` sends `result.markdown.encode("utf-8")` in-memory.
  - `src/healthbot/bot/scheduler.py` does the same for auto export.
- Risk: Confidentiality failure + misleading security expectation.

2. Anonymization canary is invalid against firewall regex; anonymizer hard-fails at runtime.
- Evidence:
  - Canary uses `999-88-7777` in `src/healthbot/llm/anonymizer.py`.
  - Firewall SSN regex excludes `9xx` in `src/healthbot/security/phi_firewall.py`.
  - `_verify_canary()` raises `AnonymizationError` when canary not detected.
- Runtime proof:
  - `PhiFirewall().contains_phi("Patient SSN: 999-88-7777") -> False`.
  - `Anonymizer(...).anonymize("No PII here")` raises canary failure.
- Additional impact:
  - Full pytest run shows broad failures concentrated in `test_claude_conversation.py` due to this.

### P1 (High)

3. Rekey rotates only a subset of encrypted tables; many encrypted tables remain under old key.
- Evidence:
  - Hardcoded `_ENCRYPTED_TABLES` in `src/healthbot/vault_ops/rekey.py` has 9 tables.
  - Schema contains 23 encrypted tables (e.g., `genetic_variants`, `knowledge_base`, `med_reminders`, `trend_cache`, `saved_messages`, `user_identity`, `workouts`, etc.) in `src/healthbot/data/schema.py`.
- Runtime proof:
  - After rekey, `observations` decrypts; `genetic_variants` decryption fails (`after_genotype: None`).

4. PHI gate misses unlabeled personal names (without NER), enabling outbound leakage through name-bearing text.
- Evidence:
  - `PhiFirewall` primarily matches labeled/intro/name-context patterns.
  - Hard-block gates rely on `contains_phi()` in:
    - `src/healthbot/research/research_packet.py`
    - `src/healthbot/bot/handlers_medical.py` (`/research_cloud`)
    - `src/healthbot/mcp/server.py` (`_safe_response`)
    - `src/healthbot/security/log_scrubber.py`
- Runtime proof:
  - `contains_phi("Should John Smith with LDL 180 take statins?") -> False`
  - Query is not blocked; log scrubber does not redact `John Smith`.

5. Clean DB claim "every text field validated" is not true for multiple persisted fields.
- Evidence:
  - `source_lab` is inserted without firewall validation in `src/healthbot/data/clean_db.py`.
  - `clean_substance_knowledge` validates only subset (`name/mechanism/half_life/clinical_summary`) while other text fields are unvalidated.
- Runtime proof:
  - `source_lab='Lab SSN 999-88-7777'` inserted successfully.
  - `research_sources='contains SSN 999-88-7777'` inserted successfully.

6. Legacy plaintext hardening migrations exist but are not automatically executed in startup path.
- Evidence:
  - Migration helpers exist:
    - `migrate_document_filenames()` in `src/healthbot/data/db.py`
    - `migrate_search_index_encryption()` in `src/healthbot/data/db.py`
  - No call sites found in runtime startup path; startup runs `run_migrations()` only.
- Runtime proof:
  - Seeded legacy plaintext rows remain plaintext after startup migration flow.

7. Tier-1 plaintext metadata (`source_lab`) can hold identifying provider text.
- Evidence:
  - Plaintext `source_lab` column in `observations` schema/migrations.
  - Populated directly from `LabResult.lab_name` and via backfill decrypt->plaintext path.
- Runtime proof:
  - `"John Smith Family Clinic"` stored in plaintext `observations.source_lab`.

### P2 (Medium)

8. Security/architecture guarantees in docs conflict with actual cloud transport behavior.
- Evidence:
  - README/CLAUDE claim identifiable medical data is "NEVER sent to any cloud service".
  - System explicitly uses Telegram file/message APIs and documents Telegram server transit.
- Risk: Incorrect trust model communicated to users.

9. `/doctorpacket` message says `/export` provides encrypted copy, but `/export` returns plaintext FHIR/CSV via Telegram.
- Evidence:
  - Message in `src/healthbot/bot/handlers_medical.py`.
  - Export implementation in `src/healthbot/bot/handlers_data.py` sends plain JSON/CSV bytes.
  - `src/healthbot/export/encrypted_export.py` exists but is not used by handlers.

10. Regression surfaced by full suite: timeline document title expectations fail due to filename handling behavior.
- Evidence:
  - Failing test `tests/test_timeline.py::TestTimelineBuild::test_document_events`.
  - Inserted document filename resolves empty in timeline title path (`Uploaded: `).

11. KB similarity test regression surfaced.
- Evidence:
  - Failing test `tests/test_kb_enrichment.py::TestKBFindSimilar::test_finds_similar_entry`.

## Test Execution Snapshot
- Command: `pytest -q`
- Result: `32 failed, 2912 passed, 5 xfailed` (runtime observed)
- Failure concentration:
  - Large cluster in `tests/test_claude_conversation.py`, primarily rooted in anonymizer canary failure.
  - Additional failures in `tests/test_kb_enrichment.py` and `tests/test_timeline.py`.

## Prior Audit Findings Carried Forward
The following were flagged in the prior pass and remain applicable:
- Outbound PHI defenses depend heavily on `PhiFirewall.contains_phi()` behavior and can be bypassed by unlabeled names when NER is unavailable.
- Plaintext storage/transport inconsistencies existed around search and export pathways (partly re-validated above).
- Documentation and operational guarantees diverged in multiple user-visible paths.

## Notes
- This file is a consolidated evidence record for reference and triage.
- No source-code fixes were applied as part of this audit; findings only.
