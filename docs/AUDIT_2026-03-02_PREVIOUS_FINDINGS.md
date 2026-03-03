# HealthBot Audit - Previous Findings Snapshot

Date captured: 2026-03-02
Source: Prior audit pass (before latest exhaustive pass).

## Previously Reported Findings

1. AI export encryption mismatch
- Export path writes encrypted `.enc`, but Telegram send path transmits plaintext markdown bytes.

2. Rekey coverage incomplete
- Rekey list includes only subset of encrypted tables.
- Newer encrypted tables (e.g., genetics, knowledge, reminders, trend cache, etc.) are not rotated.

3. Clean DB validation gaps
- Not all persisted text fields are validated by PhiFirewall before write.
- `source_lab` and selected substance knowledge fields bypass validation.

4. Documentation guarantee contradictions
- Claims that identifiable medical data is never sent to cloud conflict with Telegram transport and cloud-connected command paths.

5. `/export` messaging mismatch
- User-facing text suggests encrypted export in places where FHIR/CSV plaintext export is used.

6. PHI gate dependency risk
- Critical outbound gates rely on `PhiFirewall.contains_phi()` and can be bypassed by unlabeled names when NER is unavailable.

7. Plaintext-at-rest concerns
- Legacy/search/metadata paths can retain plaintext unless explicit migration/hardening paths are executed.

## Cross-Reference
See consolidated record: `docs/AUDIT_2026-03-02_CONSOLIDATED.md`
