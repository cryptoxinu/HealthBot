# HealthBot Audit - Latest Findings Snapshot

Date: 2026-03-02
Source: Latest exhaustive pass (line-by-line + runtime verification + full pytest run).

## Key New/Confirmed Findings

1. P0 - Anonymizer canary failure
- Canary SSN (`999-88-7777`) is not detectable by current SSN regex; anonymizer raises hard failure.
- Produces broad runtime/test breakage.

2. P0 - Encrypted AI export sent as plaintext over Telegram
- `.enc` filename can be sent with plaintext content bytes.

3. P1 - Rekey skips many encrypted tables
- Rotation list does not include all encrypted schema tables.

4. P1 - PHI gate misses unlabeled names
- Outbound checks in research/MCP/logging rely on regex-only gate when NER unavailable.

5. P1 - Clean DB validation bypasses
- Specific text fields are written without firewall validation.

6. P1 - Legacy plaintext migrations not auto-invoked
- Filename and search plaintext migration helpers exist but are not called in normal startup flow.

7. P1 - Plaintext metadata risk (`source_lab`)
- Provider/lab text can be persisted in plaintext metadata.

8. P2 - Docs/guarantee mismatch on cloud transport
- "Never sent to cloud" claim conflicts with Telegram transport realities.

9. P2 - `/export` encryption messaging mismatch
- User-facing guidance implies encryption where plaintext FHIR/CSV is sent.

10. Test status
- `pytest -q` result: `32 failed, 2912 passed, 5 xfailed`.
- Failure cluster dominated by anonymizer canary issue.

## Cross-Reference
See consolidated record: `docs/AUDIT_2026-03-02_CONSOLIDATED.md`
