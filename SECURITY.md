# Security Model

## Threat Model

### What We Protect Against
1. **Unauthorized access to health data** — Vault encryption + passphrase
2. **PHI leakage in logs/errors** — Global log scrubber + PHI firewall
3. **PHI leakage via research queries** — Hard-block (not sanitize-and-send)
4. **PHI leakage via Claude CLI** — Privacy flags + anonymization + assert_safe() + env isolation
5. **Malicious PDF ingestion** — PDF safety validation (JS/Launch/encrypted rejection)
6. **Ciphertext manipulation** — AEAD (AES-256-GCM) with per-field AAD
7. **Unauthorized Telegram access** — User ID allowlist + rate limiting
8. **Key exposure on disk** — Keys never written to files; passphrase-derived only. Note: MCP auto-unlock optionally accepts `HEALTHBOT_PASSPHRASE` as an env var for unattended startup (user-initiated, not stored by HealthBot).
9. **Session hijacking** — 30-minute auto-lock, key zeroing, on_lock consolidation

### What We Do NOT Protect Against
1. **Compromise of the running process** — If an attacker has root on your machine while HealthBot is unlocked, they can read memory. This is inherent to any local application.
2. **Physical access to unlocked machine** — Standard macOS security (FileVault, screen lock) is your defense.
3. **Side-channel attacks on AES** — We use the `cryptography` library which delegates to OpenSSL. We trust its constant-time implementations.
4. **Python GC retaining key copies** — `bytearray` zeroing is best-effort. Python's garbage collector may retain copies. Use macOS FileVault as defense-in-depth.
5. **Telegram message persistence** — We delete passphrase messages, but Telegram servers may retain copies. Use Telegram's "Secret Chat" feature for additional protection.
6. **Claude CLI's internal behavior** — We rely on Anthropic's implementation of `--no-session-persistence` to actually prevent data persistence. The flag + system prompt + tool restrictions are defense-in-depth.

## Encryption

| Component | Cipher | Key | AAD |
|-----------|--------|-----|-----|
| File blobs | AES-256-GCM | Master key | blob UUID |
| DB fields (Tier 1) | AES-256-GCM | Master key | `table.encrypted_data.row_id` |
| Clean DB (Tier 2) | AES-256-GCM | HKDF-derived clean key | `clean_table.column.row_id` |
| Claude CLI state | AES-256-GCM | Master key | `relaxed.health_data` / `relaxed.memory` |
| Backup archives | AES-256-GCM | Master key | JSON: `{"backup_id","kdf":{...}}` |
| Vector index | AES-256-GCM | Master key | index name |

**Key Derivation**: Argon2id with 64MB memory cost, 3 iterations, 4-thread parallelism. Produces 256-bit key.

**Nonce**: 12-byte random per encryption operation. Never reused (statistical guarantee with random nonces at our volume).

## PHI Firewall

Regex-based detection of:
- SSN (xxx-xx-xxxx)
- MRN (Medical Record Numbers)
- US phone numbers
- Email addresses
- Dates of birth (slash format and labeled)
- Labeled patient names (e.g., "Patient: John Smith")
- Street addresses
- ZIP codes

**Research policy**: Hard-block. If PHI is detected in an outbound research query, the query is rejected entirely — not cleaned and sent.

**Conversation policy**: Sanitize-then-send. Conversation data comes from the Clean DB (pre-anonymized, zero PII). An additional `assert_safe()` gate runs before sending.

## Single-Lane Architecture (Claude CLI)

| Component | Engine | Data Access | Use Case |
|-----------|--------|-------------|----------|
| **Conversation** | Claude CLI (cloud) | Anonymized (from Clean DB) | All free-text analysis |
| **Research** | Claude CLI (cloud) | Anonymized only | Web search, PubMed queries |
| **Anonymization** | Ollama (local, recommended) | Full PHI | Layer 3 PII detection only |

Claude CLI is the sole conversation and analysis backend. All data sent to Claude is pre-anonymized from the Clean DB (Tier 2) or through the three-layer anonymization pipeline. Ollama is recommended for Layer 3 PII anonymization — it catches context-dependent PII that regex and NER miss. It is not used for conversation.

## Claude CLI Privacy Isolation (Research Lane)

Claude CLI is used only for research queries. Every subprocess call uses multi-layer protection:

### CLI Flags
| Flag | Purpose |
|------|---------|
| `--no-session-persistence` | Session NOT saved to disk — cannot be resumed, no history file |
| `--strict-mcp-config` | Ignore user's MCP configs (blocks cortex-core, memory-keeper, etc.) |
| `--mcp-config '{"mcpServers":{}}'` | Empty MCP config — zero servers loaded at all |
| `--tools "WebSearch,WebFetch"` | Only web research tools — blocks Bash, Edit, Write, Read, NotebookEdit, ALL MCP tools |

### Environment Isolation
Subprocess receives only `PATH` and `HOME` environment variables. No Telegram tokens, WHOOP secrets, or other credentials leak to the Claude CLI process.

### System Prompt Privacy Preamble
Every system prompt sent to Claude CLI begins with explicit instructions:
- "Do NOT save, store, remember, or persist ANY data"
- "Do NOT write to any files, memory systems, or databases"
- "Treat ALL data as ephemeral"

### Single Source of Truth
Privacy flags are defined once in `llm/claude_client.py` as `_PRIVACY_FLAGS` and `_TOOL_FLAGS`. The research layer imports these — no duplication, no drift risk.

## Anonymization Pipeline

Three-layer PII stripping + identity-aware patterns + final gate for every external call:

```
Layer 1: GLiNER NER      (recommended — catches names, cities, organizations)  [LOCAL AI]
Layer 2: PhiFirewall      (always, deterministic — catches SSN, MRN, DOBs, insurance)  [LOCAL]
  └─ Identity patterns    (your name, family, DOB, email — compiled from /identity)  [LOCAL]
Layer 3: Ollama LLM       (recommended — catches context-dependent PII regex misses) [LOCAL AI]
Gate:    assert_safe()    (final gate — blocks if anything slipped through)
  ↓
Only THEN → Claude CLI (cloud, privacy-isolated subprocess)
```

All layers analyze the **original text** independently. Detected spans are merged (overlapping spans combined), then redacted in a single pass. This avoids ordering issues where one layer's redaction could break the other's pattern matching.

**Identity-aware detection**: When you configure an identity profile (`/identity`), your name, family member names, DOB, and email are compiled into regex patterns and injected into PhiFirewall. This means Layers 1+2 alone (without Ollama) can catch your personal PII deterministically — including name variants (first/last, reversed, initials) and DOB in all formats.

Without NER (GLiNER) and LLM (Ollama), only regex-based PII detection runs. Regex catches SSN, MRN, phone, email, DOB, addresses, insurance IDs, and identity-profile patterns but cannot detect contextual PII like unknown person names, city/state names, or organization names in free text.

### Layer 1: GLiNER NER (`security/ner_layer.py`)

Local AI model (~500MB) that understands language context:
- **Person names** — "Sarah Johnson called about her results" (no label needed)
- **Locations** — "patient lives in Cleveland, Ohio"
- **Organizations** — "referred from Cleveland Clinic"
- **Emails, phone numbers, SSNs** — contextual detection

Medical value preservation prevents false positives:
- Lab values ("Glucose: 105 mg/dL") are never flagged
- Medication names, wearable metrics, reference ranges are protected
- Conversation labels ("User", "Assistant") are ignored

Long texts are automatically chunked (300-char chunks, 50-char overlap) to handle the model's 384-token limit. Entities from overlapping regions are deduplicated.

### Layer 2: PhiFirewall Regex (`security/phi_firewall.py`)

Deterministic regex-based detection of:
- SSN (xxx-xx-xxxx)
- MRN (Medical Record Numbers)
- US phone numbers, email addresses
- Dates of birth (slash format, ISO format, labeled)
- Labeled patient names, doctor names, provider names
- Street addresses, ZIP codes
- Insurance/member IDs

### Layer 3: Ollama LLM (`llm/anonymizer_llm.py`)

Optional deep-scan layer using a local LLM (Ollama) to catch context-dependent PII that regex and NER miss. Runs entirely locally — no data leaves your machine.

### Final Gate: assert_safe()

Final safety gate — runs both NER (if available) and regex on the assembled payload. Raises `AnonymizationError` if any PII is detected, preventing the data from being sent.

**Policy**: Hard-block. If PHI is detected in an outbound research query, the query is rejected entirely. We do NOT sanitize-and-send.

### Where Applied
| Module | Context | On Failure |
|--------|---------|------------|
| `llm/claude_conversation.py` | Free-text conversation | Return safe error message |
| `llm/proactive.py` | Proactive lab insights | Fall back to raw deterministic signals |
| `llm/memory_store.py` | STM->LTM consolidation | Skip consolidation |
| `research/research_packet.py` | Research queries | Hard-block (return blocked packet) |

### Research Layer
Both research paths (Claude CLI and PubMed) go through `build_research_packet()` which performs a hard-block check. If PHI is detected in either the query or context, the packet is marked as blocked and never sent.

## Clean Sync Modes

The `/cleansync` command copies raw vault (Tier 1) to Clean DB (Tier 2), anonymizing all text fields. Four modes:

| Mode | Layers | Use Case |
|------|--------|----------|
| **Fast** | NER + regex + identity patterns | Quick sync, no Ollama needed |
| **Hybrid** | Fast first, Ollama on uncertain fields only (~15%) | Best balance of speed and thoroughness |
| **Full** | All three layers on every uncached field | Most thorough, slowest |
| **Rebuild** | Clear cache + full re-anonymize | After identity profile changes or Ollama model upgrades |

**Hybrid uncertainty detection**: A field is flagged for Ollama review when NER has low confidence (< 0.7), NER found something regex didn't confirm, or long text (>80 chars) had zero detections.

All modes use SHA256-keyed caching — previously anonymized fields are not reprocessed.

## Triage Safety

Emergency keywords ("chest pain", "difficulty breathing", "suicidal") trigger:
1. Immediate safety message with emergency numbers (911, 988)
2. Short-circuit: no further processing of the query

All triage logic is deterministic (regex/keyword). No LLM involvement in safety decisions.

## Known Limitations

1. PHI regex patterns are tuned for US formats. International formats (non-US phone, addresses, ID numbers) may not be detected.
2. Without GLiNER installed (regex-only mode), city/state names in free text (e.g., "I live in Cleveland, Ohio") are not caught by regex. With GLiNER installed, these are detected. City alone (without address/ZIP) is not a HIPAA identifier.
3. `assert_safe()` is defense-in-depth, not a guarantee. It uses NER + regex — the same layers as `anonymize()`. If a PII pattern isn't detected by either layer, both will miss it.
4. Lab PDF parsing depends on consistent formatting. Unusual layouts may not parse correctly.
5. Python `bytearray` zeroing is best-effort. Use macOS FileVault for defense-in-depth.
6. Rate limiting is per-process (not persistent across restarts).
7. The bot provides direct medical interpretations. Users should cross-reference with their healthcare provider for clinical decisions.
8. Claude CLI privacy depends on Anthropic's implementation of `--no-session-persistence`. The flag + system prompt + tool restrictions provide defense-in-depth.
