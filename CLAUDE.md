# CLAUDE.md -- HealthBot

## Product Vision

HealthBot is a **personal medical advisor and health executive assistant**. It should:
- Act as a knowledgeable medical doctor — give direct, specific medical interpretations
- NEVER add "I am not a doctor" disclaimers or "consult your provider" hedging
- Back every medical claim with evidence (lab data citations, PubMed references)
- Proactively monitor medical literature relevant to the user's conditions
- Serve as a medical EA — instant access to records, summaries, and PDFs on demand
- Detect undiagnosed conditions from lab patterns (hypothesis generation)
- Give specific medication/supplement timing and interaction advice
- Answer natural language health queries ("when was my liver inflamed?")
- Never hallucinate — always cite evidence. If unsure, research it automatically.
- Never delete medically relevant data — original user statements are permanent record.
- **Self-manage its own database** — Claude owns the schema. When new medical data types appear that don't fit existing tables, Claude should create new tables, sync workers, and clean DB counterparts autonomously. Medical data is unpredictable; the system must evolve without manual intervention.

## Design Priorities

1. **Accuracy** — This is medical data. Correctness beats speed every time. Prefer thorough parsing (regex primary, with cross-validation) over fast regex-only. Verify every method signature, attribute name, and return type before using it. Never let errors silently pass via try/except when correctness matters.
2. **Stability** — Medical records are irreplaceable. Defensive coding, thread safety, clean state management on lock/unlock. Every edge case (timeout during ingestion, cross-thread DB access, stale flags) must be handled explicitly.
3. **Speed** — Last priority. Users will wait for correct results. Never sacrifice accuracy or stability for faster response times.

## What Is This

HealthBot Terminal is a local-first, security-first personal health data management tool for macOS. It stores, organizes, and analyzes health data (lab results, medications, Apple Health, WHOOP wearables) in a two-tier encrypted vault. **Claude CLI is the sole analysis and conversation backend** — all free-text goes to Claude with enriched context from deterministic intelligence engines. Ollama is recommended for Layer 3 PII anonymization (catches names/cities/orgs that regex misses). All 103 `/commands` are 100% deterministic (no LLM). An **MCP server** exposes pre-anonymized health data to Claude Code and OpenClaw.

## What Leaves Your Machine vs. What Stays Local

| Data | Where It Goes | Protection |
|------|---------------|------------|
| **Health records (raw vault)** | NOWHERE — stays local | AES-256-GCM encrypted in SQLite (Tier 1) |
| **Clean DB (anonymized)** | NOWHERE — stays local | HKDF-derived key, no PII ever written, AI-accessible via MCP |
| **Claude CLI conversation** | Anthropic cloud | Pre-anonymized from Clean DB, PII-free, privacy preamble |
| **Claude CLI research** | Anthropic cloud | Anonymized (NER + regex + Ollama LLM), assert_safe gate |
| **MCP server queries** | NOWHERE — local stdio | Pre-anonymized from Clean DB, belt-and-suspenders PII check |
| **Ollama (anonymization only)** | NOWHERE — runs on your Mac | Recommended — Layer 3 PII detection |
| **PubMed queries** | NIH public API | Anonymized, hard-blocked if PHI detected |
| **Telegram messages** | Telegram servers | Bot token auth, passphrase messages deleted |
| **Vault backups** | Local disk only | AES-256-GCM encrypted, never uploaded |
| **Logs** | Local disk only | All PHI scrubbed by LogScrubFilter before writing |
| **Encryption keys** | RAM only | Never written to disk, zeroed after 30-min timeout |

**The core principle**: Your identifiable medical data (names, SSN, DOB, addresses, insurance IDs) is NEVER sent to any cloud service. The two-tier model ensures Tier 1 (raw vault, full PHI) never touches any AI, while Tier 2 (Clean DB, zero PII) is safely queryable by Claude CLI, Claude Code, and OpenClaw via MCP.

## ISOLATION RULE (MANDATORY)

This project is COMPLETELY INDEPENDENT. It has NO relationship to any other project on this machine. Do not reference, import from, modify, or even look at any other project. All code and data must remain local. Never commit secrets, API keys, or PII to the repository.

## Installation Guide (New Machine Deployment)

### Prerequisites

| Requirement | Why | How to Install |
|-------------|-----|----------------|
| **macOS** | Keychain integration, tested on macOS only | - |
| **Python 3.13+** | Required (tested on 3.14) | `brew install python@3.14` or python.org |
| **Claude CLI** | Primary analysis + conversation backend | `brew install claude-code` or [docs.anthropic.com](https://docs.anthropic.com/en/docs/claude-code) |
| **Ollama** (recommended) | Enhanced PII anonymization (Layer 3) | `brew install ollama` or [ollama.com](https://ollama.com) |
| **Telegram Bot Token** | Required — get from @BotFather on Telegram | Message @BotFather → /newbot |

### Step 1: Copy the Project

Transfer the project folder to the new machine (clone from private GitHub repo, USB drive, AirDrop, etc.).

```bash
# On new machine, wherever you put it:
cd ~/HealthBot
```

### Step 2: Install Python Dependencies

```bash
make setup                # Creates .venv, installs all core deps
```

### Step 3: Install Claude CLI

```bash
brew install claude-code  # Or: npm install -g @anthropic-ai/claude-code
```

Claude CLI is the **primary analysis and conversation backend**. All free-text messages go to Claude with enriched health data context. No API costs — uses `claude --print`.

### Step 4: First-Time Setup (Interactive Wizard)

```bash
python -m healthbot --setup
```

This walks you through:
1. **Deployment mode** → Telegram, OpenClaw/ClawdBot, or both
2. **Telegram bot token** → stored in macOS Keychain (never on disk), verified via API
3. **Claude CLI check** → verifies installation
4. **Local AI setup** → Ollama + GLiNER NER (recommended, RAM-aware model selection)
5. **Vault passphrase** → NEVER stored in plaintext. Argon2id derives the encryption key. Optional: Keychain storage for MCP auto-unlock (opt-in during setup).
6. **Identity profile** → name/email/DOB/family for enhanced PII detection (recommended)
7. **MCP setup** → auto-detects Claude Code, configures OpenClaw if selected
8. **Validation** → verifies all components are configured

Wearable integrations (WHOOP, Oura) are set up after the bot is live via
`/whoop_auth` and `/oura_auth` commands in Telegram — not during terminal setup.

Re-running `--setup` detects existing credentials and asks before overwriting.

After setup, the encrypted vault is created at `~/.healthbot/`.

### Step 5: Start the Bot

```bash
make dev                  # Or: python -m healthbot
```

The bot starts polling Telegram. Send `/start` to your bot to begin. Send `/unlock <passphrase>` to unlock the vault each session.

### Step 6 (Recommended): Install NER for Smart PII Detection

```bash
make setup-nlp            # Installs GLiNER (~500MB model download)
```

GLiNER adds intelligent NER that catches names, cities, and organizations that regex can't. **Without NER and Ollama, only regex-based PII detection runs** — regex catches SSN, MRN, phone, email, DOB, addresses, and insurance IDs but cannot detect contextual PII like personal names, city/state names, or organization names in free text.

### Step 7 (Recommended): Install Ollama for Enhanced PII Detection

```bash
brew install ollama
ollama pull llama3.3:70b-instruct-q4_K_M  # Or any model
```

Ollama provides Layer 3 PII anonymization — catches context-dependent PII that regex misses. The bot functions without it but with reduced PII coverage.

### Step 8 (Optional): MCP Server for Claude Code / OpenClaw

```bash
make setup-mcp            # Install MCP dependencies

# Register with Claude Code:
python -m healthbot --mcp-register

# Or start the MCP server manually:
make mcp-server
```

The MCP server exposes pre-anonymized health data (labs, meds, wearables, hypotheses) and the skill system (list_skills, run_skill) via stdio transport. Compatible with Claude Code and OpenClaw. Set `HEALTHBOT_PASSPHRASE` env var for automated startup.

For OpenClaw integration, see [docs/OPENCLAW_SETUP.md](docs/OPENCLAW_SETUP.md). The skill manifest at `skills/healthbot/SKILL.md` teaches OpenClaw how to use HealthBot's MCP tools effectively.

### Deploying to Another Mac (Migration)

```bash
# On OLD machine:
make backup               # Creates encrypted backup in ~/.healthbot/backups/

# Transfer the backup file + project folder to new machine

# On NEW machine:
make setup                # Install deps
python -m healthbot --restore /path/to/backup_file.enc

# Then re-run setup for secrets (Keychain doesn't transfer):
python -m healthbot --setup
```

Secrets (Telegram token, wearable OAuth) are stored in macOS Keychain and don't transfer between machines. You'll need to re-enter them on the new machine. The vault passphrase is never stored — you just need to remember it.

### What's Automatic vs. Manual

| Component | Automatic? | Notes |
|-----------|------------|-------|
| Python deps | Yes | `make setup` handles everything |
| Claude CLI | **Manual** | `brew install claude-code` (required for conversation) |
| Ollama | Semi | Offered during `--setup` with RAM-aware model recommendation |
| GLiNER NER model | Semi | Offered during `--setup`; ~500MB model downloads on first use |
| Telegram bot token | **Manual** | Create via @BotFather, enter during `--setup` (verified via API) |
| WHOOP OAuth | **Manual** | `/whoop_auth` in Telegram after bot is live |
| Oura Ring OAuth | **Manual** | `/oura_auth` in Telegram after bot is live |
| Vault passphrase | **Manual** | You choose it, enter each session |
| Encrypted DB + schema | Yes | Created automatically during `--setup` |
| Keychain entries | Yes | Stored automatically during `--setup` |
| Claude Code MCP | Auto | Auto-detected and configured if `~/.claude/settings.json` exists |

### Vault Directory Structure (Created Automatically)

```
~/.healthbot/
├── db/health.db          # Tier 1: Encrypted SQLite (raw vault, full PHI)
├── db/clean.db           # Tier 2: Anonymized SQLite (zero PII, AI-accessible)
├── vault/                # Encrypted PDF blobs
├── index/                # Encrypted vector index
├── claude/               # Claude CLI state (encrypted health data + memory)
├── backups/              # Encrypted backup archives
├── logs/                 # PHI-scrubbed log files
├── exports/              # Generated PDF reports
├── incoming/             # Temporary upload staging
└── config/app.json       # User IDs, rate limits, model settings
```

## Commands

```bash
make setup       # Create venv, install deps
make setup-nlp   # Install GLiNER NER model (recommended, ~500MB)
make setup-mcp   # Install MCP server dependencies
make dev         # Run bot in dev mode
make test        # Run full test suite
make test-sec    # Security tests only (PHI firewall, log scrubber, PDF safety)
make test-nlp    # Run NER-specific tests
make lint        # ruff check
make backup      # Create encrypted vault backup
make eval        # Run deterministic eval tests
make mcp-server  # Start MCP server (stdio transport)
make bot-start   # Start bot via botctl (background, PID file)
make bot-stop    # Stop bot
make bot-restart # Restart bot
make bot-status  # Check if bot is running
make bot-health  # Health check (Ollama, DB, vault)
make bot-logs    # Tail bot logs

# CLI flags:
python -m healthbot --clean-sync    # One-time raw vault → Clean DB sync (full mode)
python -m healthbot --mcp-register  # Print MCP registration JSON
```

## Telegram Commands (103 total)

All `/commands` are 100% deterministic (no LLM) except `/deep` (Claude CLI research) and `/research_cloud` (cloud research mode). Free-text goes to Claude CLI (anonymized).

**Session & System**
`/start` `/help` `/unlock` `/lock` `/version` `/restart` `/debug` `/audit` `/backup` `/rekey` `/feedback` `/auth_status` `/pii_alerts` `/privacy` `/redacted` `/snooze` `/preferences` `/deep` `/tokenusage`

**Health Analysis**
`/insights` `/dashboard` `/summary` `/trend` `/correlate` `/gaps` `/healthreview` `/ask` `/overdue` `/profile` `/labs` `/recommend` `/digest` `/memory` `/aboutme` `/score` `/analyze`

**Medical Tracking**
`/hypotheses` `/evidence` `/template` `/interactions` `/log` `/undo` `/symptoms` `/comorbidity` `/genetics` `/doctors` `/appointments` `/emergency`

**Medications**
`/effectiveness` `/sideeffects` `/supplements` `/retests` `/screenings`

**Lifestyle & Wellness**
`/stress` `/sleeprec` `/goals` `/timeline` `/workouts` `/remind` `/reminders`

**Data Import/Export**
`/sync` `/connectors` `/oura` `/apple_sync` `/import` `/mychart` `/fasten` `/ingest` `/upload` `/finish` `/export` `/ai_export` `/docs` `/report` `/doctorprep` `/doctorpacket` `/weeklyreport` `/monthlyreport` `/scrub_pii` `/cleansync` (fast/hybrid/full/rebuild) `/rescan` `/savedmessages`

**Wearables**
`/whoop_auth` `/oura_auth` `/wearable_status`

**Identity & Privacy**
`/identity` `/identity_check` `/identity_clear`

**Session Mode**
`/refresh` `/claude_auth` `/onboard` `/onboarding` `/research_cloud`

**System Maintenance**
`/integrity`

**Charts & Visualization**
`/trends_chart` `/lab_heatmap` `/scatter` `/sleep_chart` `/wearable_chart`

**Destructive (confirmation required)**
`/reset` `/delete` `/delete_labs` `/delete_doc`

**Natural language shortcuts**: "save this" / "unsave this" (or "forget this") work as alternatives to replying + commands — saves/unsaves the last bot response or a replied-to message.

## Architecture Overview

```
src/healthbot/                    # 15 packages
  bot/        (48)  Telegram handlers (core + sub-handlers), auth, rate limiting, formatters, routing, middleware, OAuth
  data/       (29)  SQLite + AES-256-GCM encryption, Clean DB (Tier 2), clean sync engine, models, schema, memory mixin
  export/     (14)  PDF generator, AI export, chart generator, FHIR/CSV export, weekly/monthly reports, emergency card
  importers/   (3)  WHOOP + Oura Ring OAuth v2 clients (httpx async), Apple Health auto-import
  ingest/     (22)  Lab PDF parser, clinical doc parser, Apple Health XML, MyChart CCDA/FHIR, OCR, genetic parser, PDF pipeline
  llm/        (15)  Claude CLI (conversation + research), Ollama client (anonymization only), anonymizer (3-layer + hybrid), memory, proactive
  mcp/         (3)  MCP server — exposes anonymized health data + skill tools via stdio transport
  nlu/         (5)  Medical classifier, medication parser, date parser, embeddings, onboarding
  normalize/   (1)  Lab name/unit normalization, vitals parsing
  reasoning/ (53)  Trends, correlations, insights, overdue, triage, delta, health review, data quality, interactions, watcher, hypothesis, family risk, condition extractor, intelligence auditor, knowledge base, digest, genetic risk, pharmacogenomics, pathway analysis, derived markers, lab alerts
  research/    (8)  Claude CLI research, PubMed REST, evidence store, knowledge base, query packets, schema evolution, substance researcher
  retrieval/   (3)  TF-IDF/BM25 search, encrypted vector store, citations
  security/  (12)  Key manager (+ HKDF clean key), keychain, vault, PHI firewall, log scrubber, PDF safety, NER layer, audit, PII alert, identity profile
  skills/      (2)  OpenClaw-inspired skill system — Protocol, registry, 12 built-in skill adapters
  vault_ops/   (5)  Encrypted backup, restore, schema migration, rekey, integrity check
```

## Data Flow (Two-Tier + Single-Lane Architecture)

**Two tiers of data:**
- **Tier 1 — Raw Vault** (`db/health.db`): Full PHI, AES-256-GCM encrypted, never touches any AI directly.
- **Tier 2 — Clean DB** (`db/clean.db`): Pre-anonymized, HKDF-derived key, AI-accessible via MCP. Zero PII ever written (validated by PhiFirewall on every write).

**Sync: Raw → Clean** runs on vault unlock, after ingestion, and via `/cleansync`. One-way only (no reverse path). Supports fast, hybrid, full, and rebuild modes (see Clean Sync Modes).

```
User message (Telegram)
  -> Auth check (user ID allowlist + rate limit)
  -> /command → deterministic handler (103 commands, NO LLM)
  -> Document upload → ingest pipeline (regex parsing, encrypted storage)
  -> Free text →
       1. Emergency triage (deterministic keyword check, NO LLM)
       2. Gather health data from encrypted DB
       3. Run ALL intelligence engines locally (deterministic):
          - Lab trends (slopes, direction, % change)
          - Drug-lab interactions (flagged findings)
          - Intelligence gaps (missing tests for conditions)
          - Panel gaps (incomplete lab panels)
          - Wearable trends + anomalies + recovery score
          - Active hypotheses from tracker
          - Knowledge base findings
       4. Anonymize (NER + PhiFirewall + identity patterns + Ollama Layer 3 + assert_safe)
       5. Send to Claude CLI (subprocess, stdin, privacy-isolated)
       6. Parse structured blocks (HYPOTHESIS/ACTION/RESEARCH/INSIGHT/CONDITION)
       7. Route blocks to hypothesis tracker + knowledge base
       8. Return response to user

  RESEARCH — Claude CLI (anonymized, privacy-isolated):
       1. Anonymize: GLiNER NER strips names/cities/orgs (LOCAL AI)
       2. Anonymize: PhiFirewall regex strips SSN/MRN/DOB/insurance (LOCAL)
       3. Anonymize: Ollama LLM scan (Layer 3, recommended) (LOCAL)
       4. Verify: assert_safe() blocks if ANY PII remains
       5. Only then: Claude CLI subprocess (privacy-isolated, stdin only)

  MCP SERVER — Claude Code / OpenClaw integration:
       1. Reads from Clean DB only (Tier 2, zero PII)
       2. Every tool response passes PhiFirewall.contains_phi() check
       3. stdio transport (no network exposure)
```

## Outbound Data Policies

| Channel | Policy | How |
|---------|--------|-----|
| **Conversation** (Claude CLI) | Sanitize-then-send | Data comes from Clean DB (pre-anonymized, zero PII). Privacy preamble injected. Additional assert_safe() gate before send. |
| **Research** (Claude CLI) | Hard-block | If PHI detected in query, REJECT entirely. Do NOT strip PII and send anyway. Privacy preamble injected. |
| **MCP** (Claude Code / OpenClaw) | Pre-anonymized | Reads from Clean DB only. Privacy instructions in server metadata. Belt-and-suspenders PhiFirewall check on every response. |
| **PubMed** | Hard-block | Query blocked if any PHI detected. Public API, no auth. |

## Claude CLI Isolation

All Claude CLI calls use privacy-isolated subprocess with restricted tools:

```
Privacy Preamble (injected in every prompt via _PRIVACY_PREAMBLE):
  "Do NOT save, store, remember, or persist ANY data from this conversation."
  Defined once in llm/claude_client.py, imported by research/claude_cli_client.py.

Flags (defined in llm/claude_client.py, imported by research/claude_cli_client.py):
  --no-session-persistence    Session NOT saved to disk
  --strict-mcp-config         Ignore all user MCP configs (cortex-core, etc.)
  --mcp-config '{"mcpServers":{}}' Zero MCP servers loaded
  --tools WebSearch,WebFetch  Only web research tools (blocks Bash, Edit, Write, Read)

Environment isolation:
  env={"PATH": ..., "HOME": ..., "USER": ...}  Only PATH + HOME + USER passed (no secret leakage)

Data via stdin only:
  subprocess.run(cmd, input=full_input)  NEVER in command-line args (invisible to ps)
```

## Anonymization Pipeline (Three-Layer + Identity-Aware)

```
Layer 1: GLiNER NER      (recommended — catches names, cities, organizations)  [LOCAL AI]
Layer 2: PhiFirewall      (always, deterministic — catches SSN, MRN, DOBs, insurance)  [LOCAL]
  └─ Identity patterns    (your name, family, DOB, email — compiled from /identity)  [LOCAL]
Layer 3: Ollama LLM       (recommended — catches context-dependent PII regex misses) [LOCAL AI]
Gate:    assert_safe()    (final gate — blocks if anything slipped through)

All layers analyze the ORIGINAL text → spans merged → single redaction pass.
If GLiNER not installed: falls back to regex-only (Layer 2) + Ollama (Layer 3).
If Ollama not available: graceful fallback to Layers 1+2 only.

Identity-aware: When you configure /identity, your name (+ variants), family names, DOB,
and email are compiled into regex and injected into PhiFirewall. Layers 1+2 alone
(without Ollama) can catch your personal PII deterministically.

WARNING: Without NER (GLiNER) and LLM (Ollama), only regex-based PII detection runs.
Regex catches SSN, MRN, phone, email, DOB, addresses, insurance IDs, and identity-
profile patterns but CANNOT detect contextual PII like unknown person names, city/state
names, or organization names in free text. Install both for full coverage.
```

Setup: `make setup-nlp` (installs GLiNER, downloads ~500MB model)
Ollama Layer 3: automatic when Ollama is running (no extra setup)

## Clean Sync Modes (`/cleansync`)

The `/cleansync` command copies Tier 1 (raw vault) to Tier 2 (Clean DB), anonymizing all text fields in the process. Four modes are available:

| Mode | What It Does | Speed |
|------|-------------|-------|
| **Fast** | NER + regex + identity patterns only (no Ollama) | Minutes |
| **Hybrid** (recommended) | Fast pass first, then Ollama reviews only uncertain fields | ~15% of Full time |
| **Full** | Ollama on every uncached field | Hours |
| **Rebuild** | Clear cache + full re-anonymize from scratch | Hours |

**Hybrid mode** runs a two-pass approach:
1. **Pass 1 (fast)**: NER + regex + identity patterns on all fields. Fields are classified as "certain" or "uncertain."
2. **Pass 2 (selective Ollama)**: Only uncertain fields (~15%) are sent to Ollama for review.

A field is "uncertain" when:
- NER detected something with low confidence (< 0.7)
- NER found an entity that regex didn't confirm (unknown name/location not in identity profile)
- Text is >80 chars and zero layers detected anything (potential hiding spot)

**Caching**: All anonymization results are cached (SHA256-keyed). Subsequent syncs skip already-processed fields. Safe-skipped fields (numeric values, short codes) bypass anonymization entirely.

## Security Invariants (NEVER VIOLATE)

1. ALL security gates are deterministic (regex/pattern). **NEVER cloud-LLM-based.**
2. PHI detection uses regex (`phi_firewall.py`) + optional local NER (`ner_layer.py`).
3. Triage red flags are keyword/pattern (`triage.py`). NEVER use LLM for triage.
4. Research queries: **hard-block** on PHI — if PHI is detected, the query is REJECTED entirely (not cleaned and sent). For conversation, data is pre-anonymized from Clean DB (Tier 2) which never contains PII.
5. After `anonymize()`, **always** call `assert_safe()` before sending outbound data.
6. Claude CLI: `subprocess.run` with `input=` (stdin). NEVER pass PHI in command-line args.
7. NEVER use shell `timeout` command. Use `subprocess.run(timeout=...)`.
8. All logs pass through `PhiScrubFilter` (`log_scrubber.py`) before writing to disk.
9. **No plaintext PHI on disk. Ever.** All DB fields encrypted with per-field AAD.
10. Bot acts as a knowledgeable medical advisor. Give direct interpretations backed by evidence. No disclaimers.
11. WHOOP: OAuth only. NEVER password scraping.
12. Vault passphrase NEVER stored to disk in plaintext (not in config files, not anywhere). Exceptions: (a) MCP auto-unlock may use `HEALTHBOT_PASSPHRASE` env var set by the user for unattended startup (see `docs/OPENCLAW_SETUP.md`); (b) `--setup` optionally stores passphrase in macOS Keychain (`healthbot_mcp_passphrase`) for MCP auto-unlock — user must explicitly opt in.
13. Subprocess `env=` must be explicit (PATH + HOME + USER only). No full environment inheritance.
14. All outbound subprocess calls use `_PRIVACY_FLAGS` + `_TOOL_FLAGS` from `llm/claude_client.py`.
15. NER layer is a detection AID, not a security gate replacement. Regex always runs as Layer 2.
16. NER layer (GLiNER) runs 100% locally. NEVER send raw PII to cloud for stripping.
17. Clean DB NEVER contains PII — every text write validated by `PhiFirewall.contains_phi()`.
18. Clean DB uses HKDF-derived key (context: `healthbot-clean-v1`), never the master key directly.
19. MCP server tools run final PII check on all outbound responses.
20. MCP server runs locally via stdio transport — no network exposure.
21. Ollama LLM anonymization (Layer 3) is enhancement-only, never replaces deterministic regex+NER.
22. Clean sync is one-way: raw vault → Clean DB. No reverse path exists. Supports fast/hybrid/full/rebuild modes.

## How Encryption Works

| Layer | Cipher | Key Source | AAD |
|-------|--------|-----------|-----|
| DB fields (Tier 1) | AES-256-GCM | Master key | `table.encrypted_data.row_id` |
| Clean DB (Tier 2) | AES-256-GCM | HKDF-derived clean key | `clean_table.column.row_id` |
| File blobs | AES-256-GCM | Master key | blob UUID |
| Backups | AES-256-GCM | Master key | JSON: `{"backup_id","kdf":{...}}` |
| Vector index | AES-256-GCM | Master key | index name |
| Claude CLI state | AES-256-GCM | Master key | `relaxed.health_data` / `relaxed.memory` |

- **KDF**: Argon2id — 64MB memory, 3 iterations, 4 parallelism -> 256-bit key
- **Nonce**: 12-byte random per operation (never reused at our volume)
- **AAD binding**: Prevents ciphertext swapping between rows/tables
- **Key lifecycle**: Passphrase -> Argon2id -> `bytearray` in memory -> zeroed on lock
- **Auto-lock**: 30-minute timeout, key zeroed, on_lock saves Claude conversation state

## How to Safely Add Features

### New encrypted DB field
1. Add migration in `data/schema.py` MIGRATIONS list
2. Add encrypt/decrypt in `data/db.py` using `self._encrypt(data, aad_context)`
3. AAD must include table name + column + row ID

### New outbound API/CLI call
1. Anonymize all health data: `cleaned, _ = anonymizer.anonymize(text)`
2. Verify: `anonymizer.assert_safe(cleaned)` — raises `AnonymizationError` if PII remains
3. If calling Claude CLI: use `_PRIVACY_FLAGS` + `_TOOL_FLAGS` from `llm/claude_client.py`
4. Pass data via stdin, never args. Use `subprocess.run(timeout=...)`.
5. Pass minimal `env={"PATH": ..., "HOME": ..., "USER": ...}`

### New safety/triage check
1. MUST be deterministic (regex, keyword matching, threshold)
2. Add to `reasoning/triage.py`
3. NEVER use LLM for safety decisions

### New file storage
1. Use `Vault.store_blob()` — encrypts with AES-256-GCM + AAD
2. NEVER write plaintext health data to disk (even temporarily)
3. Use `io.BytesIO()` for in-memory processing

### New Telegram command
1. Add handler method in the appropriate sub-handler (`bot/handlers_health.py`, `bot/handlers_medical.py`, `bot/handlers_data.py`, or `bot/handlers_session.py`)
2. Add delegation in `bot/handlers.py` facade
3. Register in `bot/app.py`
3. Require vault unlock for data access commands

### New reasoning module
1. Add to `reasoning/` package
2. All logic must be deterministic (no LLM calls in reasoning)
3. LLM interpretation is separate (in `llm/proactive.py`)

## What NOT to Do

- Never store plaintext PHI to disk (even in temp files)
- Never use cloud LLM for safety, triage, or PHI detection (local NER is OK)
- Never pass health data in command-line arguments
- Never use `shell=True` in subprocess calls
- Never hardcode file paths (use `shutil.which()` or `config.py` paths)
- Never import from other projects on this machine
- Never commit secrets, API keys, or PII to the repository
- Never add `anthropic` SDK — we use Claude CLI (no API key needed)
- Never duplicate `_PRIVACY_FLAGS`/`_TOOL_FLAGS` — import from `llm/claude_client.py`
- Never skip `assert_safe()` after `anonymize()` on outbound data
- Never commit macOS Keychain secrets to any file

## Key Files Reference

| File | Purpose |
|------|---------|
| `llm/claude_conversation.py` | Claude CLI conversation manager — structured block parsing, KB/hypothesis routing |
| `llm/claude_context.py` | System prompt with Medical Intelligence Protocol (HYPOTHESIS/ACTION/INSIGHT blocks) |
| `llm/claude_client.py` | Claude CLI subprocess + privacy flags |
| `llm/anonymizer.py` | Three-layer PII stripping: L1 NER + L2 regex (+ identity patterns) + L3 Ollama LLM + assert_safe(). Also provides `anonymize_fast_only()` for hybrid mode. |
| `llm/anonymizer_llm.py` | Ollama Layer 3 PII detection (shared prompt + parser) |
| `llm/ollama_client.py` | Ollama local LLM client (Layer 3 anonymization only) |
| `security/ner_layer.py` | GLiNER NER wrapper — local AI for intelligent PII detection |
| `llm/memory_store.py` | STM/LTM memory + medical journal archiving |
| `llm/proactive.py` | Deterministic signal gathering (Phase 1 only — no LLM interpretation) |
| `security/phi_firewall.py` | Regex-based PHI detection (SSN, MRN, phone, email, DOB, address) |
| `security/key_manager.py` | Argon2id KDF, master key lifecycle, 30-min auto-lock, on_lock callback |
| `security/vault.py` | Encrypted blob storage for PDFs and exports |
| `security/log_scrubber.py` | PHI redaction filter for all log output |
| `data/db/` | Tier 1: Encrypted SQLite (package, 11 submodules) — all sensitive fields use `_encrypt/_decrypt` with AAD |
| `data/clean_db/` | Tier 2: Anonymized SQLite (package, 11 submodules) — zero PII, AI-accessible, HKDF key |
| `data/clean_sync.py` | Sync engine: raw vault → anonymize → Clean DB. Supports fast/hybrid/full/rebuild modes with uncertainty detection and selective Ollama review. |
| `data/schema.py` | Schema definition + migration system |
| `research/claude_cli_client.py` | Research via Claude CLI (imports privacy flags from llm/) |
| `research/research_packet.py` | PHI hard-block gateway for all outbound research |
| `reasoning/triage.py` | Emergency keyword detection (deterministic, no LLM) |
| `vault_ops/backup.py` | tar + zstd + AES-256-GCM encrypted backups |
| `config.py` | All paths, constants, settings (no secrets) |
| `bot/handlers.py` | Telegram command facade (delegates to sub-handler modules) |
| `bot/handler_core.py` | Shared handler state, DB/LLM init, vault lock |
| `bot/handlers_health/` | Health analysis commands (package, 8 submodules) (insights, trend, ask, correlate, etc.) |
| `bot/handlers_medical.py` | Medical tracking commands (hypotheses, evidence, doctor prep) |
| `bot/handlers_data/` | Import/export/sync commands (package, 8 submodules) (WHOOP, Oura, Apple Health, FHIR) |
| `bot/handlers_onboard.py` | Interactive onboarding questionnaire flow |
| `bot/handlers_reset.py` | Vault reset/wipe with confirmation |
| `export/chart_generator.py` | In-memory matplotlib charts (trend lines, dashboard bars) |
| `export/pdf_generator.py` | In-memory PDF doctor packets (fpdf2) |
| `reasoning/delta.py` | Delta engine — what changed between lab panels |
| `reasoning/health_review.py` | Structured health review with action plan |
| `reasoning/data_quality.py` | Ingestion-time validation (fasting, units, duplicates) |
| `reasoning/interactions.py` | Medication interaction checker (curated KB) |
| `reasoning/hypothesis_tracker.py` | Fuzzy-match hypothesis dedup, upsert, evidence merge |
| `reasoning/doctor_templates.py` | Condition-specific doctor discussion templates |
| `reasoning/genetic_risk.py` | SNP catalog matching, cross-reference insights |
| `reasoning/pharmacogenomics.py` | CYP enzyme metabolizer classification |
| `reasoning/pathway_analysis.py` | Genetic variant pathway grouping |
| `reasoning/derived_markers.py` | Calculated markers (HOMA-IR, eGFR, TG/HDL, etc.) |
| `reasoning/lab_alerts.py` | Critical values, rapid changes, threshold crossings |
| `bot/overdue_pause.py` | Overdue alert pause/snooze state management |
| `importers/oura_client.py` | Oura Ring OAuth v2 client (httpx async) |
| `research/external_evidence_store.py` | Cached research with TTL, browse, cleanup |
| `ingest/clinical_doc_parser.py` | Ollama-based clinical document extraction (doctor's notes, summaries, etc.) |
| `ingest/telegram_pdf_ingest/` | PDF ingestion pipeline (package, 7 submodules): validate → encrypt → parse labs → clinical extraction → store |
| `bot/message_router/` | Routes messages (package, 7 submodules): passphrase, documents (PDF/ZIP), photos, free text |
| `bot/scheduler/` | Background jobs (package, 10 submodules): alerts, incoming folder poll, STM consolidation, backups |
| `mcp/server.py` | MCP server: 9 tools for anonymized health data + skill system |
| `mcp/entry.py` | MCP server entry: passphrase handling, KeyManager init |
| `security/pii_alert.py` | Real-time PII leak alerting — logs + Telegram push + stats |
| `skills/base.py` | Skill system Protocol, HealthContext, SkillResult, SkillRegistry |
| `skills/builtin.py` | 12 built-in skill adapters wrapping reasoning modules |

## External Integrations

| Service | Auth | Module | Notes |
|---------|------|--------|-------|
| Telegram | Bot token (Keychain) | `bot/` | python-telegram-bot v21+ async |
| Claude CLI | Local subscription | `llm/`, `research/` | Sole conversation + analysis backend, research |
| Ollama | Local (no auth) | `llm/` | Recommended — Layer 3 PII anonymization |
| MCP Server | Vault passphrase | `mcp/` | stdio transport, Claude Code / OpenClaw |
| WHOOP | OAuth 2.0 (Keychain) | `importers/whoop_client.py` | httpx async |
| Oura Ring | OAuth 2.0 (Keychain) | `importers/oura_client.py` | httpx async |
| PubMed | None (public API) | `research/pubmed_client.py` | E-utilities REST |
| Apple Health | File import | `ingest/apple_health_import.py` | XML parse |
| MyChart | File import | `ingest/mychart_import.py` | CCDA/FHIR XML |

## Skill System (OpenClaw-Inspired)

HealthBot includes a skill system inspired by OpenClaw's architecture. Each reasoning engine is wrapped as a `Skill` with a standardized Protocol:

```python
class Skill(Protocol):
    name: str
    description: str
    def run(self, ctx: HealthContext) -> SkillResult: ...
    def is_relevant(self, ctx: HealthContext) -> bool: ...
```

**12 built-in skills**: trend_analysis, interaction_check, panel_gaps, hypothesis_generator, overdue_screenings, intelligence_audit, family_risk, wearable_trends, derived_markers, lab_alerts, pathway_analysis, pharmacogenomics.

**ToolPolicy** levels: HIGH (actionable), MEDIUM (informational), LOW (speculative), NEEDS_RESEARCH (auto-queue for PubMed).

Skills are accessible via MCP (`list_skills`, `run_skill` tools) and the Telegram bot.

### Adding a New Skill
1. Create a class with `name`, `description`, `run()`, and `is_relevant()` in `skills/builtin.py`
2. Add it to `register_builtin_skills()`
3. It automatically appears in MCP `list_skills` and `run_skill`

## PII Alert System

Real-time PII leak detection and alerting. Every blocked PII event is:
- Logged to `~/.healthbot/logs/pii_alerts.log` (category + timestamp only, no PHI content)
- Counted in cumulative stats (viewable via `/pii_alerts` command)
- Optionally pushed as Telegram notification

Wired into: `anonymizer.assert_safe()`, `research_packet`, `clean_sync`, `mcp/server.py`.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Telegram bot token not found" | Run `python -m healthbot --setup` |
| Bot doesn't respond | Check `/auth_status` — verify Telegram is connected |
| Claude CLI not working | Run `claude login` in terminal, then `/claude_auth check` |
| Ollama Layer 3 not available | `ollama serve &` (starts on-demand, stops when idle) |
| GLiNER NER not loaded | Run `make setup-nlp` (recommended — catches names/cities/orgs) |
| Vault won't unlock | Wrong passphrase — there is no recovery. Create new vault if lost. |
| Clean DB out of sync | `/cleansync` in Telegram (hybrid mode recommended) or `python -m healthbot --clean-sync` |
| WHOOP/Oura won't connect | Run `/whoop_auth` or `/oura_auth` in Telegram to re-authorize |
| Tests failing after changes | `ruff check src/ tests/ && pytest tests/ -q` |

## Architecture Decision Records

**Why Claude CLI (not Anthropic SDK)?**
Claude CLI (`claude --print`) uses the user's existing subscription at zero additional cost. No API key management, no billing surprises. Subprocess isolation provides privacy guarantees (stdin-only, restricted tools, no session persistence).

**Why local-first encryption?**
Medical records are irreplaceable and highly sensitive. AES-256-GCM with per-field AAD binding, Argon2id KDF, and keys-in-RAM-only ensures data sovereignty. No cloud backup, no server dependency.

**Why two-tier DB?**
Tier 1 (raw vault) keeps full PHI for medical accuracy. Tier 2 (Clean DB) is pre-anonymized for AI access. This eliminates real-time anonymization latency for MCP queries while maintaining a single source of truth.

**Why deterministic reasoning (not LLM)?**
Medical triage, drug interactions, and PII detection must be reproducible and auditable. LLMs can hallucinate or miss edge cases. Deterministic engines run in milliseconds with predictable behavior. Claude CLI is used only for interpretation and research.

**Why macOS Keychain (not .env files)?**
Keychain is OS-level encrypted storage with biometric unlock. `.env` files are plaintext on disk, easily leaked via git or backups.

## Code Quality

- Python 3.13+ (tested on 3.14), use `str | None` union syntax
- Type hints on all functions
- Max 400 lines per file (soft cap: 900 LOC for generated/data-heavy modules)
- No hardcoded secrets
- All tests mock LLM calls (no real Ollama or Claude subprocess in tests)
- Linting: `ruff check src/ tests/`
- New/changed functions should have cyclomatic complexity <= 12
- Security paths (`security/`, `data/`, `ingest/`) must use typed exceptions — no bare `except Exception: pass` where swallowed errors could mask PII leaks or data corruption
- Startup self-check (`startup_checks.py`) runs on vault unlock — logs privacy mode, identity patterns, clean sync status, migration status
