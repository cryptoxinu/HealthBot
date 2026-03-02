# HealthBot

Local-first, security-first personal health data vault and medical advisor for macOS.

## Quick Start (5 minutes)

```bash
# 1. Install dependencies
make setup

# 2. Install Claude CLI (required for AI conversation)
brew install claude-code

# 3. First-time setup (stores secrets in macOS Keychain)
python -m healthbot --setup
# Guided wizard: Telegram bot token, vault passphrase, PII detection setup
# Wearables (WHOOP/Oura) connect later via /whoop_auth and /oura_auth
# Then /sync to sync all connected sources, /connectors to see status

# 4. Run
make dev
```

Open Telegram, send `/start` to your bot, then `/unlock <passphrase>` to begin.

## Chat Interfaces

| Interface | How | Setup |
|-----------|-----|-------|
| **Telegram** | Direct bot chat (103 commands + free text) | `python -m healthbot --setup` (choose Telegram) |
| **OpenClaw** | AI agent with MCP tool access to health data | [OpenClaw setup guide](docs/OPENCLAW_SETUP.md) |
| **Claude Code** | MCP tools during coding sessions | `python -m healthbot --mcp-register` |

## What It Does

- **Personal medical advisor** — direct, evidence-backed health interpretations
- Ingests lab PDFs (Quest, LabCorp, MyChart) via Telegram upload
- Imports Apple Health, MyChart CCDA/FHIR, WHOOP, and Oura Ring data
- `/sync` — one command syncs all connected wearables (WHOOP, Oura, Apple Health)
- `/connectors` — see all data source status, last sync dates, and setup instructions
- Stores everything in a portable encrypted vault (`~/.healthbot/`)
- AI conversations via Claude CLI — your identifiable data never leaves your machine
- Detects trends, anomalies, overdue screenings, drug interactions, and lab/wearable correlations
- Auto-generates medical hypotheses from lab patterns
- Generates doctor visit prep summaries, PDF packets, and discussion templates
- Sanitized medical research via Claude CLI + PubMed (PHI hard-blocked)
- 103 Telegram commands — all deterministic (no LLM required)
- MCP server for Claude Code / OpenClaw integration
- Real-time PII leak alerting

## Prerequisites

| Requirement | Required? | Install |
|-------------|-----------|---------|
| macOS | Yes | Keychain integration |
| Python 3.13+ | Yes | `brew install python@3.14` |
| Claude CLI | Yes | `brew install claude-code` |
| Telegram Bot Token | Yes | @BotFather on Telegram |
| Ollama | Recommended | `brew install ollama` (enhanced PII detection) |
| GLiNER NER | Recommended | `make setup-nlp` (~500MB model, catches names/cities/orgs) |

## How Your Data Stays Private

When you upload a medical PDF or type a health question, here's exactly what happens:

```
YOU (Telegram)
 │
 ├─ Upload a PDF ─────────────────────────────────────────────┐
 │   Step 1: Downloaded to your Mac (never sent to AI)        │
 │   Step 2: Parsed locally with regex (no AI involved)       │ YOUR MAC
 │   Step 3: Encrypted (AES-256-GCM) and stored in vault      │ (nothing
 │   Step 4: Anonymized copy synced to Clean DB (zero PII)    │  leaves)
 │                                                             │
 ├─ Type a health question ──────────────────────────────────┐│
 │   1. Intelligence engines run locally (trends, gaps, etc) ││
 │   2. Data pulled from Clean DB (already PII-free)         ││
 │   3. Three-layer anonymization gate (NER + regex + LLM)   ││
 │   4. assert_safe() — blocks if ANY PII remains            ││
 │                        ▼                                    │
 │              Anonymized prompt only ──────► Claude CLI       │
 │              (no names, SSN, DOB, etc)     (cloud, safe)    │
 │                                                             │
 └─ /commands (103 total) ──► 100% local, no AI ───────────────┘
```

**The key guarantee**: Your identifiable medical data (names, SSN, DOB, addresses) is NEVER sent to any cloud service. PDFs are parsed with regex — no AI touches the raw file. Claude only sees anonymized summaries from the Clean DB.

## Security Model

- **Two-tier encryption**: Raw vault (AES-256-GCM, full PHI) + Clean DB (HKDF key, zero PII)
- **Three-layer PII anonymization**: GLiNER NER + PhiFirewall regex + identity-aware patterns + Ollama LLM
- **Smart hybrid sync**: Fast pass (regex + NER + identity) runs instantly, Ollama reviews only uncertain fields
- **All secrets in macOS Keychain** — never on disk
- **Vault passphrase**: Argon2id KDF (64MB, 3 iterations) — never stored anywhere
- **Claude CLI isolation**: subprocess with stdin-only, restricted tools, no session persistence
- **Hard-block policy**: PHI in research queries is blocked, never sanitized-and-sent

## Deploying to Another Mac

```bash
# On old Mac
make backup

# Transfer backup + project folder to new machine

# On new Mac
make setup
python -m healthbot --restore /path/to/backup.enc
python -m healthbot --setup  # Re-enter Keychain secrets
```

## Full Documentation

See [CLAUDE.md](CLAUDE.md) for:
- Complete architecture map and data flow
- Security invariants
- Installation guide with all optional components
- How to safely add features
- Troubleshooting guide
- Architecture decision records

## Verifying Changes

```bash
ruff check src/ tests/ && pytest tests/ -q
```
