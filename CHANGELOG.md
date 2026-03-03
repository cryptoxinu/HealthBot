# Changelog

All notable changes to HealthBot are documented here.

## [1.0.0] — 2025-02-20

### Initial Release

Local-first encrypted health vault for macOS with Telegram bot interface, Claude CLI integration, and MCP server.

**Core Features**
- Two-tier encrypted SQLite vault (AES-256-GCM, Argon2id KDF)
- 103 deterministic Telegram `/commands`
- Claude CLI as sole conversation and analysis backend
- Three-layer PII anonymization (GLiNER NER + regex + Ollama LLM)
- PHI firewall with identity-aware patterns
- MCP server for Claude Code and OpenClaw integration

**Health Intelligence**
- Lab trend analysis with slope detection and delta tracking
- Drug-lab interaction checker with curated knowledge base
- Hypothesis generation and evidence tracking
- Derived markers (HOMA-IR, eGFR, TG/HDL ratio, etc.)
- Genetic risk analysis and pharmacogenomics (CYP enzymes)
- Family risk assessment and pathway analysis
- Critical lab alerts (rapid changes, threshold crossings)

**Data Import**
- Lab PDF parsing (regex-based with cross-validation)
- Apple Health XML import
- MyChart CCDA/FHIR import
- WHOOP and Oura Ring OAuth v2 integration
- Clinical document extraction (Ollama-based)
- OCR support via pytesseract

**Export & Visualization**
- PDF doctor prep packets
- FHIR R4 resource export
- In-memory matplotlib charts (trends, dashboards, heatmaps)
- Weekly and monthly health reports
- AI-powered export summaries

**Security**
- macOS Keychain for all secrets (never on disk)
- PHI-scrubbed logging
- Encrypted backups (tar + zstd + AES-256-GCM)
- 30-minute auto-lock with key zeroing

---

## Post-1.0.0 Updates

### Added
- Visual trend charts in AI responses
- Intelligent memory system (temporal filtering, patient constants, audit trail)
- Chart dispatch registry and health cards
- Citation system for evidence-backed responses
- Saved messages feature (`/savedmessages`, natural language save/unsave)
- Self-improving response style capture via MEMORY blocks
- Fuzzy alias matching for medication memory sync
- Heuristic name detection for enhanced PII coverage
- Self-managing database pipeline (schema evolution)
- Skill system (12 built-in skills, MCP-accessible)

### Fixed
- 153 bugs + 3 UX fixes across all 10 subsystems
- 31 bugs from end-to-end system audit
- 14 security issues from Codex audit (P0-P2)
- PHI firewall false positives on health_context and hypotheses
- assert_safe() false positives from identity patterns
- Clean sync rebuild crash (wrong column name)
- Interaction checker empty meds handling
- Patient constants source mismatch

### Hardened
- Schema evolution pipeline: PHI gates, filesystem containment, DDL validation
- Security modularization of 8 hotspot files
- Claude CLI web search tool execution
