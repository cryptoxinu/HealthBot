# Contributing to HealthBot

Thanks for your interest in contributing! HealthBot is a security-critical medical data application, so please read these guidelines carefully.

## Getting Started

1. Fork the repo and clone your fork
2. `make setup` to create a virtualenv and install dependencies
3. Create a feature branch from `main`
4. Make your changes
5. Run `make lint && make test` before submitting
6. Open a pull request

## Code Style

- **Linter**: [ruff](https://docs.astral.sh/ruff/) — run `make lint` before committing
- **Type hints**: Required on all function signatures
- **Line length**: 100 characters max
- **File length**: 400 lines soft cap (900 LOC for generated/data-heavy modules)
- **Python version**: 3.13+ (`str | None` union syntax, not `Optional[str]`)
- **Cyclomatic complexity**: <= 12 per function

## Testing

| Command | What it runs |
|---------|-------------|
| `make test` | Full test suite |
| `make test-fast` | Fast tests only (~2 min) |
| `make test-sec` | Security tests (PHI firewall, log scrubber, PDF safety) |
| `make lint` | ruff check |

All PRs must pass `make lint && make test` before merge. Tests should mock all LLM calls — no real Claude CLI or Ollama subprocess calls in tests.

## Security Rules

This project handles encrypted medical data. Security is non-negotiable:

- **Never commit secrets** (API keys, tokens, passwords, PII) — they belong in macOS Keychain
- **Never modify security modules** (`security/`, `data/`, anonymization pipeline) without prior review and discussion in an issue
- **Never use cloud LLM for safety decisions** — triage, PII detection, and PHI gates must be deterministic
- **Never write plaintext PHI to disk** — not even in temp files
- **Never use `shell=True`** in subprocess calls
- **Never skip `assert_safe()`** after `anonymize()` on outbound data
- All security-path code must use typed exceptions (no bare `except Exception: pass`)

## Pull Request Process

1. Describe what your PR does and why
2. Reference any related issues
3. Ensure `make lint && make test` passes
4. Security-sensitive changes require at least one reviewer
5. Keep PRs focused — one feature or fix per PR

## Reporting Security Vulnerabilities

If you find a security vulnerability, **do not open a public issue**. Instead, see [SECURITY.md](SECURITY.md) for responsible disclosure instructions.

## Architecture Notes

Before contributing, familiarize yourself with:

- **Two-tier encryption model**: Tier 1 (raw vault, full PHI) and Tier 2 (Clean DB, zero PII)
- **Three-layer anonymization**: NER + regex + Ollama LLM
- **Deterministic reasoning**: All `reasoning/` modules are LLM-free
- **Claude CLI isolation**: Subprocess with restricted tools, stdin-only data

See `CLAUDE.md` for the full architecture reference.

## License

By contributing, you agree that your contributions will be licensed under the GPL-3.0-or-later license.
