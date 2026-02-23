#!/usr/bin/env bash
# HealthBot system readiness check.
# Run: make doctor

set -euo pipefail

PASS=0
WARN=0
FAIL=0

check_pass() { echo "  [OK] $1"; PASS=$((PASS+1)); }
check_warn() { echo "  [--] $1"; WARN=$((WARN+1)); }
check_fail() { echo "  [!!] $1"; FAIL=$((FAIL+1)); }

echo "HealthBot Doctor"
echo "================"
echo ""

# Python version
PYTHON_VERSION=$(python3 --version 2>/dev/null | awk '{print $2}')
if [ -n "$PYTHON_VERSION" ]; then
    MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
    MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)
    if [ "$MAJOR" -ge 3 ] && [ "$MINOR" -ge 13 ]; then
        check_pass "Python $PYTHON_VERSION (>= 3.13 required)"
    else
        check_fail "Python $PYTHON_VERSION (>= 3.13 required)"
    fi
else
    check_fail "Python not found"
fi

# Virtual environment
if [ -d ".venv" ] && [ -f ".venv/bin/python" ]; then
    check_pass "Virtual environment (.venv)"
else
    check_fail "Virtual environment not found (run: make setup)"
fi

# Claude CLI
if command -v claude &>/dev/null; then
    check_pass "Claude CLI installed"
else
    check_fail "Claude CLI not found (run: brew install claude-code)"
fi

# macOS Keychain accessible
if /usr/bin/security list-keychains &>/dev/null; then
    check_pass "macOS Keychain accessible"
else
    check_fail "macOS Keychain not accessible"
fi

# Telegram bot token in Keychain
if /usr/bin/security find-generic-password -a telegram_bot_token -s com.healthbot.v1 -w &>/dev/null 2>&1; then
    check_pass "Telegram bot token (Keychain)"
else
    check_warn "Telegram bot token not configured (run: python -m healthbot --setup)"
fi

# Vault directory
VAULT_HOME="$HOME/.healthbot"
if [ -d "$VAULT_HOME" ]; then
    check_pass "Vault directory ($VAULT_HOME)"
else
    check_warn "Vault directory not found (created during --setup)"
fi

# Vault writable
if [ -d "$VAULT_HOME" ] && [ -w "$VAULT_HOME" ]; then
    check_pass "Vault directory writable"
elif [ -d "$VAULT_HOME" ]; then
    check_fail "Vault directory not writable"
fi

# Ollama (optional)
if command -v ollama &>/dev/null; then
    if ollama list &>/dev/null 2>&1; then
        check_pass "Ollama running (optional — Layer 3 PII)"
    else
        check_warn "Ollama installed but not running (optional)"
    fi
else
    check_warn "Ollama not installed (optional — Layer 3 PII)"
fi

# GLiNER NER model (optional)
if [ -d ".venv" ] && .venv/bin/python -c "from gliner import GLiNER" 2>/dev/null; then
    check_pass "GLiNER NER installed (optional — smart PII detection)"
else
    check_warn "GLiNER NER not installed (optional — run: make setup-nlp)"
fi

# Dependencies installed
if [ -d ".venv" ] && .venv/bin/python -c "import healthbot" 2>/dev/null; then
    check_pass "HealthBot package installed"
else
    check_fail "HealthBot package not installed (run: make setup)"
fi

echo ""
echo "Results: $PASS passed, $WARN warnings, $FAIL failed"

if [ "$FAIL" -gt 0 ]; then
    echo ""
    echo "Fix the [!!] items above before running HealthBot."
    exit 1
fi
