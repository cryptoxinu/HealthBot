#!/bin/bash
# Verify no plaintext PHI exists in the vault or logs.
set -euo pipefail

VAULT_HOME="${HEALTHBOT_HOME:-$HOME/.healthbot}"
PASS=true

echo "=== HealthBot Security Verification ==="
echo ""

# 1. Check no raw PDFs on disk
echo "[1] Checking for raw PDFs..."
if find "$VAULT_HOME" -name "*.pdf" 2>/dev/null | grep -q .; then
    echo "  FAIL: Raw PDF files found in vault!"
    PASS=false
else
    echo "  PASS: No raw PDFs in vault."
fi

# 2. Check encrypted blobs don't start with %PDF-
echo "[2] Checking encrypted blobs..."
for f in "$VAULT_HOME"/vault/*.enc 2>/dev/null; do
    if [ -f "$f" ]; then
        header=$(head -c 5 "$f" 2>/dev/null || true)
        if [ "$header" = "%PDF-" ]; then
            echo "  FAIL: $f starts with %PDF- (not encrypted!)"
            PASS=false
        fi
    fi
done
echo "  PASS: All blobs are encrypted."

# 3. Check logs for PHI patterns
echo "[3] Checking logs for PHI patterns..."
if [ -d "$VAULT_HOME/logs" ]; then
    for f in "$VAULT_HOME"/logs/*.log 2>/dev/null; do
        if [ -f "$f" ]; then
            # Check for SSN pattern
            if grep -qP '\d{3}-\d{2}-\d{4}' "$f" 2>/dev/null; then
                echo "  FAIL: SSN-like pattern found in $f"
                PASS=false
            fi
            # Check for email
            if grep -qP '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}' "$f" 2>/dev/null; then
                echo "  FAIL: Email pattern found in $f"
                PASS=false
            fi
        fi
    done
    echo "  PASS: Logs appear PHI-free."
else
    echo "  SKIP: No log directory found."
fi

# 4. Check manifest for key material
echo "[4] Checking manifest for secrets..."
if [ -f "$VAULT_HOME/manifest.json" ]; then
    for pattern in "master_key" "password" "passphrase" "secret_key"; do
        if grep -qi "$pattern" "$VAULT_HOME/manifest.json" 2>/dev/null; then
            echo "  FAIL: '$pattern' found in manifest.json"
            PASS=false
        fi
    done
    echo "  PASS: Manifest contains no secrets."
else
    echo "  SKIP: No manifest found."
fi

# 5. Check no .env file with secrets
echo "[5] Checking for .env files..."
REPO_DIR="$(dirname "$0")/.."
if [ -f "$REPO_DIR/.env" ]; then
    echo "  WARN: .env file exists. Verify it contains no real secrets."
else
    echo "  PASS: No .env file found."
fi

echo ""
if [ "$PASS" = true ]; then
    echo "=== ALL SECURITY CHECKS PASSED ==="
    exit 0
else
    echo "=== SECURITY CHECKS FAILED ==="
    exit 1
fi
