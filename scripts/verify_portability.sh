#!/bin/bash
# Verify vault portability (copy to new location + unlock + query).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Vault Portability Verification ==="
echo ""

.venv/bin/python -m pytest tests/test_portable_vault.py -v --tb=short

echo ""
echo "=== Portability tests passed ==="
