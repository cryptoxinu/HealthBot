#!/bin/bash
# Verify PHI firewall blocks research subprocess invocation.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "=== Research PHI Firewall Verification ==="
echo ""

# Run the specific research tests
.venv/bin/python -m pytest tests/test_research_clients.py -v --tb=short

echo ""
echo "=== Research firewall tests passed ==="
