#!/bin/bash
# Start HealthBot
set -euo pipefail
cd "$(dirname "$0")/.."
exec .venv/bin/python -m healthbot "$@"
