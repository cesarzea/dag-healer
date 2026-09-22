#!/usr/bin/env bash
# The whole loop, end to end, in one terminal.
#
#   ./scripts/demo.sh            diagnose with Claude Code (needs `claude` on PATH)
#   ./scripts/demo.sh mock       diagnose with the deterministic stand-in
set -euo pipefail
cd "$(dirname "$0")/.."

BACKEND="${1:-claude-code}"
PY=".venv/bin/python"
export PYTHONPATH=src
export SHOP_API_URL="${SHOP_API_URL:-http://localhost:8099}"

if ! curl -sSf "$SHOP_API_URL/health" >/dev/null 2>&1; then
  echo "The fake upstream is not running. In another terminal:  make api" >&2
  exit 1
fi

rule() { printf '\n\033[1m%s\033[0m\n' "$1"; }

./scripts/reset.sh >/dev/null
./scripts/inject_failure.sh reset >/dev/null

rule "1. A healthy run. This is what establishes the profile we later compare against."
$PY -m dag_healer.cli ingest

rule "2. The merchant renames total_price to order_total. Nobody tells us."
./scripts/inject_failure.sh rename >/dev/null
echo "   upstream schema changed"

rule "3. The next run fails, and files an incident with its evidence."
set +e
$PY -m dag_healer.cli ingest
set -e

rule "4. The reliability layer diagnoses it, and has to prove the repair before applying it."
$PY -m dag_healer.cli heal --latest --backend "$BACKEND"

rule "5. The pipeline runs clean again."
$PY -m dag_healer.cli ingest

rule "6. The change is recorded, and waiting for a human to confirm it."
$PY -m dag_healer.cli status
