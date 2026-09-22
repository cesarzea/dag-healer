#!/usr/bin/env bash
# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

# Make the upstream API misbehave, the way a merchant would without telling you.
#
#   ./scripts/inject_failure.sh rename    # total_price -> order_total
#   ./scripts/inject_failure.sh subtotal  # total_price -> subtotal (harder case)
#   ./scripts/inject_failure.sh transient # next 2 calls return 503
#   ./scripts/inject_failure.sh ratelimit # return 429 until reset
#   ./scripts/inject_failure.sh reset     # behave again
set -euo pipefail
API="${SHOP_API_URL:-http://127.0.0.1:8099}"

case "${1:-}" in
  rename)    body='{"rename_total_price": true, "rename_to_subtotal": false}'
             what="renaming a field nobody was told about" ;;
  subtotal)  body='{"rename_to_subtotal": true, "rename_total_price": false}'
             what="replacing the total with a subtotal: different meaning, similar size" ;;
  transient) body='{"fail_next": 2}'              ; what="the next 2 calls will return 503" ;;
  ratelimit) body='{"rate_limit": true}'          ; what="every call will return 429 until reset" ;;
  reset)     body='{"rename_total_price": false, "rename_to_subtotal": false, "fail_next": 0, "rate_limit": false}'
             what="behaving normally again" ;;
  *) echo "usage: $0 {rename|subtotal|transient|ratelimit|reset}" >&2; exit 2 ;;
esac

# The field names before and after are the whole story of the `rename` case, and
# reading them off the wire is more convincing than being told what changed.
fields_now() {
  curl -sS "$API/admin/api/orders.json" \
    | python3 -c 'import json,sys; print(", ".join(sorted(json.load(sys.stdin)["orders"][0])))' \
    2>/dev/null || echo "(upstream not reachable)"
}

# Only the schema cases get a before/after probe. Reading the payload to show a
# transient failure would consume one of the errors being injected, and report
# the probe's own 503 as the new shape.
probe=""
case "${1:-}" in rename|subtotal|reset) probe="yes" ;; esac

[ "${QUIET:-}" = "1" ] || echo "  upstream  $what"
[ -n "$probe" ] && [ "${QUIET:-}" != "1" ] && before="$(fields_now)"

curl -sS -X POST "$API/admin/state" -H 'content-type: application/json' -d "$body" >/dev/null

if [ -n "$probe" ] && [ "${QUIET:-}" != "1" ]; then
  after="$(fields_now)"
  if [ "$before" != "$after" ]; then
    echo "            was:  $before"
    echo "            now:  $after"
  else
    echo "            payload shape unchanged: $after"
  fi
fi
