#!/usr/bin/env bash
# Make the upstream API misbehave, the way a merchant would without telling you.
#
#   ./scripts/inject_failure.sh rename    # total_price -> order_total
#   ./scripts/inject_failure.sh transient # next 2 calls return 503
#   ./scripts/inject_failure.sh ratelimit # return 429 until reset
#   ./scripts/inject_failure.sh reset     # behave again
set -euo pipefail
API="${SHOP_API_URL:-http://localhost:8099}"

case "${1:-}" in
  rename)    body='{"rename_total_price": true}' ;;
  transient) body='{"fail_next": 2}' ;;
  ratelimit) body='{"rate_limit": true}' ;;
  reset)     body='{"rename_total_price": false, "fail_next": 0, "rate_limit": false}' ;;
  *) echo "usage: $0 {rename|transient|ratelimit|reset}" >&2; exit 2 ;;
esac

curl -sS -X POST "$API/admin/state" -H 'content-type: application/json' -d "$body" | python3 -m json.tool
