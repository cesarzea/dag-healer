#!/usr/bin/env bash
# Put the repo back to its pristine state: mapping v1, no baseline, no incidents.
set -euo pipefail
cd "$(dirname "$0")/.."

cat > mappings/orders.mapping.yml <<'YAML'
# Upstream-to-canonical field mapping for `orders`.
#
# This is the ONLY file the reliability layer is allowed to modify, and only
# by rewriting the `source` of an existing canonical field. It cannot add or
# remove canonical fields, cannot touch the contract, and cannot touch code.
# Every change bumps `version` and is recorded in `history`.
entity: orders
version: 1
source: fake_shop_api
endpoint: /admin/api/orders.json
records_path: orders
fields:
  order_id: id
  created_at: created_at
  currency: currency
  total_amount: total_price
  customer_id: customer_id
  line_items: line_items_count
  status: financial_status
history: []
YAML

rm -f baselines/*.json incidents/*.json incidents/*.md data/*.sqlite data/api_state.json
echo "reset: mapping v1, no baseline, no incidents"
