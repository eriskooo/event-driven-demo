#!/usr/bin/env bash
# Pošle N objednávek do order-service (výchozí http://localhost:8080 přes port-forward).
# Použití: send-orders.sh [počet] [částka]   – částka 666 vyvolá technickou chybu → retry → DLT.
set -euo pipefail

COUNT="${1:-10}"
AMOUNT="${2:-}"
BASE_URL="${ORDER_SERVICE_URL:-http://localhost:8080}"

for i in $(seq 1 "${COUNT}"); do
  amount="${AMOUNT:-$(( (RANDOM % 500) + 1 )).$(( RANDOM % 90 + 10 ))}"
  correlation_id="demo-$(date +%s)-$(printf %04x $RANDOM)-${i}"
  response=$(curl -sS -X POST "${BASE_URL}/orders" \
    -H 'Content-Type: application/json' \
    -H "X-Correlation-Id: ${correlation_id}" \
    -d "{\"customerId\":\"customer-$(( RANDOM % 5 + 1 ))\",\"amount\":${amount},\"currency\":\"CZK\"}")
  order_id=$(echo "${response}" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  echo "order ${order_id:-?}  amount ${amount}  correlationId ${correlation_id}"
done

echo "Check status: curl ${BASE_URL}/orders/<id>"
