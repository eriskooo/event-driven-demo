#!/usr/bin/env bash
# Zpřístupní UI a order-service na localhostu. Ctrl+C ukončí všechny port-forwardy.
set -euo pipefail

NS=eda-demo
FORWARDS=(
  "svc/order-service 8080:8080"
  "svc/payment-service 8081:8080"
  "svc/grafana 3000:3000"
  "svc/prometheus 9090:9090"
  "svc/kibana 5601:5601"
  "svc/elasticsearch 9200:9200"
)

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

for forward in "${FORWARDS[@]}"; do
  # shellcheck disable=SC2086 # záměrné rozdělení na "zdroj porty"
  kubectl -n "${NS}" port-forward ${forward} >/dev/null &
  pids+=($!)
done

cat <<EOF
order-service    http://localhost:8080/orders
payment-service  http://localhost:8081/actuator/health
Grafana          http://localhost:3000   (admin / admin)
Prometheus       http://localhost:9090
Kibana           http://localhost:5601   (Discover -> data view "EDA logs")
Elasticsearch    http://localhost:9200
Press Ctrl+C to stop.
EOF
wait
