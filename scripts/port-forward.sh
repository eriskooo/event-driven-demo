#!/usr/bin/env bash
# Zpřístupní na localhostu služby, které jsou v namespace eda-demo nasazené. Ctrl+C ukončí vše.
set -euo pipefail

NS=eda-demo
# služba  lokální:vzdálený port  popis
FORWARDS=(
  "order-service   8080:8080  http://localhost:8080/orders"
  "payment-service 8081:8080  http://localhost:8081/actuator/health"
  "order-process   8082:8080  http://localhost:8082/actuator/health"
  "camunda         8088:8080  http://localhost:8088/operate (demo/demo)"
  "postgres        5432:5432  jdbc:postgresql://localhost:5432/eda"
  "grafana         3000:3000  http://localhost:3000 (admin/admin)"
  "prometheus      9090:9090  http://localhost:9090"
  "kibana          5601:5601  http://localhost:5601 (data view 'EDA logs')"
  "elasticsearch   9200:9200  http://localhost:9200"
)

pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

for forward in "${FORWARDS[@]}"; do
  read -r service ports url <<<"${forward}"
  # Volitelné komponenty (monitoring, logging) nemusí být nasazené.
  kubectl -n "${NS}" get "svc/${service}" >/dev/null 2>&1 || continue
  kubectl -n "${NS}" port-forward "svc/${service}" "${ports}" >/dev/null &
  pids+=($!)
  printf '%-16s %s\n' "${service}" "${url}"
done

echo "Press Ctrl+C to stop."
wait
