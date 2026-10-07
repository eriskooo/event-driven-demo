#!/usr/bin/env bash
# Nasadí celý stack (Kafka, služby, observabilita) do namespace eda-demo a počká na rollout.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS=eda-demo

kubectl apply -k "${ROOT}/k8s"

# Pořadí odpovídá závislostem: Kafka a ES startují nejdéle, služby na Kafku čekají v initContaineru.
for deploy in kafka elasticsearch prometheus grafana order-service payment-service kibana; do
  echo ">> Waiting for deployment/${deploy}"
  kubectl -n "${NS}" rollout status "deployment/${deploy}" --timeout=600s
done
kubectl -n "${NS}" rollout status daemonset/fluent-bit --timeout=300s
echo ">> Waiting for Kibana data view job"
kubectl -n "${NS}" wait --for=condition=complete job/kibana-setup --timeout=600s

kubectl -n "${NS}" get pods
echo ">> Deployed. Run scripts/port-forward.sh to access the UIs."
