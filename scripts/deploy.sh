#!/usr/bin/env bash
# Nasadí stack do namespace eda-demo a počká na rollout.
# Použití: deploy.sh [base|monitoring|logging|full]   (výchozí base = Kafka, PostgreSQL, obě služby)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NS=eda-demo
PROFILE="${1:-base}"

case "${PROFILE}" in
  base) TARGET="${ROOT}/k8s/base" ;;
  monitoring|logging|full) TARGET="${ROOT}/k8s/overlays/${PROFILE}" ;;
  *) echo "Unknown profile '${PROFILE}', use base|monitoring|logging|full" >&2; exit 1 ;;
esac

echo ">> Deploying profile '${PROFILE}' (${TARGET})"
kubectl apply -k "${TARGET}"

# Infrastruktura první – služby na ni čekají v initContaineru.
for workload in statefulset/postgres deployment/kafka; do
  kubectl -n "${NS}" rollout status "${workload}" --timeout=300s
done
for workload in $(kubectl -n "${NS}" get deployment,daemonset -o name); do
  echo ">> Waiting for ${workload}"
  kubectl -n "${NS}" rollout status "${workload}" --timeout=600s
done
if kubectl -n "${NS}" get job kibana-setup >/dev/null 2>&1; then
  echo ">> Waiting for Kibana data view job"
  kubectl -n "${NS}" wait --for=condition=complete job/kibana-setup --timeout=600s
fi

kubectl -n "${NS}" get pods
echo ">> Deployed. Run scripts/port-forward.sh to access the services."
