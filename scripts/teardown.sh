#!/usr/bin/env bash
# Odstraní vše, co nasadil deploy.sh (včetně cluster-scoped RBAC pro Fluent Bit).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

kubectl delete -k "${ROOT}/k8s" --ignore-not-found --wait=true
echo ">> Namespace eda-demo removed. Images stay in minikube; 'minikube delete' removes the whole cluster."
