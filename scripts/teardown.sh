#!/usr/bin/env bash
# Odstraní vše, co nasadil deploy.sh v libovolném profilu – včetně dat PostgreSQL (PVC)
# a cluster-scoped RBAC pro Fluent Bit.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Profil full obsahuje nadmnožinu všech zdrojů; chybějící se ignorují.
kubectl delete -k "${ROOT}/k8s/overlays/full" --ignore-not-found --wait=true
echo ">> Namespace eda-demo removed (incl. PostgreSQL data). Local Docker images are kept."
