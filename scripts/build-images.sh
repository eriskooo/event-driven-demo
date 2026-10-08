#!/usr/bin/env bash
# Sestaví Docker image všech služeb a nahraje je do minikube (bez registry).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TAG="${IMAGE_TAG:-dev}"

# Docker Desktop Kubernetes sdílí image s lokálním Dockerem – načítání do minikube tam odpadá.
CONTEXT="$(kubectl config current-context 2>/dev/null || true)"

for service in order-service payment-service order-process; do
  echo ">> Building ${service}:${TAG}"
  # Build z adresáře služby s kontextem "." – absolutní cesta /c/... by v Git Bash s MSYS_NO_PATHCONV=1
  # pro Windows docker.exe neexistovala.
  (cd "${ROOT}/${service}" && docker build -t "${service}:${TAG}" .)
  if [[ "${CONTEXT}" == "docker-desktop" ]]; then
    continue
  fi
  echo ">> Loading ${service}:${TAG} into minikube"
  # --overwrite: jinak by minikube ponechal starý image se stejným tagem.
  minikube image load --overwrite=true "${service}:${TAG}"
done

if [[ "${CONTEXT}" == "docker-desktop" ]]; then
  echo ">> Done. Context docker-desktop uses local Docker images directly."
else
  echo ">> Done. Images in minikube:"
  minikube image ls | grep -E '(order|payment)-(service|process)' || true
fi
