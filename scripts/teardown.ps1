# Odstraní vše, co nasadil deploy.ps1 (včetně cluster-scoped RBAC pro Fluent Bit).
# Bez 'Stop': PS 5.1 by stderr nativních příkazů bral jako chybu; kontroluje se $LASTEXITCODE.

$root = Split-Path -Parent $PSScriptRoot

kubectl delete -k (Join-Path $root 'k8s') --ignore-not-found --wait=true
if ($LASTEXITCODE -ne 0) { throw 'kubectl delete failed' }
Write-Host ">> Namespace eda-demo removed. Images stay in minikube; 'minikube delete' removes the whole cluster."
