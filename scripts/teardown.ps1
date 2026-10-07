# Odstraní vše, co nasadil deploy.ps1 v libovolném profilu – včetně dat PostgreSQL (PVC)
# a cluster-scoped RBAC pro Fluent Bit.
# Bez 'Stop': PS 5.1 by stderr nativních příkazů bral jako chybu; kontroluje se $LASTEXITCODE.

$root = Split-Path -Parent $PSScriptRoot

# Profil full obsahuje nadmnožinu všech zdrojů; chybějící se ignorují.
kubectl delete -k (Join-Path $root 'k8s\overlays\full') --ignore-not-found --wait=true
if ($LASTEXITCODE -ne 0) { throw 'kubectl delete failed' }
Write-Host '>> Namespace eda-demo removed (incl. PostgreSQL data). Local Docker images are kept.'
