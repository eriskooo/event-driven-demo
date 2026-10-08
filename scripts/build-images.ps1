# Sestaví Docker image všech služeb a nahraje je do minikube (bez registry).
# Bez 'Stop': PS 5.1 by stderr nativních příkazů (průběh docker buildu) bral jako chybu; kontroluje se $LASTEXITCODE.

$root = Split-Path -Parent $PSScriptRoot
$tag = if ($env:IMAGE_TAG) { $env:IMAGE_TAG } else { 'dev' }

# Docker Desktop Kubernetes sdílí image s lokálním Dockerem – načítání do minikube tam odpadá.
$context = kubectl config current-context 2>$null
$useMinikube = $context -ne 'docker-desktop'

foreach ($service in 'order-service', 'payment-service', 'order-process') {
    Write-Host ">> Building ${service}:${tag}"
    docker build -t "${service}:${tag}" (Join-Path $root $service)
    if ($LASTEXITCODE -ne 0) { throw "docker build of $service failed" }
    if (-not $useMinikube) { continue }

    Write-Host ">> Loading ${service}:${tag} into minikube"
    # --overwrite: jinak by minikube ponechal starý image se stejným tagem.
    minikube image load --overwrite=true "${service}:${tag}"
    if ($LASTEXITCODE -ne 0) { throw "minikube image load of $service failed" }
}

if ($useMinikube) {
    Write-Host '>> Done. Images in minikube:'
    minikube image ls | Select-String -Pattern '(order|payment)-(service|process)'
} else {
    Write-Host '>> Done. Context docker-desktop uses local Docker images directly.'
}
