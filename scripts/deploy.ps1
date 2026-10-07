# Nasadí stack do namespace eda-demo a počká na rollout.
# Použití: .\deploy.ps1 [-Stack base|monitoring|logging|full]   (výchozí base = Kafka, PostgreSQL, obě služby)
# ($Profile nelze – je to vestavěná proměnná PowerShellu.)
param(
    [ValidateSet('base', 'monitoring', 'logging', 'full')]
    [string] $Stack = 'base'
)
# Bez 'Stop': PS 5.1 by stderr nativních příkazů bral jako chybu; kontroluje se $LASTEXITCODE.

$root = Split-Path -Parent $PSScriptRoot
$ns = 'eda-demo'
$target = if ($Stack -eq 'base') { Join-Path $root 'k8s\base' } else { Join-Path (Join-Path $root 'k8s\overlays') $Stack }

function Invoke-Checked([scriptblock] $command, [string] $what) {
    & $command
    if ($LASTEXITCODE -ne 0) { throw "$what failed" }
}

Write-Host ">> Deploying profile '$Stack' ($target)"
Invoke-Checked { kubectl apply -k $target } 'kubectl apply'

# Infrastruktura první – služby na ni čekají v initContaineru.
foreach ($workload in 'statefulset/postgres', 'deployment/kafka') {
    Invoke-Checked { kubectl -n $ns rollout status $workload --timeout=300s } "rollout of $workload"
}
foreach ($workload in (kubectl -n $ns get deployment,daemonset -o name)) {
    Write-Host ">> Waiting for $workload"
    Invoke-Checked { kubectl -n $ns rollout status $workload --timeout=600s } "rollout of $workload"
}
kubectl -n $ns get job kibana-setup 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host '>> Waiting for Kibana data view job'
    Invoke-Checked { kubectl -n $ns wait --for=condition=complete job/kibana-setup --timeout=600s } 'kibana-setup job'
}

kubectl -n $ns get pods
Write-Host '>> Deployed. Run scripts\port-forward.ps1 to access the services.'
