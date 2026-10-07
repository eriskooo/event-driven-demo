# Nasadí celý stack (Kafka, služby, observabilita) do namespace eda-demo a počká na rollout.
# Bez 'Stop': PS 5.1 by stderr nativních příkazů bral jako chybu; kontroluje se $LASTEXITCODE.

$root = Split-Path -Parent $PSScriptRoot
$ns = 'eda-demo'

function Invoke-Checked([scriptblock] $command, [string] $what) {
    & $command
    if ($LASTEXITCODE -ne 0) { throw "$what failed" }
}

Invoke-Checked { kubectl apply -k (Join-Path $root 'k8s') } 'kubectl apply'

# Pořadí odpovídá závislostem: Kafka a ES startují nejdéle, služby na Kafku čekají v initContaineru.
foreach ($deploy in 'kafka', 'elasticsearch', 'prometheus', 'grafana', 'order-service', 'payment-service', 'kibana') {
    Write-Host ">> Waiting for deployment/$deploy"
    Invoke-Checked { kubectl -n $ns rollout status "deployment/$deploy" --timeout=600s } "rollout of $deploy"
}
Invoke-Checked { kubectl -n $ns rollout status daemonset/fluent-bit --timeout=300s } 'rollout of fluent-bit'
Write-Host '>> Waiting for Kibana data view job'
Invoke-Checked { kubectl -n $ns wait --for=condition=complete job/kibana-setup --timeout=600s } 'kibana-setup job'

kubectl -n $ns get pods
Write-Host '>> Deployed. Run scripts\port-forward.ps1 to access the UIs.'
