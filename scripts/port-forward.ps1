# Zpřístupní na localhostu služby, které jsou v namespace eda-demo nasazené. Ctrl+C ukončí vše.
$ns = 'eda-demo'
$forwards = @(
    @('order-service', '8080:8080', 'http://localhost:8080/orders'),
    @('payment-service', '8081:8080', 'http://localhost:8081/actuator/health'),
    @('order-process', '8082:8080', 'http://localhost:8082/actuator/health'),
    @('camunda', '8088:8080', 'http://localhost:8088/operate (demo/demo)'),
    @('postgres', '5432:5432', 'jdbc:postgresql://localhost:5432/eda'),
    @('grafana', '3000:3000', 'http://localhost:3000 (admin/admin)'),
    @('prometheus', '9090:9090', 'http://localhost:9090'),
    @('kibana', '5601:5601', "http://localhost:5601 (data view 'EDA logs')"),
    @('elasticsearch', '9200:9200', 'http://localhost:9200')
)

$processes = @()
foreach ($f in $forwards) {
    # Volitelné komponenty (monitoring, logging) nemusí být nasazené.
    kubectl -n $ns get "svc/$($f[0])" 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { continue }
    $processes += Start-Process -FilePath kubectl -ArgumentList @('-n', $ns, 'port-forward', "svc/$($f[0])", $f[1]) `
        -NoNewWindow -PassThru -RedirectStandardOutput ([System.IO.Path]::GetTempFileName())
    Write-Host ('{0,-16} {1}' -f $f[0], $f[2])
}
Write-Host 'Press Ctrl+C to stop.'

try {
    Wait-Process -Id $processes.Id
} finally {
    $processes | Where-Object { -not $_.HasExited } | Stop-Process -Force
}
