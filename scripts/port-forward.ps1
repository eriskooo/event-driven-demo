# Zpřístupní UI a order-service na localhostu. Ctrl+C (nebo zavření okna) ukončí všechny port-forwardy.
$ns = 'eda-demo'
$forwards = @(
    @('svc/order-service', '8080:8080'),
    @('svc/payment-service', '8081:8080'),
    @('svc/grafana', '3000:3000'),
    @('svc/prometheus', '9090:9090'),
    @('svc/kibana', '5601:5601'),
    @('svc/elasticsearch', '9200:9200')
)

$processes = foreach ($f in $forwards) {
    Start-Process -FilePath kubectl -ArgumentList @('-n', $ns, 'port-forward', $f[0], $f[1]) -NoNewWindow -PassThru `
        -RedirectStandardOutput ([System.IO.Path]::GetTempFileName())
}

Write-Host @'
order-service    http://localhost:8080/orders
payment-service  http://localhost:8081/actuator/health
Grafana          http://localhost:3000   (admin / admin)
Prometheus       http://localhost:9090
Kibana           http://localhost:5601   (Discover -> data view "EDA logs")
Elasticsearch    http://localhost:9200
Press Ctrl+C to stop.
'@

try {
    Wait-Process -Id $processes.Id
} finally {
    $processes | Where-Object { -not $_.HasExited } | Stop-Process -Force
}
