# Pošle N objednávek do order-service (výchozí http://localhost:8080 přes port-forward).
# Použití: .\send-orders.ps1 [-Count 10] [-Amount 666]   – částka 666 vyvolá technickou chybu → retry → DLT.
param(
    [int] $Count = 10,
    [decimal] $Amount = 0,
    [string] $BaseUrl = $(if ($env:ORDER_SERVICE_URL) { $env:ORDER_SERVICE_URL } else { 'http://localhost:8080' })
)
$ErrorActionPreference = 'Stop'

for ($i = 1; $i -le $Count; $i++) {
    $orderAmount = if ($Amount -gt 0) { $Amount } else { [math]::Round((Get-Random -Minimum 1.0 -Maximum 500.0), 2) }
    $correlationId = "demo-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())-$((Get-Random -Maximum 65536).ToString('x4'))-$i"
    $body = @{ customerId = "customer-$(Get-Random -Minimum 1 -Maximum 6)"; amount = $orderAmount; currency = 'CZK' } |
        ConvertTo-Json -Compress
    $order = Invoke-RestMethod -Method Post -Uri "$BaseUrl/orders" -ContentType 'application/json' `
        -Headers @{ 'X-Correlation-Id' = $correlationId } -Body $body
    Write-Host ("order {0}  amount {1}  correlationId {2}" -f $order.id, $orderAmount, $correlationId)
}

Write-Host "Check status: Invoke-RestMethod $BaseUrl/orders/<id>"
