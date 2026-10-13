. (Join-Path $PSScriptRoot 'common.ps1')
Wait-LabConnectApi
$definition = Get-Content -Raw (Join-Path $script:LabRoot 'connectors/bank-mysql.json') | ConvertFrom-Json
$existing = $false
try {
    $null = Invoke-RestMethod -Uri "$script:ConnectUrl/connectors/$script:ConnectorName/config" -TimeoutSec 15
    $existing = $true
} catch {
    if ($null -eq $_.Exception.Response -or [int]$_.Exception.Response.StatusCode -ne 404) { throw }
}
if ($existing) {
    $body = $definition.config | ConvertTo-Json -Depth 100
    $null = Invoke-RestMethod -Method Put -Uri "$script:ConnectUrl/connectors/$script:ConnectorName/config" -ContentType 'application/json' -Body $body -TimeoutSec 30
    Write-Host 'Existing connector configuration updated; its stored offsets are preserved.'
} else {
    $body = $definition | ConvertTo-Json -Depth 100
    $null = Invoke-RestMethod -Method Post -Uri "$script:ConnectUrl/connectors" -ContentType 'application/json' -Body $body -TimeoutSec 30
    Write-Host 'Connector registered. Initial snapshot begins automatically.'
}
$status = Wait-LabConnector
$status | ConvertTo-Json -Depth 20
