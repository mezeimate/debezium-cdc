. (Join-Path $PSScriptRoot 'common.ps1')
$status = Invoke-RestMethod -Uri "$script:ConnectUrl/connectors/$script:ConnectorName/status" -TimeoutSec 15
$status | ConvertTo-Json -Depth 20
$null = Wait-LabConnector -TimeoutSeconds 5
