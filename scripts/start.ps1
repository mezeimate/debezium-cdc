. (Join-Path $PSScriptRoot 'common.ps1')
Invoke-LabCompose -Arguments @('up', '-d', '--wait', '--wait-timeout', '240')
Wait-LabConnectApi
Write-Host 'Services ready. Adminer: http://localhost:8080 | Kafka UI: http://localhost:8081'
Write-Host 'Next: run scripts/register-connector.ps1, then scripts/status.ps1.'
