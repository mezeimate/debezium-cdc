. (Join-Path $PSScriptRoot 'common.ps1')
Write-Host 'This test stops only Connect and changes demo rows 9002/9003. Use it in this shared lab only.'
$null = Wait-LabConnector
$null = Invoke-LabSql -Sql 'DELETE FROM PAYMENT WHERE id IN (9002,9003);'
Start-Sleep -Seconds 2
$seedOffsets = @(Get-LabPaymentOffsets)
$null = Invoke-LabSql -Sql "INSERT INTO PAYMENT (id, account_id, amount, currency, status, created_at, updated_at) VALUES (9002,1001,40.00,'HUF','NEW',NOW(3),NOW(3));"
# offset.flush.interval.ms=1000; allow a checkpoint before stopping Connect.
Start-Sleep -Seconds 3
$seedEvents = @(Read-LabPaymentEvents -Offsets $seedOffsets)
$seedCreate = @($seedEvents | Where-Object { $_.Id -eq 9002 -and $null -ne $_.Value -and $_.Value.op -eq 'c' })
if ($seedCreate.Count -eq 0) {
    throw 'Did not observe streaming INSERT for seed 9002. Finish the initial snapshot and rerun; inspect connector logs if it persists.'
}
$checkpoint = @(Get-LabPaymentOffsets)
Write-Host "Pre-outage Kafka end offsets: $($checkpoint | ConvertTo-Json -Compress)"
try {
    Invoke-LabCompose -Arguments @('stop', 'connect')
    Write-Host 'Connect stopped. MySQL and Kafka keep running. Writing UPDATE, INSERT, DELETE.'
    $null = Invoke-LabSql -Sql "UPDATE PAYMENT SET amount=42.42, status='RECOVERED', updated_at=NOW(3) WHERE id=9002; INSERT INTO PAYMENT (id,account_id,amount,currency,status,created_at,updated_at) VALUES (9003,1002,3.33,'HUF','NEW',NOW(3),NOW(3)); DELETE FROM PAYMENT WHERE id=9003;"
} finally {
    Invoke-LabCompose -Arguments @('start', 'connect')
}
$null = Wait-LabConnector
$events = @(Read-LabPaymentEvents -Offsets $checkpoint -TimeoutMs 15000)
$testEvents = @($events | Where-Object { $_.Id -in @(9002, 9003) })
$updated = @($testEvents | Where-Object { $_.Id -eq 9002 -and $null -ne $_.Value -and $_.Value.op -eq 'u' -and $null -ne $_.Value.before -and $null -ne $_.Value.after -and $_.Value.after.status -eq 'RECOVERED' })
$created = @($testEvents | Where-Object { $_.Id -eq 9003 -and $null -ne $_.Value -and $_.Value.op -eq 'c' })
$deleted = @($testEvents | Where-Object { $_.Id -eq 9003 -and $null -ne $_.Value -and $_.Value.op -eq 'd' -and $null -ne $_.Value.before -and $null -eq $_.Value.after })
$tombstones = @($testEvents | Where-Object { $_.Id -eq 9003 -and $null -eq $_.Value })
$snapshots = @($testEvents | Where-Object { $null -ne $_.Value -and $_.Value.op -eq 'r' })
foreach ($event in $testEvents) {
    $operation = 'tombstone'
    if ($null -ne $event.Value) { $operation = $event.Value.op }
    Write-Host "Observed AFTER checkpoint: id=$($event.Id), op=$operation"
    Write-Host $event.Raw
}
if ($updated.Count -eq 0 -or $created.Count -eq 0 -or $deleted.Count -eq 0 -or $tombstones.Count -eq 0 -or $snapshots.Count -gt 0) {
    throw "Recovery verification failed: u9002=$($updated.Count), c9003=$($created.Count), d9003=$($deleted.Count), tombstone9003=$($tombstones.Count), snapshots=$($snapshots.Count). Inspect Connect logs and the topic."
}
Write-Host 'PASS: outage UPDATE/INSERT/DELETE and tombstone arrived after the checkpoint, with no new snapshot for recovery rows.'
Write-Host 'PAYMENT 9002 remains with status RECOVERED. PAYMENT 9003 was deleted. Reruns reset only those demo rows.'
