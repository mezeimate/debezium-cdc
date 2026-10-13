param([ValidateRange(0, 60)][int]$DelaySeconds = 3)
. (Join-Path $PSScriptRoot 'common.ps1')
$null = Wait-LabConnector
Write-Host 'Watch bank-cdc.bank.PAYMENT in Kafka UI (newest records). Demo key: id=9001.'
$null = Invoke-LabSql -Sql 'DELETE FROM PAYMENT WHERE id=9001;'
Write-Host 'INSERT PAYMENT 9001: expect op=c.'
$null = Invoke-LabSql -Sql "INSERT INTO PAYMENT (id, account_id, amount, currency, status, created_at, updated_at) VALUES (9001, 1001, 12.34, 'HUF', 'NEW', NOW(3), NOW(3));"
Start-Sleep -Seconds $DelaySeconds
Write-Host 'UPDATE PAYMENT 9001: expect op=u, before and after.'
$null = Invoke-LabSql -Sql "UPDATE PAYMENT SET amount=56.78, status='SETTLED', updated_at=NOW(3) WHERE id=9001;"
Start-Sleep -Seconds $DelaySeconds
Write-Host 'DELETE PAYMENT 9001: expect op=d, after=null, followed by a Kafka tombstone.'
$null = Invoke-LabSql -Sql 'DELETE FROM PAYMENT WHERE id=9001;'
Start-Sleep -Seconds $DelaySeconds
Write-Host 'Demo finished. A rerun can also produce a cleanup DELETE if a previous run was interrupted.'
