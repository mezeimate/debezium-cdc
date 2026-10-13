Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:LabRoot = Split-Path -Parent $PSScriptRoot
$script:ConnectUrl = 'http://localhost:8083'
$script:ConnectorName = 'bank-mysql'
$script:PaymentTopic = 'bank-cdc.bank.PAYMENT'

function Invoke-LabCompose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [int[]]$AllowedExitCodes = @(0),
        [string]$StderrFile
    )
    if ($StderrFile) {
        # Windows PowerShell 5.1 wraps native stderr in ErrorRecords. Capture it
        # with Continue, then decide using both the process exit code and log.
        $previousPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = 'Continue'
            & docker compose --project-directory $script:LabRoot -f (Join-Path $script:LabRoot 'docker-compose.yml') @Arguments 2> $StderrFile
            $code = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousPreference
        }
    } else {
        & docker compose --project-directory $script:LabRoot -f (Join-Path $script:LabRoot 'docker-compose.yml') @Arguments
        $code = $LASTEXITCODE
    }
    if ($AllowedExitCodes -notcontains $code) {
        $details = ''
        if ($StderrFile -and (Test-Path $StderrFile)) { $details = Get-Content -Raw $StderrFile }
        throw "docker compose failed (exit $code): $($Arguments -join ' ')`n$details"
    }
}

function Invoke-LabSql {
    param([Parameter(Mandatory = $true)][string]$Sql)
    Invoke-LabCompose -Arguments @('exec', '-T', '-e', 'MYSQL_PWD=root_lab_password', 'mysql', 'mysql', '-u', 'root', 'bank', '--execute', $Sql)
}

function Wait-LabConnectApi {
    param([int]$TimeoutSeconds = 120)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $null = Invoke-RestMethod -Uri "$script:ConnectUrl/connectors" -TimeoutSec 10
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    } while ((Get-Date) -lt $deadline)
    throw 'Kafka Connect REST API did not become ready. Check: docker compose logs connect'
}

function Wait-LabConnector {
    param([int]$TimeoutSeconds = 120)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $status = $null
        try {
            $status = Invoke-RestMethod -Uri "$script:ConnectUrl/connectors/$script:ConnectorName/status" -TimeoutSec 10
        } catch {
            Start-Sleep -Seconds 2
            continue
        }
        if ($status.connector.state -eq 'FAILED') {
            throw "Connector FAILED: $($status.connector | ConvertTo-Json -Depth 20)"
        }
        $tasks = @($status.tasks)
        foreach ($task in $tasks) {
            if ($task.state -eq 'FAILED') {
                throw "Connector task FAILED: $($task | ConvertTo-Json -Depth 20)"
            }
        }
        if ($status.connector.state -eq 'RUNNING' -and $tasks.Count -gt 0 -and @($tasks | Where-Object { $_.state -ne 'RUNNING' }).Count -eq 0) {
            return $status
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)
    throw 'Connector and tasks did not become RUNNING. Run scripts/status.ps1 and inspect connect logs.'
}

function Get-LabPaymentOffsets {
    $lines = @(Invoke-LabCompose -Arguments @('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-get-offsets.sh', '--bootstrap-server', 'kafka:9092', '--topic', $script:PaymentTopic, '--time', '-1'))
    $offsets = @()
    foreach ($line in $lines) {
        if ([string]$line -match '^bank-cdc\.bank\.PAYMENT:(\d+):(\d+)$') {
            $offsets += [pscustomobject]@{ Partition = [int]$Matches[1]; Offset = [long]$Matches[2] }
        }
    }
    if ($offsets.Count -eq 0) {
        throw 'The PAYMENT topic has no partitions yet. Register the connector and wait for the initial snapshot.'
    }
    return $offsets
}

function Test-LabConsumerIdleTimeout {
    param([string]$StderrText)
    if ($StderrText -notmatch 'org\.apache\.kafka\.common\.errors\.TimeoutException' -or
        $StderrText -notmatch 'Processed a total of [0-9]+ messages') { return $false }
    # Remove only Windows PowerShell's native-stderr wrapper metadata. The
    # original Kafka diagnostics remain and must contain no other error class.
    $diagnostics = [regex]::Replace($StderrText, '(?m)^\s*\+\s*(?:CategoryInfo|FullyQualifiedErrorId)\s*:.*$', '')
    $classPattern = '(?:[A-Za-z_$][A-Za-z0-9_$]*\.)+(?:[A-Za-z_$][A-Za-z0-9_$]*(?:Exception|Error)|Exception|Error)\b|\b[A-Z][A-Za-z0-9_$]+(?:Exception|Error)\b'
    foreach ($match in [regex]::Matches($diagnostics, $classPattern)) {
        if ($match.Value -ne 'org.apache.kafka.common.errors.TimeoutException') { return $false }
    }
    foreach ($line in ($diagnostics -split "`r?`n")) {
        if ($line -match '(?i)\bERROR\b' -and $line -notmatch 'ERROR Error processing message, terminating consumer process:') { return $false }
    }
    return $true
}

function Read-LabPaymentEvents {
    param([Parameter(Mandatory = $true)][object[]]$Offsets, [int]$TimeoutMs = 10000)
    foreach ($offset in $Offsets) {
        $stderrFile = [System.IO.Path]::GetTempFileName()
        try {
            $lines = @(Invoke-LabCompose -Arguments @('exec', '-T', 'kafka', '/opt/kafka/bin/kafka-console-consumer.sh', '--bootstrap-server', 'kafka:9092', '--topic', $script:PaymentTopic, '--partition', [string]$offset.Partition, '--offset', [string]$offset.Offset, '--timeout-ms', [string]$TimeoutMs, '--consumer-property', 'enable.auto.commit=false', '--property', 'print.key=true', '--property', "key.separator=`t", '--property', 'null.literal=null') -AllowedExitCodes @(0, 1) -StderrFile $stderrFile)
            $consumerExitCode = $LASTEXITCODE
            $stderrText = Get-Content -Raw $stderrFile
            if ($consumerExitCode -eq 1 -and -not (Test-LabConsumerIdleTimeout -StderrText $stderrText)) {
                throw "Kafka console consumer failed with exit 1 (not its expected idle timeout): $stderrText"
            }
        } finally {
            Remove-Item -Force $stderrFile
        }
        foreach ($line in $lines) {
            $parts = ([string]$line).Split(@([char]9), 2)
            if ($parts.Count -ne 2) { continue }
            $key = $parts[0] | ConvertFrom-Json
            if ($null -ne $key.PSObject.Properties['payload']) { $key = $key.payload }
            $value = $null
            if ($parts[1] -ne 'null') {
                $value = $parts[1] | ConvertFrom-Json
                if ($null -ne $value.PSObject.Properties['payload']) { $value = $value.payload }
            }
            [pscustomobject]@{ Id = [long]$key.id; Value = $value; Raw = [string]$line }
        }
    }
}
