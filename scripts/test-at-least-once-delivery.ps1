param([switch]$SkipBuild, [int]$TimeoutSeconds = 120)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$token = if ($env:WAREHOUSE_API_TOKEN) { $env:WAREHOUSE_API_TOKEN } else { 'warehouse-api-6d3f9c8a' }
$headers = @{ 'X-API-Key' = $token }
$id = [Guid]::NewGuid().ToString('N')
$operationId = "at-least-once-$id"
$owner = "at-least-once-owner-$id"
$sku = 'sku'
$body = @{ operation_id=$operationId; owner_id=$owner; sku=$sku; delta=73; event_version=1 } | ConvertTo-Json -Compress

function Wait-Balance([int]$expected) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $value = Invoke-RestMethod -Uri "http://localhost:8082/v1/balances/$owner/$sku" -Headers $headers -TimeoutSec 2
            if ($value.balance -eq $expected) { return $value }
        } catch {}
        Start-Sleep -Milliseconds 200
    }
    throw "Balance did not reach $expected"
}

Push-Location $root
try {
    Write-Host '[1/6] Building application with deterministic producer message IDs'
    if (-not $SkipBuild) {
        docker compose build warehouse | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    }

    Write-Host '[2/6] Starting primary with a crash point after quorum commit'
    docker compose stop haproxy warehouse-2 | Out-Null
    $env:WAREHOUSE_FAIL_AFTER_REPLICATE_ID = $operationId
    docker compose up -d --force-recreate warehouse | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Primary startup failed' }
    Wait-Balance 0 | Out-Null

    Write-Host '[3/6] Sending operation and forcing loss of the HTTP acknowledgement'
    $ackLost = $false
    try {
        $response = Invoke-RestMethod -Method Post -Uri 'http://localhost:8082/v1/operations' -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec 15
        if ($response) { throw 'Request unexpectedly returned a successful acknowledgement' }
    } catch {
        $ackLost = $true
    }
    if (-not $ackLost) { throw 'HTTP acknowledgement was not interrupted' }

    Write-Host '[4/6] Verifying automatic delivery from the quorum log after restart'
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $recovered = Wait-Balance 73
    $timer.Stop()
    $container = docker compose ps -q warehouse
    $restartCount = [int](docker inspect --format '{{.RestartCount}}' $container)
    if ($restartCount -lt 1) { throw 'Crash/restart was not observed' }

    Write-Host '[5/6] Disabling the failpoint and retrying the ambiguous request'
    Remove-Item Env:WAREHOUSE_FAIL_AFTER_REPLICATE_ID -ErrorAction SilentlyContinue
    docker compose up -d --force-recreate warehouse | Out-Null
    Wait-Balance 73 | Out-Null
    $retry = Invoke-RestMethod -Method Post -Uri 'http://localhost:8082/v1/operations' -Headers $headers -ContentType 'application/json' -Body $body -TimeoutSec 15
    if ($retry.status -ne 'duplicate' -or $retry.balance -ne 73) { throw 'Retry was not deduplicated' }

    Write-Host '[6/6] Reporting at-least-once guarantee'
    [pscustomobject]@{
        result='PASSED'
        guarantee='at-least-once'
        acknowledgement='lost after quorum commit'
        restart_count=$restartCount
        redelivery_ms=$timer.ElapsedMilliseconds
        recovered_balance=$recovered.balance
        retry_status=$retry.status
        duplicate_effect='none'
    } | Format-List
} catch {
    Write-Error "AT-LEAST-ONCE TEST FAILED: $($_.Exception.Message)"
    docker compose logs --no-color --tail 120 warehouse nats-1 nats-2 nats-3 | Out-Host
    exit 1
} finally {
    Remove-Item Env:WAREHOUSE_FAIL_AFTER_REPLICATE_ID -ErrorAction SilentlyContinue
    docker compose up -d --force-recreate warehouse | Out-Null
    docker compose up -d warehouse-2 | Out-Null
    docker compose up -d --no-deps haproxy | Out-Null
    Pop-Location
}
