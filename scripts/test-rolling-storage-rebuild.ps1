param(
    [int]$TimeoutSeconds = 1800,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$token = $(if ($env:WAREHOUSE_API_TOKEN) { $env:WAREHOUSE_API_TOKEN } else { "warehouse-api-6d3f9c8a" })
$headers = @{ "X-API-Key" = $token }
$runId = [Guid]::NewGuid().ToString("N")
$owner = "rolling-rebuild-$runId"
$sku = "sku"
$body1 = @{ operation_id="rolling-before-$runId"; owner_id=$owner; sku=$sku; delta=101 } | ConvertTo-Json -Compress
$body2 = @{ operation_id="rolling-replica-$runId"; owner_id=$owner; sku=$sku; delta=37 } | ConvertTo-Json -Compress
$body3 = @{ operation_id="rolling-primary-$runId"; owner_id=$owner; sku=$sku; delta=19 } | ConvertTo-Json -Compress

function Get-VerifiedVolume([string]$Service, [string]$ExpectedComposeVolume) {
    $container = docker compose ps -q $Service
    if (-not $container) { throw "Container for '$Service' was not found" }
    $info = docker inspect $container | ConvertFrom-Json
    $mount = $info[0].Mounts | Where-Object { $_.Destination -eq "/data" } | Select-Object -First 1
    if (-not $mount -or $mount.Type -ne "volume" -or -not $mount.Name) {
        throw "Service '$Service' does not have a named /data volume"
    }
    $volume = docker volume inspect ([string]$mount.Name) | ConvertFrom-Json
    if ($volume[0].Labels.'com.docker.compose.project' -ne 'test' -or
        $volume[0].Labels.'com.docker.compose.volume' -ne $ExpectedComposeVolume) {
        throw "Refusing to remove unverified volume '$($mount.Name)'"
    }
    [string]$mount.Name
}

function Read-Metric([string]$Text, [string]$Name) {
    $match = [regex]::Match($Text, "(?m)^$([regex]::Escape($Name))\s+([0-9.eE+-]+)$")
    if (-not $match.Success) { throw "Metric '$Name' is missing" }
    [double]::Parse($match.Groups[1].Value, [Globalization.CultureInfo]::InvariantCulture)
}

function Wait-FullyCaughtUp([int]$Port, [string]$Name) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $live = Invoke-RestMethod -Headers $headers "http://127.0.0.1:$Port/live" -TimeoutSec 2
            $metrics = (Invoke-WebRequest -UseBasicParsing -Headers $headers "http://127.0.0.1:$Port/metrics" -TimeoutSec 5).Content
            $replication = Read-Metric $metrics 'warehouse_replication_lag'
            $history = Read-Metric $metrics 'warehouse_history_lag'
            if ($live.status -eq 'ok' -and $replication -eq 0 -and $history -eq 0) { return $metrics }
        } catch { }
        Start-Sleep -Seconds 2
    }
    throw "$Name did not fully rebuild within $TimeoutSeconds seconds"
}

function Wait-PublicWrite([string]$Body, [int]$ExpectedBalance) {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $result = Invoke-RestMethod -Method Post -Headers $headers -Uri 'http://127.0.0.1:8080/v1/operations' `
                -ContentType 'application/json' -Body $Body -TimeoutSec 10
            if ($result.balance -eq $ExpectedBalance) { return $result }
        } catch { }
        Start-Sleep -Milliseconds 200
    }
    throw "Public writer did not accept operation with expected balance $ExpectedBalance"
}

function Rebuild-Service([string]$Service, [string]$ComposeVolume, [int]$Port) {
    $volume = Get-VerifiedVolume $Service $ComposeVolume
    Write-Host "Verified $Service volume: $volume"
    docker compose stop -t 0 $Service | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot stop $Service" }
    docker compose rm -f $Service | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot remove $Service container" }
    docker volume rm $volume | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot remove verified volume $volume" }
    $timer = [Diagnostics.Stopwatch]::StartNew()
    docker compose up -d $Service | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot start $Service on an empty volume" }
    $metrics = Wait-FullyCaughtUp $Port $Service
    $timer.Stop()
    [pscustomobject]@{
        service = $Service
        volume = $volume
        rebuild_ms = $timer.ElapsedMilliseconds
        memory_bytes = [int64](Read-Metric $metrics 'warehouse_process_memory_bytes')
        lsm_write_buffer_bytes = [int64](Read-Metric $metrics 'warehouse_lsm_write_buffer_bytes')
    }
}

Push-Location $root
try {
    Write-Host "[1/8] Building and starting the current HA topology"
    if (-not $SkipBuild) {
        docker compose build warehouse
        if ($LASTEXITCODE -ne 0) { throw "Warehouse build failed" }
    }
    docker compose up -d warehouse warehouse-2 haproxy
    if ($LASTEXITCODE -ne 0) { throw "HA topology startup failed" }
    $null = Wait-FullyCaughtUp 8082 'primary'
    $null = Wait-FullyCaughtUp 8081 'replica'

    Write-Host "[2/8] Writing control operation before rolling rebuild"
    $first = Wait-PublicWrite $body1 101
    if ($first.status -notin @('applied','duplicate')) { throw "Initial control operation failed" }

    Write-Host "[3/8] Rebuilding replica local storage from JetStream"
    $replica = Rebuild-Service 'warehouse-2' 'warehouse-2-data' 8081
    $replicaRead = Invoke-RestMethod -Headers $headers "http://127.0.0.1:8081/v1/balances/$owner/$sku" -TimeoutSec 10
    if ($replicaRead.balance -ne 101) { throw "Replica rebuilt an incorrect balance" }

    Write-Host "[4/8] Moving writes to the rebuilt replica"
    $primaryVolume = Get-VerifiedVolume 'warehouse' 'warehouse-data'
    Write-Host "Verified primary volume before handoff: $primaryVolume"
    docker compose stop -t 0 warehouse | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot stop primary for writer handoff" }
    $second = Wait-PublicWrite $body2 138
    if ($second.status -notin @('applied','duplicate')) { throw "Replica writer handoff failed" }

    Write-Host "[5/8] Rebuilding primary local storage from JetStream"
    docker compose rm -f warehouse | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot remove primary container" }
    docker volume rm $primaryVolume | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot remove verified volume $primaryVolume" }
    $primaryTimer = [Diagnostics.Stopwatch]::StartNew()
    docker compose up -d warehouse | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot start primary on an empty volume" }
    $primaryMetrics = Wait-FullyCaughtUp 8082 'primary'
    $primaryTimer.Stop()
    $primary = [pscustomobject]@{
        service = 'warehouse'
        volume = $primaryVolume
        rebuild_ms = $primaryTimer.ElapsedMilliseconds
        memory_bytes = [int64](Read-Metric $primaryMetrics 'warehouse_process_memory_bytes')
        lsm_write_buffer_bytes = [int64](Read-Metric $primaryMetrics 'warehouse_lsm_write_buffer_bytes')
    }
    $primaryRead = Invoke-RestMethod -Headers $headers "http://127.0.0.1:8082/v1/balances/$owner/$sku" -TimeoutSec 10
    if ($primaryRead.balance -ne 138) { throw "Primary rebuilt an incorrect balance" }

    Write-Host "[6/8] Returning writer ownership to primary"
    docker compose stop -t 0 warehouse-2 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot stop replica for writer return" }
    $third = Wait-PublicWrite $body3 157
    if ($third.status -notin @('applied','duplicate')) { throw "Primary writer return failed" }
    docker compose up -d warehouse-2 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Cannot return replica" }
    $replicaMetrics = Wait-FullyCaughtUp 8081 'replica after return'

    Write-Host "[7/8] Verifying balances and at-least-once deduplication"
    foreach ($port in 8081,8082) {
        $read = Invoke-RestMethod -Headers $headers "http://127.0.0.1:$port/v1/balances/$owner/$sku" -TimeoutSec 10
        if ($read.balance -ne 157) { throw "Node on port $port has balance $($read.balance), expected 157" }
    }
    $duplicate = Wait-PublicWrite $body1 157
    if ($duplicate.status -ne 'duplicate') { throw "Deduplication was not rebuilt" }

    Write-Host "[8/8] Reporting rebuilt storage limits"
    docker compose up -d --no-deps haproxy | Out-Null
    [pscustomobject]@{
        result = 'PASSED'
        replica_rebuild_ms = $replica.rebuild_ms
        primary_rebuild_ms = $primary.rebuild_ms
        replica_memory_bytes = $replica.memory_bytes
        primary_memory_bytes = $primary.memory_bytes
        replica_lsm_write_buffer_bytes = [int64](Read-Metric $replicaMetrics 'warehouse_lsm_write_buffer_bytes')
        primary_lsm_write_buffer_bytes = $primary.lsm_write_buffer_bytes
        final_balance = 157
        duplicate_status = $duplicate.status
        availability = 'rolling; one application node kept online'
    } | Format-List
} catch {
    Write-Error "ROLLING STORAGE REBUILD FAILED: $($_.Exception.Message)"
    docker compose logs --no-color --tail 150 warehouse warehouse-2 nats-1 nats-2 nats-3
    docker compose up -d warehouse warehouse-2 haproxy | Out-Null
    exit 1
} finally {
    Pop-Location
}
