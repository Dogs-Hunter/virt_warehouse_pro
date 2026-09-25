param(
    [int]$TimeoutSeconds = 1800,
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$apiHeaders = @{ "X-API-Key" = $(if ($env:WAREHOUSE_API_TOKEN) { $env:WAREHOUSE_API_TOKEN } else { "warehouse-api-6d3f9c8a" }) }
$PSDefaultParameterValues['Invoke-RestMethod:Headers'] = $apiHeaders; $PSDefaultParameterValues['Invoke-WebRequest:Headers'] = $apiHeaders
$projectDirectory = Split-Path -Parent $PSScriptRoot
$runId = [Guid]::NewGuid().ToString("N")
$operationId = "volume-loss-$runId"
$ownerId = "volume-owner-$runId"
$sku = "volume-sku"
$expectedBalance = 41

function Wait-Warehouse([string]$Uri = 'http://localhost:8080/live') {
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    while ([DateTime]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 200
        try {
            $health = Invoke-RestMethod $Uri -TimeoutSec 2
            if ($health.status -eq "ok") { return }
        } catch {
            # Expected while full replicated-log replay is running.
        }
    }
    throw "Warehouse did not recover within $TimeoutSeconds seconds"
}

Push-Location $projectDirectory
try {
    Write-Host "[0/7] Building and starting current warehouse image"
    if (-not $SkipBuild) {
        docker compose build warehouse
        if ($LASTEXITCODE -ne 0) { throw "Warehouse image build failed" }
    }
    docker compose up -d --force-recreate warehouse warehouse-2
    if ($LASTEXITCODE -ne 0) { throw "Cannot recreate warehouse nodes" }
    docker compose up -d --no-deps haproxy
    if ($LASTEXITCODE -ne 0) { throw "Cannot start HAProxy" }
    Wait-Warehouse 'http://localhost:8080/live'

    Write-Host "[1/7] Writing control operation to quorum log and local WAL"
    $body = @{
        operation_id = $operationId
        owner_id = $ownerId
        sku = $sku
        delta = $expectedBalance
    } | ConvertTo-Json -Compress
    $created = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/operations" `
        -ContentType "application/json" -Body $body -TimeoutSec 10
    if ($created.status -ne "applied" -or $created.balance -ne $expectedBalance) {
        throw "Control operation was not applied"
    }

    Write-Host "[2/7] Resolving exact application volume"
    $container = docker compose ps -q warehouse
    if (-not $container) { throw "Warehouse container not found" }
    $containerInfo = docker inspect $container | ConvertFrom-Json
    $mount = $containerInfo[0].Mounts | Where-Object { $_.Destination -eq "/data" } | Select-Object -First 1
    if (-not $mount -or $mount.Type -ne "volume" -or -not $mount.Name) {
        throw "Expected named volume mounted at /data"
    }
    $volumeName = [string]$mount.Name
    $volumeInfo = docker volume inspect $volumeName | ConvertFrom-Json
    $composeVolume = $volumeInfo[0].Labels.'com.docker.compose.volume'
    if ($composeVolume -ne "warehouse-data") {
        throw "Refusing to remove unverified volume '$volumeName'"
    }
    Write-Host "Verified target volume: $volumeName"

    Write-Host "[3/7] Removing application container"
    docker compose stop -t 0 warehouse
    if ($LASTEXITCODE -ne 0) { throw "Cannot stop warehouse" }
    docker compose rm -f warehouse
    if ($LASTEXITCODE -ne 0) { throw "Cannot remove warehouse container" }

    Write-Host "[4/7] Deleting only verified local application volume"
    docker volume rm $volumeName
    if ($LASTEXITCODE -ne 0) { throw "Cannot remove $volumeName" }

    Write-Host "[5/7] Starting with an empty application disk"
    $timer = [Diagnostics.Stopwatch]::StartNew()
    docker compose up -d warehouse
    if ($LASTEXITCODE -ne 0) { throw "Cannot start warehouse" }
    docker compose up -d --no-deps haproxy
    if ($LASTEXITCODE -ne 0) { throw "Cannot start HAProxy" }
    Wait-Warehouse 'http://localhost:8082/live'
    $timer.Stop()

    Write-Host "[6/7] Checking state rebuilt from quorum log"
    $balance = Invoke-RestMethod "http://localhost:8082/v1/balances/$ownerId/$sku" -TimeoutSec 5
    if ($balance.balance -ne $expectedBalance) {
        throw "Recovered balance is $($balance.balance), expected $expectedBalance"
    }

    Write-Host "[7/7] Checking deduplication after full rebuild"
    docker compose stop -t 0 warehouse-2 | Out-Null
    $readyDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $ready = $null
    while ([DateTime]::UtcNow -lt $readyDeadline) {
        try {
            $ready = Invoke-WebRequest 'http://localhost:8082/ready' -TimeoutSec 2
            if ($ready.StatusCode -eq 200) { break }
        } catch {}
        Start-Sleep -Milliseconds 200
    }
    if (-not $ready -or $ready.StatusCode -ne 200) { throw 'Rebuilt node did not acquire the writer lease' }
    $duplicate = Invoke-RestMethod -Method Post -Uri "http://localhost:8082/v1/operations" `
        -ContentType "application/json" -Body $body -TimeoutSec 10
    if ($duplicate.status -ne "duplicate" -or $duplicate.balance -ne $expectedBalance) {
        throw "Deduplication failed after full rebuild"
    }

    [pscustomobject]@{
        result = "PASSED"
        deleted_volume = $volumeName
        full_rebuild_ms = $timer.ElapsedMilliseconds
        recovered_balance = $balance.balance
        duplicate_status = $duplicate.status
    } | Format-List
} catch {
    Write-Error "LOCAL VOLUME LOSS TEST FAILED: $($_.Exception.Message)"
    docker compose logs --no-color --tail 100 warehouse nats-1 nats-2 nats-3
    exit 1
} finally {
    docker compose up -d warehouse-2 | Out-Null
    Pop-Location
}
