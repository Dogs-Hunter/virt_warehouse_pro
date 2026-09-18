param([int]$TimeoutSeconds = 120)

$ErrorActionPreference = "Stop"
$apiHeaders = @{ "X-API-Key" = $(if ($env:WAREHOUSE_API_TOKEN) { $env:WAREHOUSE_API_TOKEN } else { "warehouse-api-6d3f9c8a" }) }
$PSDefaultParameterValues['Invoke-RestMethod:Headers'] = $apiHeaders; $PSDefaultParameterValues['Invoke-WebRequest:Headers'] = $apiHeaders
$projectDirectory = Split-Path -Parent $PSScriptRoot
$id = [Guid]::NewGuid().ToString("N")
$owner = "app-failover-$id"
$sku = "sku"

function Send-Delta([string]$operationId, [int]$delta) {
    $body = @{ operation_id=$operationId; owner_id=$owner; sku=$sku; delta=$delta } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/operations" -ContentType "application/json" -Body $body -TimeoutSec 10
}

Push-Location $projectDirectory
try {
    Write-Host "[1/6] Starting two application nodes and proxy"
    docker compose up -d --force-recreate warehouse warehouse-2 haproxy
    if ($LASTEXITCODE -ne 0) { throw "Cluster startup failed" }

    Write-Host "[2/6] Writing through primary route"
    $first = Send-Delta "before-$id" 17
    if ($first.balance -ne 17) { throw "Initial write failed" }

    Write-Host "[3/6] Abruptly stopping primary application"
    docker compose stop -t 0 warehouse | Out-Null
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $second = $null
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $second = Send-Delta "after-$id" 23
            if ($second.balance -eq 40) { break }
        } catch { Start-Sleep -Milliseconds 50 }
    }
    $timer.Stop()
    if (-not $second -or $second.balance -ne 40) { throw "Backup did not accept the write" }

    Write-Host "[4/6] Verifying public reads on backup"
    $backup = Invoke-RestMethod "http://localhost:8080/v1/balances/$owner/$sku"
    if ($backup.balance -ne 40) { throw "Backup balance mismatch" }

    Write-Host "[5/6] Returning primary"
    docker compose up -d warehouse | Out-Null
    $primary = $null
    while ([DateTime]::UtcNow -lt $deadline) {
        try {
            $primary = Invoke-RestMethod "http://localhost:8082/v1/balances/$owner/$sku" -TimeoutSec 1
            if ($primary.balance -eq 40) { break }
        } catch { Start-Sleep -Milliseconds 100 }
    }
    if (-not $primary -or $primary.balance -ne 40) { throw "Primary did not catch up" }

    Write-Host "[6/6] Verifying proxy returned to primary"
    $final = Invoke-RestMethod "http://localhost:8080/v1/balances/$owner/$sku"
    if ($final.balance -ne 40) { throw "Final proxy read mismatch" }

    [pscustomobject]@{ result="PASSED"; application_failover_ms=$timer.ElapsedMilliseconds; backup_balance=$backup.balance; recovered_primary_balance=$primary.balance } | Format-List
} catch {
    Write-Error "APPLICATION FAILOVER TEST FAILED: $($_.Exception.Message)"
    docker compose logs --no-color --tail 100 haproxy warehouse warehouse-2
    docker compose up -d warehouse | Out-Null
    exit 1
} finally { Pop-Location }
