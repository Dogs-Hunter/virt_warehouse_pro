param(
    [switch]$SkipBuild,
    [int]$Operations = 5000000,
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$apiHeaders = @{ "X-API-Key" = $(if ($env:WAREHOUSE_API_TOKEN) { $env:WAREHOUSE_API_TOKEN } else { "warehouse-api-6d3f9c8a" }) }
$PSDefaultParameterValues['Invoke-RestMethod:Headers'] = $apiHeaders; $PSDefaultParameterValues['Invoke-WebRequest:Headers'] = $apiHeaders
$projectDirectory = Split-Path -Parent $PSScriptRoot
$runId = "quorum-$([Guid]::NewGuid().ToString('N'))"
$outputFile = [IO.Path]::GetTempFileName()
$errorFile = [IO.Path]::GetTempFileName()
$stoppedService = $null

function Get-JetStreamStatus([string]$PreferredService = '') {
    $services = @("nats-1", "nats-2", "nats-3")
    if ($PreferredService) { $services = @($PreferredService) + @($services | Where-Object { $_ -ne $PreferredService }) }
    foreach ($service in $services) {
        $container = docker compose ps -q $service
        if ($LASTEXITCODE -ne 0 -or -not $container) {
            continue
        }
        try {
            $json = docker exec $container wget -q -O - "http://127.0.0.1:8222/jsz?streams=true"
            if ($LASTEXITCODE -eq 0) {
                return $json | ConvertFrom-Json
            }
        } catch {
            # Try the next surviving server.
        }
    }
    throw "No NATS monitoring endpoint is reachable"
}

function Get-StreamDetail($status) {
    foreach ($account in $status.account_details) {
        foreach ($stream in $account.stream_detail) {
            if ($stream.name -eq "WAREHOUSE_OPERATIONS") {
                return $stream
            }
        }
    }
    throw "WAREHOUSE_OPERATIONS stream not found"
}

Push-Location $projectDirectory
try {
    Write-Host "[1/6] Reading quorum state"
    $beforeStatus = Get-JetStreamStatus
    $beforeStream = Get-StreamDetail $beforeStatus
    $leader = $beforeStream.cluster.leader
    if ($leader -notin @("nats-1", "nats-2", "nats-3")) {
        throw "Unexpected stream leader: $leader"
    }
    $stoppedService = $leader
    $beforeMessages = [int64]$beforeStream.state.messages
    $beforeBalance = (Invoke-RestMethod "http://localhost:8080/v1/balances/owner-0/sku-0").balance
    Write-Host "Current leader: $leader; messages before: $beforeMessages"

    Write-Host "[2/6] Starting $Operations operations"
    $arguments = @(
        "compose", "--profile", "benchmark", "run", "--rm", "--no-deps",
        "-e", "LOAD_RUN_ID=$runId",
        "-e", "LOAD_OPERATIONS=$Operations",
        "-e", "WAREHOUSE_URL=http://haproxy:8080",
        "-e", "LOAD_RETRIES=100",
        "-e", "LOAD_RETRY_MS=50",
        "loadgen"
    )
    $load = Start-Process -FilePath "docker" `
        -ArgumentList $arguments `
        -WorkingDirectory $projectDirectory `
        -RedirectStandardOutput $outputFile `
        -RedirectStandardError $errorFile `
        -WindowStyle Hidden `
        -PassThru

    Start-Sleep -Milliseconds 500
    if ($load.HasExited) {
        throw "Load generator finished before fault injection; increase Operations"
    }

    Write-Host "[3/6] Abruptly stopping stream leader $leader"
    $failoverTimer = [Diagnostics.Stopwatch]::StartNew()
    docker compose stop -t 0 $leader
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to stop $leader"
    }

    $deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $newLeader = $null
    while ([DateTime]::UtcNow -lt $deadline) {
        Start-Sleep -Milliseconds 100
        try {
            $status = Get-JetStreamStatus
            $candidate = (Get-StreamDetail $status).cluster.leader
            if ($candidate -and $candidate -ne $leader) {
                $newLeader = $candidate
                break
            }
        } catch {
            # Expected during election.
        }
    }
    $failoverTimer.Stop()
    if (-not $newLeader) {
        throw "No new stream leader elected within $TimeoutSeconds seconds"
    }
    Write-Host "New leader: $newLeader after $($failoverTimer.ElapsedMilliseconds) ms"

    Write-Host "[4/6] Waiting for load to finish"
    if (-not $load.WaitForExit($TimeoutSeconds * 1000)) {
        $load.Kill()
        throw "Load generator did not finish within $TimeoutSeconds seconds"
    }
    $stdout = Get-Content -LiteralPath $outputFile -Raw
    $stderr = Get-Content -LiteralPath $errorFile -Raw
    Write-Host $stdout
    if ($load.ExitCode -ne 0) {
        throw "Load generator failed with exit $($load.ExitCode): $stderr"
    }
    $appliedMatch = [regex]::Match($stdout, '(?m)^applied=(\d+)$')
    $duplicateMatch = [regex]::Match($stdout, '(?m)^duplicates=(\d+)$')
    if ($stdout -notmatch "operations=$Operations" -or -not $appliedMatch.Success -or -not $duplicateMatch.Success -or
        ([int64]$appliedMatch.Groups[1].Value + [int64]$duplicateMatch.Groups[1].Value) -ne $Operations) {
        throw "Unexpected load result"
    }

    Write-Host "[5/6] Checking exact materialized balance"
    $afterBalance = (Invoke-RestMethod "http://localhost:8080/v1/balances/owner-0/sku-0").balance
    $expectedDelta = [int64]($Operations / 10000)
    if (($afterBalance - $beforeBalance) -ne $expectedDelta) {
        throw "Balance delta is $($afterBalance - $beforeBalance), expected $expectedDelta"
    }

    Write-Host "[6/6] Returning failed node and checking stream size"
    docker compose up -d $leader
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to restart $leader"
    }
    $stoppedService = $null
    $catchupDeadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
    $afterStream = $null
    $caughtUp = $false
    while ([DateTime]::UtcNow -lt $catchupDeadline) {
        Start-Sleep -Milliseconds 200
        try {
            $candidateStatus = Get-JetStreamStatus
            $candidateStream = Get-StreamDetail $candidateStatus
            # Followers may briefly expose a stale replica list after restart.
            # The stream leader is authoritative for catch-up completion.
            $afterStream = Get-StreamDetail (Get-JetStreamStatus $candidateStream.cluster.leader)
            $returnedReplica = $afterStream.cluster.replicas |
                Where-Object { $_.name -eq $leader } |
                Select-Object -First 1
            if ($returnedReplica -and $returnedReplica.current -and
                ([int64]$returnedReplica.lag -eq 0)) {
                $caughtUp = $true
                break
            }
        } catch {
            # Expected while the returned replica rejoins and catches up.
        }
    }
    if (-not $caughtUp) {
        throw "Returned replica $leader did not catch up within $TimeoutSeconds seconds"
    }
    $expectedMessages = [int64][Math]::Ceiling($Operations / 1000.0)
    $messageDelta = [int64]$afterStream.state.messages - $beforeMessages
    if ($messageDelta -ne $expectedMessages) {
        throw "Replicated message delta is $messageDelta, expected $expectedMessages"
    }

    [pscustomobject]@{
        result = "PASSED"
        old_leader = $leader
        new_leader = $newLeader
        failover_ms = $failoverTimer.ElapsedMilliseconds
        operations = $Operations
        operation_messages = $messageDelta
        balance_delta = $afterBalance - $beforeBalance
    } | Format-List
} catch {
    Write-Error "QUORUM FAILOVER TEST FAILED: $($_.Exception.Message)"
    docker compose logs --tail 80 nats-1 nats-2 nats-3 warehouse
    exit 1
} finally {
    if ($stoppedService) {
        docker compose up -d $stoppedService | Out-Null
    }
    Remove-Item -LiteralPath $outputFile, $errorFile -Force -ErrorAction SilentlyContinue
    Pop-Location
}
