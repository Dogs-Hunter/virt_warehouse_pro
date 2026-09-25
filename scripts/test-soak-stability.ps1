param(
    [string]$Name = 'soak-100k',
    [int]$DurationMinutes = 60,
    [int]$Concurrency = 64,
    [int]$PauseSeconds = 5,
    [int]$MaxMemoryMB = 1024,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $root 'benchmark-data'
$dataset = Join-Path $dataDirectory "$Name.jsonl"
$manifestPath = Join-Path $dataDirectory "$Name.manifest.json"
$runStamp = [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss')
$csvPath = Join-Path $dataDirectory "soak-$runStamp.csv"
$logPath = Join-Path $dataDirectory "soak-$runStamp.log"

function Metric([string]$Text, [string]$Name) {
    $match = [regex]::Match($Text, "(?m)^$([regex]::Escape($Name))\s+([0-9.eE+-]+)$")
    if (-not $match.Success) { throw "Metric '$Name' is missing" }
    [double]::Parse($match.Groups[1].Value, [Globalization.CultureInfo]::InvariantCulture)
}

function OutputValue([string]$Text, [string]$Name) {
    $match = [regex]::Match($Text, "(?m)^$([regex]::Escape($Name))=([0-9.]+)$")
    if (-not $match.Success) { throw "Load result '$Name' is missing" }
    [double]::Parse($match.Groups[1].Value, [Globalization.CultureInfo]::InvariantCulture)
}

function Read-Metrics([int]$Port) {
    $text = (Invoke-WebRequest -UseBasicParsing -TimeoutSec 10 "http://127.0.0.1:$Port/metrics").Content
    [pscustomobject]@{
        memory = Metric $text 'warehouse_process_memory_bytes'
        replication_lag = Metric $text 'warehouse_replication_lag'
        history_lag = Metric $text 'warehouse_history_lag'
        unavailable = Metric $text 'warehouse_unavailable_total'
        quorum = Metric $text 'warehouse_quorum_available'
        writer = Metric $text 'warehouse_writer_lease_owned'
        stream_bytes = Metric $text 'warehouse_stream_bytes'
        stream_max_bytes = Metric $text 'warehouse_stream_max_bytes'
        memory_dedup_committed = Metric $text 'warehouse_memory_dedup_committed'
        memory_dedup_pending = Metric $text 'warehouse_memory_dedup_pending'
        balance_positions = Metric $text 'warehouse_balance_positions'
        lsm_cache_bytes = Metric $text 'warehouse_lsm_cache_bytes'
        lsm_write_buffer_bytes = Metric $text 'warehouse_lsm_write_buffer_bytes'
        bloom_ready = Metric $text 'warehouse_disk_bloom_ready'
    }
}

function Read-StableMetrics([int]$Port) {
    $lastError = $null
    for ($attempt = 1; $attempt -le 5; $attempt++) {
        try {
            $value = Read-Metrics $Port
            if ($value.quorum -eq 1) { return $value }
            $lastError = "quorum metric is zero"
        } catch {
            $lastError = $_.Exception.Message
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Metrics on port $Port remained unhealthy after retries: $lastError"
}

Push-Location $root
try {
    if ($DurationMinutes -lt 1) { throw 'DurationMinutes must be at least 1' }
    if ($Concurrency -lt 1) { throw 'Concurrency must be positive' }
    if (-not (Test-Path $dataset) -or -not (Test-Path $manifestPath)) {
        throw "Dataset is missing. Run: .\scripts\prepare-performance-dataset.ps1 -Name $Name -Operations 100000 -SkipBuild"
    }
    $manifest = Get-Content $manifestPath | ConvertFrom-Json
    $hash = (Get-FileHash -Algorithm SHA256 $dataset).Hash.ToLowerInvariant()
    if ($hash -ne $manifest.sha256) { throw 'Dataset checksum mismatch' }

    Write-Host '[1/4] Starting current HA topology'
    if (-not $SkipBuild) {
        docker compose build warehouse | Out-Null
        if ($LASTEXITCODE -ne 0) { throw 'Build failed' }
    }
    docker compose up -d warehouse warehouse-2 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Topology startup failed' }
    docker compose up -d --no-deps --force-recreate haproxy | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'HAProxy startup failed' }

    Write-Host 'Waiting for application nodes to become healthy'
    $startupDeadline = [DateTime]::UtcNow.AddMinutes(10)
    do {
        try {
            $startupPrimary = Read-StableMetrics 8082
            $startupReplica = Read-StableMetrics 8081
            break
        } catch {
            if ([DateTime]::UtcNow -ge $startupDeadline) { throw "Application nodes did not become healthy within 10 minutes: $($_.Exception.Message)" }
            Start-Sleep -Seconds 2
        }
    } while ($true)

    Write-Host 'Waiting for disk dedup filters to warm before measurement'
    $warmDeadline = [DateTime]::UtcNow.AddMinutes(10)
    do {
        $warmPrimary = Read-StableMetrics 8082
        $warmReplica = Read-StableMetrics 8081
        if ($warmPrimary.bloom_ready -eq 1 -and $warmReplica.bloom_ready -eq 1) { break }
        if ([DateTime]::UtcNow -ge $warmDeadline) { throw 'Disk dedup filters did not warm within 10 minutes' }
        Start-Sleep -Seconds 2
    } while ($true)

    $primaryBefore = Read-StableMetrics 8082
    $replicaBefore = Read-StableMetrics 8081
    $deadline = [DateTime]::UtcNow.AddMinutes($DurationMinutes)
    $started = [Diagnostics.Stopwatch]::StartNew()
    $rows = [Collections.Generic.List[object]]::new()
    $totalOperations = [int64]0
    $totalWrites = [int64]0
    $totalReads = [int64]0
    $cycle = 0

    Write-Host "[2/4] Running $DurationMinutes minute 20% write / 80% read soak"
    while ([DateTime]::UtcNow -lt $deadline) {
        $cycle++
        $runId = "soak-$runStamp-$cycle"
        $output = docker compose --profile benchmark run --rm --no-deps `
            --entrypoint warehouse-prepared-loadgen `
            -v "${dataDirectory}:/dataset:ro" `
            -e PREPARED_MODE=mixed `
            -e PREPARED_DATASET="/dataset/$Name.jsonl" `
            -e WAREHOUSE_URL=http://haproxy:8080 `
            -e LOAD_CONCURRENCY=$Concurrency `
            -e LOAD_RUN_ID=$runId loadgen 2>&1
        $text = $output -join "`n"
        Add-Content -Path $logPath -Value "`n===== cycle $cycle =====`n$text"
        if ($LASTEXITCODE -ne 0) { throw "Load cycle $cycle failed; details: $logPath" }

        $operations = [int64](OutputValue $text 'operations')
        $writes = [int64](OutputValue $text 'writes')
        $reads = [int64](OutputValue $text 'reads')
        $ops = OutputValue $text 'operations_per_second'
        $p95 = OutputValue $text 'request_p95_ms'
        if ($writes * 5 -ne $operations -or $reads * 5 -ne $operations * 4) {
            throw "Cycle $cycle did not preserve the 20/80 ratio"
        }

        $primary = Read-StableMetrics 8082
        $replica = Read-StableMetrics 8081
        if (($primary.writer + $replica.writer) -ne 1) { throw "Writer ownership invalid after cycle $cycle" }

        $totalOperations += $operations
        $totalWrites += $writes
        $totalReads += $reads
        $rows.Add([pscustomobject]@{
            cycle = $cycle
            elapsed_minutes = [math]::Round($started.Elapsed.TotalMinutes, 2)
            operations_per_second = $ops
            p95_ms = $p95
            primary_memory_bytes = [int64]$primary.memory
            replica_memory_bytes = [int64]$replica.memory
            replication_lag = [int64][math]::Max($primary.replication_lag, $replica.replication_lag)
            history_lag = [int64][math]::Max($primary.history_lag, $replica.history_lag)
            stream_bytes = [int64]$primary.stream_bytes
        })
        Write-Host ("cycle={0} elapsed_min={1:N1} ops_s={2:N0} p95_ms={3:N1} memory_mb={4:N0}/{5:N0} lag={6}/{7}" -f `
            $cycle, $started.Elapsed.TotalMinutes, $ops, $p95, ($primary.memory / 1MB), ($replica.memory / 1MB), `
            [math]::Max($primary.replication_lag, $replica.replication_lag), [math]::Max($primary.history_lag, $replica.history_lag))

        if ($PauseSeconds -gt 0 -and [DateTime]::UtcNow.AddSeconds($PauseSeconds) -lt $deadline) {
            Start-Sleep -Seconds $PauseSeconds
        }
    }

    Write-Host '[3/4] Checking final health and exporting samples'
    $started.Stop()
    $rows | Export-Csv -NoTypeInformation -Encoding utf8 $csvPath
    $primaryAfter = Read-StableMetrics 8082
    $replicaAfter = Read-StableMetrics 8081
    $throughputs = @($rows | ForEach-Object { [double]$_.operations_per_second })
    $p95Values = @($rows | ForEach-Object { [double]$_.p95_ms })
    $memoryValues = @($rows | ForEach-Object { [double]$_.primary_memory_bytes; [double]$_.replica_memory_bytes })
    $lagValues = @($rows | ForEach-Object { [double]$_.replication_lag })
    $historyValues = @($rows | ForEach-Object { [double]$_.history_lag })
    $unavailableDelta = [int64](($primaryAfter.unavailable + $replicaAfter.unavailable) - ($primaryBefore.unavailable + $replicaBefore.unavailable))
    $peakMemory = [int64](($memoryValues | Measure-Object -Maximum).Maximum)
    if ($peakMemory -gt $MaxMemoryMB * 1MB) { throw "Peak application memory exceeded ${MaxMemoryMB} MiB: $([math]::Round($peakMemory / 1MB, 1)) MiB" }

    Write-Host '[4/4] Reporting stability result'
    [pscustomobject]@{
        result = 'PASSED'
        duration_minutes = [math]::Round($started.Elapsed.TotalMinutes, 2)
        cycles = $cycle
        operations = $totalOperations
        writes = $totalWrites
        reads = $totalReads
        write_percent = 20
        read_percent = 80
        average_ops_per_second = [math]::Round(($throughputs | Measure-Object -Average).Average, 1)
        minimum_ops_per_second = [math]::Round(($throughputs | Measure-Object -Minimum).Minimum, 1)
        maximum_p95_ms = [math]::Round(($p95Values | Measure-Object -Maximum).Maximum, 3)
        peak_memory_bytes = $peakMemory
        memory_limit_bytes = [int64]($MaxMemoryMB * 1MB)
        peak_replication_lag = [int64](($lagValues | Measure-Object -Maximum).Maximum)
        peak_history_lag = [int64](($historyValues | Measure-Object -Maximum).Maximum)
        unavailable_responses = $unavailableDelta
        stream_growth_bytes = [int64]($primaryAfter.stream_bytes - $primaryBefore.stream_bytes)
        stream_used_percent = [math]::Round(100 * $primaryAfter.stream_bytes / $primaryAfter.stream_max_bytes, 2)
        final_memory_dedup_committed = [int64][math]::Max($primaryAfter.memory_dedup_committed, $replicaAfter.memory_dedup_committed)
        final_memory_dedup_pending = [int64][math]::Max($primaryAfter.memory_dedup_pending, $replicaAfter.memory_dedup_pending)
        final_balance_positions = [int64][math]::Max($primaryAfter.balance_positions, $replicaAfter.balance_positions)
        final_lsm_cache_bytes = [int64][math]::Max($primaryAfter.lsm_cache_bytes, $replicaAfter.lsm_cache_bytes)
        final_lsm_write_buffer_bytes = [int64][math]::Max($primaryAfter.lsm_write_buffer_bytes, $replicaAfter.lsm_write_buffer_bytes)
        samples_file = $csvPath
        full_log_file = $logPath
    } | Format-List
} catch {
    Write-Error "SOAK TEST FAILED: $($_.Exception.Message)"
    Write-Host "Full log: $logPath"
    exit 1
} finally {
    Pop-Location
}
