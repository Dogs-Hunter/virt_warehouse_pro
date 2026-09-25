param(
    [int]$DurationMinutes = 15,
    [string]$DatasetName = 'soak-100k',
    [int]$ConcurrencyPerRegion = 32,
    [int]$ChaosEveryCycles = 5,
    [int]$TimeoutSeconds = 180,
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$dataDirectory = Join-Path $root 'benchmark-data'
$dataset = Join-Path $dataDirectory "$DatasetName.jsonl"
$manifestPath = Join-Path $dataDirectory "$DatasetName.manifest.json"
$runStamp = [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss')
$samplesPath = Join-Path $dataDirectory "russia-survival-$runStamp.csv"
$regions = @(
    [pscustomobject]@{ name='moscow';       rtt=15  },
    [pscustomobject]@{ name='saint-petersburg'; rtt=25  },
    [pscustomobject]@{ name='kazan';        rtt=40  },
    [pscustomobject]@{ name='yekaterinburg';rtt=60  },
    [pscustomobject]@{ name='novosibirsk';  rtt=85  },
    [pscustomobject]@{ name='krasnoyarsk';  rtt=105 },
    [pscustomobject]@{ name='irkutsk';      rtt=135 },
    [pscustomobject]@{ name='vladivostok';  rtt=180 }
)

function Metric([string]$Text,[string]$Name) {
    $match=[regex]::Match($Text,"(?m)^$([regex]::Escape($Name))\s+([0-9.eE+-]+)$")
    if(-not $match.Success){throw "Metric '$Name' is missing"}
    [double]::Parse($match.Groups[1].Value,[Globalization.CultureInfo]::InvariantCulture)
}

function Output-Number([string]$Text,[string]$Name) {
    $match=[regex]::Match($Text,"(?m)^$([regex]::Escape($Name))=([0-9.]+)$")
    if(-not $match.Success){throw "Load output '$Name' is missing"}
    [double]::Parse($match.Groups[1].Value,[Globalization.CultureInfo]::InvariantCulture)
}

function Read-Metrics([int]$Port) {
    $text=(Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 "http://127.0.0.1:$Port/metrics").Content
    [pscustomobject]@{
        writer=Metric $text 'warehouse_writer_lease_owned'
        quorum=Metric $text 'warehouse_quorum_available'
        replication_lag=Metric $text 'warehouse_replication_lag'
        history_lag=Metric $text 'warehouse_history_lag'
        bloom=Metric $text 'warehouse_disk_bloom_ready'
        memory=Metric $text 'warehouse_process_memory_bytes'
    }
}

function Wait-Cluster([int]$Minutes=10) {
    $deadline=[DateTime]::UtcNow.AddMinutes($Minutes)
    do {
        try {
            $primary=Read-Metrics 8082; $replica=Read-Metrics 8081
            if($primary.quorum -eq 1 -and $replica.quorum -eq 1 -and
               $primary.bloom -eq 1 -and $replica.bloom -eq 1 -and
               ($primary.writer+$replica.writer) -eq 1){return @($primary,$replica)}
        } catch {}
        Start-Sleep -Seconds 2
    } while([DateTime]::UtcNow -lt $deadline)
    throw 'HA application cluster did not become ready'
}

function Get-StreamLeader {
    foreach($service in @('nats-1','nats-2','nats-3')){
        try {
            $container=docker compose ps -q $service
            $json=docker exec $container wget -q -O - 'http://127.0.0.1:8222/jsz?streams=true'|ConvertFrom-Json
            $stream=$json.account_details.stream_detail|Where-Object {$_.name -eq 'WAREHOUSE_OPERATIONS'}|Select-Object -First 1
            if($stream.cluster.leader){return [string]$stream.cluster.leader}
        } catch {}
    }
    throw 'Cannot resolve JetStream leader'
}

function Invoke-Chaos([int]$Index) {
    switch($Index % 3){
        0 {
            $m1=Read-Metrics 8082; $writer=if($m1.writer -eq 1){'warehouse'}else{'warehouse-2'}
            Write-Host "  chaos: killing active writer $writer"
            docker compose stop -t 0 $writer|Out-Null
            Start-Sleep -Seconds 3
            docker compose up -d $writer|Out-Null
            return "writer:$writer"
        }
        1 {
            $leader=Get-StreamLeader
            Write-Host "  chaos: killing JetStream leader $leader"
            docker compose stop -t 0 $leader|Out-Null
            Start-Sleep -Seconds 3
            docker compose up -d $leader|Out-Null
            return "jetstream:$leader"
        }
        2 {
            Write-Host '  chaos: restarting HAProxy'
            docker compose restart haproxy|Out-Null
            return 'haproxy:restart'
        }
    }
}

Push-Location $root
$temporary=[Collections.Generic.List[string]]::new()
try {
    if($DurationMinutes -lt 1){throw 'DurationMinutes must be at least 1'}
    if(-not(Test-Path $dataset)-or -not(Test-Path $manifestPath)){throw "Dataset '$DatasetName' is missing"}
    $manifest=Get-Content $manifestPath|ConvertFrom-Json
    if(-not $SkipBuild){docker compose build warehouse|Out-Host;if($LASTEXITCODE-ne 0){throw 'Build failed'}}

    Write-Host '[1/4] Starting two applications, HAProxy and three-node JetStream'
    docker compose up -d --force-recreate warehouse warehouse-2 haproxy|Out-Host
    if($LASTEXITCODE-ne 0){throw 'Topology startup failed'}
    Wait-Cluster|Out-Null

    Write-Host "[2/4] Running nationwide traffic for $DurationMinutes minutes"
    $deadline=[DateTime]::UtcNow.AddMinutes($DurationMinutes)
    $rows=[Collections.Generic.List[object]]::new();$cycle=0;$total=[int64]0;$faults=0
    while([DateTime]::UtcNow -lt $deadline){
        $cycle++;$processes=@();$cycleTimer=[Diagnostics.Stopwatch]::StartNew()
        foreach($region in $regions){
            $stdout=[IO.Path]::GetTempFileName();$stderr=[IO.Path]::GetTempFileName();$temporary.Add($stdout);$temporary.Add($stderr)
            $arguments=@('compose','--profile','benchmark','run','--rm','--no-deps','--entrypoint','warehouse-prepared-loadgen',
                '-v',"${dataDirectory}:/dataset:ro",'-e','PREPARED_MODE=mixed','-e',"PREPARED_DATASET=/dataset/$DatasetName.jsonl",
                '-e','WAREHOUSE_URL=http://haproxy:8080','-e',"LOAD_CONCURRENCY=$ConcurrencyPerRegion",
                '-e','LOAD_RETRY_SECONDS=90',
                '-e',"LOAD_RTT_MS=$($region.rtt)",'-e',"LOAD_RUN_ID=ru-$runStamp-$cycle-$($region.name)",'loadgen')
            $process=Start-Process docker -ArgumentList $arguments -WorkingDirectory $root -WindowStyle Hidden -PassThru `
                -RedirectStandardOutput $stdout -RedirectStandardError $stderr
            $processes += [pscustomobject]@{region=$region;process=$process;stdout=$stdout;stderr=$stderr}
        }

        $fault='none'
        if($ChaosEveryCycles -gt 0 -and $cycle % $ChaosEveryCycles -eq 0){
            Start-Sleep -Milliseconds 500
            $fault=Invoke-Chaos ([int]($cycle/$ChaosEveryCycles)-1);$faults++
        }

        $cycleOps=[int64]0;$worstP95=0.0
        foreach($entry in $processes){
            if(-not $entry.process.WaitForExit($TimeoutSeconds*1000)){try{$entry.process.Kill()}catch{};throw "Region $($entry.region.name) timed out in cycle $cycle"}
            $stdout=Get-Content -Raw $entry.stdout;$stderr=Get-Content -Raw $entry.stderr
            if($entry.process.ExitCode-ne 0){throw "Region $($entry.region.name) failed in cycle ${cycle}: $stderr"}
            $operations=[int64](Output-Number $stdout 'operations');$p95=Output-Number $stdout 'request_p95_ms'
            if($operations-ne [int64]$manifest.operations){throw "Region $($entry.region.name) returned $operations operations"}
            $cycleOps+=$operations;$worstP95=[math]::Max($worstP95,$p95)
        }
        $cycleTimer.Stop();$total+=$cycleOps
        $metrics=Wait-Cluster
        $opsPerSecond=$cycleOps/$cycleTimer.Elapsed.TotalSeconds
        $rows.Add([pscustomobject]@{cycle=$cycle;fault=$fault;operations=$cycleOps;operations_per_second=[math]::Round($opsPerSecond,1);worst_region_p95_ms=[math]::Round($worstP95,3);primary_memory=[int64]$metrics[0].memory;replica_memory=[int64]$metrics[1].memory;replication_lag=[math]::Max($metrics[0].replication_lag,$metrics[1].replication_lag);history_lag=[math]::Max($metrics[0].history_lag,$metrics[1].history_lag)})
        Write-Host ("cycle={0} fault={1} ops_s={2:N0} p95_ms={3:N1} lag={4}/{5}" -f $cycle,$fault,$opsPerSecond,$worstP95,[math]::Max($metrics[0].replication_lag,$metrics[1].replication_lag),[math]::Max($metrics[0].history_lag,$metrics[1].history_lag))
    }

    Write-Host '[3/4] Waiting for every replica and read model to catch up'
    $catchup=[DateTime]::UtcNow.AddMinutes(10)
    do{$metrics=Wait-Cluster;if([math]::Max($metrics[0].replication_lag,$metrics[1].replication_lag)-eq 0 -and [math]::Max($metrics[0].history_lag,$metrics[1].history_lag)-eq 0){break};Start-Sleep -Seconds 1}while([DateTime]::UtcNow-lt$catchup)
    if([math]::Max($metrics[0].replication_lag,$metrics[1].replication_lag)-ne 0 -or [math]::Max($metrics[0].history_lag,$metrics[1].history_lag)-ne 0){throw 'Cluster did not fully catch up'}
    $rows|Export-Csv -NoTypeInformation -Encoding utf8 $samplesPath
    $rates=@($rows|ForEach-Object {[double]$_.operations_per_second});$p95s=@($rows|ForEach-Object {[double]$_.worst_region_p95_ms});$mem=@($rows|ForEach-Object {[double]$_.primary_memory;[double]$_.replica_memory})

    Write-Host '[4/4] Reporting nationwide survival result'
    [pscustomobject]@{result='PASSED';regions=$regions.Count;duration_minutes=$DurationMinutes;cycles=$cycle;chaos_events=$faults;operations=$total;write_percent=20;read_percent=80;average_operations_per_second=[math]::Round(($rates|Measure-Object -Average).Average,1);minimum_operations_per_second=[math]::Round(($rates|Measure-Object -Minimum).Minimum,1);maximum_region_p95_ms=[math]::Round(($p95s|Measure-Object -Maximum).Maximum,3);peak_application_memory_bytes=[int64](($mem|Measure-Object -Maximum).Maximum);final_replication_lag=0;final_history_lag=0;samples_file=$samplesPath}|Format-List
} catch {
    Write-Error "RUSSIA SURVIVAL TEST FAILED: $($_.Exception.Message)"
    docker compose up -d nats-1 nats-2 nats-3 warehouse warehouse-2 haproxy|Out-Null
    exit 1
} finally {
    foreach($path in $temporary){Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue}
    Pop-Location
}
