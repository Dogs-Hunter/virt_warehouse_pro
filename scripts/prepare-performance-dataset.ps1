param([int]$Operations = 1000000, [int]$BatchSize = 1000, [string]$Name = 'prepared-1', [switch]$SkipBuild)
$ErrorActionPreference='Stop';$root=Split-Path -Parent $PSScriptRoot;$directory=Join-Path $root 'benchmark-data';$dataset=Join-Path $directory "$Name.jsonl";$manifest=Join-Path $directory "$Name.manifest.json"
Push-Location $root
try {
 New-Item -ItemType Directory -Force $directory|Out-Null
 if (-not $SkipBuild) { docker compose build warehouse | Out-Host; if ($LASTEXITCODE -ne 0) { throw 'Build failed' } }
 Write-Host "Preparing $Operations operations outside the measured interval"
 docker compose --profile benchmark run --rm --no-deps --entrypoint warehouse-prepared-loadgen -v "${directory}:/dataset" -e PREPARED_MODE=prepare -e PREPARED_DATASET="/dataset/$Name.jsonl" -e LOAD_OPERATIONS=$Operations -e LOAD_BATCH_SIZE=$BatchSize -e LOAD_RUN_ID=$Name loadgen | Out-Host
 if ($LASTEXITCODE -ne 0) { throw 'Dataset generator failed' }
 $hash=(Get-FileHash -Algorithm SHA256 $dataset).Hash.ToLowerInvariant();$bytes=(Get-Item $dataset).Length
 [ordered]@{name=$Name;operations=$Operations;batch_size=$BatchSize;bytes=$bytes;sha256=$hash;created_utc=[DateTime]::UtcNow.ToString('o')}|ConvertTo-Json|Set-Content -Encoding utf8 $manifest
 [pscustomobject]@{result='PASSED';dataset=$dataset;manifest=$manifest;operations=$Operations;batch_size=$BatchSize;bytes=$bytes;sha256=$hash}|Format-List
}catch{Write-Error "DATASET PREPARATION FAILED: $($_.Exception.Message)";exit 1}finally{Pop-Location}
