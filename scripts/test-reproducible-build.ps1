param([switch]$NoCache)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    Write-Host '[1/4] Validating locked dependency graph'
    if (-not (Test-Path 'Cargo.lock')) { throw 'Cargo.lock is missing' }
    docker run --rm -v "${root}:/work" -v warehouse-cargo-test-registry:/usr/local/cargo/registry -w /work `
        rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f `
        cargo metadata --locked --format-version 1 --no-deps | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Locked dependency graph is invalid' }

    Write-Host '[2/4] Running Rust tests in the pinned toolchain'
    docker run --rm -v "${root}:/work" -v warehouse-cargo-test-registry:/usr/local/cargo/registry -v warehouse-cargo-test-target:/work/target -w /work `
        rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f `
        cargo test --locked
    if ($LASTEXITCODE -ne 0) { throw 'Rust tests failed' }

    Write-Host '[3/4] Building the pinned container image'
    $arguments = @('build', '--pull', '--tag', 'test-warehouse:reproducible')
    if ($NoCache) { $arguments += '--no-cache' }
    $arguments += '.'
    & docker @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Container build failed' }

    Write-Host '[4/4] Verifying binaries and Compose configuration'
    docker run --rm --entrypoint /bin/sh test-warehouse:reproducible -c `
        'test -x /usr/local/bin/warehouse-lab -a -x /usr/local/bin/warehouse-loadgen -a -x /usr/local/bin/warehouse-chaos -a -x /usr/local/bin/warehouse-prepared-loadgen'
    if ($LASTEXITCODE -ne 0) { throw 'One or more runtime binaries are missing' }
    docker compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw 'Compose configuration is invalid' }

    [pscustomobject]@{
        result = 'PASSED'
        rust = '1.90.0'
        dependency_lock = 'Cargo.lock'
        unit_tests = 'passed'
        image = 'test-warehouse:reproducible'
        clean_build = [bool]$NoCache
    } | Format-List
} catch {
    Write-Error "REPRODUCIBLE BUILD TEST FAILED: $($_.Exception.Message)"
    exit 1
} finally {
    Pop-Location
}
