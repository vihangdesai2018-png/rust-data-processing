param(
  # Which bench target to run: pipelines | ingestion | map_reduce | profiling | all
  [ValidateSet('pipelines','ingestion','map_reduce','profiling','all')]
  [string]$Bench = 'all',

  # Faster Criterion run (useful for local iteration).
  [switch]$Quick,

  # Optional Criterion benchmark filter substring.
  [string]$Filter,

  # Used when -Quick is set (passed to Criterion).
  [int]$SampleSize = 10,
  [int]$WarmupSeconds = 1,
  [int]$MeasureSeconds = 2,

  # Avoid network access (cargo --offline + rustup offline env).
  [switch]$Offline
)

$scriptDir = $PSScriptRoot
$repoRoot = if (Test-Path (Join-Path $scriptDir '..\\Cargo.toml')) { (Resolve-Path (Join-Path $scriptDir '..')).Path } else { (Get-Location).Path }
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-CommandExists([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found on PATH: $name"
  }
}

try {
  Set-Location $repoRoot

  # Avoid rustup network calls during benchmark runs unless explicitly desired.
  $env:RUSTUP_NO_UPDATE_CHECK = '1'
  if ($Offline) {
    $env:RUSTUP_OFFLINE = '1'
    $env:CARGO_NET_OFFLINE = 'true'
  } else {
    if (Test-Path Env:CARGO_NET_OFFLINE) { Remove-Item Env:CARGO_NET_OFFLINE -ErrorAction SilentlyContinue }
  }

  Assert-CommandExists cargo
  Assert-CommandExists rustc

  Write-Host "== Benchmark runner =="
  Write-Host ("pwd: " + (Get-Location).Path)
  (& cargo --version 2>&1) | Write-Host
  (& rustc --version 2>&1) | Write-Host

  $benches = @()
  switch ($Bench) {
    'pipelines' { $benches = @('pipelines') }
    'ingestion' { $benches = @('ingestion') }
    'map_reduce' { $benches = @('map_reduce') }
    'profiling' { $benches = @('profiling') }
    'all' { $benches = @('pipelines','ingestion','map_reduce','profiling') }
  }

  foreach ($b in $benches) {
    Write-Host ""
    Write-Host ("== Running: cargo bench --bench " + $b + " ==")

    $cargoArgs = @('bench', '--bench', $b, '--locked')
    if ($Offline) { $cargoArgs += '--offline' }

    $crit = @()
    if ($Quick) {
      $crit += @('--sample-size', "$SampleSize", '--warm-up-time', "$WarmupSeconds", '--measurement-time', "$MeasureSeconds")
    }
    if (-not [string]::IsNullOrWhiteSpace($Filter)) {
      $crit += @("$Filter")
    }
    if ($crit.Count -gt 0) {
      $cargoArgs += '--'
      $cargoArgs += $crit
    }

    & cargo @cargoArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  }
} catch {
  Write-Host ("FAIL: " + $_.ToString())
  exit 1
}

