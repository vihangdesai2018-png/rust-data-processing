param(
  [switch]$Offline,
  [switch]$SkipExcel
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptDir = $PSScriptRoot
$repoRoot = if (Test-Path (Join-Path $scriptDir '..\\Cargo.toml')) { (Resolve-Path (Join-Path $scriptDir '..')).Path } else { (Get-Location).Path }

if (Get-Command sccache -ErrorAction SilentlyContinue) {
  $env:RUSTC_WRAPPER = "sccache"
}

Push-Location $repoRoot
try {
  if (Get-Command sccache -ErrorAction SilentlyContinue) {
    $env:RUSTC_WRAPPER = "sccache"
  }

  $env:RUSTUP_NO_UPDATE_CHECK = '1'
  if ($Offline) {
    $env:RUSTUP_OFFLINE = '1'
    $env:CARGO_NET_OFFLINE = 'true'
  } else {
    if (Test-Path Env:CARGO_NET_OFFLINE) { Remove-Item Env:CARGO_NET_OFFLINE -ErrorAction SilentlyContinue }
  }

  $cargoArgs = @('test', '--locked', '--test', 'deep_tests', '--features', 'deep_tests')
  if ($Offline) { $cargoArgs += '--offline' }

  Write-Host "== Deep tests (CSV/JSON/Parquet) =="
  & cargo @cargoArgs
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

  if (-not $SkipExcel) {
    Write-Host ""
    Write-Host "== Deep tests (Excel) [requires feature excel_test_writer] =="
    $excelArgs = @('test', '--locked', '--test', 'deep_tests', '--features', 'deep_tests excel_test_writer')
    if ($Offline) { $excelArgs += '--offline' }
    & cargo @excelArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
  }
} finally {
  Pop-Location
}

