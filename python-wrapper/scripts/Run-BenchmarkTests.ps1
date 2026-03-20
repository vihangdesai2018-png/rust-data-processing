<#
.SYNOPSIS
  Run pytest-benchmark tests (@pytest.mark.benchmark).

.EXAMPLE
  .\scripts\Run-BenchmarkTests.ps1
  .\scripts\Run-BenchmarkTests.ps1 -Build
#>
param(
  [switch]$Build,
  [string[]]$PytestArgs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$wrapperRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
  throw "uv not found on PATH. Install from https://docs.astral.sh/uv/"
}

Push-Location $wrapperRoot
try {
  if ($Build) {
    uv run maturin develop --release
  }
  $args = @('run', 'pytest', '-q', '-m', 'benchmark') + $PytestArgs
  & uv @args
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
  Pop-Location
}
