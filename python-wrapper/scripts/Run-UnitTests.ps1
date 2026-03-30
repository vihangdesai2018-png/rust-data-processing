<#
.SYNOPSIS
  Run pytest unit/integration tests (excludes @pytest.mark.deep and @pytest.mark.benchmark).

.EXAMPLE
  .\scripts\Run-UnitTests.ps1
  .\scripts\Run-UnitTests.ps1 -Build
  .\scripts\Run-UnitTests.ps1 -PytestArgs @('-v', '--tb=short')
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
  $args = @('run', 'pytest', '-q', '-m', 'not deep and not benchmark') + $PytestArgs
  & uv @args
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
  Pop-Location
}
