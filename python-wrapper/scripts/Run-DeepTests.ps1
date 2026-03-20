<#
.SYNOPSIS
  Run deep parity tests (@pytest.mark.deep; uses repo tests/fixtures/deep).

.EXAMPLE
  .\scripts\Run-DeepTests.ps1
  .\scripts\Run-DeepTests.ps1 -Build
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
  $args = @('run', 'pytest', '-q', '-m', 'deep') + $PytestArgs
  & uv @args
  if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
finally {
  Pop-Location
}
