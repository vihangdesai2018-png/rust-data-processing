#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Interactive release: show last tag and versions, bump Cargo/pyproject/locks/CHANGELOG, commit, push main, tag v*, push tag.

.DESCRIPTION
  Delegates to scripts/release.py (Python 3.10+). Run from repo root, or this script will cd to the repository root.

  For flags (dry-run, non-interactive, etc.) see:  python scripts/release.py --help

.EXAMPLE
  ./scripts/release_tag.ps1
#>
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
  $python = Get-Command py -ErrorAction SilentlyContinue
}
if (-not $python) {
  throw 'Python not found. Install Python 3.10+ and ensure `python` or `py` is on PATH.'
}

$releasePy = Join-Path $ScriptDir 'release.py'
if (-not (Test-Path -LiteralPath $releasePy)) {
  throw "Missing $releasePy"
}

& $python.Path $releasePy @args
