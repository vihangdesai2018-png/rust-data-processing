param(
  [switch]$Offline,
  [switch]$All
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (Get-Command sccache -ErrorAction SilentlyContinue) {
  $env:RUSTC_WRAPPER = "sccache"
}

# Avoid rustup network calls during doc builds unless explicitly desired.
$env:RUSTUP_NO_UPDATE_CHECK = '1'
if ($Offline) {
  $env:RUSTUP_OFFLINE = '1'
  $env:CARGO_NET_OFFLINE = 'true'
} else {
  if (Test-Path Env:CARGO_NET_OFFLINE) { Remove-Item Env:CARGO_NET_OFFLINE -ErrorAction SilentlyContinue }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "== Rustdoc (cargo doc --no-deps) =="
$docArgs = @('doc', '--no-deps', '--locked')
if ($Offline) { $docArgs += '--offline' }
& cargo @docArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if (-not $All) {
  Write-Host "Done. Open: target/doc/rust_data_processing/index.html"
  exit 0
}

Write-Host "== Python API docs (pdoc; requires uv + maturin) =="
$pyRoot = Join-Path $repoRoot 'python-wrapper'
Set-Location $pyRoot
& uv sync --group dev
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& uv run maturin develop --release
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$pyOut = Join-Path $repoRoot '_site/python'
if (Test-Path $pyOut) {
  Remove-Item -Recurse -Force $pyOut
}
New-Item -ItemType Directory -Path $pyOut | Out-Null
& uv run pdoc -d google -o $pyOut rust_data_processing rust_data_processing.examples
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Set-Location $repoRoot
Write-Host "Done."
Write-Host "  Rust:  target/doc/rust_data_processing/index.html"
Write-Host "  Python: _site/python/index.html"
