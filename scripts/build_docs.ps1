param(
  [switch]$Offline
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

Write-Host "== Rustdoc (cargo doc --no-deps) =="
$docArgs = @('doc', '--no-deps', '--locked')
if ($Offline) { $docArgs += '--offline' }
& cargo @docArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
