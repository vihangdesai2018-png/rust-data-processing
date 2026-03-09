param(
  # sccache release version without the leading "v"
  [string]$Version = "0.14.0",
  # Persist RUSTC_WRAPPER using setx (requires new shell to take effect)
  [switch]$PersistEnv
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Assert-CommandExists([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found on PATH: $name"
  }
}

function Ensure-Dir([string]$path) {
  if (-not (Test-Path $path)) {
    New-Item -ItemType Directory -Path $path | Out-Null
  }
}

Assert-CommandExists powershell

$destDir = Join-Path $env:USERPROFILE ".cargo\\bin"
Ensure-Dir $destDir

$zipName = "sccache-v$Version-x86_64-pc-windows-msvc.zip"
$url = "https://github.com/mozilla/sccache/releases/download/v$Version/$zipName"

$tmpZip = Join-Path $env:TEMP $zipName
$tmpDir = Join-Path $env:TEMP ("sccache-v{0}" -f $Version)

Write-Host "Downloading $url"
Invoke-WebRequest -Uri $url -OutFile $tmpZip

Write-Host "Extracting $tmpZip"
Remove-Item $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
Expand-Archive -Path $tmpZip -DestinationPath $tmpDir -Force

$exe = Get-ChildItem -Path $tmpDir -Recurse -Filter "sccache.exe" | Select-Object -First 1
if (-not $exe) {
  throw "sccache.exe not found inside extracted archive: $tmpDir"
}

$destExe = Join-Path $destDir "sccache.exe"
Copy-Item -Path $exe.FullName -Destination $destExe -Force

Write-Host "Installed: $destExe"

# Ensure ~/.cargo/bin is on PATH for this session (best-effort)
if ($env:Path -notlike "*$destDir*") {
  $env:Path = $env:Path + ";" + $destDir
}

Write-Host ""
Write-Host "Verifying install:"
& $destExe --version

# Enable sccache for current session
$env:RUSTC_WRAPPER = "sccache"
Write-Host ""
Write-Host "Enabled for this session:"
Write-Host "  RUSTC_WRAPPER=$env:RUSTC_WRAPPER"

if ($PersistEnv) {
  Write-Host ""
  Write-Host "Persisting env vars (takes effect in new shells)..."
  setx RUSTC_WRAPPER sccache | Out-Null
}

Write-Host ""
Write-Host "Next (compile-only check):"
Write-Host "  sccache --zero-stats"
Write-Host "  cargo test --features excel --no-run"
Write-Host "  sccache --show-stats"

