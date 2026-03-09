param(
  [switch]$NoVsDevCmd,
  [switch]$Offline
)

$scriptDir = $PSScriptRoot
$repoRoot = if (Test-Path (Join-Path $scriptDir 'Cargo.toml')) { $scriptDir } else { Split-Path -Parent $scriptDir }

if (Get-Command sccache -ErrorAction SilentlyContinue) {
  $env:RUSTC_WRAPPER = "sccache"
}

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Write all output to test_run.log (repo root)
$PreferredLogPath = Join-Path $repoRoot 'test_run.log'
$LogPath = $PreferredLogPath
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Initialize-Log {
  try {
    [System.IO.File]::WriteAllText($PreferredLogPath, "", $Utf8NoBom)
    $script:LogPath = $PreferredLogPath
  } catch {
    $fallback = Join-Path $repoRoot ("test_run_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
    [System.IO.File]::WriteAllText($fallback, "", $Utf8NoBom)
    $script:LogPath = $fallback
    Write-Host "NOTE: 'test_run.log' is locked; writing to '$fallback' instead."
  }
}

Initialize-Log

function Write-LogLine([string]$line) {
  try {
    [System.IO.File]::AppendAllText($LogPath, ($line + [System.Environment]::NewLine), $Utf8NoBom)
  } catch {
    # If the preferred log file becomes locked mid-run, fall back to a timestamped file.
    if ($LogPath -eq $PreferredLogPath) {
      $fallback = Join-Path $repoRoot ("test_run_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
      [System.IO.File]::WriteAllText($fallback, "", $Utf8NoBom)
      $script:LogPath = $fallback
      Write-Host "NOTE: 'test_run.log' became locked; switching logs to '$fallback'."
      [System.IO.File]::AppendAllText($LogPath, ($line + [System.Environment]::NewLine), $Utf8NoBom)
      return
    }
    throw
  }
}

function Write-Section([string]$title) {
  $ts = (Get-Date).ToString("s")
  $line = "[$ts] $title"
  Write-Host $line
  Write-LogLine $line
}

function Write-And-LogPipelineOutput {
  param(
    [Parameter(ValueFromPipeline = $true)]
    $InputObject
  )
  process {
    if ($InputObject -is [System.Management.Automation.ErrorRecord]) {
      $line = $InputObject.ToString()
      if ($line -eq 'System.Management.Automation.RemoteException') {
        return
      }
    } else {
      $line = $InputObject.ToString()
    }
    Write-Host $line
    Write-LogLine $line
  }
}

function Invoke-Logged([scriptblock]$Command, [string]$OnFailMessage) {
  # Native commands (like cargo) often write non-error output to stderr.
  # In Windows PowerShell this can surface as non-terminating errors which would
  # become terminating with $ErrorActionPreference='Stop'. Temporarily relax it.
  $oldEap = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    & $Command 2>&1 | Write-And-LogPipelineOutput
    $code = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $oldEap
  }

  if ($code -ne 0) { throw "$OnFailMessage (exit code $code)" }
}

# Avoid rustup network calls during test runs unless explicitly desired.
$env:RUSTUP_NO_UPDATE_CHECK = '1'
if ($Offline) {
  $env:RUSTUP_OFFLINE = '1'
}

# Cargo offline mode control. Some environments set this implicitly.
# Default: force online (clear offline) unless -Offline was requested.
if ($Offline) {
  $env:CARGO_NET_OFFLINE = 'true'
} else {
  if (Test-Path Env:CARGO_NET_OFFLINE) { Remove-Item Env:CARGO_NET_OFFLINE -ErrorAction SilentlyContinue }
}

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
  $cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'
  if (Test-Path $cargoBin) {
    $env:Path = $env:Path + ';' + $cargoBin
  }
}

function Assert-CommandExists([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found on PATH: $name"
  }
}

Assert-CommandExists cargo
Assert-CommandExists rustc

function Find-VsWhereExe {
  $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
  if (Test-Path $vswhere) { return $vswhere }
  return $null
}

function Find-LinkExePath {
  $vswhere = Find-VsWhereExe
  if (-not $vswhere) { return $null }

  # Locate link.exe inside the latest VS instance with C++ tools installed.
  $link = & $vswhere `
    -latest `
    -products '*' `
    -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
    -find 'VC\Tools\MSVC\**\bin\Hostx64\x64\link.exe' 2>$null |
    Select-Object -First 1

  if ([string]::IsNullOrWhiteSpace($link)) { return $null }
  return $link
}

function Resolve-VsDevCmdBat {
  $vswhere = Find-VsWhereExe
  if (-not $vswhere) { return $null }

  # Prefer BuildTools, fall back to any VS instance.
  $installPath = & $vswhere -latest -products Microsoft.VisualStudio.Product.BuildTools -property installationPath 2>$null
  if ([string]::IsNullOrWhiteSpace($installPath)) {
    $installPath = & $vswhere -latest -property installationPath 2>$null
  }
  if ([string]::IsNullOrWhiteSpace($installPath)) { return $null }

  $vsDevCmd = Join-Path $installPath 'Common7\Tools\VsDevCmd.bat'
  if (Test-Path $vsDevCmd) { return $vsDevCmd }
  return $null
}

function Invoke-Tests {
  Write-Section "== Environment =="
  ("pwd: " + (Get-Location).Path) | Write-And-LogPipelineOutput
  (& cargo --version 2>&1) | Write-And-LogPipelineOutput
  (& rustc --version 2>&1) | Write-And-LogPipelineOutput
  $cno = if ([string]::IsNullOrWhiteSpace($env:CARGO_NET_OFFLINE)) { "<unset>" } else { $env:CARGO_NET_OFFLINE }
  $ruo = if ([string]::IsNullOrWhiteSpace($env:RUSTUP_OFFLINE)) { "<unset>" } else { $env:RUSTUP_OFFLINE }
  ("CARGO_NET_OFFLINE: " + $cno) | Write-And-LogPipelineOutput
  ("RUSTUP_OFFLINE: " + $ruo) | Write-And-LogPipelineOutput
  Write-LogLine ""

  $cargoArgs = @('test', '--locked')
  if ($Offline) { $cargoArgs += '--offline' }

  Write-Section "== Unit tests (library) =="
  Invoke-Logged { cargo @cargoArgs --lib } "cargo test --lib failed"

  Write-Host ""
  Write-LogLine ""
  Write-Section "== Integration tests (tests/) =="
  Invoke-Logged { cargo @cargoArgs --tests } "cargo test --tests failed"
}

try {
  # First try running tests directly in the current shell.
  Invoke-Tests
  exit 0
} catch {
  $directError = $_
  Write-Host ""
  Write-Host "Direct test run failed; attempting VS Developer environment fallback..."
  Write-Host ""
  Write-LogLine ""
  Write-Section "Direct test run failed; attempting VS DevCmd fallback"
  Write-LogLine $directError.ToString()

  if ($NoVsDevCmd) { throw $directError }

  $vsDevCmd = Resolve-VsDevCmdBat
  if (-not $vsDevCmd) { throw $directError }

  $linkPath = Find-LinkExePath
  if ($linkPath) {
    Write-Host "Note: link.exe exists at:`n  $linkPath`nBut it is not on PATH for this PowerShell session."
    Write-Host ""
    Write-LogLine ("link.exe exists at: " + $linkPath)
  }

  $tempCmd = Join-Path $env:TEMP ("rust-data-processing-vs-env-{0}.cmd" -f $PID)
  $offlineLine = if ($Offline) { "set RUSTUP_OFFLINE=1" } else { "" }
  $cargoOfflineLine = if ($Offline) { "set CARGO_NET_OFFLINE=true" } else { "set CARGO_NET_OFFLINE=" }
  $cargoTestOfflineLine = if ($Offline) { "set CARGO_TEST_OFFLINE=--offline" } else { "set CARGO_TEST_OFFLINE=" }
  $cmdContent = @"
@echo off
setlocal EnableExtensions

cd /d "$repoRoot" || exit /b 1
call "$vsDevCmd" -arch=amd64 || exit /b 1

set RUSTUP_NO_UPDATE_CHECK=1
$offlineLine
$cargoOfflineLine
$cargoTestOfflineLine

if not defined WindowsSdkDir (
  echo ERROR: WindowsSdkDir not set. Install a Windows 10/11 SDK via Visual Studio Installer.
  exit /b 1
)

cargo test --locked %CARGO_TEST_OFFLINE% --lib || exit /b 1
cargo test --locked %CARGO_TEST_OFFLINE% --tests || exit /b 1
"@

  Set-Content -Path $tempCmd -Value $cmdContent -Encoding Ascii
  Write-Section "== VS DevCmd fallback run =="
  $oldEap = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    (& cmd.exe /c "`"$tempCmd`"" 2>&1) | Write-And-LogPipelineOutput
    $exitCode = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $oldEap
  }
  Remove-Item -Path $tempCmd -ErrorAction SilentlyContinue
  exit $exitCode
}
