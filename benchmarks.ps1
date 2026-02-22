param(
  [double]$MaxMedianMsFor1M = 500,
  [switch]$Offline
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# Write all output to benchmarks.log (repo root)
$PreferredLogPath = Join-Path $PSScriptRoot 'benchmarks.log'
$LogPath = $PreferredLogPath
$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)

function Initialize-Log {
  try {
    [System.IO.File]::WriteAllText($PreferredLogPath, "", $Utf8NoBom)
    $script:LogPath = $PreferredLogPath
  } catch {
    $fallback = Join-Path $PSScriptRoot ("benchmarks_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
    [System.IO.File]::WriteAllText($fallback, "", $Utf8NoBom)
    $script:LogPath = $fallback
    Write-Host "NOTE: 'benchmarks.log' is locked; writing to '$fallback' instead."
  }
}

Initialize-Log

function Write-LogLine([string]$line) {
  try {
    [System.IO.File]::AppendAllText($LogPath, ($line + [System.Environment]::NewLine), $Utf8NoBom)
  } catch {
    if ($LogPath -eq $PreferredLogPath) {
      $fallback = Join-Path $PSScriptRoot ("benchmarks_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss"))
      [System.IO.File]::WriteAllText($fallback, "", $Utf8NoBom)
      $script:LogPath = $fallback
      Write-Host "NOTE: 'benchmarks.log' became locked; switching logs to '$fallback'."
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

function Assert-CommandExists([string]$name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command not found on PATH: $name"
  }
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
    $line
  }
}

function Invoke-LoggedAndCapture([scriptblock]$Command, [string]$OnFailMessage) {
  # Native commands (like cargo) often write non-error output to stderr.
  # In Windows PowerShell this can surface as non-terminating errors which would
  # become terminating with $ErrorActionPreference='Stop'. Temporarily relax it.
  $oldEap = $ErrorActionPreference
  $ErrorActionPreference = 'Continue'
  try {
    $lines = @(& $Command 2>&1 | Write-And-LogPipelineOutput)
    $code = $LASTEXITCODE
  } finally {
    $ErrorActionPreference = $oldEap
  }

  if ($code -ne 0) { throw "$OnFailMessage (exit code $code)" }
  return ,$lines
}

function Parse-DurationToMs([string]$s) {
  $t = $s.Trim()
  $m = [regex]::Match($t, '^(?<num>\d+(?:\.\d+)?)(?:\s*)(?<unit>\S+)$')
  if (-not $m.Success) { throw "Unrecognized duration token: '$s'" }

  $num = [double]$m.Groups['num'].Value
  $unitRaw = $m.Groups['unit'].Value
  # Drop trailing punctuation/brackets, but keep unicode letters (e.g. µ).
  $unitClean = [regex]::Replace($unitRaw, '[^A-Za-zµμ]+$', '')
  $u = $unitClean.ToLowerInvariant()

  if ($u.EndsWith('ms')) { return $num }
  if ($u.EndsWith('ns')) { return $num / 1000000.0 }
  if ($u.EndsWith('us')) { return $num / 1000.0 }
  if ($u.EndsWith('s')) {
    # Criterion prints microseconds using a unicode micro prefix + 's'. The exact micro character can vary
    # by terminal/font/encoding, so treat any "<non-ascii>s" (or any "Xs" that isn't ms/ns/us) as µs.
    if ($u -eq 's') { return $num * 1000.0 }
    return $num / 1000.0
  }

  throw "Unhandled unit: '$unitRaw'"
}

function Get-CriterionTimeTripletMs {
  param(
    [string[]]$Lines,
    [string]$BenchmarkId
  )

  $idx = -1
  $idPattern = ([regex]::Escape($BenchmarkId) + '(?!\d)')
  for ($i = 0; $i -lt $Lines.Length; $i++) {
    if ($Lines[$i] -match $idPattern) {
      $idx = $i
      break
    }
  }
  if ($idx -lt 0) { return $null }

  # Find the first "time: [a b c]" line after the benchmark id.
  for ($j = $idx; $j -lt [Math]::Min($Lines.Length, $idx + 50); $j++) {
    $line = $Lines[$j]
    if ($line -notmatch 'time:' ) { continue }

    $tokenPattern = '\d+(?:\.\d+)?\s*\S+'
    $dur = [regex]::Matches($line, $tokenPattern)
    if ($dur.Count -ge 3) {
      $lowMs = Parse-DurationToMs $dur[0].Value
      $midMs = Parse-DurationToMs $dur[1].Value
      $highMs = Parse-DurationToMs $dur[2].Value
      return [pscustomobject]@{
        low_ms = $lowMs
        mid_ms = $midMs
        high_ms = $highMs
        raw = $line.Trim()
      }
    }
  }

  return $null
}

try {
  # Avoid rustup network calls during benchmark runs unless explicitly desired.
  $env:RUSTUP_NO_UPDATE_CHECK = '1'
  if ($Offline) {
    $env:RUSTUP_OFFLINE = '1'
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

  Assert-CommandExists cargo
  Assert-CommandExists rustc

  Write-Section "== Environment =="
  ("pwd: " + (Get-Location).Path) | Write-And-LogPipelineOutput | Out-Null
  (& cargo --version 2>&1) | Write-And-LogPipelineOutput | Out-Null
  (& rustc --version 2>&1) | Write-And-LogPipelineOutput | Out-Null
  $cno = if ([string]::IsNullOrWhiteSpace($env:CARGO_NET_OFFLINE)) { "<unset>" } else { $env:CARGO_NET_OFFLINE }
  $ruo = if ([string]::IsNullOrWhiteSpace($env:RUSTUP_OFFLINE)) { "<unset>" } else { $env:RUSTUP_OFFLINE }
  ("CARGO_NET_OFFLINE: " + $cno) | Write-And-LogPipelineOutput | Out-Null
  ("RUSTUP_OFFLINE: " + $ruo) | Write-And-LogPipelineOutput | Out-Null
  Write-LogLine ""

  Write-Section "== Benchmarks (criterion): pipelines =="
  $cargoArgs = @('bench', '--bench', 'pipelines', '--locked')
  if ($Offline) { $cargoArgs += '--offline' }

  $lines = Invoke-LoggedAndCapture { cargo @cargoArgs } "cargo bench --bench pipelines failed"

  Write-Host ""
  Write-LogLine ""
  Write-Section "== Results summary =="
  Write-LogLine "PASS criteria: pipelines/filter_map_reduce_sum/1000000 median < $MaxMedianMsFor1M ms"
  Write-Host "PASS criteria: pipelines/filter_map_reduce_sum/1000000 median < $MaxMedianMsFor1M ms"

  # Baseline reference (from Feb 21, 2026 run).
  $baseline = @{
    '10000'   = @{ low_ms = 0.51876; mid_ms = 0.52445; high_ms = 0.53044 }
    '100000'  = @{ low_ms = 11.461;  mid_ms = 11.721;  high_ms = 12.032 }
    '1000000' = @{ low_ms = 105.13;  mid_ms = 107.94;  high_ms = 111.66 }
  }

  foreach ($n in @('10000','100000','1000000')) {
    $id = "pipelines/filter_map_reduce_sum/$n"
    $triplet = Get-CriterionTimeTripletMs -Lines $lines -BenchmarkId $id
    if (-not $triplet) {
      Write-Host "WARN: Could not parse Criterion timing for $id"
      Write-LogLine "WARN: Could not parse Criterion timing for $id"
      continue
    }

    $base = $baseline[$n]
    $line = "{0,-36} median={1,8:N3} ms (low={2:N3}, high={3:N3}) | baseline median={4:N3} ms" -f `
      $id, $triplet.mid_ms, $triplet.low_ms, $triplet.high_ms, $base.mid_ms
    Write-Host $line
    Write-LogLine $line
  }

  $oneM = Get-CriterionTimeTripletMs -Lines $lines -BenchmarkId 'pipelines/filter_map_reduce_sum/1000000'
  if (-not $oneM) {
    Write-Host ""
    Write-LogLine ""
    Write-Host "FAIL: Could not parse timing for pipelines/filter_map_reduce_sum/1000000"
    Write-LogLine "FAIL: Could not parse timing for pipelines/filter_map_reduce_sum/1000000"
    exit 2
  }

  $pass = ($oneM.mid_ms -lt $MaxMedianMsFor1M)
  Write-Host ""
  Write-LogLine ""
  if ($pass) {
    $msg = "PASS: 1,000,000 rows median {0:N3} ms < {1:N3} ms" -f $oneM.mid_ms, $MaxMedianMsFor1M
    Write-Host $msg
    Write-LogLine $msg
    exit 0
  } else {
    $msg = "FAIL: 1,000,000 rows median {0:N3} ms >= {1:N3} ms" -f $oneM.mid_ms, $MaxMedianMsFor1M
    Write-Host $msg
    Write-LogLine $msg
    exit 1
  }
} catch {
  Write-Host ""
  Write-LogLine ""
  Write-Host ("FAIL: " + $_.ToString())
  Write-LogLine ("FAIL: " + $_.ToString())
  exit 1
}

