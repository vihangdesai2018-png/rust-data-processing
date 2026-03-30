#!/usr/bin/env pwsh
<#
.SYNOPSIS
  Create an annotated version tag on main and push it to origin (triggers crates.io + PyPI workflows).

.DESCRIPTION
  Run this only after version bumps, CHANGELOG, and merge to main are done (see Planning/RELEASE_CHECKLIST.md).
  CI requires the tag to point at a commit that is an ancestor of origin/main.

.PARAMETER Version
  SemVer without or with a leading v (e.g. 0.2.0 or v0.2.0). The git tag will always be vX.Y.Z.

.PARAMETER Remote
  Git remote name (default: origin).

.PARAMETER MainBranch
  Main branch name (default: main).

.PARAMETER SkipVersionCheck
  Do not verify root and python-wrapper Cargo/pyproject versions match the requested release.

.PARAMETER AllowDirty
  Allow uncommitted changes (default: require a clean working tree).

.EXAMPLE
  ./scripts/release_tag.ps1 0.2.0

.EXAMPLE
  ./scripts/release_tag.ps1 v0.2.0 -WhatIf
#>
[CmdletBinding(SupportsShouldProcess)]
param(
  [Parameter(Mandatory = $true, Position = 0)]
  [string]$Version,

  [string]$Remote = 'origin',
  [string]$MainBranch = 'main',
  [switch]$SkipVersionCheck,
  [switch]$AllowDirty
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Normalize-ReleaseVersion([string]$raw) {
  $t = $raw.Trim()
  if ([string]::IsNullOrWhiteSpace($t)) {
    throw 'Version must not be empty.'
  }
  if ($t -match '^v(.+)$') {
    $t = $Matches[1]
  }
  if ($t -notmatch '^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$') {
    throw "Version '$raw' does not look like SemVer (e.g. 0.2.0 or 0.2.0-beta.1)."
  }
  return $t
}

function Get-PackageVersionFromToml([string]$path, [string]$sectionMarker) {
  $lines = Get-Content -LiteralPath $path
  $inSection = $false
  foreach ($line in $lines) {
    if ($line -match '^\s*\[') {
      $inSection = ($line.Trim() -eq $sectionMarker)
      continue
    }
    if ($inSection -and $line -match '^\s*version\s*=\s*"([^"]+)"') {
      return $Matches[1]
    }
  }
  throw "Could not find version under $sectionMarker in $path"
}

function Get-PyProjectVersion([string]$path) {
  $lines = Get-Content -LiteralPath $path
  $inProject = $false
  foreach ($line in $lines) {
    if ($line -match '^\s*\[project\]') {
      $inProject = $true
      continue
    }
    if ($line -match '^\s*\[') {
      $inProject = $false
      continue
    }
    if ($inProject -and $line -match '^\s*version\s*=\s*"([^"]+)"') {
      return $Matches[1]
    }
  }
  throw "Could not find [project] version in $path"
}

function Invoke-Git {
  param([Parameter(Position = 0)][string[]]$Args)
  $argStr = $Args -join ' '
  & git @Args
  if ($LASTEXITCODE -ne 0) {
    throw "git $argStr failed with exit $LASTEXITCODE"
  }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $repoRoot

$semVer = Normalize-ReleaseVersion $Version
$tagName = "v$semVer"

$pyRoot = Join-Path $repoRoot 'python-wrapper'

if (-not $SkipVersionCheck) {
  $rootVer = Get-PackageVersionFromToml (Join-Path $repoRoot 'Cargo.toml') '[package]'
  $pyVer = Get-PyProjectVersion (Join-Path $pyRoot 'pyproject.toml')
  $pyCargoVer = Get-PackageVersionFromToml (Join-Path $pyRoot 'Cargo.toml') '[package]'

  if ($rootVer -ne $semVer -or $pyVer -ne $semVer -or $pyCargoVer -ne $semVer) {
    throw @"
Version mismatch. Requested release '$semVer' but repo has:
  Cargo.toml (root):            $rootVer
  python-wrapper/pyproject.toml: $pyVer
  python-wrapper/Cargo.toml:     $pyCargoVer
Bump all three to '$semVer' (see Planning/RELEASE_CHECKLIST.md §1) or pass -SkipVersionCheck.
"@
  }
}

& git rev-parse --git-dir 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
  throw 'Not a git repository (git rev-parse --git-dir failed).'
}

$status = git status --porcelain
if (-not $AllowDirty -and $status) {
  throw "Working tree is dirty. Commit or stash changes, or pass -AllowDirty.`n$status"
}

$branch = (git branch --show-current).Trim()
if ($branch -ne $MainBranch) {
  throw "Must be on branch '$MainBranch' (currently on '$branch')."
}

Write-Host "== fetch $Remote $MainBranch =="
Invoke-Git @('fetch', $Remote, $MainBranch)

Write-Host "== pull $Remote $MainBranch (ff-only) =="
Invoke-Git @('pull', '--ff-only', $Remote, $MainBranch)

$upstream = "$Remote/$MainBranch"
$head = (git rev-parse HEAD).Trim()
$mainSha = (git rev-parse $upstream).Trim()
if ($head -ne $mainSha) {
  throw @"
HEAD does not match $upstream.
  Local:  $head
  Remote: $mainSha
Push or reset so you release exactly what is on $upstream (push local main first if you are ahead).
"@
}

git merge-base --is-ancestor $head $mainSha | Out-Null
if ($LASTEXITCODE -ne 0) {
  throw "Tagged commit is not an ancestor of $upstream (unexpected)."
}

Write-Host "== verify tag $tagName does not exist =="
$existing = git tag -l $tagName
if ($existing) {
  throw "Tag '$tagName' already exists locally. Delete only if it was not pushed: git tag -d $tagName"
}

$ref = "refs/tags/$tagName"
$remoteTag = git ls-remote $Remote $ref
if ($remoteTag) {
  throw "Tag '$tagName' already exists on $Remote. Use a new version."
}

$msg = "Release $tagName"
if (-not $PSCmdlet.ShouldProcess("$Remote $tagName", "Create annotated tag and push")) {
  Write-Host "WhatIf: would run: git tag -a $tagName -m `"$msg`"; git push $Remote $tagName"
  exit 0
}

Write-Host "== tag $tagName =="
Invoke-Git @('tag', '-a', $tagName, '-m', $msg)

Write-Host "== push $Remote $tagName =="
Invoke-Git @('push', $Remote, $tagName)

Write-Host "Done. CI should run rust_release.yml and python_release.yml for $tagName."
