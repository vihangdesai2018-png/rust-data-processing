# Release checklist (crates.io + PyPI)

Use this when shipping **both** the Rust crate and the **Python** package (`python-wrapper/`). For Rust-only or Python-only hotfixes, follow the relevant sections only.

## 1) Version alignment (do this first)

Bump **all** of these to the **same** SemVer (e.g. `0.2.0`):

| File | Field |
|------|--------|
| **`Cargo.toml`** (repo root) | `[package] version` |
| **`python-wrapper/pyproject.toml`** | `[project] version` |
| **`python-wrapper/Cargo.toml`** | `[package] version` |

PyPI uses **`pyproject.toml`** as the distribution version; the extension crate version should match for maintainability.

## 2) Changelog + CI

1. Add a section for the new version in **`CHANGELOG.md`** ([Keep a Changelog](https://keepachangelog.com/)).
2. Open a PR; ensure **GitHub Actions** are green:
   - **`Rust CI`** (`.github/workflows/rust_ci.yml` — fmt, clippy, tests, ubuntu `--features ci_expanded`)
   - **`Python wrapper CI`** (maturin + pytest)
   - **`Documentation`** (`.github/workflows/docs.yml` — rustdoc + pdoc build; on `main`, refreshes GitHub Pages if configured — see [`Planning/DOCUMENTATION.md`](DOCUMENTATION.md))
3. Merge to **`main`**.

## 3) Publish Rust crate (crates.io)

**Preferred:** after merging to **`main`**, push tag **`v*`** (use **`./scripts/release_tag.ps1 X.Y.Z`** from §4) — **`rust_release.yml`** publishes via **`CRATES_IO_TOKEN`**.

**Manual alternative** (from repo root, after merge):

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo publish --dry-run
cargo publish
```

`cargo publish` fails if that version already exists — bump and repeat. If you use **CI** for this version, do not also run **`cargo publish`** locally.

## 4) Publish Python package (PyPI)

### One-time setup (GitHub secrets)

Add **two** repository secrets under **Settings → Secrets and variables → Actions** (step-by-step: **`Planning/How_TO_deploy.md`** § *GitHub: add secrets for crates.io and PyPI*). Once **`CRATES_IO_TOKEN`** and **`PYPI_API_TOKEN`** are set, tagged releases can publish without further token setup.

| Secret name | Used by | Created at |
|-------------|---------|------------|
| **`CRATES_IO_TOKEN`** | **`.github/workflows/rust_release.yml`** | [crates.io](https://crates.io) → account → API tokens |
| **`PYPI_API_TOKEN`** | **`.github/workflows/python_release.yml`** | [pypi.org](https://pypi.org) → account → API tokens |

### On each release (CI publish — recommended)

Releases are **not** automatic on every merge: you choose when to cut a tag after **`main`** already contains the version bump and green CI.

Use **one** flow: complete steps **1–2** (versions + changelog + merge to **`main`**), then run the release script (or the raw git commands below). Do **not** also `cargo publish` locally for the same version, or the GitHub job will fail with “already uploaded”.

1. Confirm **`python-wrapper/pyproject.toml`** version matches the Rust crate (step 1).
2. Merge your release PR into **`main`** and ensure CI is green. Push **`main`** to **`origin`** so `HEAD` matches **`origin/main`**.
3. From repo root, on **`main`**, create and push the version tag (script **verifies** the three package versions match the argument unless you pass **`-SkipVersionCheck`**):

   ```powershell
   ./scripts/release_tag.ps1 0.2.0
   ```

   Optional: **`-WhatIf`** to print the git commands without tagging; **`-AllowDirty`** if you intentionally have a dirty tree (not recommended).

   Equivalent manual commands:

   ```bash
   git fetch origin main
   git checkout main && git pull --ff-only origin main
   git tag -a v0.2.0 -m "Release v0.2.0"
   git push origin v0.2.0
   ```

4. **Actions** runs:
   - **`rust_release.yml`** — verifies the tag is on **`origin/main`**, then **`cargo publish --locked`**.
   - **`python_release.yml`** — same guard, then builds wheels and uploads to **PyPI**.

Tags pointing at commits **not** on **`main`** are rejected (no publish).

### Manual `cargo publish` (optional)

If you publish the Rust crate from your machine instead of **`rust_release.yml`**, skip pushing a tag until you are ready, or disable that workflow temporarily — avoid double-publishing the same version.

### Local dry run (optional)

```bash
cd python-wrapper
uv run maturin build --release -o dist --find-interpreter --sdist
# Inspect python-wrapper/dist/*.whl and .tar.gz
```

### Custom wheels (optional)

- **DB ingestion**: rebuild with **`maturin build --features db`** (see **`python-wrapper/README_DEV.md`**). Default CI wheels do **not** enable **`db`** unless you change **`[tool.maturin]`** / workflow args.

## 5) After publish

1. **GitHub Releases** — create a release for tag **`vX.Y.Z`**; paste **`CHANGELOG`** notes.
2. **docs.rs** — updates automatically for the published Rust version.
3. **PyPI** — verify the new version appears and `pip install rust-data-processing==X.Y.Z` works.

## Reference

- **`Planning/How_TO_deploy.md`** — packaging details, CI matrix, **`abi3`** note, feature flags.
- **`python-wrapper/PARITY.md`** — Rust ↔ Python API matrix.
