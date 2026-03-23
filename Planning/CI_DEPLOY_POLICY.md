# CI and deploy policy (Phase 1a)

This document records the **branching**, **CI**, and **registry publish** choices for this repo (see `PHASE1A_PLAN.md` §4–5).

## Branching

- **One branch per small story** (roughly 1–2 hours of work), merged via **PR into `main`**.
- Keep PRs reviewable: single workflow change, single doc update, or a tight feature slice.

## CI: what runs when

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **`.github/workflows/rust_ci.yml`** | PRs + push to **`main`** (+ nightly schedule) | **Required merge gate**: `cargo fmt`, `cargo clippy` (warnings allowed), `cargo test` + doctests on **ubuntu + Windows**; **ubuntu** `cargo test --features ci_expanded` (`db_connectorx` excluded — OpenSSL/Perl). **Security**: dependency diff review on PRs + RustSec `cargo audit`. |
| **`.github/workflows/python_ci.yml`** | PRs + push to **`main`** (path-filtered) | `maturin develop` + pytest; **Ubuntu + Py 3.12:** `maturin build` + `uv pip install` wheel smoke |
| **`.github/workflows/rust_release.yml`** | Push tag **`v*`** | Publish to **crates.io** (only if tag is on **`main`** — see below) |
| **`.github/workflows/python_release.yml`** | Push tag **`v*`** | Publish to **PyPI** (same **main** guard) |

## “No merges to main if checks fail” (required branch protection)

GitHub only blocks merges when **branch protection / rulesets** require passing checks.

### Required checks to enable for `main`

In **Settings → Branches → Branch protection rules** (or **Rulesets**), require status checks to pass before merging, and select:
- `Security — dependency review (PR)`
- `Security — RustSec cargo audit`
- `ubuntu-latest — fmt, clippy, tests`
- `windows-latest — fmt, clippy, tests`
- `ubuntu — ci_expanded (no db_connectorx)`

Recommended additional toggles:
- **Require branches to be up to date before merging** (so the latest `main` is included in the tested commit)
- **Require a pull request before merging** (no direct pushes)

## Deploy / registry policy (chosen: **A + main guard**)

We evaluated:

| Option | Summary |
|--------|---------|
| **A — Tagged release** | Publish when a **version tag** is pushed (typical, explicit). |
| **B — Merge to `main` + version heuristic** | Publish on every merge if version changed / not on registry — easy to mis-trigger; needs robust “already published” detection. |
| **C — TestPyPI on merge, PyPI on tag** | More moving parts; deferred unless we need a staging index. |

**Outcome:** **Option A** — publish **crates.io** and **PyPI** only when a **`v*`** tag is pushed, with an extra guard: the tagged commit must already be an **ancestor of `origin/main`** (merge the release PR to **`main`**, then tag that commit). This avoids publishing from tags cut on feature branches.

### crates.io constraints

- The **same version cannot be published twice**; a failed publish after a partial success is rare but `cargo publish` is mostly atomic.
- **Automation:** GitHub Actions uses **`CRATES_IO_TOKEN`** → **`CARGO_REGISTRY_TOKEN`** (see `How_TO_deploy.md`).
- **“Already published”:** `cargo publish` fails with an error from the registry; no separate probe step is required for Phase 1a.

### PyPI constraints

- Same idea: **version must increase** (or use a post-release segment you accept).
- **Automation:** **`PYPI_API_TOKEN`** in GitHub Secrets.

## Release checklist pointer

Concrete steps (version bumps, tag, secrets): **`Planning/RELEASE_CHECKLIST.md`**.
