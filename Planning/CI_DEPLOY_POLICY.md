# CI and deploy policy (Phase 1a)

This document records the **branching**, **CI**, and **registry publish** choices for this repo (see `PHASE1A_PLAN.md` §4–5).

## Branching

- **One branch per small story** (roughly 1–2 hours of work), merged via **PR into `main`**.
- Keep PRs reviewable: single workflow change, single doc update, or a tight feature slice.

## CI: what runs when

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **`.github/workflows/rust_ci.yml`** | PRs + push to **`main`** | `cargo fmt`, `cargo clippy` (warnings allowed), `cargo test` + doctests on **ubuntu + Windows**; **ubuntu** `cargo test --features ci_expanded` (`db_connectorx` excluded — OpenSSL/Perl; test DB locally) |
| **`.github/workflows/python_ci.yml`** | PRs + push to **`main`** (path-filtered) | `maturin develop` + pytest |
| **`.github/workflows/rust_release.yml`** | Push tag **`v*`** | Publish to **crates.io** (only if tag is on **`main`** — see below) |
| **`.github/workflows/python_release.yml`** | Push tag **`v*`** | Publish to **PyPI** (same **main** guard) |

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
