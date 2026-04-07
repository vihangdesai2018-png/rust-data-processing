# Deploy: crates.io + PyPI (Rust + Python wrapper)

This project is a Rust library (**crates.io**) and a Python package (**PyPI**) built with **PyO3 + maturin** from **`python-wrapper/`** (no `setup.py`).

---

## Deploy to crates.io (Rust)

Full checklist: **[`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md)** (Rust + Python alignment).

### Prerequisites

- Root **`Cargo.toml`**: correct **name**, **version**, **description**, **license** (`MIT OR Apache-2.0`), **repository**, **readme**, **keywords**, **categories**, **rust-version**
- **`LICENSE-MIT`**, **`LICENSE-APACHE`**
- **`CHANGELOG.md`** updated for the release
- Local: `cargo fmt --check`, `cargo clippy`, `cargo test`; `cargo publish --dry-run` succeeds

### Publish

```bash
cargo login <CRATES_IO_TOKEN>   # one-time per machine
cargo publish --dry-run
cargo publish
```

### After publish

- Tag the release (e.g. `v0.1.0`) and push the tag (see release checklist).
- docs.rs builds automatically for the published crate version.

---

## Deploy to PyPI (Python wrapper)

### Layout (Phase 1a)

| Path | Role |
|------|------|
| **`python-wrapper/pyproject.toml`** | PEP 517 metadata, `[tool.maturin]`, Python version / classifiers |
| **`python-wrapper/Cargo.toml`** | PyO3 extension crate (`cdylib` → `rust_data_processing._rust_data_processing`) |
| **`python-wrapper/rust_data_processing/`** | Pure Python surface (`__init__.py`, …) |

The extension links the workspace crate **`rust-data-processing`** via `path = ".."`.

### Local development (editable install)

From **`python-wrapper/`** (with **uv** + Rust toolchain):

```bash
uv sync --group dev
uv run maturin develop --release
```

Rebuild after Rust or PyO3 changes.

### Build wheels + sdist locally

From **`python-wrapper/`**:

```bash
uv run maturin build --release
```

Artifacts default to **`python-wrapper/target/wheels/`** (or `dist/` if you pass `-o dist`).

### CI (GitHub Actions)

| Workflow | When | What |
|----------|------|------|
| **`.github/workflows/rust_ci.yml`** | PRs + pushes to **`main`** | **`cargo fmt`**, **`clippy`**, **`cargo test`** (incl. doctests) on **ubuntu + windows**; **ubuntu** **`cargo test --features ci_expanded`** (not **`db_connectorx`** — OpenSSL/Perl; test DB locally) |
| **`.github/workflows/python_ci.yml`** | PRs + pushes to `main` (when wrapper / library / lockfile change) | `uv` + `maturin develop --release` + `pytest` on **ubuntu / windows / macOS** × **3.11 / 3.12**; **ubuntu + 3.12** also **`maturin build`** + **`uv pip install`** wheel + import smoke |
| **`.github/workflows/rust_release.yml`** | Push tag **`v*`** | **Guard:** tag must point at a commit on **`origin/main`** → **`cargo publish --locked`** → **crates.io** |
| **`.github/workflows/python_release.yml`** | Push tag **`v*`** | **Same guard** → **`PyO3/maturin-action`** wheels + **PyPI** |

#### Release policy: merge to `main`, then tag

Publishing to **crates.io** and **PyPI** does **not** run on every push to `main`. It runs when you push a **version tag** (`v0.2.0`, …), and **only if** that tag’s commit is already an **ancestor of `origin/main`** (i.e. you merged the release PR to `main` first, then tagged that merge commit). Tags on commits that never reached `main` are rejected.

### GitHub: add secrets for crates.io and PyPI

Do this once per GitHub repository (or when rotating tokens).

#### A) Open the secrets page

1. On GitHub, open your repo (**e.g.** `your-org/rust-data-processing`).
2. **Settings** (repo settings, not your profile).
3. In the left sidebar: **Secrets and variables** → **Actions**.
4. **New repository secret** (repeat for each secret below).

#### B) **crates.io** token → secret `CRATES_IO_TOKEN`

1. Sign in to **[crates.io](https://crates.io)** (same account that owns or is allowed to publish the crate).
2. Click your avatar → **Account Settings**.
3. Open **API Tokens** (or **Publish** / **API** depending on UI).
4. **New Token** — choose a name (e.g. `github-actions-rust-data-processing`).
5. Set permissions so it can **publish** this crate (for a new crate, the first successful `cargo publish` still requires you to be logged in as the crate owner).
6. **Generate** and **copy the token** (it may only be shown once).

7. Back in GitHub **Actions** secrets:
   - **Name:** `CRATES_IO_TOKEN`
   - **Secret:** paste the crates.io token.

The workflow **`rust_release.yml`** sets `CARGO_REGISTRY_TOKEN` from this secret for `cargo publish`.

#### C) **PyPI** token → secret `PYPI_API_TOKEN`

1. Sign in to **[pypi.org](https://pypi.org)**.
2. **Account settings** → **API tokens** → **Add API token**.
3. **Token name** (e.g. `github-actions-rust-data-processing`).
4. **Scope:** prefer a **project-scoped** token for the `rust-data-processing` project once the project exists; for the very first upload you may need a user-wide token, then narrow scope later.
5. **Create** and **copy the token** (starts with `pypi-`).

6. In GitHub **Actions** secrets:
   - **Name:** `PYPI_API_TOKEN`
   - **Secret:** paste the PyPI token.

The workflow **`python_release.yml`** passes this to **`pypa/gh-action-pypi-publish`** (PyPI expects API-token auth via this pattern).

#### D) After secrets exist

1. Merge your release to **`main`** (version bumps + changelog, CI green).
2. Create and push the tag on **`main`** (see **[`RELEASE_CHECKLIST.md`](RELEASE_CHECKLIST.md)**):

   ```bash
   git fetch origin main
   git checkout main && git pull origin main
   git tag -a v0.2.0 -m "Release v0.2.0"
   git push origin v0.2.0
   ```

3. Watch **Actions**: **Rust (crates.io release)** and **Python (PyPI release)** should succeed. If a secret is missing or wrong, the publish step fails with an auth error.

**Optional:** [PyPI Trusted Publishing](https://docs.pypi.org/trusted-publishers/) (OIDC) avoids long-lived **`PYPI_API_TOKEN`**; configure it on PyPI and replace the password step in **`python_release.yml`** per PyPI’s guide.

### Optional: `abi3` / stable Python ABI

**Not enabled** today. Turning on **`abi3`** (e.g. PyO3 `abi3-py310`) reduces the number of wheels (one binary can target multiple Python versions) but **restricts** which PyO3 APIs you may use. If you adopt it later:

- Add the appropriate **`pyo3`** features in **`python-wrapper/Cargo.toml`**
- Set **`[tool.maturin] py-limited-api = true`** (and compatible `requires-python`) per [maturin docs](https://www.maturin.rs/limitations.html)

---

## Version alignment (Phase 1a)

Keep these **in sync** for each release:

| Location | Field |
|----------|--------|
| Repo root **`Cargo.toml`** | `[package] version` |
| **`python-wrapper/pyproject.toml`** | `[project] version` |
| **`python-wrapper/Cargo.toml`** | `[package] version` (extension crate; should match the Python package) |

The **Python distribution version** on PyPI is taken from **`pyproject.toml`**.

### Feature flags in wheels

Default **GitHub** wheel builds use **`python-wrapper/Cargo.toml`** defaults ( **`excel`** enabled on the path dependency; **DB** is **not** enabled unless you add **`features = ["db"]`** under **`[tool.maturin]`** or pass **`--features db`** to **`maturin build`**).

Document in **`python-wrapper/README_DEV.md`** and **`API.md`** which optional capabilities need a **custom wheel build**.

---

## CI / deploy policy

Branching, when workflows run, and why we use **tag + `main`** for publishes: **[`CI_DEPLOY_POLICY.md`](CI_DEPLOY_POLICY.md)**.

## Reporting bugs

- Use **GitHub Issues** (link from the repo **README**).
- Optional: add issue templates under **`.github/ISSUE_TEMPLATE/`**.
