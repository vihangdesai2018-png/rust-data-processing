# Documentation builds and hosting

End-user readable API documentation is produced in two ways: **Rust** via **rustdoc** (`cargo doc`) and **Python** via **pdoc**. CI assembles both into a single static site deployed to **GitHub Pages** on pushes to `main`.

## Published URLs

| Audience | What | URL |
| --- | --- | --- |
| Rust (released crate) | docs.rs for the version published on crates.io | [docs.rs/rust-data-processing](https://docs.rs/rust-data-processing) |
| Rust + Python (main branch) | Combined site from CI (requires Pages setup below) | `https://<owner>.github.io/<repo>/` — for this repo: [rust-data-processing GitHub Pages](https://vihangdesai2018-png.github.io/rust-data-processing/) |

Until the first successful **crates.io** publish, docs.rs may be empty; use the **GitHub Pages** link for the latest **main** rustdoc.

## CI workflow

- Workflow file: [`.github/workflows/docs.yml`](../.github/workflows/docs.yml).
- **On every pull request:** builds rustdoc and Python pdoc; does **not** deploy.
- **On push to `main`:** builds the same artifacts and **deploys** to GitHub Pages using the official `actions/deploy-pages` flow.

Rust steps: `cargo doc --no-deps --locked` → output copied to `site/rust/`.

Python steps (in `python-wrapper/`): `uv sync --group dev`, `maturin develop --release`, then `pdoc -d google -o …/site/python rust_data_processing`.

The landing page is committed at [`landing/index.html`](landing/index.html) and copied to `site/index.html` during the assemble step.

## One-time GitHub Pages setup (maintainers)

1. Repo **Settings → Pages**.
2. Under **Build and deployment**, set **Source** to **GitHub Actions** (not “Deploy from a branch”).
3. Merge a workflow that deploys via `actions/deploy-pages` (already present in `docs.yml`). The first successful run on `main` publishes the site.

If Pages is not configured, the **Documentation** workflow should still go green for **build** jobs; **deploy** will fail until Settings are updated.

## Local builds

### Rust only (Windows / PowerShell)

```powershell
./scripts/build_docs.ps1
```

Output: `target/doc/` — open `target/doc/rust_data_processing/index.html`.

### Rust + Python site (mirror of CI)

```powershell
./scripts/build_docs.ps1 -All
```

Then:

- Rust: `target/doc/rust_data_processing/index.html`
- Python: `_site/python/index.html` (under repo root, created by the script)

### Manual Python pdoc (from `python-wrapper/`)

```bash
uv sync --group dev
uv run maturin develop --release
uv run pdoc -d google -o ../_site/python rust_data_processing
```

## Issue triage and reporting

See [ISSUE_TRIAGE.md](ISSUE_TRIAGE.md) and root [README.md § Reporting bugs](../README.md#reporting-bugs).
