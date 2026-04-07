# Python bindings documentation index

- **[README.md](../README.md)** — user-facing quick start
- **[API.md](../API.md)** — Python API reference (bound surface)
- **[PARITY.md](../PARITY.md)** — Rust ↔ Python parity matrix
- **[README_DEV.md](../README_DEV.md)** — maturin, uv, tests, packaging

## HTML API docs (pdoc)

CI builds browsable HTML on each push to `main` (combined with Rust rustdoc). Entry point:

- [GitHub Pages — Python API (pdoc)](https://vihangdesai2018-png.github.io/rust-data-processing/python/) — landing + Rust rustdoc: [site root](https://vihangdesai2018-png.github.io/rust-data-processing/)
- **Examples** in pdoc come from the repo’s [`docs/python/README.md`](../../docs/python/README.md) (included via `rust_data_processing.examples`). Published URL: `python/examples.html` (copy of the module page).

Local build (from `python-wrapper/` after `uv sync --group dev`):

```bash
uv run maturin develop --release
uv run pdoc -d google -o ../_site/python rust_data_processing rust_data_processing.examples
```

From the repository root (PowerShell), the same output is produced by `./scripts/build_docs.ps1 -All` → `_site/python/index.html`.

Rust-side reference: repository root **`API.md`**, **`README.md`**, and **`docs/REDUCE_AGG_SEMANTICS.md`** (aggregate semantics). The Python package mirrors the main crate APIs where practical; gaps are listed in **`PARITY.md`** and optionally in a local **`Planning/PHASE1A_PLAN.md`** if you maintain one.

Maintainer overview of doc hosting: **`docs/DOCUMENTATION.md`**.
