# Python wrapper — developer guide

## Layout

| Path | Role |
|------|------|
| `Cargo.toml` | PyO3 extension crate (`_rust_data_processing`); depends on `rust-data-processing` via `path = ".."` |
| `src/lib.rs` | `#[pymodule]` and `#[pyclass]` bindings |
| `pyproject.toml` | PEP 517 build (`maturin`), project metadata, **uv** `dependency-groups.dev` |
| `rust_data_processing/__init__.py` | Stable Python import surface |
| `tests/` | `pytest` smoke tests |

## Tooling with uv

```bash
cd python-wrapper
uv sync --group dev
```

## Build / install editable

```bash
uv run maturin develop --release
```

Rebuild after Rust changes. For a one-off wheel:

```bash
uv run maturin build --release
```

## Tests

Requires a successful `maturin develop` (or install) so `_rust_data_processing` exists:

```bash
uv run pytest
```

## Rust features

The extension enables **`excel`** on the path dependency so spreadsheet ingestion matches common Python expectations. SQL (Polars-backed) remains on via the library default features.

To change features, edit `python-wrapper/Cargo.toml` under `[dependencies] rust-data-processing`.

## Versioning

Keep `python-wrapper/pyproject.toml` `[project] version` aligned with the Rust crate version you are binding until the Python package is published independently.

## Crates.io vs path

Local development always uses the workspace-adjacent crate via `path = ".."`. Publishing the Python package does **not** replace publishing the Rust crate to crates.io; PyPI wheels bundle the compiled extension linked against that source tree at build time.
