# Python wrapper — developer guide

## Layout

| Path | Role |
|------|------|
| `Cargo.toml` | PyO3 extension crate (`_rust_data_processing`); depends on `rust-data-processing` via `path = ".."` |
| `src/lib.rs` | `#[pymodule]`, `DataSet`, `DataFrame`, `SqlContext`, `ExecutionEngine`, processing + SQL + reports |
| `src/convert.rs` | Shared Python ↔ Rust parsing (`schema`, validation spec, profile/outlier options, …) |
| `pyproject.toml` | PEP 517 build (`maturin`), project metadata, **uv** `dependency-groups.dev` |
| `rust_data_processing/__init__.py` | Stable imports + JSON helpers (`profile_dataset`, `transform_apply`, …) |
| `tests/` | `pytest`: smoke, bindings, SQL / deep / observability / mapping / ingestion parity, benchmarks |
| `scripts/*.ps1` | Windows: `Run-UnitTests`, `Run-DeepTests`, `Run-BenchmarkTests` |

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

Requires a successful `maturin develop` (or install) so `_rust_data_processing` exists. Tests use **only** the Python package (they do not shell out to `cargo test`), so they exercise the PyO3 surface.

```bash
uv run pytest
```

- Quick pass (skip slow deep + timing tests):  
  `uv run pytest -m "not deep and not benchmark"`
- Deep parity (repo `tests/fixtures/deep`, mirrors `tests/deep_tests.rs`):  
  `uv run pytest -m deep`
- Benchmarks (`pytest-benchmark`, mirrors Criterion-style workloads):  
  `uv run pytest -m benchmark`

On Windows, use **`scripts/Run-UnitTests.ps1`**, **`scripts/Run-DeepTests.ps1`**, and **`scripts/Run-BenchmarkTests.ps1`** (see `scripts/README.md`; optional `-Build` runs `maturin develop --release` first).

## Rust features

The extension enables **`excel`** on the path dependency so spreadsheet ingestion matches common Python expectations. SQL (Polars-backed) remains on via the library default features.

To change features, edit `python-wrapper/Cargo.toml` under `[dependencies] rust-data-processing`.

### Optional: DB ingestion (`ConnectorX`)

The extension crate defines a Cargo feature **`db`** that enables `db_connectorx` on the parent crate (large dependency graph).

```bash
uv run maturin develop --release --features db
# or
cargo build --release --features db
```

Without it, `ingest_from_db` / `ingest_from_db_infer` are still exported but return the same “disabled” error as Rust unless you rebuild with `db`.

### Ingestion observers (Python)

Path-based ingest `options` may include `observer` (`on_success`, `on_failure`, `on_alert`) and `alert_at_or_above`, matching `IngestionOptions` in Rust. See `API.md`.

## Versioning

Keep `python-wrapper/pyproject.toml` `[project] version` aligned with the Rust crate version you are binding until the Python package is published independently.

## Crates.io vs path

Local development always uses the workspace-adjacent crate via `path = ".."`. Publishing the Python package does **not** replace publishing the Rust crate to crates.io; PyPI wheels bundle the compiled extension linked against that source tree at build time.
