# rust-data-processing (Python)

Python bindings for the [`rust-data-processing`](../README.md) crate: schema-first ingestion from CSV, JSON, Parquet, and Excel into an in-memory **`DataSet`**.

This directory is the **only** supported place for Python packaging, docs, and dev workflow for the bindings.

## Requirements

- **Python** 3.10+
- **Rust** toolchain matching the parent crate (`rust-version` in the repo root `Cargo.toml`)
- **[uv](https://docs.astral.sh/uv/)** (recommended) or pip + virtualenv

## Install (editable, local)

From this directory:

```bash
uv sync --group dev
uv run maturin develop --release
```

Then:

```python
import rust_data_processing as rdp

ds = rdp.ingest_from_path_infer("../tests/fixtures/people.csv")
print(ds.row_count())

# Same crate powers reduce, SQL, and lazy pipelines (see API.md):
print(rdp.processing_reduce(ds, "score", "mean"))
print(rdp.sql_query_dataset(ds, "SELECT id FROM df WHERE score > 90").row_count())
subset = rdp.DataFrame.from_dataset(ds).select(["id", "name"]).collect()
```

Full surface area: [API.md](./API.md) (ingestion, `processing_*`, `DataFrame`, `SqlContext`, SQL, transform JSON, profiling, validation, outliers, `ExecutionEngine`).

## Documentation

| Doc | Purpose |
|-----|---------|
| [API.md](./API.md) | Python API overview |
| [PARITY.md](./PARITY.md) | Rust ↔ Python parity matrix |
| [README_DEV.md](./README_DEV.md) | Build, test, and packaging notes |
| [docs/README.md](./docs/README.md) | Doc index |

## License

Same as the Rust crate: **MIT OR Apache-2.0**. Full license texts live in the repository root (`LICENSE-MIT`, `LICENSE-APACHE`).

## Publishing wheels

Use [maturin](https://www.maturin.rs/) (see `README_DEV.md`). Prebuilt wheels for Windows, macOS, and Linux are a Phase 1 goal; CI wiring may follow separately.
