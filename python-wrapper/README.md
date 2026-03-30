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

- **Local:** `uv run maturin build --release` (see `README_DEV.md`).
- **CI:** `.github/workflows/python_ci.yml` builds and tests on every qualifying PR/push; **`.github/workflows/python_release.yml`** uploads to **PyPI** when you push a tag **`v*`** whose commit is already on **`main`** (requires **`PYPI_API_TOKEN`**). Rust publishing uses **`.github/workflows/rust_release.yml`** and **`CRATES_IO_TOKEN`** under the same tag + **main** rule — see **`Planning/How_TO_deploy.md`**.
- **Checklist:** keep `pyproject.toml`, root `Cargo.toml`, and `python-wrapper/Cargo.toml` versions aligned — see **`Planning/RELEASE_CHECKLIST.md`** and **`Planning/How_TO_deploy.md`**.
