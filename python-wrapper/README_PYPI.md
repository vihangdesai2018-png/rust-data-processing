# rust-data-processing

![Phase 1 scope: sources → rust-data-processing → Python / optional AI & ML surfaces](https://raw.githubusercontent.com/vihangdesai2018-png/rust-data-processing/main/docs/images/phase-1-scope-overview.png)

Python bindings for the **[rust-data-processing](https://docs.rs/rust-data-processing)** crate: schema-first ingestion from CSV, JSON, Parquet, and Excel into an in-memory **`DataSet`**, with profiling, validation, Polars-backed pipelines, and SQL.

*Infographic: Phase 1 — single-node, library-first flow (ingest → `DataSet`, pipelines, SQL, profile, validate, outliers, transforms, parallel execution, PyO3 bindings, optional chatbot / notebook story).*

This page is the **PyPI** project description (Python-only). Clone the [repository](https://github.com/vihangdesai2018-png/rust-data-processing) for developer setup, Rust sources, and the full monorepo README.

## Install

```bash
pip install rust-data-processing
```

Requires **Python 3.10+**.

## Quick start

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "name", "data_type": "utf8"},
]
ds = rdp.ingest_from_path("path/to/data.csv", schema, {"format": "csv"})
print("rows", ds.row_count())

report = rdp.profile_dataset(ds, {"head_rows": 50, "quantiles": [0.5]})
print("profile rows sampled", report["row_count"])

validation = rdp.validate_dataset(
    ds,
    {"checks": [{"kind": "not_null", "column": "id", "severity": "error"}]},
)
print("checks", validation["summary"]["total_checks"])
```

## Documentation

| | Link |
| --- | --- |
| **Python examples (HTML, pdoc)** | [GitHub Pages — examples](https://vihangdesai2018-png.github.io/rust-data-processing/python/examples.html) |
| **Python API (HTML, pdoc)** | [GitHub Pages — Python](https://vihangdesai2018-png.github.io/rust-data-processing/python/) |
| **Python API (markdown)** | [API.md in the repository](https://github.com/vihangdesai2018-png/rust-data-processing/blob/main/python-wrapper/API.md) |
| **Combined site (landing + Rust rustdoc)** | [GitHub Pages — home](https://vihangdesai2018-png.github.io/rust-data-processing/) |
| **Rust crate API** | [docs.rs/rust-data-processing](https://docs.rs/rust-data-processing) |
| **Repository** | [github.com/vihangdesai2018-png/rust-data-processing](https://github.com/vihangdesai2018-png/rust-data-processing) |

## License

MIT OR Apache-2.0 - see [LICENSE-MIT](https://github.com/vihangdesai2018-png/rust-data-processing/blob/main/LICENSE-MIT) and [LICENSE-APACHE](https://github.com/vihangdesai2018-png/rust-data-processing/blob/main/LICENSE-APACHE) in the repository.
