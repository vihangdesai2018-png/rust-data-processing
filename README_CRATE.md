# rust-data-processing

**Rust** library: schema-first ingestion (CSV, JSON, Parquet, Excel with Cargo features) into an in-memory [`DataSet`](https://docs.rs/rust-data-processing/latest/rust_data_processing/types/struct.DataSet.html), plus Polars-backed pipelines, optional SQL, profiling, validation, and map/reduce-style processing.

This file is the **crate README** shown on [crates.io](https://crates.io/crates/rust-data-processing) and at the top of [docs.rs](https://docs.rs/rust-data-processing) (Rust-only). The [repository’s `README.md`](https://github.com/vihangdesai2018-png/rust-data-processing/blob/main/README.md) is the full monorepo overview (including Python).

## Documentation

| | Link |
| --- | --- |
| **Rust API (module tree)** | Use the **crate** index on this docs.rs page (left sidebar). |
| **Repository** | [github.com/vihangdesai2018-png/rust-data-processing](https://github.com/vihangdesai2018-png/rust-data-processing) |
| **Markdown API overview** | [`API.md`](./API.md) (shipped in this crate) |
| **Rust examples & cookbook** | [`docs/rust/README.md`](./docs/rust/README.md) |
| **HTML site (Rust + Python pages)** | [GitHub Pages](https://vihangdesai2018-png.github.io/rust-data-processing/) — use **Rust (rustdoc)** for this crate; [setup](https://github.com/vihangdesai2018-png/rust-data-processing/blob/main/docs/DOCUMENTATION.md) if the site is empty. |

## Quick start (Rust)

```rust
use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
use rust_data_processing::types::{DataType, Field, Schema};

let schema = Schema::new(vec![
    Field::new("id", DataType::Int64),
    Field::new("name", DataType::Utf8),
]);
let _ds = ingest_from_path("path/to/data.csv", &schema, &IngestionOptions::default())
    .expect("ingest");
```

More patterns: [`docs/rust/README.md`](./docs/rust/README.md).

## Features (Cargo)

- `default`: includes `sql` (Polars-backed SQL via `polars-sql`).
- `excel`: Excel workbook ingestion (`calamine`).
- `sql`: Polars SQL (on by default; use `default-features = false` to drop).
- `db_connectorx`: optional DB → Arrow → `DataSet`.
- `arrow` / `serde_arrow`: Arrow interop helpers.

Full list: [`Cargo.toml`](./Cargo.toml) `[features]`.

## License

`MIT OR Apache-2.0` - see [LICENSE-MIT](https://github.com/vihangdesai2018-png/rust-data-processing/blob/main/LICENSE-MIT) and [LICENSE-APACHE](https://github.com/vihangdesai2018-png/rust-data-processing/blob/main/LICENSE-APACHE).
