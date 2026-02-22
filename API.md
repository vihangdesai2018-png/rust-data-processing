# API overview (rust-data-processing)

This file is a **high-level, human-friendly overview** of the current API surface.

For the canonical, always-up-to-date API docs, use **Rustdoc**:

```powershell
cargo doc --no-deps --open
```

This generates HTML docs at `target/doc/rust_data_processing/index.html`.

## Public modules

- `rust_data_processing::types`
  - Schema/data model types: `Schema`, `Field`, `DataType`, `DataSet`, `Value`
- `rust_data_processing::ingestion`
  - Unified entrypoint: `ingest_from_path`
  - Options/types: `IngestionOptions`, `IngestionFormat`, `ExcelSheetSelection`, `IngestionRequest`
  - Observability: `IngestionObserver`, `IngestionSeverity`, `StdErrObserver`, `FileObserver`, `CompositeObserver`
- `rust_data_processing::processing`
  - In-memory transformations: `filter`, `map`, `reduce`, `ReduceOp`
- `rust_data_processing::error`
  - Errors/results: `IngestionError`, `IngestionResult<T>`

## What data can be consumed?

### File formats (auto-detected by extension)

- **CSV**: `.csv`
- **JSON**: `.json`, `.ndjson` (nested fields supported via dot paths like `user.name`)
- **Parquet**: `.parquet`, `.pq`
- **Excel/workbooks**: `.xlsx`, `.xls`, `.xlsm`, `.xlsb`, `.ods` (requires feature `excel`)

### Supported logical types

- `DataType::Int64`, `DataType::Float64`, `DataType::Bool`, `DataType::Utf8`
- Nulls are represented as `Value::Null` (e.g. empty CSV/Excel cells or JSON `null`)

## Most common entrypoint

- `rust_data_processing::ingestion::ingest_from_path(path, schema, options) -> IngestionResult<DataSet>`
  - Auto-detects format from extension unless `options.format` is set
  - Calls observer hooks (`on_success` / `on_failure` / `on_alert`) when configured

## Unified ingestion examples (CSV / JSON / Parquet / Excel)

### CSV (auto-detect by extension)

```rust
use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]);

    let ds = ingest_from_path("people.csv", &schema, &IngestionOptions::default())?;
    println!("rows={}", ds.row_count());
    Ok(())
}
```

### JSON (auto-detect by extension, nested field paths)

```rust
use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("user.name", DataType::Utf8),
    ]);

    let ds = ingest_from_path("events.json", &schema, &IngestionOptions::default())?;
    println!("rows={}", ds.row_count());
    Ok(())
}
```

### Parquet (auto-detect by extension)

```rust
use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("active", DataType::Bool),
    ]);

    let ds = ingest_from_path("data.parquet", &schema, &IngestionOptions::default())?;
    println!("rows={}", ds.row_count());
    Ok(())
}
```

### Force a format explicitly (override inference)

```rust
use rust_data_processing::ingestion::{ingest_from_path, IngestionFormat, IngestionOptions};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64)]);

    let opts = IngestionOptions {
        format: Some(IngestionFormat::Csv),
        ..Default::default()
    };

    let ds = ingest_from_path("input_without_extension", &schema, &opts)?;
    println!("rows={}", ds.row_count());
    Ok(())
}
```

### Observability (stderr logging + alerting)

```rust
use std::sync::Arc;

use rust_data_processing::ingestion::{
    ingest_from_path, IngestionOptions, IngestionSeverity, StdErrObserver,
};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64)]);

    let opts = IngestionOptions {
        observer: Some(Arc::new(StdErrObserver::default())),
        alert_at_or_above: IngestionSeverity::Critical,
        ..Default::default()
    };

    // Missing files are treated as Critical and will trigger `on_alert` at this threshold.
    let _err = ingest_from_path("does_not_exist.csv", &schema, &opts).unwrap_err();
    Ok(())
}
```

### Excel (requires `excel` feature)

Enable the feature:

```toml
rust-data-processing = { path = ".", features = ["excel"] }
```

Example:

```rust
use rust_data_processing::ingestion::{
    ingest_from_path, ExcelSheetSelection, IngestionFormat, IngestionOptions,
};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]);

    let opts = IngestionOptions {
        format: Some(IngestionFormat::Excel),
        excel_sheet_selection: ExcelSheetSelection::Sheet("Sheet1".to_string()),
        ..Default::default()
    };

    let ds = ingest_from_path("workbook.xlsx", &schema, &opts)?;
    println!("rows={}", ds.row_count());
    Ok(())
}
```

## Format-specific entrypoints (lower-level)

- `rust_data_processing::ingestion::csv::ingest_csv_from_path`
- `rust_data_processing::ingestion::json::ingest_json_from_path` / `ingest_json_from_str`
- `rust_data_processing::ingestion::parquet::ingest_parquet_from_path`

## Cargo features

- `excel`: enables Excel ingestion support
- `excel_test_writer`: enables Excel integration tests that generate `.xlsx` at runtime

## Processing pipelines (Epic 1 / Story 1.2)

The processing layer operates on `types::DataSet` in-memory:

- **Filter**: `processing::filter(&DataSet, predicate) -> DataSet`
- **Map**: `processing::map(&DataSet, mapper) -> DataSet`
- **Reduce**: `processing::reduce(&DataSet, column, ReduceOp) -> Option<Value>`
  - `ReduceOp::Count` counts rows (including nulls)
  - `ReduceOp::{Sum, Min, Max}` operate on numeric columns and ignore nulls

Example:

```rust
use rust_data_processing::processing::{filter, map, reduce, ReduceOp};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let schema = Schema::new(vec![
    Field::new("id", DataType::Int64),
    Field::new("active", DataType::Bool),
    Field::new("score", DataType::Float64),
]);

let ds = DataSet::new(
    schema,
    vec![
        vec![Value::Int64(1), Value::Bool(true), Value::Float64(10.0)],
        vec![Value::Int64(2), Value::Bool(false), Value::Float64(20.0)],
        vec![Value::Int64(3), Value::Bool(true), Value::Null],
    ],
);

let active_idx = ds.schema.index_of("active").unwrap();
let filtered = filter(&ds, |row| matches!(row.get(active_idx), Some(Value::Bool(true))));

let mapped = map(&filtered, |row| {
    let mut out = row.to_vec();
    if let Some(Value::Float64(v)) = out.get(2) {
        out[2] = Value::Float64(v * 1.1);
    }
    out
});

let sum = reduce(&mapped, "score", ReduceOp::Sum).unwrap();
assert_eq!(sum, Value::Float64(11.0));
```

### Benchmarks (Story 1.2.5)

Benchmarks are implemented using Criterion in `benches/pipelines.rs`.

```powershell
cargo bench --bench pipelines
```

