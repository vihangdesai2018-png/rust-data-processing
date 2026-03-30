# API overview (rust-data-processing)

This file is a **high-level, human-friendly overview** of the current API surface.

**License:** `MIT OR Apache-2.0` (see `LICENSE-MIT` and `LICENSE-APACHE` in the repo root).

For the canonical, always-up-to-date API docs, use **Rustdoc**:

```powershell
./scripts/build_docs.ps1
```

This generates HTML docs at `target/doc/rust_data_processing/index.html`.

## Public modules

- `rust_data_processing::types`
  - Schema/data model types: `Schema`, `Field`, `DataType`, `DataSet`, `Value`
- `rust_data_processing::ingestion`
  - Unified entrypoint: `ingest_from_path`
  - Options/types: `IngestionOptions`, `IngestionOptionsBuilder`, `IngestionFormat`, `ExcelSheetSelection`, `IngestionRequest`
  - Observability: `IngestionObserver`, `IngestionSeverity`, `StdErrObserver`, `FileObserver`, `CompositeObserver`
- `rust_data_processing::pipeline`
  - DataFrame-centric pipeline API (Polars-backed): `DataFrame`, `Predicate`, `Agg`, `JoinKind`, `CastMode`
- `rust_data_processing::processing`
  - In-memory transforms: `filter`, `map`, `reduce`, `ReduceOp`, `VarianceKind`
  - Multi-column / debugging helpers: `feature_wise_mean_std`, `FeatureMeanStd`, `arg_max_row`, `arg_min_row`, `top_k_by_frequency`
- `rust_data_processing::execution`
  - Execution engine for processing pipelines: `ExecutionEngine`, `ExecutionOptions`
  - Monitoring: `ExecutionObserver`, `ExecutionEvent`, `ExecutionMetrics`
- `rust_data_processing::error`
  - Errors/results: `IngestionError`, `IngestionResult<T>`
- `rust_data_processing::sql` (feature: `sql`)
  - Optional SQL module (Phase 1 placeholder)

## What data can be consumed?

### File formats (auto-detected by extension)

- **CSV**: `.csv`
- **JSON**: `.json`, `.ndjson` (nested fields supported via dot paths like `user.name`)
- **Parquet**: `.parquet`, `.pq`
- **Excel/workbooks**: `.xlsx`, `.xls`, `.xlsm`, `.xlsb`, `.ods`

### Supported logical types

- `DataType::Int64`, `DataType::Float64`, `DataType::Bool`, `DataType::Utf8`
- Nulls are represented as `Value::Null` (e.g. empty CSV/Excel cells or JSON `null`)

## Most common entrypoint

- `rust_data_processing::ingestion::ingest_from_path(path, schema, options) -> IngestionResult<DataSet>`
  - Auto-detects format from extension unless `options.format` is set
  - Calls observer hooks (`on_success` / `on_failure` / `on_alert`) when configured

When you only need to override a couple options, prefer `IngestionOptionsBuilder`:

```rust
use rust_data_processing::ingestion::IngestionOptionsBuilder;
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]);

    let ds = IngestionOptionsBuilder::new()
        .ingest_from_path("people.csv", &schema)?;

    println!("rows={}", ds.row_count());
    Ok(())
}
```

## DataFrame-centric pipelines (Phase 1)

Use `rust_data_processing::pipeline::DataFrame` for DataFrame-centric transforms that compile to a lazy plan (Polars-backed)
and collect into a `DataSet`:

```rust
use rust_data_processing::pipeline::{DataFrame, Predicate};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("active", DataType::Bool),
        Field::new("score", DataType::Float64),
    ]),
    vec![
        vec![Value::Int64(1), Value::Bool(true), Value::Float64(10.0)],
        vec![Value::Int64(2), Value::Bool(false), Value::Float64(20.0)],
    ],
);

let out = DataFrame::from_dataset(&ds)
    .unwrap()
    .filter(Predicate::NotNull {
        column: "score".to_string(),
    })
    .unwrap()
    .collect()
    .unwrap();

assert_eq!(out.row_count(), 2);
```

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

### Excel

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

- `excel`: backwards-compatibility feature flag (Excel ingestion is enabled by default)
- `excel_test_writer`: enables Excel integration tests that generate `.xlsx` at runtime

## Processing pipelines (Epic 1 / Story 1.2)

The processing layer operates on `types::DataSet` in-memory:

- **Filter**: `processing::filter(&DataSet, predicate) -> DataSet`
- **Map**: `processing::map(&DataSet, mapper) -> DataSet`
- **Reduce**: `processing::reduce(&DataSet, column, ReduceOp) -> Option<Value>`
  - `ReduceOp::Count` counts rows (including nulls)
  - `ReduceOp::{Sum, Min, Max}` operate on numeric columns and ignore nulls
  - `ReduceOp::Mean`, `Variance(VarianceKind)`, `StdDev(VarianceKind)`, `SumSquares`, `L2Norm` (Welford-based where applicable; mean/std/var as `Float64`)
  - `ReduceOp::CountDistinctNonNull` for numeric, UTF-8, or bool columns
- **Pipeline scalar reduce**: `pipeline::DataFrame::reduce(self, column, ReduceOp)` (Polars-backed; `sum` delegates to `reduce`)
- **Feature-wise mean/std (one pass)**: `processing::feature_wise_mean_std(&DataSet, &[&str], VarianceKind) -> Option<Vec<(String, FeatureMeanStd)>>` and `pipeline::DataFrame::feature_wise_mean_std(self, &[&str], VarianceKind)` (all listed columns must be `Int64`/`Float64`)
- **Arg max/min row**: `processing::arg_max_row`, `processing::arg_min_row` → `Option<Option<(usize, Value)>>` (outer `None` = missing column)
- **Top‑k by frequency**: `processing::top_k_by_frequency(&DataSet, column, k) -> Option<Vec<(Value, i64)>>`
- **Group-by ML aggregates**: `pipeline::DataFrame::group_by(keys, &[Agg::...])` supports `Mean`, `StdDev`, `Min`, `Max`, `Sum`, `CountRows`, `CountDistinctNonNull`, etc.

Semantics for nulls, all-null groups, and casting: see `Planning/REDUCE_AGG_SEMANTICS.md`.

### Example: filter → map → sum (baseline)

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

### Example: mean, variance, std dev, sum of squares, L2 norm, distinct count

`VarianceKind::Population` uses divisor \(n\); `Sample` uses \(n-1\) (undefined → `Value::Null` if \(n < 2\)).

```rust
use rust_data_processing::processing::{reduce, ReduceOp, VarianceKind};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("x", DataType::Float64),
        Field::new("label", DataType::Utf8),
    ]),
    vec![
        vec![Value::Float64(1.0), Value::Utf8("a".to_string())],
        vec![Value::Float64(2.0), Value::Utf8("b".to_string())],
        vec![Value::Null, Value::Utf8("a".to_string())],
    ],
);

let _mean = reduce(&ds, "x", ReduceOp::Mean);
let _var_pop = reduce(&ds, "x", ReduceOp::Variance(VarianceKind::Population));
let _std_sample = reduce(&ds, "x", ReduceOp::StdDev(VarianceKind::Sample));
let _ss = reduce(&ds, "x", ReduceOp::SumSquares);
let _l2 = reduce(&ds, "x", ReduceOp::L2Norm);
let _dc = reduce(&ds, "label", ReduceOp::CountDistinctNonNull);
```

### Example: Polars-backed scalar `DataFrame::reduce`

Same `ReduceOp` as in-memory; pays conversion to a Polars plan + one `collect`.

```rust
use rust_data_processing::pipeline::DataFrame;
use rust_data_processing::processing::{reduce, ReduceOp, VarianceKind};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![Field::new("x", DataType::Float64)]),
    vec![vec![Value::Float64(1.0)], vec![Value::Float64(3.0)]],
);

let mem = reduce(&ds, "x", ReduceOp::Mean).unwrap();
let pol = DataFrame::from_dataset(&ds)
    .unwrap()
    .reduce("x", ReduceOp::Mean)
    .unwrap()
    .unwrap();
assert_eq!(mem, pol);
```

### Example: feature-wise mean and std (one table scan)

All listed columns must be `Int64` or `Float64`. Returns `None` if any name is missing or non-numeric.

```rust
use rust_data_processing::pipeline::DataFrame;
use rust_data_processing::processing::{feature_wise_mean_std, VarianceKind};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("a", DataType::Int64),
        Field::new("b", DataType::Float64),
    ]),
    vec![
        vec![Value::Int64(10), Value::Float64(1.0)],
        vec![Value::Int64(20), Value::Float64(2.0)],
    ],
);

let cols = ["a", "b"];
let mem = feature_wise_mean_std(&ds, &cols, VarianceKind::Sample).unwrap();
let pol = DataFrame::from_dataset(&ds)
    .unwrap()
    .feature_wise_mean_std(&cols, VarianceKind::Sample)
    .unwrap();
assert_eq!(mem.len(), pol.len());
```

### Example: arg max / arg min row and top-k frequency

First row wins on ties. `top_k_by_frequency` ignores nulls; sorts by count descending, then by a stable value key.

```rust
use rust_data_processing::processing::{arg_max_row, arg_min_row, top_k_by_frequency};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("score", DataType::Int64),
        Field::new("region", DataType::Utf8),
    ]),
    vec![
        vec![Value::Int64(10), Value::Utf8("west".to_string())],
        vec![Value::Int64(99), Value::Utf8("east".to_string())],
        vec![Value::Int64(50), Value::Utf8("west".to_string())],
    ],
);

let _max_at = arg_max_row(&ds, "score").unwrap().unwrap(); // (row_index, value)
let _min_at = arg_min_row(&ds, "score").unwrap().unwrap();
let top_regions = top_k_by_frequency(&ds, "region", 2).unwrap();
assert!(!top_regions.is_empty());
```

### Example: `group_by` with mean, std dev, count-distinct (ML-style)

```rust
use rust_data_processing::pipeline::{Agg, DataFrame};
use rust_data_processing::processing::VarianceKind;
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("grp", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("tag", DataType::Utf8),
    ]),
    vec![
        vec![Value::Utf8("A".to_string()), Value::Float64(10.0), Value::Utf8("x".to_string())],
        vec![Value::Utf8("A".to_string()), Value::Float64(20.0), Value::Utf8("y".to_string())],
        vec![Value::Utf8("B".to_string()), Value::Null, Value::Utf8("z".to_string())],
    ],
);

let _out = DataFrame::from_dataset(&ds)
    .unwrap()
    .group_by(
        &["grp"],
        &[
            Agg::Mean {
                column: "score".to_string(),
                alias: "mu".to_string(),
            },
            Agg::StdDev {
                column: "score".to_string(),
                alias: "sd".to_string(),
                kind: VarianceKind::Sample,
            },
            Agg::CountDistinctNonNull {
                column: "tag".to_string(),
                alias: "n_tag".to_string(),
            },
            Agg::CountRows {
                alias: "n".to_string(),
            },
        ],
    )
    .unwrap()
    .collect()
    .unwrap();
```

## Execution engine (Epic 1 / Story 1.3)

For parallel execution (and built-in throttling + metrics), use `rust_data_processing::execution`.

- **Parallel ops**:
  - `ExecutionEngine::filter_parallel(&DataSet, predicate) -> DataSet`
  - `ExecutionEngine::map_parallel(&DataSet, mapper) -> DataSet`
- **Throttling / resource management**:
  - `ExecutionOptions { num_threads, chunk_size, max_in_flight_chunks }`
- **Monitoring**:
  - Subscribe to `ExecutionEvent`s via `ExecutionObserver`
  - Read counters/timings via `ExecutionEngine::metrics().snapshot()`

Example:

```rust
use std::sync::Arc;

use rust_data_processing::execution::{ExecutionEngine, ExecutionOptions, StdErrExecutionObserver};
use rust_data_processing::processing::ReduceOp;
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

let engine = ExecutionEngine::new(ExecutionOptions {
    num_threads: Some(4),
    chunk_size: 1_024,
    max_in_flight_chunks: 4,
})
.with_observer(Arc::new(StdErrExecutionObserver::default()));

let active_idx = ds.schema.index_of("active").unwrap();
let filtered = engine.filter_parallel(&ds, |row| matches!(row.get(active_idx), Some(Value::Bool(true))));

let mapped = engine.map_parallel(&filtered, |row| {
    let mut out = row.to_vec();
    if let Some(Value::Float64(v)) = out.get(2) {
        out[2] = Value::Float64(v * 1.1);
    }
    out
});

let sum = engine.reduce(&mapped, "score", ReduceOp::Sum).unwrap();
assert_eq!(sum, Value::Float64(11.0));

let snap = engine.metrics().snapshot();
println!("rows_processed={}", snap.rows_processed);
```

### Benchmarks (Story 1.2.5)

Benchmarks are implemented using Criterion in `benches/pipelines.rs`.

```powershell
cargo bench --bench pipelines
```

