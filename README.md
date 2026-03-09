# rust-data-processing

Small Rust library for ingesting common file formats (CSV / JSON / Parquet / Excel) into an in-memory
`DataSet`, with basic schema validation, a DataFrame-centric pipeline API (Polars-backed), and optional observability hooks.

- **API docs**: generate with `./scripts/build_docs.ps1` (see below)
- **Status**: library APIs are in `src/lib.rs`; the binary (`src/main.rs`) is currently just a placeholder.

## Platform support

- **Supported OSes**: Windows, Linux, and macOS.
- **Works out of the box**: the library is written in portable Rust (no OS-specific runtime assumptions).
- **Build prerequisites**:
  - **macOS**: install Xcode Command Line Tools (`xcode-select --install`) for the system linker/C toolchain.
  - **Linux**: install a basic build toolchain (e.g. GCC/Clang via your distro’s `build-essential` equivalent).
  - **Windows**: see [Development on Windows (toolchain + linker)](#development-on-windows-toolchain--linker).

  Parquet support pulls in native compression dependencies (e.g. `zstd-sys`); Cargo will build them automatically once a C toolchain is available.

- **Benchmarks**:
  - `cargo bench --bench pipelines` is cross-platform.
  - `benchmarks.ps1` is a Windows/PowerShell convenience wrapper; on Linux/macOS you can run it via `pwsh` or just run `cargo bench` directly.
  - `scripts/run_benchmarks.ps1` runs all Criterion benchmarks (pipelines + ingestion + map/reduce).

## Quick start (library usage)

Add to your `Cargo.toml` (example):

```toml
[dependencies]
rust-data-processing = { path = "." }
```

Ingest a file using a schema:

```rust
use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]);

    // Auto-detect format from file extension (.csv/.json/.parquet/...).
    let ds = ingest_from_path("tests/fixtures/people.csv", &schema, &IngestionOptions::default())?;
    println!("rows={}", ds.row_count());
    Ok(())
}
```

Prefer builder-style options when you only need to override a couple knobs:

```rust
use rust_data_processing::ingestion::IngestionOptionsBuilder;
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]);

    let ds = IngestionOptionsBuilder::new()
        .ingest_from_path("tests/fixtures/people.csv", &schema)?;

    println!("rows={}", ds.row_count());
    Ok(())
}
```

## DataFrame-centric pipelines (Polars-backed) (Phase 1)

Use `rust_data_processing::pipeline::DataFrame` for a DataFrame-centric pipeline API that compiles to a lazy plan and
collects into a `DataSet`:

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
        vec![Value::Int64(2), Value::Bool(true), Value::Float64(20.0)],
        vec![Value::Int64(3), Value::Bool(false), Value::Float64(30.0)],
    ],
);

let out = DataFrame::from_dataset(&ds)?
    .filter(Predicate::Eq {
        column: "active".to_string(),
        value: Value::Bool(true),
    })?
    .multiply_f64("score", 2.0)?
    .collect()?;

assert_eq!(out.row_count(), 2);
```

## What data can be consumed? (Epic 1 / Stories 1.1–1.2)

### File formats (auto-detected by extension)

- **CSV**: `.csv` (must include headers)
- **JSON**: `.json` (array-of-objects) and `.ndjson` (newline-delimited objects)
  - Nested fields are supported via **dot paths** in schema field names (e.g. `user.name`)
- **Parquet**: `.parquet`, `.pq`
- **Excel/workbooks**: `.xlsx`, `.xls`, `.xlsm`, `.xlsb`, `.ods`

### Supported value types

You define a `Schema` using these logical types:

- `DataType::Int64`
- `DataType::Float64`
- `DataType::Bool`
- `DataType::Utf8`

Ingestion yields a `DataSet` whose cells are `Value::{Int64, Float64, Bool, Utf8, Null}`.

- **Null handling**:
  - CSV: empty/whitespace-only cells become `Value::Null`
  - JSON: explicit `null` becomes `Value::Null`
  - Excel: empty cells become `Value::Null`
  - Parquet: nulls become `Value::Null`

## Processing pipelines (Epic 1 / Story 1.2)

Once you have a `DataSet` (typically from `ingestion::ingest_from_path`), you can apply in-memory
transformations using `rust_data_processing::processing`:

- `filter(&DataSet, predicate) -> DataSet`
- `map(&DataSet, mapper) -> DataSet`
- `reduce(&DataSet, column, ReduceOp) -> Option<Value>`

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

### Execution engine (parallel pipelines) (Story 1.3)

If you want **parallel filter/map**, plus **throttling** and **real-time metrics**, use `rust_data_processing::execution`:

```rust
use rust_data_processing::execution::{ExecutionEngine, ExecutionOptions};
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
});

let active_idx = ds.schema.index_of("active").unwrap();
let filtered = engine.filter_parallel(&ds, |row| matches!(row.get(active_idx), Some(Value::Bool(true))));
let mapped = engine.map_parallel(&filtered, |row| row.to_vec());
let sum = engine.reduce(&mapped, "score", ReduceOp::Sum).unwrap();

let metrics = engine.metrics().snapshot();
println!("rows_processed={}, elapsed={:?}", metrics.rows_processed, metrics.elapsed);
```

### More examples: reduce ops and missing columns

```rust
use rust_data_processing::processing::{reduce, ReduceOp};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let schema = Schema::new(vec![Field::new("score", DataType::Float64)]);
let ds = DataSet::new(schema, vec![vec![Value::Float64(1.0)], vec![Value::Null]]);

assert_eq!(reduce(&ds, "score", ReduceOp::Count), Some(Value::Int64(2)));
assert_eq!(reduce(&ds, "score", ReduceOp::Sum), Some(Value::Float64(1.0)));
assert_eq!(reduce(&ds, "missing", ReduceOp::Sum), None);
```

### Benchmarks (Story 1.2.5)

Criterion benchmarks live under `benches/` (currently `benches/pipelines.rs`).

```powershell
cargo bench --bench pipelines
```

Additional benchmark targets:

- `cargo bench --bench ingestion`
  - Generates 20k-row fixtures (CSV / JSON array / NDJSON / nested JSON / Parquet; Excel when enabled)
  - Measures schema-known vs schema-inferred and a “warm vs rotating files” proxy for cache effects
- `cargo bench --bench map_reduce`
  - Benchmarks filter/map/reduce on in-memory `DataSet` vs `ExecutionEngine` parallel path

Convenience runner (Windows / PowerShell):

```powershell
./scripts/run_benchmarks.ps1 -Quick
```

### Observability (failure/alert hooks)

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

    // Missing files are treated as Critical (and will trigger `on_alert` at this threshold).
    let _ = ingest_from_path("does_not_exist.csv", &schema, &opts).unwrap_err();
    Ok(())
}
```

## Supported formats

- **CSV**: headers required; schema fields must exist; columns may be reordered.
- **JSON**: supports JSON array of objects or NDJSON; nested fields via dot paths (e.g. `user.name`).
- **Parquet**: validates required columns; uses the Parquet record API for reading.
- **Excel**: behind the Cargo feature `excel`.

## Features

- `excel`: enable Excel ingestion (adds `calamine`)
- `excel_test_writer`: enables Excel integration tests that generate an `.xlsx` at runtime
- `sql`: enables the (currently minimal) `rust_data_processing::sql` module

## Run tests

```powershell
./scripts/run_unit_tests.ps1
```

## Generate API docs (Rustdoc)

Rust has built-in API documentation via **Rustdoc**.

```powershell
./scripts/build_docs.ps1
```

## Deep tests (large/realistic fixtures)

Deep tests are **not** run as part of `./scripts/run_unit_tests.ps1`. They are feature-gated behind `deep_tests`.

```powershell
./scripts/run_deep_tests.ps1
```

## Development on Windows (toolchain + linker)

Rust installs its tools into:

- `%USERPROFILE%\.cargo\bin` (example: `C:\Users\Vihan\.cargo\bin`)

That directory must be on your `PATH` so `rustc`, `cargo`, and `rustup` can be found.

If you see `error: linker 'link.exe' not found`, install **Build Tools for Visual Studio 2026** and select:

- **Desktop development with C++**
- **MSVC v144 - VS 2026 C++ x64/x86 build tools**
- **Windows 10/11 SDK**

Then open the project from **Developer PowerShell for VS 2026** (or restart your terminal) and rerun:

```powershell
cargo test
```

### Verify toolchain

```powershell
where.exe rustc
rustc --version
cargo --version
rustup --version
```

### Fix PATH for the current PowerShell session (no restart)

```powershell
$env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' + `
            [Environment]::GetEnvironmentVariable('Path','User')
```

### Ensure `%USERPROFILE%\.cargo\bin` is on your *User* PATH (permanent)

```powershell
$cargoBin = Join-Path $env:USERPROFILE '.cargo\bin'
$userPath = [Environment]::GetEnvironmentVariable('Path','User')
if ([string]::IsNullOrWhiteSpace($userPath)) { $userPath = '' }

$parts = $userPath -split ';' | Where-Object { $_ -and $_.Trim() -ne '' }
if ($parts -notcontains $cargoBin) {
  [Environment]::SetEnvironmentVariable('Path', (@($parts + $cargoBin) -join ';'), 'User')
}
```

After changing the *User* PATH, **restart your terminal** (or log out/in) so new shells inherit it.