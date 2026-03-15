# Developer README (rust-data-processing)

This doc is for contributors working inside the repo. Keep it in sync with the actual module structure under `src/`
and the Phase 1 tracker in `Planning/PHASE1_PLAN.md`.

## Quick navigation

- **Public API surface**: `src/lib.rs` (module exports + default-on `sql`)
- **Core data model**: `src/types.rs` (`Schema`, `Field`, `DataType`, `DataSet`, `Value`)
- **Errors**: `src/error.rs` (`IngestionError`, `IngestionResult<T>`)
- **Ingestion**: `src/ingestion/`
- **DataFrame pipelines (Polars-backed)**: `src/pipeline/mod.rs`
- **In-memory transforms**: `src/processing/`
- **Parallel execution engine**: `src/execution/`
- **SQL module (feature: `sql`, enabled by default)**: `src/sql/mod.rs`
- **Benchmarks**: `benches/` (`pipelines`, `ingestion`, `map_reduce`)
- **Scripts (Windows/PowerShell convenience)**: `scripts/`

## Module map (what does what)

### `types` (`src/types.rs`)

- **Purpose**: Engine-agnostic in-memory representation (`DataSet`) plus schema/value types.
- **Rule of thumb**: Keep this free of Polars/DataFusion types. Treat it as the stable “public model”.

### `error` (`src/error.rs`)

- **Purpose**: Single public error model for ingestion + engine delegation.
- **Guideline**: When an underlying engine/library produces a useful structured error, wrap it as
  `IngestionError::Engine { message, source }` so callers can keep the source chain.

### `ingestion` (`src/ingestion/`)

Files:

- `mod.rs`: module exports and public re-exports.
- `unified.rs`: **main entrypoints**:
  - `ingest_from_path(path, schema, options)`
  - `infer_schema_from_path(path, options)` (lossy inference)
  - `ingest_from_path_infer(path, options)` convenience path
  - `IngestionOptions`, `IngestionFormat`, `ExcelSheetSelection`, `IngestionRequest`
- `builder.rs`: `IngestionOptionsBuilder` (engine-agnostic config builder).
- `observability.rs`: observer traits + implementations (`StdErrObserver`, `FileObserver`, etc.).
- `polars_bridge.rs`: conversion helpers between `DataSet` and Polars plus Polars-error mapping.
- `csv.rs`, `json.rs`, `parquet.rs`, `excel.rs`: format-specific ingestion implementations.

Guidelines:

- **Public signatures** should stay in crate types (`Schema`, `DataSet`, `Value`, `IngestionOptions`).
- Prefer **delegation** to Polars reads/scans where it improves correctness/perf; fall back to custom logic where required.
- Keep ingestion behavior consistent across formats (especially **null semantics** and **schema mismatch** behavior).

### `pipeline` (`src/pipeline/mod.rs`)

- **Purpose**: DataFrame-centric pipeline API (Polars-backed) that keeps public signatures in crate types.
- **Key type**: `pipeline::DataFrame` which compiles to a Polars `LazyFrame` internally and collects to `DataSet`.
- **Guideline**: Avoid leaking Polars types in public APIs (internal fields can be Polars).

### `processing` (`src/processing/`)

- `filter.rs`: in-memory row filtering
- `map.rs`: in-memory row mapping
- `reduce.rs`: reductions (`ReduceOp::{Count, Sum, Min, Max}`)

Guidelines:

- Keep this layer **simple, deterministic**, and easy to benchmark.
- `reduce()` returns `None` for missing columns; preserve that behavior across other APIs where it makes sense.

### `execution` (`src/execution/`)

- **Purpose**: parallel execution wrapper around `processing`, with:
  - chunking
  - throttling (`max_in_flight_chunks`)
  - real-time metrics + observer hooks

Files:

- `mod.rs`: `ExecutionEngine`, `ExecutionOptions`, public observer/metrics exports.
- `observer.rs`: observer traits + default stderr observer.
- `semaphore.rs`: simple throttle primitive.

Guidelines:

- Keep scheduling behavior stable enough for benchmarks.
- When adding new parallel ops, ensure metrics/observer events remain coherent and useful.

### `sql` (`src/sql/mod.rs`) (feature: `sql`)

- **Purpose**: Polars-backed SQL wrapper (compiles SQL → Polars lazy plan).
- **Guideline**: Keep the rest of the crate Polars-first and DataFrame-centric; don’t let SQL drive public type leakage.

## Common workflows

### Build + test

```powershell
cargo test
```

### Run benchmarks

```powershell
cargo bench --bench pipelines
cargo bench --bench ingestion
cargo bench --bench map_reduce
```

Convenience wrapper (Windows):

```powershell
./scripts/run_benchmarks.ps1 -Quick
```

### Generate Rustdoc

```powershell
./scripts/build_docs.ps1
```

### Feature flags

- **Default**: SQL is enabled (Polars-backed).
- **Enable Excel ingestion**:

```powershell
cargo test --features excel
```

- **Disable default features (including SQL)**:

```powershell
cargo test --no-default-features
```

- **Deep tests**:

```powershell
./scripts/run_deep_tests.ps1
```

## Making changes safely (project conventions)

- **Start at the public API**: if the change affects users, begin in `src/lib.rs` and `API.md`/`README.md`.
- **Keep public types engine-agnostic**: don’t expose Polars types in public signatures unless we intentionally “graduate” them.
- **Error handling**:
  - Use `IngestionError::SchemaMismatch` for missing/invalid shape.
  - Use `IngestionError::ParseError` for cell-level parsing/type issues.
  - Use `IngestionError::Engine` to preserve underlying engine/library error sources.
- **Benchmarks & parity**:
  - If you change behavior in ingestion/pipelines, update or add benchmarks/tests to keep parity expectations explicit.
- **Docs upkeep**:
  - If you add/rename modules or public APIs, update **both** `README.md` and this file (`README_DEV.md`).
  - Keep the roadmap summary in `README.md` aligned with `Planning/PHASE1_PLAN.md`.

