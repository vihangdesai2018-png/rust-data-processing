# Developer README (rust-data-processing)

This doc is for contributors working inside the repo. Keep it in sync with the actual module structure under `src/`
and the Phase 1 tracker in `Planning/PHASE1_PLAN.md`.

## Quick navigation

- **User-facing examples**: root `README.md` (cookbook + map/reduce aggregates); `API.md` (full aggregate / `group_by` snippets)
- **Reduce / aggregate semantics**: `Planning/REDUCE_AGG_SEMANTICS.md`
- **Public API surface**: `src/lib.rs` (module exports + default-on `sql`)
- **Core data model**: `src/types.rs` (`Schema`, `Field`, `DataType`, `DataSet`, `Value`)
- **Errors**: `src/error.rs` (`IngestionError`, `IngestionResult<T>`)
- **Ingestion**: `src/ingestion/`
- **DB ingestion (ConnectorX, feature-gated)**: `src/ingestion/db.rs`
- **DataFrame pipelines (Polars-backed)**: `src/pipeline/mod.rs`
- **In-memory transforms**: `src/processing/`
- **Parallel execution engine**: `src/execution/`
- **SQL module (feature: `sql`, enabled by default)**: `src/sql/mod.rs`
- **Transform spec**: `src/transform.rs` (`TransformSpec`, `TransformStep`)
- **Profiling**: `src/profiling/mod.rs`
- **Validation**: `src/validation/mod.rs`
- **Outliers**: `src/outliers/mod.rs`
- **CDC boundary types**: `src/cdc/mod.rs`
- **Benchmarks**: `benches/` (`pipelines`, `ingestion`, `map_reduce`, `profiling`, `validation`, `outliers`)
- **Scripts (Windows/PowerShell convenience)**: `scripts/`
- **Python bindings (PyO3 + maturin + uv)**: `python-wrapper/` (see `python-wrapper/README_DEV.md`)

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
- `reduce.rs`: reductions (`ReduceOp` including mean/variance/std/sum-squares/L2/count-distinct)
- `multi.rs`: `feature_wise_mean_std`, `arg_max_row` / `arg_min_row`, `top_k_by_frequency`

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

### `transform` (`src/transform.rs`)

- **Purpose**: Serde-friendly, end-user “mapping spec” (`TransformSpec`) for common ETL operations (rename/select/drop/cast/fill/derive),
  compiled to `pipeline::DataFrame` wrappers.
- **Guideline**: Keep public inputs/outputs in crate types (`Schema`, `DataSet`, `DataType`, `Value`) so Python and other bindings can wrap it.

### `profiling` (`src/profiling/mod.rs`)

- **Purpose**: Polars-backed column profiling (row count, nulls, distinct (non-null), numeric min/max/mean + quantiles) with deterministic sampling.
- **Guideline**: Reports should be stable, engine-agnostic structs with stable JSON/Markdown renderers.

### `validation` (`src/validation/mod.rs`)

- **Purpose**: Validation DSL (`ValidationSpec`, `Check`) compiled to Polars expressions, producing stable reports with JSON/Markdown renderers.
- **Guideline**: Prefer “collect once” plans; keep example collection bounded (`max_examples` style).

### `outliers` (`src/outliers/mod.rs`)

- **Purpose**: Outlier detection primitives (IQR / z-score / MAD) with explainable outputs and stable renderers.
- **Guideline**: Keep results explainable (stats + thresholds + examples) and deterministic under `SamplingMode`.

### `cdc` (`src/cdc/mod.rs`)

- **Purpose**: Dependency-free boundary types for CDC events and a batch-first source trait (Phase 1 spike).
- **Guideline**: Do not add a heavy default CDC dependency; keep this as an interface boundary.

### `ingestion::db` (`src/ingestion/db.rs`) (feature: `db_connectorx`)

- **Purpose**: Direct DB ingestion via ConnectorX (DB → Arrow → `DataSet`).
- **Guideline**: Keep API read-only and minimal; document platform prerequisites (e.g., OpenSSL toolchain constraints on some platforms).

## Common workflows

### GitHub Actions (summary)

- **`.github/workflows/rust_ci.yml`** — on every PR and push to **`main`**: `fmt`, `clippy`, tests (ubuntu + Windows), plus **ubuntu** `cargo test --features ci_expanded` (no **`db_connectorx`** — avoids OpenSSL/Perl in CI).
- **`.github/workflows/rust_release.yml`** — on tag **`v*`** (commit must be on **`main`**): `cargo publish --dry-run` then `cargo publish`.
- Policy write-up: **`Planning/CI_DEPLOY_POLICY.md`**.

### Build + test

```powershell
cargo test
```

### Run feature-gated test suites

```powershell
# Same as GitHub Actions “expanded” job (no ConnectorX → no OpenSSL/Perl on this path)
cargo test --locked --features ci_expanded

# Deep tests (large fixtures)
./scripts/run_deep_tests.ps1

# Excel ingestion (reader)
cargo test --features excel

# Excel tests that generate an .xlsx at runtime
cargo test --features excel_test_writer

# DB ingestion (ConnectorX) — see “Windows: OpenSSL / perl” below
cargo test --features db_connectorx
```

### Windows: OpenSSL / “perl not found” / build hang

**`cargo test --all-features`** or **`--features db_connectorx`** enables **ConnectorX**, which pulls **OpenSSL**. On Windows, **openssl-sys** may try to **compile OpenSSL from source** and needs **Perl** (e.g. **Strawberry Perl**). A failed configure step can look like a **hang**; **incremental** builds may keep retrying until you **clean**.

**Recover:** `cargo clean` (or `cargo clean -p openssl-sys`), then prefer **`cargo test --locked --features ci_expanded`** for parity with CI. Use **`db_connectorx`** only when Perl/OpenSSL (or **WSL** / **Linux**) is set up.

### Run benchmarks

```powershell
cargo bench --bench pipelines
cargo bench --bench ingestion
cargo bench --bench map_reduce
# map_reduce bench includes: filter/map/sum, scalar mean/variance (mem vs Polars), feature_wise_mean_std,
# arg_max / top_k_by_frequency, and Polars group_by ML-style aggs
cargo bench --bench profiling
cargo bench --bench validation
cargo bench --bench outliers
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

- **Enable DB ingestion (ConnectorX)**:

```powershell
cargo test --features db_connectorx
```

- **Disable default features (including SQL)**:

```powershell
cargo test --no-default-features
```

- **`ci_expanded`**: `deep_tests` + `excel_test_writer` + `arrow` + `serde_arrow` (matches **`rust_ci.yml`**; no **`db_connectorx`**).
- **Deep tests**:

```powershell
./scripts/run_deep_tests.ps1
```

  These run `tests/deep_tests.rs` and include **in-memory vs Polars** parity for `reduce`, **`feature_wise_mean_std`**, plus **`group_by`** / **arg max/min** / **top‑k** on Seattle CSV, `job_runs_sample.json`, and the Apache Parquet fixture (where applicable).

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

