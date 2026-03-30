# rust-data-processing

Rust library for ingesting common file formats (CSV / JSON / Parquet / Excel (feature-gated)) into an in-memory `DataSet`, with schema
validation + inference helpers, in-memory + parallel processing primitives, and a Polars-backed DataFrame-centric pipeline API.
Phase 1 also targets profiling/validation/outlier-detection APIs and report formats, while keeping a small engine-agnostic public
surface (SQL support is Polars-backed).

## Documentation (read the APIs online)

| | Link |
| --- | --- |
| **Combined Rust + Python (main branch, HTML)** | [GitHub Pages — rust-data-processing](https://vihangdesai2018-png.github.io/rust-data-processing/) — *enable Pages → GitHub Actions in repo Settings if the site is not live yet; see [`Planning/DOCUMENTATION.md`](Planning/DOCUMENTATION.md).* |
| **Rust crate on crates.io** | [docs.rs — rust-data-processing](https://docs.rs/rust-data-processing) *(populates after the first successful publish)* |
| **Markdown API guides** | [`API.md`](API.md) (Rust); Python: [`python-wrapper/API.md`](python-wrapper/API.md) |

**Examples (minimal):**

```rust
use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
use rust_data_processing::types::{DataType, Field, Schema};

fn main() -> Result<(), rust_data_processing::IngestionError> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]);
    let ds = ingest_from_path("tests/fixtures/people.csv", &schema, &IngestionOptions::default())?;
    println!("rows={}", ds.row_count());
    Ok(())
}
```

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "name", "data_type": "utf8"},
]
ds = rdp.ingest_from_path("tests/fixtures/people.csv", schema, {"format": "csv"})
print("rows", ds.row_count())
```

Generate the same HTML as CI locally: `./scripts/build_docs.ps1` (Rust only) or `./scripts/build_docs.ps1 -All` (Rust + Python → `_site/python/`). Maintainer notes: [`Planning/DOCUMENTATION.md`](Planning/DOCUMENTATION.md).

## Reporting bugs

- Open a **[GitHub Issue](https://github.com/vihangdesai2018-png/rust-data-processing/issues)** and use **Bug Report** or **Feature Request** so we get version, OS, and repro steps.
- **Security:** do not file publicly — read [`SECURITY.md`](SECURITY.md).
- How we triage and prioritize: [`Planning/ISSUE_TRIAGE.md`](Planning/ISSUE_TRIAGE.md).

- **Status**: library APIs are in `src/lib.rs`; the binary (`src/main.rs`) is currently just a placeholder.
- **Developer guide**: see `README_DEV.md` (module map, workflows, conventions)
- **Benchmark snapshot (pipeline bench)**: on this repo (Windows, Criterion), `filter → map → reduce(sum)`:
  - **In-memory `processing`**:
    - 100k rows: ~10.42 ms median (\(\approx 9.6\) million rows/sec)
    - 1M rows: ~113.5 ms median (\(\approx 8.8\) million rows/sec)
  - **Polars-backed `pipeline::DataFrame` (lazy)**:
    - 100k rows: ~7.80 ms median (\(\approx 12.8\) million rows/sec)
    - 1M rows: ~74.10 ms median (\(\approx 13.5\) million rows/sec)
  - **Reproduce**: `cargo bench --bench pipelines -- --warm-up-time 2 --measurement-time 6 --sample-size 50`

## Phase 1 scope (roadmap)

Canonical checklist lives in `Planning/PHASE1_PLAN.md`; this section is the README-friendly summary.

- [x] Polars-first delegation for ingestion + DataFrame-centric pipelines
- [x] Polars-backed SQL support (default-on)
- [x] Engine-agnostic configuration (`IngestionOptionsBuilder`) + unified error model (`IngestionError`)
- [x] Benchmarks + parity checks (ingestion + pipelines + end-to-end)
- [x] Cookbook examples (Polars-first docs + SQL examples)
- [x] “Pit of success” defaults (sane knobs; avoid promising engine-specific tuning we can’t support)
- [x] Feature flags + dependency surface minimization
- [x] Transformation wrappers + end-user transformation schema/spec (“to/from”) on top of Polars + existing in-memory layers
- [x] Feature-gated direct DB ingestion via ConnectorX (DB → Arrow → `DataSet`) + compatibility research notes
- [x] CDC feasibility spike + interface boundary (Phase 2 candidate)
- [x] Profiling APIs: metrics set + sampling/large-data modes
- [x] Validation APIs: DSL + built-in checks + severity handling + reporting
- [x] Outlier detection: primitives + explainable outputs

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
  - `scripts/run_benchmarks.ps1` runs all Criterion benchmarks (pipelines + ingestion + map/reduce + profiling + validation + outliers).

## Python bindings

Bindings live under **`python-wrapper/`** (**PyO3** + **maturin** + **uv**). User-facing docs: **`python-wrapper/README.md`**, **`python-wrapper/API.md`**, **`python-wrapper/README_DEV.md`**. The native module calls this crate; Polars stays on the Rust side.

### Python quickstart

**From a checkout** (Rust + [uv](https://docs.astral.sh/uv/) required):

```bash
cd python-wrapper
uv sync --group dev
uv run maturin develop --release
```

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "name", "data_type": "utf8"},
]
ds = rdp.ingest_from_path("tests/fixtures/people.csv", schema, {"format": "csv"})
print("rows", ds.row_count())

report = rdp.profile_dataset(ds, {"head_rows": 50, "quantiles": [0.5]})
print("profile rows sampled", report["row_count"])

validation = rdp.validate_dataset(
    ds,
    {"checks": [{"kind": "not_null", "column": "id", "severity": "error"}]},
)
print("checks", validation["summary"]["total_checks"])
```

**From PyPI** (after you publish a release — see **`Planning/RELEASE_CHECKLIST.md`**):

```bash
pip install rust-data-processing
```

Use the same `import rust_data_processing as rdp` pattern; point `ingest_from_path` at your own CSV/JSON/Parquet files and schema.

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

## SQL queries (Polars-backed) (Phase 1)

The `rust_data_processing::sql` module compiles SQL into a Polars lazy plan and returns a `pipeline::DataFrame`.

Single-table query (table name is `df`):

```rust
use rust_data_processing::pipeline::DataFrame;
use rust_data_processing::sql;

let out = sql::query(
    &DataFrame::from_dataset(&ds)?,
    "SELECT id, score FROM df WHERE active = TRUE ORDER BY id DESC LIMIT 10",
)?
.collect()?;
```

Multi-table JOINs via a context:

```rust
use rust_data_processing::pipeline::DataFrame;
use rust_data_processing::sql;

let mut ctx = sql::Context::new();
ctx.register("people", &DataFrame::from_dataset(&people)?)?;
ctx.register("scores", &DataFrame::from_dataset(&scores)?)?;

let out = ctx
    .execute("SELECT p.id, p.name, s.score FROM people p JOIN scores s ON p.id = s.id")?
    .collect()?;
```

## Direct DB ingestion (ConnectorX) (feature-gated)

Enable the feature:

```powershell
cargo test --features db_connectorx
```

Example (Postgres):

```rust
use rust_data_processing::ingestion::ingest_from_db_infer;

// Example: cxprotocol=binary for Postgres.
let ds = ingest_from_db_infer(
    "postgresql://user:pass@localhost:5432/db?cxprotocol=binary",
    "SELECT id, score, active FROM my_table",
)?;
println!("rows={}", ds.row_count());
```

## End-user transformation spec (TransformSpec) (Phase 1)

`transform::TransformSpec` is a serde-friendly “mapping spec” that compiles to our Polars-backed pipeline wrappers
while keeping the public API engine-agnostic.

```rust
use rust_data_processing::pipeline::CastMode;
use rust_data_processing::transform::{TransformSpec, TransformStep};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("score", DataType::Int64),
        Field::new("weather", DataType::Utf8),
    ]),
    vec![
        vec![Value::Int64(1), Value::Int64(10), Value::Utf8("drizzle".to_string())],
        vec![Value::Int64(2), Value::Null, Value::Utf8("rain".to_string())],
    ],
);

let out_schema = Schema::new(vec![
    Field::new("id", DataType::Int64),
    Field::new("score_f", DataType::Float64),
    Field::new("wx", DataType::Utf8),
]);

let spec = TransformSpec::new(out_schema.clone())
    .with_step(TransformStep::Rename {
        pairs: vec![("weather".to_string(), "wx".to_string())],
    })
    .with_step(TransformStep::Rename {
        pairs: vec![("score".to_string(), "score_f".to_string())],
    })
    .with_step(TransformStep::Cast {
        column: "score_f".to_string(),
        to: DataType::Float64,
        mode: CastMode::Lossy,
    })
    .with_step(TransformStep::FillNull {
        column: "score_f".to_string(),
        value: Value::Float64(0.0),
    })
    .with_step(TransformStep::Select {
        columns: vec!["id".to_string(), "score_f".to_string(), "wx".to_string()],
    });

let out = spec.apply(&ds)?;
assert_eq!(out.schema, out_schema);
```

## Profiling (Phase 1)

Use `profiling::profile_dataset` to compute common metrics. For large data, start with deterministic sampling via `Head(n)`.

```rust
use rust_data_processing::profiling::{profile_dataset, ProfileOptions, SamplingMode};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![Field::new("score", DataType::Float64)]),
    vec![vec![Value::Float64(1.0)], vec![Value::Null], vec![Value::Float64(3.0)]],
);

let rep = profile_dataset(
    &ds,
    &ProfileOptions {
        sampling: SamplingMode::Head(2),
        quantiles: vec![0.5],
    },
)?;

assert_eq!(rep.row_count, 2);
assert_eq!(rep.columns[0].null_count, 1);
```

## Validation (Phase 1)

Define checks with `validation::ValidationSpec` and render the report as JSON/Markdown.

```rust
use rust_data_processing::validation::{validate_dataset, Check, Severity, ValidationSpec};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![Field::new("email", DataType::Utf8)]),
    vec![
        vec![Value::Utf8("ada@example.com".to_string())],
        vec![Value::Null],
        vec![Value::Utf8("not-an-email".to_string())],
    ],
);

let spec = ValidationSpec::new(vec![
    Check::NotNull { column: "email".to_string(), severity: Severity::Error },
    Check::RegexMatch {
        column: "email".to_string(),
        pattern: r"^[^@]+@[^@]+\.[^@]+$".to_string(),
        severity: Severity::Warn,
        strict: true,
    },
]);

let rep = validate_dataset(&ds, &spec)?;
assert!(rep.summary.total_checks >= 2);
```

## Outlier detection (Phase 1)

```rust
use rust_data_processing::outliers::{detect_outliers_dataset, OutlierMethod, OutlierOptions};
use rust_data_processing::profiling::SamplingMode;
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![Field::new("x", DataType::Float64)]),
    vec![
        vec![Value::Float64(1.0)],
        vec![Value::Float64(1.0)],
        vec![Value::Float64(1.0)],
        vec![Value::Float64(1000.0)],
    ],
);

let rep = detect_outliers_dataset(
    &ds,
    "x",
    OutlierMethod::Iqr { k: 1.5 },
    &OutlierOptions { sampling: SamplingMode::Full, max_examples: 3 },
)?;

assert!(rep.outlier_count >= 1);
```

## CDC interface boundary (Phase 1 spike)

The `cdc` module defines crate-owned boundary types for CDC events without picking a concrete CDC implementation dependency.

```rust
use rust_data_processing::cdc::{CdcEvent, CdcOp, RowImage, SourceMeta, TableRef};
use rust_data_processing::types::Value;

let ev = CdcEvent {
    meta: SourceMeta { source: Some("db".to_string()), checkpoint: None },
    table: TableRef::with_schema("public", "users"),
    op: CdcOp::Insert,
    before: None,
    after: Some(RowImage::new(vec![
        ("id".to_string(), Value::Int64(1)),
        ("name".to_string(), Value::Utf8("Ada".to_string())),
    ])),
};

assert_eq!(ev.op, CdcOp::Insert);
```

## Cookbook (Phase 1)

### Stable transformation wrappers (Polars-backed, engine-agnostic types)

Rename + cast + fill nulls:

```rust
use rust_data_processing::pipeline::DataFrame;
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("score", DataType::Int64),
    ]),
    vec![vec![Value::Int64(1), Value::Int64(10)], vec![Value::Int64(2), Value::Null]],
);

let out = DataFrame::from_dataset(&ds)?
    .rename(&[("score", "score_i")])?
    .cast("score_i", DataType::Float64)?
    .fill_null("score_i", Value::Float64(0.0))?
    .collect()?;
```

Group-by aggregates:

```rust
use rust_data_processing::pipeline::{Agg, DataFrame};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("grp", DataType::Utf8),
        Field::new("score", DataType::Float64),
    ]),
    vec![
        vec![Value::Utf8("A".to_string()), Value::Float64(1.0)],
        vec![Value::Utf8("A".to_string()), Value::Float64(2.0)],
        vec![Value::Utf8("B".to_string()), Value::Null],
    ],
);

let out = DataFrame::from_dataset(&ds)?
    .group_by(
        &["grp"],
        &[
            Agg::Sum {
                column: "score".to_string(),
                alias: "sum_score".to_string(),
            },
            Agg::CountRows {
                alias: "cnt".to_string(),
            },
        ],
    )?
    .collect()?;
```

Per-key **mean**, **sample std dev**, and **count-distinct** (e.g. categorical cardinality per group):

```rust
use rust_data_processing::pipeline::{Agg, DataFrame};
use rust_data_processing::processing::VarianceKind;
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("grp", DataType::Utf8),
        Field::new("score", DataType::Float64),
        Field::new("label", DataType::Utf8),
    ]),
    vec![
        vec![Value::Utf8("A".to_string()), Value::Float64(10.0), Value::Utf8("x".to_string())],
        vec![Value::Utf8("A".to_string()), Value::Float64(20.0), Value::Utf8("y".to_string())],
        vec![Value::Utf8("B".to_string()), Value::Null, Value::Utf8("z".to_string())],
    ],
);

let _out = DataFrame::from_dataset(&ds)?
    .group_by(
        &["grp"],
        &[
            Agg::Mean {
                column: "score".to_string(),
                alias: "mu_score".to_string(),
            },
            Agg::StdDev {
                column: "score".to_string(),
                alias: "sd_score".to_string(),
                kind: VarianceKind::Sample,
            },
            Agg::CountDistinctNonNull {
                column: "label".to_string(),
                alias: "n_labels".to_string(),
            },
        ],
    )?
    .collect()?;
```

**Semantics** (nulls, all-null groups, `SUM` vs `MEAN`, casting): see `Planning/REDUCE_AGG_SEMANTICS.md`. More API examples: `API.md` § *Processing pipelines*.

Join two DataFrames:

```rust
use rust_data_processing::pipeline::{DataFrame, JoinKind};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let people = DataSet::new(
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("name", DataType::Utf8),
    ]),
    vec![
        vec![Value::Int64(1), Value::Utf8("Ada".to_string())],
        vec![Value::Int64(2), Value::Utf8("Grace".to_string())],
    ],
);
let scores = DataSet::new(
    Schema::new(vec![Field::new("id", DataType::Int64), Field::new("score", DataType::Float64)]),
    vec![
        vec![Value::Int64(1), Value::Float64(9.0)],
        vec![Value::Int64(3), Value::Float64(7.0)],
    ],
);

let out = DataFrame::from_dataset(&people)?
    .join(DataFrame::from_dataset(&scores)?, &["id"], &["id"], JoinKind::Inner)?
    .collect()?;
```

## What data can be consumed? (Epic 1 / Stories 1.1–1.2)

### File formats (auto-detected by extension)

- **CSV**: `.csv` (must include headers)
- **JSON**: `.json` (array-of-objects) and `.ndjson` (newline-delimited objects)
  - Nested fields are supported via **dot paths** in schema field names (e.g. `user.name`)
- **Parquet**: `.parquet`, `.pq`
- **Excel/workbooks**: `.xlsx`, `.xls`, `.xlsm`, `.xlsb`, `.ods` (requires feature `excel`)

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
- `reduce(&DataSet, column, ReduceOp) -> Option<Value>` — includes **mean**, **variance**, **std dev** (`VarianceKind::{Population, Sample}`), **sum of squares**, **L2 norm**, **count distinct** (non-null), plus **count** / **sum** / **min** / **max**
- `feature_wise_mean_std(&DataSet, &[&str], VarianceKind)` — one pass over rows for mean + std on several numeric columns (`FeatureMeanStd`)
- `arg_max_row` / `arg_min_row` — first row index where a column is max/min (ties: smallest index)
- `top_k_by_frequency` — top‑\(k\) `(value, count)` pairs for label-style columns

Polars-backed equivalents for whole-frame scalars: `pipeline::DataFrame::reduce`, `feature_wise_mean_std`. **Semantics**: `Planning/REDUCE_AGG_SEMANTICS.md`.

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

### Mean, variance, std dev, norms, and distinct counts

```rust
use rust_data_processing::processing::{reduce, ReduceOp, VarianceKind};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("x", DataType::Float64),
        Field::new("cat", DataType::Utf8),
    ]),
    vec![
        vec![Value::Float64(2.0), Value::Utf8("a".to_string())],
        vec![Value::Float64(4.0), Value::Utf8("b".to_string())],
    ],
);

let mean = reduce(&ds, "x", ReduceOp::Mean).unwrap();
let std_s = reduce(&ds, "x", ReduceOp::StdDev(VarianceKind::Sample)).unwrap();
let l2 = reduce(&ds, "x", ReduceOp::L2Norm).unwrap();
let d = reduce(&ds, "cat", ReduceOp::CountDistinctNonNull).unwrap();
// mean == 3.0, std_s is sqrt(sample var of [2,4]), l2 == hypot(2,4), d == 2 distinct labels
assert!(matches!(mean, Value::Float64(_)));
assert!(matches!(d, Value::Int64(2)));
```

### Polars-backed `DataFrame::reduce` (same `ReduceOp`)

```rust
use rust_data_processing::pipeline::DataFrame;
use rust_data_processing::processing::{reduce, ReduceOp};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![Field::new("x", DataType::Float64)]),
    vec![vec![Value::Float64(1.0)], vec![Value::Float64(3.0)]],
);

let mem = reduce(&ds, "x", ReduceOp::Mean).unwrap();
let pol = DataFrame::from_dataset(&ds).unwrap().reduce("x", ReduceOp::Mean).unwrap().unwrap();
assert_eq!(mem, pol);
```

### Feature-wise mean and std in one pass (memory vs Polars)

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
        vec![Value::Int64(1), Value::Float64(10.0)],
        vec![Value::Int64(3), Value::Float64(20.0)],
    ],
);

let cols = ["a", "b"];
let mem = feature_wise_mean_std(&ds, &cols, VarianceKind::Sample).unwrap();
let pol = DataFrame::from_dataset(&ds)
    .unwrap()
    .feature_wise_mean_std(&cols, VarianceKind::Sample)
    .unwrap();
assert_eq!(mem[0].0, pol[0].0);
```

### Arg max / arg min row and top‑k label frequencies

```rust
use rust_data_processing::processing::{arg_max_row, top_k_by_frequency};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let ds = DataSet::new(
    Schema::new(vec![
        Field::new("id", DataType::Int64),
        Field::new("region", DataType::Utf8),
    ]),
    vec![
        vec![Value::Int64(1), Value::Utf8("west".to_string())],
        vec![Value::Int64(2), Value::Utf8("east".to_string())],
        vec![Value::Int64(3), Value::Utf8("west".to_string())],
    ],
);

let (_row, _val) = arg_max_row(&ds, "id").unwrap().unwrap();
let top = top_k_by_frequency(&ds, "region", 2).unwrap();
assert!(!top.is_empty());
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

### More examples: counts, missing columns, all-null numeric

```rust
use rust_data_processing::processing::{reduce, ReduceOp, VarianceKind};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};

let schema = Schema::new(vec![Field::new("score", DataType::Float64)]);
let ds = DataSet::new(schema, vec![vec![Value::Float64(1.0)], vec![Value::Null]]);

assert_eq!(reduce(&ds, "score", ReduceOp::Count), Some(Value::Int64(2)));
assert_eq!(reduce(&ds, "score", ReduceOp::Sum), Some(Value::Float64(1.0)));
assert_eq!(reduce(&ds, "missing", ReduceOp::Sum), None);

let all_null = DataSet::new(
    Schema::new(vec![Field::new("x", DataType::Float64)]),
    vec![vec![Value::Null], vec![Value::Null]],
);
assert_eq!(reduce(&all_null, "x", ReduceOp::Mean), Some(Value::Null));
assert_eq!(
    reduce(&all_null, "x", ReduceOp::Variance(VarianceKind::Sample)),
    Some(Value::Null)
);
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
  - In-memory vs parallel **filter/map/sum**; **scalar** mean/variance (memory vs Polars); **feature_wise_mean_std** (one pass vs Polars vs naive multi-`reduce`); **arg_max** / **top_k_by_frequency**; Polars **group_by** with mean/std/count-distinct-style `Agg`s
- `cargo bench --bench profiling`
  - Benchmarks `profiling::profile_dataset` (full vs head sampling)
- `cargo bench --bench validation`
  - Benchmarks `validation::validate_dataset` (built-in checks and reporting overhead)
- `cargo bench --bench outliers`
  - Benchmarks `outliers::detect_outliers_dataset` (full vs sampled)

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
- `sql`: enable Polars-backed SQL support (adds `polars-sql`). **Enabled by default**.
- `db_connectorx`: enable direct DB ingestion via ConnectorX (DB → Arrow → `DataSet`) including Postgres/MySQL/MS SQL/Oracle sources
- `arrow`: enable Arrow interop helpers (adds `arrow`)
- `serde_arrow`: enable Serde-based Arrow interop helpers (adds `serde` + `serde_arrow`)
- Note: ConnectorX’s Postgres support uses OpenSSL; on Windows you may need additional build prerequisites (e.g. Perl for vendored OpenSSL or a system OpenSSL install).

Disable default features (including SQL) if you want a smaller dependency surface:

```toml
[dependencies]
rust-data-processing = { path = ".", default-features = false }
```

## “Pit of success” defaults (Phase 1)

- **Ingestion defaults**: format is auto-detected by extension; observers are off by default; failures are surfaced as `IngestionError` (no automatic retries).
- **Execution defaults**: `ExecutionOptions::default()` uses available parallelism, a moderate `chunk_size`, and throttles in-flight chunks.
- **Polars pipelines + SQL**: we intentionally avoid exposing engine-specific tuning knobs in the public API. If you need low-level tuning, use Polars’ own configuration (e.g. environment variables) and treat behavior as Polars-owned.

## Run tests

```powershell
./scripts/run_unit_tests.ps1
```

## Generate API docs (Rustdoc + optional Python pdoc)

Rust uses **Rustdoc**; Python bindings use **pdoc** (same as CI). See [Documentation](#documentation-read-the-apis-online) for published links.

```powershell
./scripts/build_docs.ps1              # Rust only → target/doc/
./scripts/build_docs.ps1 -All         # Rust + Python → target/doc/ and _site/python/
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

## License

Dual-licensed under your choice of **Apache License 2.0** or **MIT**. See [`LICENSE-APACHE`](LICENSE-APACHE) and [`LICENSE-MIT`](LICENSE-MIT).

SPDX-License-Identifier: `MIT OR Apache-2.0`

## Publishing to crates.io

Maintainers: see [`Planning/RELEASE_CHECKLIST.md`](Planning/RELEASE_CHECKLIST.md) and [`How_TO_deploy.md`](Planning/How_TO_deploy.md). After the first successful `cargo publish`, API docs appear on [docs.rs](https://docs.rs/rust-data-processing) for the published version.