# Python quick start and examples

This page collects **Python** snippets for the `rust-data-processing` package (PyO3 extension). The [repository README](../../README.md) leads with a short Python quick start; the canonical API reference is [`python-wrapper/API.md`](../../python-wrapper/API.md). Rust snippets live in [`docs/rust/README.md`](../rust/README.md).

Install (from PyPI after release, or from a checkout with `maturin develop` — see [`Planning/RELEASE_CHECKLIST.md`](../../Planning/RELEASE_CHECKLIST.md)):

```bash
pip install rust-data-processing
```

```python
import rust_data_processing as rdp
```

## Quick start (library usage)

Ingest a file with an explicit schema (format can be set or inferred from the extension):

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "name", "data_type": "utf8"},
]
ds = rdp.ingest_from_path(
    "tests/fixtures/people.csv",
    schema,
    {"format": "csv"},
)
print("rows", ds.row_count())
```

Infer schema from the file, then ingest (two passes — same idea as Rust’s `ingest_with_inferred_schema`):

```python
ds, schema = rdp.ingest_with_inferred_schema("tests/fixtures/people.csv")
print(schema[0]["name"], ds.row_count())
```

## DataFrame-centric pipelines (Polars-backed) (Phase 1)

Use `DataFrame.from_dataset` for a lazy plan; chain methods and `collect()` to a `DataSet`:

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "active", "data_type": "bool"},
    {"name": "score", "data_type": "float64"},
]
rows = [
    [1, True, 10.0],
    [2, True, 20.0],
    [3, False, 30.0],
]
ds = rdp.DataSet(schema, rows)

out = (
    rdp.DataFrame.from_dataset(ds)
    .filter_eq("active", True)
    .multiply_f64("score", 2.0)
    .collect()
)
assert out.row_count() == 2
```

## SQL queries (Polars-backed) (Phase 1)

Single-table SQL — the `DataSet` is registered as table `df`; `sql_query_dataset` returns a materialized `DataSet`:

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "active", "data_type": "bool"},
    {"name": "score", "data_type": "float64"},
]
rows = [
    [1, True, 10.0],
    [2, True, 20.0],
    [3, False, 30.0],
]
ds = rdp.DataSet(schema, rows)

out = rdp.sql_query_dataset(
    ds,
    "SELECT id, score FROM df WHERE active = TRUE ORDER BY id DESC LIMIT 10",
)
```

Multi-table JOINs via `SqlContext` (`execute` returns a lazy `DataFrame`; call `collect()` for a `DataSet`):

```python
import rust_data_processing as rdp

people = rdp.DataSet(
    [
        {"name": "id", "data_type": "int64"},
        {"name": "name", "data_type": "utf8"},
    ],
    [[1, "Ada"], [2, "Grace"]],
)
scores = rdp.DataSet(
    [
        {"name": "id", "data_type": "int64"},
        {"name": "score", "data_type": "float64"},
    ],
    [[1, 9.0], [3, 7.0]],
)

ctx = rdp.SqlContext()
ctx.register("people", rdp.DataFrame.from_dataset(people))
ctx.register("scores", rdp.DataFrame.from_dataset(scores))

out = ctx.execute(
    "SELECT p.id, p.name, s.score FROM people p JOIN scores s ON p.id = s.id"
).collect()
```

## Direct DB ingestion (ConnectorX) (feature-gated)

The native module must be built with the **`db`** Cargo feature (see [`python-wrapper/README_DEV.md`](../../python-wrapper/README_DEV.md)). Then:

```python
ds = rdp.ingest_from_db_infer(
    "postgresql://user:pass@localhost:5432/db?cxprotocol=binary",
    "SELECT id, score, active FROM my_table",
)
print("rows", ds.row_count())
```

## End-user transformation spec (TransformSpec) (Phase 1)

`transform_apply` accepts a dict (or JSON string) in the same serde shape as Rust’s `TransformSpec`:

```python
import rust_data_processing as rdp

schema_in = [
    {"name": "id", "data_type": "int64"},
    {"name": "score", "data_type": "int64"},
    {"name": "weather", "data_type": "utf8"},
]
rows = [[1, 10, "drizzle"], [2, None, "rain"]]
ds = rdp.DataSet(schema_in, rows)

spec = {
    "output_schema": {
        "fields": [
            {"name": "id", "data_type": "Int64"},
            {"name": "score_f", "data_type": "Float64"},
            {"name": "wx", "data_type": "Utf8"},
        ]
    },
    "steps": [
        {"Rename": {"pairs": [["weather", "wx"]]}},
        {"Rename": {"pairs": [["score", "score_f"]]}},
        {
            "Cast": {
                "column": "score_f",
                "to": "Float64",
                "mode": "lossy",
            }
        },
        {"FillNull": {"column": "score_f", "value": {"Float64": 0.0}}},
        {"Select": {"columns": ["id", "score_f", "wx"]}},
    ],
}
out = rdp.transform_apply(ds, spec)
assert out.column_names() == ["id", "score_f", "wx"]
```

## Profiling (Phase 1)

```python
import rust_data_processing as rdp

schema = [{"name": "score", "data_type": "float64"}]
rows = [[1.0], [None], [3.0]]
ds = rdp.DataSet(schema, rows)

rep = rdp.profile_dataset(
    ds,
    {"head_rows": 2, "quantiles": [0.5]},
)
assert rep["row_count"] == 2
assert rep["columns"][0]["null_count"] == 1
```

## Validation (Phase 1)

```python
import rust_data_processing as rdp

schema = [{"name": "email", "data_type": "utf8"}]
rows = [
    ["ada@example.com"],
    [None],
    ["not-an-email"],
]
ds = rdp.DataSet(schema, rows)

rep = rdp.validate_dataset(
    ds,
    {
        "checks": [
            {"kind": "not_null", "column": "email", "severity": "error"},
            {
                "kind": "regex_match",
                "column": "email",
                "pattern": r"^[^@]+@[^@]+\.[^@]+$",
                "severity": "warn",
                "strict": True,
            },
        ],
    },
)
assert rep["summary"]["total_checks"] >= 2
```

## Outlier detection (Phase 1)

```python
import rust_data_processing as rdp

schema = [{"name": "x", "data_type": "float64"}]
rows = [[1.0], [1.0], [1.0], [1000.0]]
ds = rdp.DataSet(schema, rows)

rep = rdp.detect_outliers(
    ds,
    "x",
    {"kind": "iqr", "k": 1.5},
    {"sampling": "full", "max_examples": 3},
)
assert rep["outlier_count"] >= 1
```

## CDC interface boundary (Phase 1 spike)

The `rust_data_processing.cdc` submodule exposes plain Python types aligned with the Rust `cdc` module (no connector ships yet):

```python
from rust_data_processing.cdc import CdcEvent, CdcOp, RowImage, SourceMeta, TableRef

ev = CdcEvent(
    meta=SourceMeta(source="db", checkpoint=None),
    table=TableRef.with_schema("public", "users"),
    op=CdcOp.INSERT,
    before=None,
    after=RowImage.new([("id", 1), ("name", "Ada")]),
)
assert ev.op == CdcOp.INSERT
```

## Cookbook (Phase 1)

### Stable transformation wrappers (Polars-backed)

Rename + cast + fill nulls:

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "score", "data_type": "int64"},
]
rows = [[1, 10], [2, None]]
ds = rdp.DataSet(schema, rows)

out = (
    rdp.DataFrame.from_dataset(ds)
    .rename([("score", "score_i")])
    .cast("score_i", "float64")
    .fill_null("score_i", 0.0)
    .collect()
)
```

Group-by aggregates:

```python
schema = [
    {"name": "grp", "data_type": "utf8"},
    {"name": "score", "data_type": "float64"},
]
rows = [
    ["A", 1.0],
    ["A", 2.0],
    ["B", None],
]
ds = rdp.DataSet(schema, rows)

out = (
    rdp.DataFrame.from_dataset(ds)
    .group_by(
        ["grp"],
        [
            {"type": "sum", "column": "score", "alias": "sum_score"},
            {"type": "count_rows", "alias": "cnt"},
        ],
    )
    .collect()
)
```

Per-key **mean**, **sample std dev**, and **count-distinct**:

```python
schema = [
    {"name": "grp", "data_type": "utf8"},
    {"name": "score", "data_type": "float64"},
    {"name": "label", "data_type": "utf8"},
]
rows = [
    ["A", 10.0, "x"],
    ["A", 20.0, "y"],
    ["B", None, "z"],
]
ds = rdp.DataSet(schema, rows)

_ = (
    rdp.DataFrame.from_dataset(ds)
    .group_by(
        ["grp"],
        [
            {"type": "mean", "column": "score", "alias": "mu_score"},
            {
                "type": "std_dev",
                "column": "score",
                "alias": "sd_score",
                "kind": "sample",
            },
            {
                "type": "count_distinct_non_null",
                "column": "label",
                "alias": "n_labels",
            },
        ],
    )
    .collect()
)
```

**Semantics** (nulls, all-null groups, `SUM` vs `MEAN`, casting): see [`Planning/REDUCE_AGG_SEMANTICS.md`](../../Planning/REDUCE_AGG_SEMANTICS.md). More detail: [`python-wrapper/API.md`](../../python-wrapper/API.md) § *Processing pipelines*.

Join two DataFrames:

```python
people_schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "name", "data_type": "utf8"},
]
people_rows = [[1, "Ada"], [2, "Grace"]]
people = rdp.DataSet(people_schema, people_rows)

scores_schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "score", "data_type": "float64"},
]
scores_rows = [[1, 9.0], [3, 7.0]]
scores = rdp.DataSet(scores_schema, scores_rows)

out = (
    rdp.DataFrame.from_dataset(people)
    .join(rdp.DataFrame.from_dataset(scores), ["id"], ["id"], "inner")
    .collect()
)
```

## Processing pipelines (Epic 1 / Story 1.2)

In-memory helpers mirror `rust_data_processing::processing`:

- `processing_filter(ds, predicate)` — `predicate` receives one row as `list`
- `processing_map(ds, mapper)`
- `processing_reduce(ds, column, op)` — op names: `count`, `sum`, `min`, `max`, `mean`, `variance_population`, `variance_sample`, `stddev_population`, `stddev_sample`, `sum_squares`, `l2_norm`, `count_distinct_non_null`, …
- `processing_feature_wise_mean_std`, `processing_arg_max_row`, `processing_arg_min_row`, `processing_top_k_by_frequency`

Polars-backed equivalents: `DataFrame.reduce`, `DataFrame.feature_wise_mean_std`. **Semantics**: [`Planning/REDUCE_AGG_SEMANTICS.md`](../../Planning/REDUCE_AGG_SEMANTICS.md).

Example:

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "active", "data_type": "bool"},
    {"name": "score", "data_type": "float64"},
]
rows = [
    [1, True, 10.0],
    [2, False, 20.0],
    [3, True, None],
]
ds = rdp.DataSet(schema, rows)

filtered = rdp.processing_filter(ds, lambda row: row[1] is True)
mapped = rdp.processing_map(
    filtered,
    lambda row: [
        row[0],
        row[1],
        row[2] * 1.1 if row[2] is not None else None,
    ],
)
s = rdp.processing_reduce(mapped, "score", "sum")
assert s == 11.0
```

### Mean, variance, norms, distinct counts

```python
schema = [
    {"name": "x", "data_type": "float64"},
    {"name": "cat", "data_type": "utf8"},
]
rows = [[2.0, "a"], [4.0, "b"]]
ds = rdp.DataSet(schema, rows)

mean = rdp.processing_reduce(ds, "x", "mean")
std_s = rdp.processing_reduce(ds, "x", "stddev_sample")
l2 = rdp.processing_reduce(ds, "x", "l2_norm")
d = rdp.processing_reduce(ds, "cat", "count_distinct_non_null")
assert mean is not None and d == 2
```

### Polars-backed `DataFrame.reduce` (same op names)

```python
mem = rdp.processing_reduce(ds, "x", "mean")
pol = rdp.DataFrame.from_dataset(ds).reduce("x", "mean")
assert pol is not None and mem == pol
```

### Feature-wise mean and std (memory vs Polars)

```python
schema = [
    {"name": "a", "data_type": "int64"},
    {"name": "b", "data_type": "float64"},
]
rows = [[1, 10.0], [3, 20.0]]
ds = rdp.DataSet(schema, rows)

cols = ["a", "b"]
mem = rdp.processing_feature_wise_mean_std(ds, cols, "sample")
pol = rdp.DataFrame.from_dataset(ds).feature_wise_mean_std(cols, "sample")
assert mem[0]["column"] == pol[0]["column"]
```

### Arg max / arg min row and top‑k label frequencies

```python
schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "region", "data_type": "utf8"},
]
rows = [[1, "west"], [2, "east"], [3, "west"]]
ds = rdp.DataSet(schema, rows)

assert rdp.processing_arg_max_row(ds, "id") is not None
assert len(rdp.processing_top_k_by_frequency(ds, "region", 2)) >= 1
```

### Execution engine (parallel pipelines) (Story 1.3)

```python
import rust_data_processing as rdp

schema = [
    {"name": "id", "data_type": "int64"},
    {"name": "active", "data_type": "bool"},
    {"name": "score", "data_type": "float64"},
]
rows = [
    [1, True, 10.0],
    [2, False, 20.0],
    [3, True, None],
]
ds = rdp.DataSet(schema, rows)

engine = rdp.ExecutionEngine(
    {"num_threads": 4, "chunk_size": 1024, "max_in_flight_chunks": 4}
)
active_idx = 1
filtered = engine.filter_parallel(ds, lambda row: row[active_idx] is True)
mapped = engine.map_parallel(filtered, lambda row: list(row))
s = engine.reduce(mapped, "score", "sum")
metrics = engine.metrics_snapshot()
print("rows_processed", metrics["rows_processed"], "elapsed", metrics.get("elapsed_seconds"))
```

### More examples: counts, missing columns, all-null numeric

```python
schema = [{"name": "score", "data_type": "float64"}]
rows = [[1.0], [None]]
ds = rdp.DataSet(schema, rows)

assert rdp.processing_reduce(ds, "score", "count") == 2
assert rdp.processing_reduce(ds, "score", "sum") == 1.0
assert rdp.processing_reduce(ds, "missing", "sum") is None

all_null = rdp.DataSet(
    [{"name": "x", "data_type": "float64"}],
    [[None], [None]],
)
assert rdp.processing_reduce(all_null, "x", "mean") is None
```

### Benchmarks (Story 1.2.5)

Criterion benchmarks are run on the **Rust** crate (`cargo bench`, `scripts/run_benchmarks.ps1`). The Python package calls the same native code paths; for throughput numbers, use the Rust workflow or see the [repository README](../../README.md) benchmark snapshot.

### Observability (failure/alert hooks)

Pass `observer` / `alert_at_or_above` in the ingestion `options` dict (see [`python-wrapper/API.md`](../../python-wrapper/API.md) § *Ingestion observability*):

```python
import rust_data_processing as rdp

schema = [{"name": "id", "data_type": "int64"}]

def on_alert(ctx, severity, message):
    print(severity, message)

try:
    rdp.ingest_from_path(
        "does_not_exist.csv",
        schema,
        {
            "format": "csv",
            "alert_at_or_above": "critical",
            "observer": {"on_alert": on_alert},
        },
    )
except ValueError:
    pass
```

## See also

- [`python-wrapper/README.md`](../../python-wrapper/README.md) — install and dev workflow  
- [`python-wrapper/API.md`](../../python-wrapper/API.md) — full Python API  
- [`docs/rust/README.md`](../rust/README.md) — Rust mirror of this page  
- [`Planning/REDUCE_AGG_SEMANTICS.md`](../../Planning/REDUCE_AGG_SEMANTICS.md) — aggregate semantics
