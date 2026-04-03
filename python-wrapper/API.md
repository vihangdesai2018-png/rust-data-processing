# Python API (`rust_data_processing`)

Bindings for the [`rust-data-processing`](../README.md) crate. Types stay in **crate-owned** form (`DataSet`, lazy `DataFrame`); Polars is not exposed to Python.

**License:** MIT OR Apache-2.0 (same as the Rust crate; texts in repo root).

---

## Conventions

- **Schema**: `list[dict]` with `name` (`str`) and `data_type` (`"int64"` \| `"float64"` \| `"bool"` \| `"utf8"`; aliases like `"string"` accepted).
- **Row**: `list` of `None` / `int` / `float` / `bool` / `str`, aligned to schema order.
- **Errors**: Most failures are `ValueError` with the Rust display string; file I/O issues may surface as `OSError`.
- **JSON reports**: Raw strings are available from `*_json` functions; helpers in `__init__.py` parse them into `dict` (`profile_dataset`, `validate_dataset`, `detect_outliers`).

---

## Version

| Name | Meaning |
|------|---------|
| `__version__` | Package metadata when installed; else falls back to `extension_version()`. |
| `extension_version()` | `python-wrapper/Cargo.toml` version of the native module. |

---

## Class: `DataSet`

`DataSet(schema, rows)`

| Method | Returns |
|--------|---------|
| `row_count()` | `int` |
| `column_names()` | `list[str]` |
| `schema()` | schema list (dicts) |
| `to_rows()` | `list[list]` of scalars / `None` |

---

## Ingestion

| Function | Returns |
|----------|---------|
| `ingest_from_path(path, schema, options=None)` | `DataSet` |
| `infer_schema_from_path(path, options=None)` | schema list |
| `ingest_from_path_infer(path, options=None)` | `DataSet` |
| `ingest_with_inferred_schema(path, options=None)` | `(DataSet, schema list)` — convenience; same two-step behavior as Rust. |
| `ingest_from_db(conn, query, schema)` | `DataSet` — SQL → Arrow → dataset (requires extension built with **`db`** Cargo feature; see [README_DEV](README_DEV.md)) |
| `ingest_from_db_infer(conn, query)` | `DataSet` — inferred schema (same feature gate) |

**Database data without the `db` feature:** You only need **`--features db`** if you want **`ingest_from_db` / `ingest_from_db_infer`** (ConnectorX inside the native extension). If you already use **psycopg2**, **SQLAlchemy**, **asyncpg**, or any other Python DB API, run your query in Python, convert rows to `list[list]` aligned to a [schema](#conventions), and use **`DataSet(schema, rows)`**. Profiling, validation, SQL-on-`DataSet`, and pipelines work the same; you are not required to enable `db`.

**`options`**: optional `dict` — `format` (`"csv"`, `"json"`, `"parquet"`, `"excel"`, …), optional `excel_sheet_selection` (`mode`, `name`, `names` — see [README](README.md)), plus **observability** (below).

### Ingestion observability (`options` dict)

Matches Rust `IngestionOptions` / `IngestionObserver`:

| Key | Type | Meaning |
|-----|------|---------|
| `alert_at_or_above` | `str` | `"info"` \| `"warning"` \| `"error"` \| `"critical"` — when a failed ingest’s severity is ≥ this, Python `on_alert` is invoked (if set). Default aligns with Rust: `"critical"`. |
| `observer` | `dict` | Optional callbacks (any subset): `on_success(ctx, stats)`, `on_failure(ctx, severity, message)`, `on_alert(ctx, severity, message)`. `ctx` is a dict with `path`, `format`; `stats` has `rows`. |

```python
ingest_from_path(
    "data.csv",
    schema,
    {
        "observer": {
            "on_success": lambda ctx, st: print(st["rows"]),
            "on_failure": lambda ctx, sev, msg: print(sev, msg),
        },
        "alert_at_or_above": "error",
    },
)
```

---

## SQL (Polars-backed)

| Function / type | Role |
|-----------------|------|
| `sql_query_dataset(dataset, sql)` | Register `dataset` as table `df`, run SQL, `collect()` to `DataSet`. Use `FROM df`. |
| `SqlContext` | `register(name, DataFrame)`, `execute(sql)` → lazy `DataFrame` for multi-table queries. |

---

## Lazy `DataFrame` (pipeline)

`DataFrame.from_dataset(ds)` then chain and `collect()` → `DataSet`.

| Method | Notes |
|--------|--------|
| `filter_eq(column, value)` | Equality on a single column. |
| `filter_not_null(column)` | |
| `filter_mod_eq_int64(column, modulus, equals)` | |
| `select(columns)` | `list[str]` |
| `rename(pairs)` | `list[tuple[str,str]]` |
| `drop(columns)` | |
| `cast(column, data_type)` | e.g. `"float64"`. |
| `cast_with_mode(column, data_type, mode)` | `mode`: `"strict"` \| `"lossy"`. |
| `fill_null(column, value)` | |
| `with_literal(name, value)` | |
| `multiply_f64`, `add_f64` | In-place column math. |
| `with_mul_f64`, `with_add_f64` | Derived columns. |
| `group_by(keys, aggs)` | `aggs`: list of dicts (see below). |
| `join(other, left_on, right_on, how)` | `how`: `inner` / `left` / `right` / `full`. |
| `collect()`, `collect_with_schema(schema)` | |
| `reduce(column, op)`, `sum(column)` | `op` same string names as `processing_reduce`. |
| `feature_wise_mean_std(columns, std_kind=None)` | `std_kind`: `"sample"` (default) or `"population"`. Returns list of dicts `column`, `mean`, `std_dev`. |

### Aggregation dicts (`group_by`)

Each dict has `"type"` and fields depending on type:

| `type` | Extra keys |
|--------|------------|
| `count_rows` | `alias` |
| `count_not_null` | `column`, `alias` |
| `sum`, `min`, `max`, `mean` | `column`, `alias` |
| `variance`, `std_dev` | `column`, `alias`, optional `kind` (`population` / `sample`) |
| `sum_squares`, `l2_norm` | `column`, `alias` |
| `count_distinct_non_null` | `column`, `alias` |

---

## In-memory processing (no Polars)

| Function | Role |
|----------|------|
| `processing_filter(dataset, predicate)` | `predicate` receives one row as `list`; return truthy to keep. |
| `processing_map(dataset, mapper)` | `mapper` returns a new row `list` (same length as schema). |
| `processing_reduce(dataset, column, op)` | Returns scalar or `None`. |
| `processing_feature_wise_mean_std(dataset, columns, std_kind=None)` | List of `{column, mean, std_dev}`. |
| `processing_arg_max_row`, `processing_arg_min_row` | `(row_index, value)` or `None` if empty; unknown column → `ValueError`. |
| `processing_top_k_by_frequency(dataset, column, k)` | List of `[value, count]`. |

### Reduce op names (`processing_reduce`, `DataFrame.reduce`)

`count`, `sum`, `min`, `max`, `mean`, `variance_population`, `variance_sample`, `stddev_population`, `stddev_sample`, `sum_squares`, `l2_norm`, `count_distinct_non_null` (plus common aliases — see Rust docs).

---

## Transform spec

| Function | Role |
|----------|------|
| `transform_apply_json(dataset, spec_json)` | `spec_json` is JSON for Rust `TransformSpec` (serde shape). |
| `transform_apply(dataset, spec)` | `spec` may be a `str` or a `dict` (serialized to JSON internally). |

Serde uses Rust enum names for steps, e.g. `Select`, `Rename`, `Cast`, `FillNull`, … and `DataType` variants as strings (`Int64`, `Float64`, …).

---

## Profiling

| Function | Returns |
|----------|---------|
| `profile_dataset_json(dataset, options=None)` | JSON string |
| `profile_dataset_markdown(dataset, options=None)` | Markdown string |
| `profile_dataset(dataset, options=None)` | `dict` (parsed JSON) |

**`options`**: optional `quantiles` list; sampling via `sampling: "full"`, or `head_rows: N`, or `sampling: {"head": N}`.

---

## Validation

| Function | Returns |
|----------|---------|
| `validate_dataset_json(dataset, spec)` | JSON string |
| `validate_dataset_markdown(dataset, spec)` | Markdown |
| `validate_dataset(dataset, spec)` | `dict` |

**`spec`**: `dict` with `checks` (list) and optional `max_examples`. Each check has `kind`: `not_null`, `range_f64`, `regex_match`, `in_set`, `unique`, plus fields per kind (`column`, `severity`, …).

---

## Outliers

| Function | Returns |
|----------|---------|
| `detect_outliers_json(dataset, column, method, options=None)` | JSON string |
| `detect_outliers_markdown(...)` | Markdown |
| `detect_outliers(...)` | `dict` |

**`method`**: `{"kind": "iqr", "k": 1.5}` or `z_score` / `mad` with `threshold`.

**`options`**: optional `max_examples`, `head_rows`, or `sampling` (`"full"` or `{"head": n}`).

---

## Execution engine

`ExecutionEngine(options=None, on_execution_event=None)`

- **`options`**: `num_threads`, `chunk_size`, `max_in_flight_chunks` (same as Rust `ExecutionOptions`).
- **`on_execution_event`**: optional callable `(event_dict) -> None`. Each event is a plain dict with a `kind` string (`run_started`, `chunk_started`, `chunk_finished`, `throttle_waited`, `reduce_started`, `reduce_finished`, `run_finished`, …) plus fields mirroring Rust `ExecutionEvent` in `src/execution/observer.rs` (durations in seconds, nested `metrics` on `run_finished`).

| Method | Notes |
|--------|--------|
| `filter_parallel(dataset, predicate)` | Chunked Rayon filter; **predicate** is a Python callable taking one row `list` → `bool`. The GIL is acquired per row (same pattern as other PyO3 + Rayon bridges). |
| `map_parallel(dataset, mapper)` | Chunked Rayon map; **mapper** returns a new row `list` (same width as schema). |
| `reduce(dataset, column, op)` | Sequential reduce; updates metrics and emits observer events. |
| `metrics_snapshot()` | `dict` with `run_id`, `rows_processed`, chunk counters, `elapsed_seconds`, etc. |

For strictly single-threaded row Python logic without chunk scheduling, use `processing_filter` / `processing_map`.

---

## CDC (boundary types only)

Submodule **`rust_data_processing.cdc`** exposes dataclasses aligned with `rust_data_processing::cdc`: `CdcOp`, `TableRef`, `RowImage`, `SourceMeta`, `CdcCheckpoint`, `CdcEvent`. No connector or native events are produced yet; use for contracts and future tooling.

---

## Not wrapped in Phase 1a

- **PyArrow / pandas** conversion (optional future extra).
- **DB ingestion** works from Python only when the extension is built with `--features db` (ConnectorX + sources in the parent crate).

See [Planning/PHASE1A_PLAN.md](../Planning/PHASE1A_PLAN.md) §2.2 and [PARITY.md](PARITY.md).

---

## Examples tour (Rust docs mirrored loosely)

```python
# Ingest + observer
ds = ingest_from_path_infer("events.csv", {"observer": {"on_success": lambda c, s: None}})

# SQL
out = sql_query_dataset(ds, "SELECT * FROM df LIMIT 100")

# TransformSpec JSON
out2 = transform_apply(ds, {"output_schema": {...}, "steps": [...]})

# Reports
rep = profile_dataset(ds, {"quantiles": [0.5, 0.95]})
val = validate_dataset(ds, {"checks": [{"kind": "not_null", "column": "id", "severity": "error"}]})
out3 = detect_outliers(ds, "x", {"kind": "iqr", "k": 1.5})

# Engine + parallel row ops
eng = ExecutionEngine({"chunk_size": 4096}, on_execution_event=lambda e: None)
small = eng.filter_parallel(ds, lambda row: row[0] is not None)
eng.reduce(small, "score", "mean")
```

---

## See also

- [README.md](README.md) — install and quick start  
- [README_DEV.md](README_DEV.md) — maturin, uv, tests, **`db` feature**  
- [PARITY.md](PARITY.md) — Rust ↔ Python matrix  
- Root [API.md](../API.md) — Rust API  
- [REDUCE_AGG_SEMANTICS.md](../Planning/REDUCE_AGG_SEMANTICS.md) — aggregate semantics  
