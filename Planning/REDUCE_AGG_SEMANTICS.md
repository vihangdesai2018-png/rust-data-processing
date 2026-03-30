# Reduce & aggregate semantics

This document describes behavior shared by `processing::reduce`, `processing::multi`, and Polars-backed `pipeline::DataFrame` helpers (`reduce`, `group_by`, `feature_wise_mean_std`).

## Null handling

- **Numeric aggregates** (sum, min, max, mean, variance, std dev, sum of squares, L2 norm): **nulls are ignored**; only non-null values participate.
- **`ReduceOp::Count`**: counts **rows** in the dataset (includes nulls in that column).
- **`CountNotNull` / non-null counts**: count only non-null cells.
- **`CountDistinctNonNull`**: distinct values among **non-null** cells only (null is not a distinct category).

## All-null or empty inputs

- **Mean, variance, sample std dev, sum of squares, L2 norm**: if there is **no** participating non-null numeric value, the result is **`Value::Null`** (not `0`).
- **Population variance / population std dev** with a **single** non-null value: variance is **`0`**, std dev is **`0`**.
- **Sample variance / sample std dev** with **fewer than two** non-null values: **`Value::Null`** (undefined).
- **Sum / min / max** with no non-null values: **`Value::Null`**.
- **Polars `group_by` note**: some engines return **`0`** for **`sum`** over an all-null group; **mean / std** in Polars typically stay **null** for all-null groups. Prefer **mean / std** when you need “no data” vs “zero total”.

## Float rounding and parity

- In-memory stats use **`f64`** (Welford for variance). **Very large `Int64`** values converted to `f64` can differ slightly from Polars at the ULP level; integration tests allow a small **relative** tolerance in those cases.
- **Reports** (profiling, validation markdown/json) should format floats deterministically at the presentation layer if you need stable diffs.

## Casting (`CastMode`)

- Pipeline **`cast` / `cast_with_mode`** control strict vs lossy casts into our logical types; **`feature_wise_mean_std`** and scalar **`DataFrame::reduce`** use **strict cast to Polars `Float64`** for numeric stats, consistent with explicit **`CastMode::Strict`** behavior for those expressions.

## Group-by (ML-oriented)

`pipeline::Agg` supports per-group:

- **CountRows**, **CountNotNull**, **Sum**, **Min**, **Max**, **Mean**, **Variance**, **StdDev**, **SumSquares**, **L2Norm**, **CountDistinctNonNull**.

Combine multiple `Agg` variants in one `DataFrame::group_by` call for feature summaries keyed by categorical columns.

## Multi-column helpers

- **`feature_wise_mean_std`**: one scan over rows; all listed columns must be **`Int64`** or **`Float64`**.
- **`arg_max_row` / `arg_min_row`**: first row index on ties.
- **`top_k_by_frequency`**: non-null value counts, sorted by count desc then value key (stable tie-break).

## Examples in this repo

Copy-pastable Rust snippets (filter/map/reduce, mean/variance/std, `DataFrame::reduce`, `feature_wise_mean_std`, arg max/min, top‑k, `group_by` with `Agg`) live in:

- **`API.md`** — section *Processing pipelines (Epic 1 / Story 1.2)*
- **`README.md`** — *Processing pipelines*, *Cookbook* → group-by, and the ML-oriented subsections under processing
