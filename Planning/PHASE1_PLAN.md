# Phase 1 Plan (Cursor-friendly tracking)

This file is meant to be edited directly in Cursor as you work. It provides:
- Checkboxes for each unit of work
- A simple "Done columns" view (Kanban-style)

How to use:
- Move items between columns by cut/paste
- Mark completion by switching `[ ]` → `[x]`
- Keep this focused on Phase 1 (Epics 7 + 10 + required baseline refactor work)

## Kanban (Backlog / In Progress / Done)

| Backlog | In Progress | Done |
|---|---|---|
| - [ ]  | - [ ]  | - [x] Historical baseline implemented (pre-Polars strategy shift) |
| - [ ]  | - [ ]  | - [x] 0.3.1 Delegation boundaries (see `ENGINE_STRATEGY.md`) |
| - [ ]  | - [ ]  | - [x] 0.3.3 Refactor ingestion APIs to delegate to Polars scans/reads |
| - [ ]  | - [ ]  | - [x] 0.3.4 Refactor transforms/pipeline APIs to delegate to Polars lazy plan |
| - [ ] 7.1.1 Public API shape + naming conventions (ergonomic surface) | - [ ]  | - [x] 0.3.2 Engine selection strategy (see `ENGINE_STRATEGY.md`) |
| - [ ]  | - [ ]  | - [x] 0.3.4 Benchmarks suite: ingestion (20k) + map/reduce + end-to-end ingest→reduce |
| - [ ]  | - [ ]  | - [x] 0.3.4 Bench dimensions: warm vs rotating files, schema known vs inferred, JSON array vs NDJSON vs nested, Excel variability (feature-gated) |
| - [ ] 7.1.2 Builder-based configuration (avoid long arg lists) | - [ ]  | - [ ]  |
| - [ ] 7.1.3 Error model + diagnostics (actionable messages) | - [ ]  | - [ ]  |
| - [ ] 7.1.4 Cookbook examples (Rust-first docs) | - [ ]  | - [ ]  |
| - [ ] 7.2.1 "Pit of success" defaults (parallelism, memory, retries) | - [ ]  | - [ ]  |
| - [ ] 7.2.2 Feature flags & minimal dependency surface for a cargo library | - [ ]  | - [ ]  |
| - [ ] 10.1.1 Profiling metrics set (nulls, distinct, quantiles, etc.) | - [ ]  | - [ ]  |
| - [ ] 10.1.2 Sampling/streaming-friendly profiling modes | - [ ]  | - [ ]  |
| - [ ] 10.1.3 Profile report formats (JSON + Markdown) | - [ ]  | - [ ]  |
| - [ ] 10.2.1 Validation DSL (schema + rule declarations) | - [ ]  | - [ ]  |
| - [ ] 10.2.2 Built-in checks (ranges, uniqueness, regex, nullability) | - [ ]  | - [ ]  |
| - [ ] 10.2.3 Severity handling (warn vs fail) + reporting | - [ ]  | - [ ]  |
| - [ ] 10.3.1 Outlier detection primitives (IQR / z-score / MAD) | - [ ]  | - [ ]  |
| - [ ] 10.3.2 Explainable outputs (why flagged) | - [ ]  | - [ ]  |

## Engine support checks (per Phase 1 unit)

Use this as a short “research checklist” while implementing each item.

- [x] **0.3.1 Delegation boundaries**
  - **Check**: Which parts are naturally **Polars-native** (LazyFrame + expressions) vs which require **DataFusion** (SQL-first) vs **custom** (Excel advanced).
  - **Preferred (Phase 1)**: Polars for execution; keep DataFusion as optional backend only if SQL becomes core.

- [x] **0.3.2 Engine selection strategy (Polars default; DataFusion optional)**
  - **Check**: Do we need **SQL completeness** as a product requirement?
    - If **yes**: DataFusion is usually preferred for SQL-first usage.
    - If **no / minimal SQL**: Polars-first is simpler; `polars-sql` can cover some SQL but has gaps.
  - **Check**: Can DataFusion be used standalone as the Phase 1 core cargo library?
    - **Answer to validate**: Yes, it’s designed to be embedded as a library (`SessionContext`) with SQL + DataFrame APIs.

- [x] **0.3.3 Refactor ingestion APIs to delegate to scans/reads**
  - **CSV / Parquet**
    - **Check**: Polars support (strong) vs DataFusion support (also strong).
    - **Preferred (Phase 1)**: Polars-first to keep one primary engine.
  - **JSON**
    - **Check**: Polars JSON read support for your JSON shape (records vs nested vs NDJSON).
    - **Check**: DataFusion JSON support is typically more limited / format-specific; validate before committing.
    - **Preferred (Phase 1)**: Polars-first if it meets needs; otherwise keep a custom ingestion shim.
  - **Excel (.xlsx / multi-tab / legacy .xls)**
    - **Check**: Is Polars Rust ecosystem “good enough” for Excel read, or do we need a **custom** reader (likely)?
    - **Check**: DataFusion does not typically cover Excel ingestion directly.
    - **Preferred (Phase 1)**: Treat Excel as **custom connector** unless the Polars Rust story is clearly solid for your requirements.
  - [x] **Added**: schema inference helpers for benchmarking/quick exploration (`infer_schema_from_path`, `ingest_from_path_infer`)

- [x] **0.3.4 Benchmarks + parity checks**
  - **Check**: Are results identical after delegation (null semantics, type coercions, datetime parsing)?
  - **Preferred (Phase 1)**: Benchmark Polars-delegated path as the reference; document deltas vs historical behavior.
  - [x] **Ingestion (20k)**: CSV / JSON array / NDJSON / nested JSON / Parquet (Excel optional via feature)
  - [x] **Cache axis**: warm (same path) vs “cold-ish” rotating identical copies
  - [x] **Schema axis**: schema known vs schema inferred (nested JSON stays schema-known)
  - [x] **Map/Reduce**: in-memory vs `ExecutionEngine` parallel path; plus ingest→reduce end-to-end
  - [x] **Runner**: PowerShell convenience runner for all benches (`scripts/run_benchmarks.ps1`)

- **7.1.1 Public API shape + naming conventions**
  - **Check**: Polars-first API vs SQL-first API (DataFusion-style) — which do we want to present publicly?
  - **Preferred (Phase 1)**: Polars-first, DataFrame-centric API; SQL support optional behind a feature.

- **7.1.2 Builder-based configuration**
  - **Check**: Builder configs should be engine-agnostic; avoid leaking Polars/DataFusion types publicly unless intentional.
  - **Preferred (Phase 1)**: Engine-agnostic configs, Polars-backed implementation.

- **7.1.3 Error model + diagnostics**
  - **Check**: Map Polars/DataFusion errors into a single public error type (and keep their details).
  - **Preferred (Phase 1)**: Unified error model regardless of engine.

- **7.1.4 Cookbook examples**
  - **Check**: Provide Polars-first examples; optionally include “SQL example” only if we decide SQL is supported.
  - **Preferred (Phase 1)**: Polars-first docs; SQL docs optional.

- **7.2.1 "Pit of success" defaults**
  - **Check**: What knobs exist in Polars for parallelism/memory/streaming behaviors? Don’t promise knobs that don’t exist.
  - **Preferred (Phase 1)**: Safe defaults in our wrapper; minimal surfacing of engine-specific tuning.

- **7.2.2 Feature flags & minimal dependency surface**
  - **Check**: Keep DataFusion behind a feature flag if included; keep Excel behind a feature flag if it pulls heavy deps.
  - **Preferred (Phase 1)**: Small default feature set; opt-in connectors.

- **10.1.1 Profiling metrics set**
  - **Check**: Can Polars compute these efficiently on LazyFrames? (some may require materialization; verify).
  - **Preferred (Phase 1)**: Implement on Polars; keep an interface that could support DataFusion later.

- **10.1.2 Sampling/large-data modes**
  - **Check**: Polars streaming coverage for operations you need (some operations may fall back to in-memory).
  - **Preferred (Phase 1)**: Sampling-first design to avoid relying on experimental streaming guarantees.

- **10.1.3 Profile report formats**
  - **Check**: No engine dependency; pure product-layer.
  - **Preferred (Phase 1)**: Engine-agnostic.

- **10.2.1 Validation DSL / API**
  - **Check**: How checks compile to Polars expressions (preferred) vs requiring row-wise UDFs (avoid where possible).
  - **Preferred (Phase 1)**: Expression-first checks (vectorized).

- **10.2.2 Built-in checks**
  - **Check**: For each check, confirm Polars support (regex, set membership, uniqueness, etc.) in Rust API.
  - **Preferred (Phase 1)**: Polars expressions; custom only when unavoidable.

- **10.2.3 Severity + reporting**
  - **Check**: No engine dependency; pure product-layer.
  - **Preferred (Phase 1)**: Engine-agnostic.

- **10.3.1 Outlier detection primitives**
  - **Check**: Can we compute required stats with Polars expressions/aggregations without full collect?
  - **Preferred (Phase 1)**: Polars-first; document when sampling/materialization occurs.

- **10.3.2 Explainability outputs**
  - **Check**: No engine dependency; ensure reports include thresholds + stats used.
  - **Preferred (Phase 1)**: Engine-agnostic.

## Notes / decisions (keep short)

- Engine baseline: **Polars-first**. DataFusion remains an optional backend, mainly for SQL completeness and extensibility.
- Packaging: Phase 1 ships as a **cargo library** (avoid requiring a running service).
- Decision record / delegation matrix: see `ENGINE_STRATEGY.md`.

