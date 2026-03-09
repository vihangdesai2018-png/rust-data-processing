# Engine Strategy & Delegation Matrix (Phase 1)

This document implements Phase 1 stories:
- **0.3.1** Define Polars delegation boundaries (what stays, what goes)
- **0.3.2** Engine selection strategy (Polars default; DataFusion optional)

Scope: **Phase 1 ships as a cargo library**. We optimize for a stable, ergonomic Rust API and integrated data quality tooling.

## Decision summary (Phase 1)

- **Primary engine**: **Polars (Rust)** for single-node DataFrame execution (LazyFrame + expressions).
- **Optional engine**: **DataFusion** *only* if/when SQL completeness becomes a product requirement (behind a feature flag).
- **Custom connectors**: Excel (and other non-Arrow-native formats) remain **custom** until proven “good enough” via Polars ecosystem support.

Rationale:
- Our Phase 1 differentiators (**#6 Ergonomics**, **#10 Data Quality/Profiling**) sit *above* the compute engine and ship fastest with **one default engine**.
- DataFusion is excellent for SQL-first systems and can be embedded as a library (via `SessionContext`), but adding it early increases surface area and maintenance.

## Delegation matrix (what engine does what)

Legend:
- **Prefer** = default choice for Phase 1
- **Optional** = supported only behind a feature flag or only if required
- **Custom** = we own implementation / adapter

### Ingestion & IO

| Capability | Polars | DataFusion | Custom | Phase 1 decision / notes |
|---|---:|---:|---:|---|
| CSV read | Prefer | Optional |  | Both are strong; choose Polars-first to keep one engine. |
| Parquet read | Prefer | Optional |  | Both are strong; Polars-first. |
| JSON read (records / NDJSON) | Prefer (validate shapes) | Optional (validate) | Fallback if needed | Validate your JSON shapes; Polars usually better for DataFrame workflows. |
| Deeply nested JSON/Parquet | Partial | Partial | Fallback if needed | Phase 1: don’t promise deep nesting beyond what engine supports; document limits. |
| Excel read: `.xlsx` single-sheet | Maybe | No | Prefer | Validate Polars ecosystem; if not robust, keep current custom Excel reader. |
| Excel read: multi-tab | Maybe | No | Prefer | Multi-sheet is frequently “custom connector” territory in Rust. |
| Excel read: legacy `.xls` | Unlikely | No | Prefer | Treat legacy as custom; define a support policy. |
| Object store reads (S3/GCS/Azure) | Partial / evolving | Better ecosystem fit | Optional | Phase 1: keep object store connector behind a feature; don’t block Phase 1 on this. |

### Transformations (DataFrame operations)

| Capability | Polars | DataFusion | Custom | Phase 1 decision / notes |
|---|---:|---:|---:|---|
| filter / select / with_columns | Prefer | Optional |  | Express everything as Polars expressions when possible. |
| group_by / aggregation | Prefer | Optional |  | Prefer Polars; ensure behavior differences (nulls, types) are documented. |
| joins | Prefer | Optional |  | Prefer Polars; spill-to-disk is “later/backlog.” |
| sorting | Prefer | Optional |  | Prefer Polars. |
| UDFs | Use sparingly | Use sparingly |  | Prefer vectorized expressions; UDFs hurt performance and portability. |

### SQL (user-facing)

| Capability | Polars (`polars-sql`) | DataFusion | Custom | Phase 1 decision / notes |
|---|---:|---:|---:|---|
| Basic SQL (SELECT/WHERE/JOIN/GROUP BY) | Maybe | Prefer |  | If SQL is “nice to have,” `polars-sql` may be enough; validate required syntax. |
| Broad SQL feature coverage | Limited | Prefer |  | If SQL becomes a core product requirement, DataFusion is the better choice. |
| SQL UDFs / extensibility | Limited | Prefer |  | DataFusion is built around extensibility. |

### Phase 1 differentiators (Ergonomics + Data Quality)

| Capability | Polars | DataFusion | Custom | Phase 1 decision / notes |
|---|---:|---:|---:|---|
| Ergonomic Rust API (builders, docs, defaults) |  |  | Prefer | Engine-agnostic product layer: we own this. |
| Profiling (nulls, quantiles, top-k, etc.) | Prefer | Optional |  | Implement using Polars computations; keep output stable and engine-agnostic. |
| Validation DSL & checks | Prefer (expressions) | Optional |  | Compile checks to expressions; custom only where engine lacks primitives. |
| Reporting (JSON/Markdown) |  |  | Prefer | Pure product layer. |

## Engine selection strategy (0.3.2)

### What we ship in Phase 1

- A cargo library whose **public API is engine-agnostic** (our types, configs, errors, reports).
- An internal **engine adapter** implementation:
  - **Default**: Polars adapter
  - **Optional (feature-flagged)**: DataFusion adapter *only if required*

### When we add DataFusion (criteria)

Add (feature-flagged) DataFusion support when one of these becomes true:
- **SQL completeness is required** (customers demand broad SQL syntax/behavior)
- We need **DataFusion’s extensibility hooks** (optimizer rules, physical planning customization)
- We intentionally target a **SQL-first** user experience (not just DataFrame-first)

### When we do *not* add DataFusion (yet)

- If Phase 1 is primarily **DataFrame workflows** + quality tooling
- If SQL is optional and `polars-sql` (or no SQL) is acceptable
- If adding DataFusion would significantly increase:
  - build times / dependency surface
  - API complexity
  - ongoing maintenance for two engines

### Can DataFusion be used standalone for our product?

Yes. DataFusion is designed to be embedded as a Rust library:
- Primary entry point is `SessionContext`
- Supports **SQL API** and **DataFrame API**

Phase 1 decision: **keep Polars as the default** and keep DataFusion as a future option unless SQL completeness is a must-have.

## Deliverable for 0.3.1: “Delegation boundaries” output

The tables above are the initial delegation boundaries. As we refactor baseline features (stories **0.3.3–0.3.4**), we will tighten this into a “supported matrix” that reflects:
- exact file formats and dialects supported
- exact behavior differences (types, null semantics, parsing)
- performance notes and benchmarks

