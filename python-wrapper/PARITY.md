# Rust ↔ Python API parity matrix

Status of Python (`rust_data_processing`) vs the main crate (`rust-data-processing`). Review when cutting releases.

| Rust module / API | Python surface | Notes |
|-------------------|----------------|-------|
| `ingestion::ingest_from_path` | `ingest_from_path` | |
| `ingestion::infer_schema_from_path` | `infer_schema_from_path` | |
| `ingestion::ingest_from_path_infer` | `ingest_from_path_infer`, `ingest_with_inferred_schema` | |
| `ingestion::IngestionOptions` + observer | `options` dict: `format`, `excel_sheet_selection`, **`watermark_column`**, **`watermark_exclusive_above`**, **`observer`**, **`alert_at_or_above`** | Observer keys: `on_success`, `on_failure`, `on_alert` |
| `ingestion::discover_hive_partitioned_files`, `paths_from_glob`, `paths_from_explicit_list`, `parse_partition_segment` | Same names on `rust_data_processing` | `discover_hive_partitioned_files` returns list of dicts |
| `ingestion::ingest_from_db` | `ingest_from_db` | Needs `maturin` / `cargo` build with `--features db` on the extension |
| `ingestion::ingest_from_db_infer` | `ingest_from_db_infer` | Same |
| `types::DataSet` | `DataSet` | |
| `processing::*` row ops | `processing_*` + `ExecutionEngine.filter_parallel` / `map_parallel` | Parallel methods use Rayon; Python callbacks take the GIL per row |
| `execution::ExecutionEngine` | `ExecutionEngine` | `on_execution_event` ctor arg → execution metrics / chunk events |
| `pipeline::DataFrame` | `DataFrame` | |
| `sql` | `sql_query_dataset`, `SqlContext` | |
| `transform::TransformSpec` | `transform_apply` / `transform_apply_json` | JSON serde shape |
| `profiling` | `profile_dataset*` | |
| `validation` | `validate_dataset*` | |
| `outliers` | `detect_outliers*` | |
| `cdc` (types only) | `rust_data_processing.cdc` | Pure Python dataclasses; no Rust wire-up yet |
| `ingestion` observability (`StdErrObserver`, …) | Use Python `observer` dict or compose in app | |
| Optional: pandas / pyarrow | Not in runtime API | Dev-only: `pyarrow` used in `tests/test_deep_parity.py` for parquet column selection vs Rust/Polars |

**Python tests (no `cargo test`):** `pytest` under `python-wrapper/tests/` mirrors `tests/deep_tests.rs` (`-m deep`), `tests/sql.rs`, `tests/ingestion_observability.rs`, `tests/mapping_spec.rs`, CSV/JSON fixture ingestion, plus `pytest-benchmark` workloads (`-m benchmark`) aligned with Criterion benches.

**Legend:** “\*” = JSON or Markdown helpers plus dict-parsed helpers in `__init__.py`.
