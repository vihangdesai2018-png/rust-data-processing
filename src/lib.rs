//! `rust-data-processing` is a small library for ingesting common file formats into an in-memory
//! [`types::DataSet`], using a user-provided [`types::Schema`].
//!
//! The primary entrypoint is [`ingestion::ingest_from_path`], which can auto-detect the ingestion
//! format from the file extension (or you can force a format via [`ingestion::IngestionOptions`]).
//!
//! ## What you can ingest (Epic 1 / Story 1.1)
//!
//! **File formats (auto-detected by extension):**
//!
//! - **CSV**: `.csv`
//! - **JSON**: `.json` (array-of-objects) and `.ndjson` (newline-delimited objects)
//! - **Parquet**: `.parquet`, `.pq`
//! - **Excel/workbooks**: `.xlsx`, `.xls`, `.xlsm`, `.xlsb`, `.ods`
//!
//! **Schema + value types:**
//!
//! Ingestion produces a [`types::DataSet`] whose cells are typed [`types::Value`]s matching a
//! user-provided [`types::Schema`]. Supported logical types are:
//!
//! - [`types::DataType::Int64`]
//! - [`types::DataType::Float64`]
//! - [`types::DataType::Bool`]
//! - [`types::DataType::Utf8`]
//!
//! Across formats, empty cells / empty strings / explicit JSON `null` map to [`types::Value::Null`].
//!
//! ## Quick examples: ingest data
//!
//! ```no_run
//! use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
//! use rust_data_processing::types::{DataType, Field, Schema};
//!
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64),
//!     Field::new("name", DataType::Utf8),
//! ]);
//! // Auto-detects by extension (.csv/.json/.parquet/.xlsx/...).
//! let ds = ingest_from_path("data.csv", &schema, &IngestionOptions::default())?;
//! println!("rows={}", ds.row_count());
//! # Ok(())
//! # }
//! ```
//!
//! JSON supports nested field paths using dot notation in the schema (e.g. `user.name`):
//!
//! ```no_run
//! use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
//! use rust_data_processing::types::{DataType, Field, Schema};
//!
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64),
//!     Field::new("user.name", DataType::Utf8),
//! ]);
//! let ds = ingest_from_path("events.ndjson", &schema, &IngestionOptions::default())?;
//! println!("rows={}", ds.row_count());
//! # Ok(())
//! # }
//! ```
//!
//! ## Modules
//!
//! - [`ingestion`]: unified ingestion entrypoints and format-specific implementations
//! - [`types`]: schema + in-memory dataset types
//! - [`processing`]: in-memory dataset transformations (filter/map/reduce, feature-wise stats, arg max/min, top‑k frequency)
//! - [`execution`]: execution engine for parallel pipelines + throttling + metrics
//! - `sql`: SQL support (Polars-backed; enabled by default)
//! - [`transform`]: serde-friendly transformation spec compiled to pipeline wrappers
//! - [`profiling`]: Polars-backed profiling metrics + sampling modes
//! - [`validation`]: validation DSL + built-in checks + reporting
//! - [`outliers`]: outlier detection primitives + explainable outputs
//! - [`cdc`]: CDC boundary types (Phase 1 spike)
//! - [`error`]: error types used across ingestion
//!
//! ## Processing example (1.2 pipeline)
//!
//! ```rust
//! use rust_data_processing::processing::{filter, map, reduce, ReduceOp};
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64),
//!     Field::new("active", DataType::Bool),
//!     Field::new("score", DataType::Float64),
//! ]);
//! let ds = DataSet::new(
//!     schema,
//!     vec![
//!         vec![Value::Int64(1), Value::Bool(true), Value::Float64(10.0)],
//!         vec![Value::Int64(2), Value::Bool(false), Value::Float64(20.0)],
//!         vec![Value::Int64(3), Value::Bool(true), Value::Null],
//!     ],
//! );
//!
//! let active_idx = ds.schema.index_of("active").unwrap();
//! let filtered = filter(&ds, |row| matches!(row.get(active_idx), Some(Value::Bool(true))));
//! let mapped = map(&filtered, |row| {
//!     let mut out = row.to_vec();
//!     // score *= 2.0
//!     if let Some(Value::Float64(v)) = out.get(2) {
//!         out[2] = Value::Float64(v * 2.0);
//!     }
//!     out
//! });
//!
//! let sum = reduce(&mapped, "score", ReduceOp::Sum).unwrap();
//! assert_eq!(sum, Value::Float64(20.0));
//! ```
//!
//! ## Execution engine example (1.3 parallel pipeline)
//!
//! ```no_run
//! use rust_data_processing::execution::{ExecutionEngine, ExecutionOptions};
//! use rust_data_processing::processing::ReduceOp;
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! # fn main() {
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64),
//!     Field::new("active", DataType::Bool),
//!     Field::new("score", DataType::Float64),
//! ]);
//! let ds = DataSet::new(
//!     schema,
//!     vec![
//!         vec![Value::Int64(1), Value::Bool(true), Value::Float64(10.0)],
//!         vec![Value::Int64(2), Value::Bool(false), Value::Float64(20.0)],
//!         vec![Value::Int64(3), Value::Bool(true), Value::Null],
//!     ],
//! );
//!
//! let engine = ExecutionEngine::new(ExecutionOptions {
//!     num_threads: Some(4),
//!     chunk_size: 1_024,
//!     max_in_flight_chunks: 4,
//! });
//!
//! let active_idx = ds.schema.index_of("active").unwrap();
//! let filtered = engine.filter_parallel(&ds, |row| matches!(row.get(active_idx), Some(Value::Bool(true))));
//! let mapped = engine.map_parallel(&filtered, |row| row.to_vec());
//! let sum = engine.reduce(&mapped, "score", ReduceOp::Sum).unwrap();
//! assert_eq!(sum, Value::Float64(30.0));
//!
//! let snapshot = engine.metrics().snapshot();
//! println!("rows_processed={}", snapshot.rows_processed);
//! # }
//! ```
//!
//! ## Quick examples: Phase 1 modules
//!
//! ### TransformSpec (declarative ETL)
//!
//! ```rust
//! use rust_data_processing::pipeline::CastMode;
//! use rust_data_processing::transform::{TransformSpec, TransformStep};
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! let ds = DataSet::new(
//!     Schema::new(vec![
//!         Field::new("id", DataType::Int64),
//!         Field::new("score", DataType::Int64),
//!     ]),
//!     vec![vec![Value::Int64(1), Value::Int64(10)], vec![Value::Int64(2), Value::Null]],
//! );
//!
//! let out_schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64),
//!     Field::new("score_f", DataType::Float64),
//! ]);
//!
//! let spec = TransformSpec::new(out_schema.clone())
//!     .with_step(TransformStep::Rename { pairs: vec![("score".to_string(), "score_f".to_string())] })
//!     .with_step(TransformStep::Cast { column: "score_f".to_string(), to: DataType::Float64, mode: CastMode::Lossy })
//!     .with_step(TransformStep::FillNull { column: "score_f".to_string(), value: Value::Float64(0.0) });
//!
//! let out = spec.apply(&ds).unwrap();
//! assert_eq!(out.schema, out_schema);
//! ```
//!
//! ### Profiling (metrics + deterministic sampling)
//!
//! ```rust
//! use rust_data_processing::profiling::{profile_dataset, ProfileOptions, SamplingMode};
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! let ds = DataSet::new(
//!     Schema::new(vec![Field::new("x", DataType::Float64)]),
//!     vec![vec![Value::Float64(1.0)], vec![Value::Null], vec![Value::Float64(3.0)]],
//! );
//!
//! let rep = profile_dataset(
//!     &ds,
//!     &ProfileOptions { sampling: SamplingMode::Head(2), quantiles: vec![0.5] },
//! )
//! .unwrap();
//! assert_eq!(rep.row_count, 2);
//! ```
//!
//! ### Validation (DSL + reporting)
//!
//! ```rust
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//! use rust_data_processing::validation::{validate_dataset, Check, Severity, ValidationSpec};
//!
//! let ds = DataSet::new(
//!     Schema::new(vec![Field::new("email", DataType::Utf8)]),
//!     vec![vec![Value::Utf8("ada@example.com".to_string())], vec![Value::Null]],
//! );
//!
//! let spec = ValidationSpec::new(vec![
//!     Check::NotNull { column: "email".to_string(), severity: Severity::Error },
//! ]);
//!
//! let rep = validate_dataset(&ds, &spec).unwrap();
//! assert_eq!(rep.summary.total_checks, 1);
//! ```
//!
//! ### Outliers (IQR / z-score / MAD)
//!
//! ```rust
//! use rust_data_processing::outliers::{detect_outliers_dataset, OutlierMethod, OutlierOptions};
//! use rust_data_processing::profiling::SamplingMode;
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! let ds = DataSet::new(
//!     Schema::new(vec![Field::new("x", DataType::Float64)]),
//!     vec![
//!         vec![Value::Float64(1.0)],
//!         vec![Value::Float64(1.0)],
//!         vec![Value::Float64(1.0)],
//!         vec![Value::Float64(1000.0)],
//!     ],
//! );
//!
//! let rep = detect_outliers_dataset(
//!     &ds,
//!     "x",
//!     OutlierMethod::Iqr { k: 1.5 },
//!     &OutlierOptions { sampling: SamplingMode::Full, max_examples: 3 },
//! )
//! .unwrap();
//! assert!(rep.outlier_count >= 1);
//! ```
//!
//! ### SQL (Polars-backed)
//!
//! ```no_run
//! # #[cfg(feature = "sql")]
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! use rust_data_processing::pipeline::DataFrame;
//! use rust_data_processing::sql;
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! let ds = DataSet::new(
//!     Schema::new(vec![Field::new("id", DataType::Int64), Field::new("active", DataType::Bool)]),
//!     vec![vec![Value::Int64(1), Value::Bool(true)]],
//! );
//! let out = sql::query(&DataFrame::from_dataset(&ds)?, "SELECT id FROM df WHERE active = TRUE")?
//!     .collect()?;
//! assert_eq!(out.row_count(), 1);
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "sql"))]
//! # fn main() {}
//! ```
//!
//! For more end-to-end examples, see the repository `README.md` and `API.md` (processing / aggregates).
//! Aggregate semantics: `docs/REDUCE_AGG_SEMANTICS.md`.
//!
//! ### Reduce operations
//!
//! - [`processing::ReduceOp::Count`]: counts rows (including nulls)
//! - [`processing::ReduceOp::Sum`], [`processing::ReduceOp::Min`], [`processing::ReduceOp::Max`]:
//!   operate on numeric columns and ignore nulls. If all values are null, these return
//!   `Some(Value::Null)`.
//! - [`processing::ReduceOp::Mean`], [`processing::ReduceOp::Variance`], [`processing::ReduceOp::StdDev`]:
//!   use a numerically stable one-pass (Welford) accumulation; mean / sum-of-squares / L2 norm are
//!   returned as [`types::Value::Float64`]. Sample variance / std dev require at least two values.
//! - [`processing::ReduceOp::CountDistinctNonNull`]: distinct non-null values (also for UTF-8 and bool).
//! - [`pipeline::DataFrame::reduce`] provides the Polars-backed equivalent for whole-frame scalars.
//! - [`processing::feature_wise_mean_std`]: one scan, mean + std for several numeric columns; [`pipeline::DataFrame::feature_wise_mean_std`] for Polars.
//! - [`processing::arg_max_row`], [`processing::arg_min_row`], [`processing::top_k_by_frequency`]: row extrema and label top‑k.

pub mod cdc;
pub mod error;
pub mod execution;
pub mod ingestion;
pub mod outliers;
pub mod pipeline;
pub mod processing;
pub mod profiling;
#[cfg(feature = "sql")]
pub mod sql;
pub mod transform;
pub mod types;
pub mod validation;

pub use error::{IngestionError, IngestionResult};
