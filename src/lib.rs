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
//! - **Excel/workbooks** (requires the Cargo feature `excel`): `.xlsx`, `.xls`, `.xlsm`, `.xlsb`, `.ods`
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
//! - [`processing`]: in-memory dataset transformations (filter/map/reduce)
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
//! ### Reduce operations
//!
//! - [`processing::ReduceOp::Count`]: counts rows (including nulls)
//! - [`processing::ReduceOp::Sum`], [`processing::ReduceOp::Min`], [`processing::ReduceOp::Max`]:
//!   operate on numeric columns and ignore nulls. If all values are null, these return
//!   `Some(Value::Null)`.

pub mod error;
pub mod ingestion;
pub mod processing;
pub mod types;

pub use error::{IngestionError, IngestionResult};
