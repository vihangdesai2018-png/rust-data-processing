//! In-memory data transformations.
//!
//! The processing layer operates on [`crate::types::DataSet`] values produced by ingestion.
//! It is intentionally simple and purely in-memory for now.
//!
//! Currently implemented:
//!
//! - [`filter()`]: row filtering by predicate
//! - [`map()`]: row mapping by user function
//! - [`reduce()`]: common reductions (count/sum/min/max)
//!
//! ## Example: filter → map → reduce
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
//! // Keep only active rows.
//! let active_idx = ds.schema.index_of("active").unwrap();
//! let filtered = filter(&ds, |row| matches!(row.get(active_idx), Some(Value::Bool(true))));
//!
//! // Apply a multiplier to score.
//! let mapped = map(&filtered, |row| {
//!     let mut out = row.to_vec();
//!     if let Some(Value::Float64(v)) = out.get(2) {
//!         out[2] = Value::Float64(v * 1.1);
//!     }
//!     out
//! });
//!
//! // Sum scores (nulls ignored).
//! let sum = reduce(&mapped, "score", ReduceOp::Sum).unwrap();
//! assert_eq!(sum, Value::Float64(11.0));
//! ```

pub mod filter;
pub mod map;
pub mod reduce;

pub use filter::filter;
pub use map::map;
pub use reduce::{reduce, ReduceOp};

