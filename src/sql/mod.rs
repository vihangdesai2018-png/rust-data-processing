//! SQL support (Polars-backed).
//!
//! This module is implemented as a thin wrapper around Polars SQL: it compiles SQL into a Polars
//! logical plan (a `LazyFrame`) and returns a [`crate::pipeline::DataFrame`].
//!
//! Design goals:
//! - Keep public signatures in crate types (no Polars types in signatures)
//! - Preserve underlying engine errors via `IngestionError::Engine { source, .. }`

use crate::error::{IngestionError, IngestionResult};
use crate::pipeline::DataFrame;

use polars_sql::SQLContext;

/// Default single-table name used by [`query`].
pub const DEFAULT_TABLE: &str = "df";

/// Execute a SQL query against a single [`DataFrame`].
///
/// The input is registered as the table [`DEFAULT_TABLE`] (i.e. `df`), so callers should write
/// queries like: `SELECT ... FROM df WHERE ...`.
///
/// # Example
///
/// ```no_run
/// use rust_data_processing::pipeline::DataFrame;
/// use rust_data_processing::sql;
/// use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// let ds = DataSet::new(
///     Schema::new(vec![
///         Field::new("id", DataType::Int64),
///         Field::new("active", DataType::Bool),
///     ]),
///     vec![
///         vec![Value::Int64(1), Value::Bool(true)],
///         vec![Value::Int64(2), Value::Bool(false)],
///     ],
/// );
///
/// let out = sql::query(
///     &DataFrame::from_dataset(&ds)?,
///     "SELECT id FROM df WHERE active = TRUE ORDER BY id",
/// )?
/// .collect()?;
///
/// assert_eq!(out.row_count(), 1);
/// # Ok(())
/// # }
/// ```
pub fn query(df: &DataFrame, sql: &str) -> IngestionResult<DataFrame> {
    let mut ctx = Context::new();
    ctx.register(DEFAULT_TABLE, df)?;
    ctx.execute(sql)
}

/// A SQL execution context that can register multiple tables and execute queries.
///
/// This is the preferred entrypoint for JOINs across multiple [`DataFrame`]s.
///
/// # Example (JOIN)
///
/// ```no_run
/// use rust_data_processing::pipeline::DataFrame;
/// use rust_data_processing::sql;
/// use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// let people = DataSet::new(
///     Schema::new(vec![
///         Field::new("id", DataType::Int64),
///         Field::new("name", DataType::Utf8),
///     ]),
///     vec![
///         vec![Value::Int64(1), Value::Utf8("Ada".to_string())],
///         vec![Value::Int64(2), Value::Utf8("Grace".to_string())],
///     ],
/// );
/// let scores = DataSet::new(
///     Schema::new(vec![
///         Field::new("id", DataType::Int64),
///         Field::new("score", DataType::Float64),
///     ]),
///     vec![vec![Value::Int64(1), Value::Float64(98.5)]],
/// );
///
/// let mut ctx = sql::Context::new();
/// ctx.register("people", &DataFrame::from_dataset(&people)?)?;
/// ctx.register("scores", &DataFrame::from_dataset(&scores)?)?;
///
/// let out = ctx
///     .execute("SELECT p.id, p.name, s.score FROM people p JOIN scores s ON p.id = s.id")?
///     .collect()?;
///
/// assert_eq!(out.row_count(), 1);
/// # Ok(())
/// # }
/// ```
pub struct Context {
    inner: SQLContext,
}

impl Context {
    /// Create an empty SQL context.
    pub fn new() -> Self {
        Self {
            inner: SQLContext::new(),
        }
    }

    /// Register a [`DataFrame`] as a SQL table.
    pub fn register(&mut self, name: &str, df: &DataFrame) -> IngestionResult<()> {
        if name.trim().is_empty() {
            return Err(IngestionError::SchemaMismatch {
                message: "sql table name must be non-empty".to_string(),
            });
        }
        self.inner.register(name, df.lazy_clone());
        Ok(())
    }

    /// Execute a SQL query and return a lazy [`DataFrame`].
    pub fn execute(&mut self, sql: &str) -> IngestionResult<DataFrame> {
        let lf = self
            .inner
            .execute(sql)
            .map_err(|e| IngestionError::Engine {
                message: "failed to execute sql query".to_string(),
                source: Box::new(e),
            })?;
        Ok(DataFrame::from_lazyframe(lf))
    }
}

impl Default for Context {
    fn default() -> Self {
        Self::new()
    }
}
