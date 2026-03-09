use std::error::Error as StdError;

use thiserror::Error;

/// Convenience result type for ingestion operations.
pub type IngestionResult<T> = Result<T, IngestionError>;

/// Error type returned by ingestion functions.
///
/// This is a single error enum shared across CSV/JSON/Parquet/Excel ingestion.
#[derive(Debug, Error)]
pub enum IngestionError {
    /// Underlying I/O error (e.g. file not found, permission denied).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Excel ingestion error.
    #[error("excel error: {0}")]
    Excel(#[from] calamine::Error),

    /// CSV ingestion error.
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    /// Parquet ingestion error.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Underlying engine error (e.g. Polars, optional SQL engine), preserved with a source chain.
    ///
    /// This is used when the engine produces a structured error that callers may want to inspect
    /// or log in detail. The top-level message is a stable, human-readable summary, while the
    /// original engine error is preserved via [`std::error::Error::source`].
    #[error("{message}: {source}")]
    Engine {
        message: String,
        #[source]
        source: Box<dyn StdError + Send + Sync + 'static>,
    },

    /// The input does not conform to the provided schema (missing required fields/columns, etc.).
    #[error("schema mismatch: {message}")]
    SchemaMismatch { message: String },

    /// A value could not be parsed into the required [`crate::types::DataType`].
    #[error("failed to parse value at row {row} column '{column}': {message} (raw='{raw}')")]
    ParseError {
        row: usize,
        column: String,
        raw: String,
        message: String,
    },
}
