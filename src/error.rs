use thiserror::Error;

/// Convenience result type for ingestion operations.
pub type IngestionResult<T> = Result<T, IngestionError>;

/// Error type returned by ingestion functions.
///
/// This is a single error enum shared across CSV/JSON/Parquet (and optional Excel) ingestion.
#[derive(Debug, Error)]
pub enum IngestionError {
    /// Underlying I/O error (e.g. file not found, permission denied).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[cfg(feature = "excel")]
    /// Excel ingestion error (feature-gated behind `excel`).
    #[error("excel error: {0}")]
    Excel(#[from] calamine::Error),

    /// CSV ingestion error.
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),

    /// Parquet ingestion error.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

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
