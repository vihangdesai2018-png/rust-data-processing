//! Ingestion entrypoints and implementations.
//!
//! Most callers should use [`ingest_from_path`] (from [`unified`]) which:
//!
//! - auto-detects format by file extension (or you can override via [`IngestionOptions`])
//! - performs ingestion into an in-memory [`crate::types::DataSet`]
//! - optionally reports success/failure/alerts to an [`IngestionObserver`]
//!
//! Format-specific functions are also available under:
//! - [`csv`]
//! - [`json`]
//! - [`parquet`]

pub mod csv;
#[cfg(feature = "excel")]
pub mod excel;
pub mod json;
pub mod parquet;
pub mod observability;
pub mod unified;

pub use observability::{
    CompositeObserver, FileObserver, IngestionContext, IngestionObserver, IngestionSeverity, IngestionStats,
    StdErrObserver,
};
pub use unified::{ingest_from_path, ExcelSheetSelection, IngestionFormat, IngestionOptions, IngestionRequest};
