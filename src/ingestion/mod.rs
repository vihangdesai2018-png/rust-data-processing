//! Ingestion entrypoints and implementations.
//!
//! Most callers should use [`ingest_from_path`] (from [`unified`]) which:
//!
//! - auto-detects format by file extension (or you can override via [`IngestionOptions`])
//! - performs ingestion into an in-memory [`crate::types::DataSet`]
//! - optionally reports success/failure/alerts to an [`IngestionObserver`]
//!
//! For ergonomic configuration, prefer [`IngestionOptionsBuilder`] over constructing
//! [`IngestionOptions`] directly.
//!
//! Format-specific functions are also available under:
//! - [`csv`]
//! - [`excel`]
//! - [`json`]
//! - [`parquet`]

pub mod csv;
pub mod builder;
pub mod excel;
pub mod json;
pub mod parquet;
pub mod observability;
pub mod unified;
pub(crate) mod polars_bridge;

pub use observability::{
    CompositeObserver, FileObserver, IngestionContext, IngestionObserver, IngestionSeverity, IngestionStats,
    StdErrObserver,
};
pub use builder::IngestionOptionsBuilder;
pub use unified::{
    ingest_from_path, ingest_from_path_infer, infer_schema_from_path, ExcelSheetSelection, IngestionFormat,
    IngestionOptions, IngestionRequest,
};
