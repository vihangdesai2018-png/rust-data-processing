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
#[cfg(feature = "excel")]
pub mod excel;
#[cfg(not(feature = "excel"))]
pub mod excel {
    //! Excel ingestion stubs when the `excel` feature is disabled.
    //!
    //! This keeps the public module path stable (`rust_data_processing::ingestion::excel`)
    //! while avoiding pulling Excel dependencies into the default build.

    use std::path::Path;

    use crate::error::{IngestionError, IngestionResult};
    use crate::types::{DataSet, Schema};

    fn disabled() -> IngestionError {
        IngestionError::SchemaMismatch {
            message: "excel ingestion is disabled; enable Cargo feature 'excel'".to_string(),
        }
    }

    pub fn ingest_excel_from_path(
        _path: impl AsRef<Path>,
        _sheet_name: Option<&str>,
        _schema: &Schema,
    ) -> IngestionResult<DataSet> {
        Err(disabled())
    }

    pub fn ingest_excel_workbook_from_path(
        _path: impl AsRef<Path>,
        _sheet_names: Option<&[&str]>,
        _schema: &Schema,
    ) -> IngestionResult<DataSet> {
        Err(disabled())
    }

    pub fn infer_excel_schema_from_path(
        _path: impl AsRef<Path>,
        _sheet_name: Option<&str>,
    ) -> IngestionResult<Schema> {
        Err(disabled())
    }

    pub fn infer_excel_schema_workbook_from_path(
        _path: impl AsRef<Path>,
        _sheet_names: Option<&[&str]>,
    ) -> IngestionResult<Schema> {
        Err(disabled())
    }
}
pub mod json;
pub mod parquet;
#[cfg(feature = "db_connectorx")]
pub mod db;
#[cfg(not(feature = "db_connectorx"))]
pub mod db {
    //! Direct DB ingestion stubs when `db_connectorx` is disabled.
    //!
    //! Enable with `--features db_connectorx` plus a source, e.g. `--features db_mysql`.

    use crate::error::{IngestionError, IngestionResult};
    use crate::types::{DataSet, Schema};

    fn disabled() -> IngestionError {
        IngestionError::SchemaMismatch {
            message: "db ingestion is disabled; enable Cargo feature 'db_connectorx'".to_string(),
        }
    }

    pub fn ingest_from_db(_conn: &str, _query: &str, _schema: &Schema) -> IngestionResult<DataSet> {
        Err(disabled())
    }

    pub fn ingest_from_db_infer(_conn: &str, _query: &str) -> IngestionResult<DataSet> {
        Err(disabled())
    }
}
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

pub use db::{ingest_from_db, ingest_from_db_infer};
