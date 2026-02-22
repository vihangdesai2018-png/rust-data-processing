//! Unified ingestion entrypoint.
//!
//! Most callers should use [`ingest_from_path`], which ingests a file into an in-memory
//! [`crate::types::DataSet`] using a provided [`crate::types::Schema`].
//!
//! - If [`IngestionOptions::format`] is `None`, the ingestion format is inferred from the file
//!   extension.
//! - If an [`super::observability::IngestionObserver`] is provided, success/failure/alerts are
//!   reported to it.

use std::path::{Path, PathBuf};
use std::fmt;
use std::sync::Arc;
use std::error::Error as StdError;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, Schema};

use super::observability::{IngestionContext, IngestionObserver, IngestionSeverity, IngestionStats};
use super::{csv, json, parquet};

/// Supported ingestion formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionFormat {
    /// Comma-separated values.
    Csv,
    /// JSON array-of-objects or NDJSON.
    Json,
    /// Apache Parquet.
    Parquet,
    /// Spreadsheet/workbook formats (feature-gated behind `excel`).
    Excel,
}

impl IngestionFormat {
    /// Parse an ingestion format from a file extension (case-insensitive).
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_ascii_lowercase().as_str() {
            "csv" => Some(Self::Csv),
            "json" | "ndjson" => Some(Self::Json),
            "parquet" | "pq" => Some(Self::Parquet),
            "xlsx" | "xls" | "xlsm" | "xlsb" | "ods" => Some(Self::Excel),
            _ => None,
        }
    }
}

/// How to choose sheet(s) when ingesting an Excel workbook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExcelSheetSelection {
    /// Ingest the first sheet (default).
    First,
    /// Ingest a single named sheet.
    Sheet(String),
    /// Ingest all sheets and concatenate rows.
    AllSheets,
    /// Ingest only the listed sheets (in order) and concatenate rows.
    Sheets(Vec<String>),
}

impl Default for ExcelSheetSelection {
    fn default() -> Self {
        Self::First
    }
}

/// Options controlling unified ingestion behavior.
///
/// Use [`Default`] for common cases.
#[derive(Clone)]
pub struct IngestionOptions {
    /// If `None`, auto-detect format from file extension.
    pub format: Option<IngestionFormat>,
    /// Excel-specific options.
    pub excel_sheet_selection: ExcelSheetSelection,
    /// Optional observer for logging/alerts.
    pub observer: Option<Arc<dyn IngestionObserver>>,
    /// Severity threshold at which `on_alert` is invoked.
    pub alert_at_or_above: IngestionSeverity,
}

impl fmt::Debug for IngestionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngestionOptions")
            .field("format", &self.format)
            .field("excel_sheet_selection", &self.excel_sheet_selection)
            .field("observer_set", &self.observer.is_some())
            .field("alert_at_or_above", &self.alert_at_or_above)
            .finish()
    }
}

impl Default for IngestionOptions {
    fn default() -> Self {
        Self {
            format: None,
            excel_sheet_selection: ExcelSheetSelection::default(),
            observer: None,
            alert_at_or_above: IngestionSeverity::Critical,
        }
    }
}

/// Unified ingestion entry point for path-based sources.
///
/// - If `options.format` is `None`, format is inferred from the file extension.
/// - Use `options.excel_sheet_selection` for Excel multi-tab behavior.
///
/// When an observer is configured, this function reports:
///
/// - `on_success` on success, with row count stats
/// - `on_failure` on failure, with a computed severity
/// - `on_alert` on failure when the computed severity is >= `options.alert_at_or_above`
///
/// # Examples
///
/// ## CSV (auto-detect by extension)
///
/// ```no_run
/// use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
/// use rust_data_processing::types::{DataType, Field, Schema};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64),
///     Field::new("name", DataType::Utf8),
/// ]);
///
/// // Uses `.csv` to select CSV ingestion.
/// let ds = ingest_from_path("people.csv", &schema, &IngestionOptions::default())?;
/// println!("rows={}", ds.row_count());
/// # Ok(())
/// # }
/// ```
///
/// ## JSON (auto-detect by extension, with nested field paths)
///
/// ```no_run
/// use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
/// use rust_data_processing::types::{DataType, Field, Schema};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// // JSON supports nested field access via dot paths.
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64),
///     Field::new("user.name", DataType::Utf8),
/// ]);
///
/// let ds = ingest_from_path("events.json", &schema, &IngestionOptions::default())?;
/// println!("rows={}", ds.row_count());
/// # Ok(())
/// # }
/// ```
///
/// ## Parquet (auto-detect by extension)
///
/// ```no_run
/// use rust_data_processing::ingestion::{ingest_from_path, IngestionOptions};
/// use rust_data_processing::types::{DataType, Field, Schema};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64),
///     Field::new("active", DataType::Bool),
/// ]);
///
/// let ds = ingest_from_path("data.parquet", &schema, &IngestionOptions::default())?;
/// println!("rows={}", ds.row_count());
/// # Ok(())
/// # }
/// ```
///
/// ## Force a format explicitly (override extension inference)
///
/// ```no_run
/// use rust_data_processing::ingestion::{ingest_from_path, IngestionFormat, IngestionOptions};
/// use rust_data_processing::types::{DataType, Field, Schema};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// let schema = Schema::new(vec![Field::new("id", DataType::Int64)]);
///
/// let opts = IngestionOptions {
///     format: Some(IngestionFormat::Csv),
///     ..Default::default()
/// };
///
/// // Useful when a file has no extension or you want to override inference.
/// let ds = ingest_from_path("input_without_extension", &schema, &opts)?;
/// println!("rows={}", ds.row_count());
/// # Ok(())
/// # }
/// ```
///
/// ## Observability (stderr logging + alert threshold)
///
/// ```no_run
/// use std::sync::Arc;
///
/// use rust_data_processing::ingestion::{
///     ingest_from_path, IngestionOptions, IngestionSeverity, StdErrObserver,
/// };
/// use rust_data_processing::types::{DataType, Field, Schema};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// let schema = Schema::new(vec![Field::new("id", DataType::Int64)]);
///
/// let opts = IngestionOptions {
///     observer: Some(Arc::new(StdErrObserver::default())),
///     alert_at_or_above: IngestionSeverity::Critical,
///     ..Default::default()
/// };
///
/// // Missing files are treated as Critical and will trigger `on_alert` at this threshold.
/// let _err = ingest_from_path("does_not_exist.csv", &schema, &opts).unwrap_err();
/// # Ok(())
/// # }
/// ```
///
/// ## Excel (feature-gated)
///
/// To ingest Excel files, enable the `excel` feature in your `Cargo.toml`:
///
/// ```toml
/// rust-data-processing = { path = ".", features = ["excel"] }
/// ```
///
/// Example (requires the `excel` feature). Marked `no_run` so it is **compiled** by doctests
/// (no "not tested" banner), but not executed (it expects a real `workbook.xlsx` file).
///
/// ```no_run
/// use rust_data_processing::ingestion::{
///     ingest_from_path, ExcelSheetSelection, IngestionFormat, IngestionOptions,
/// };
/// use rust_data_processing::types::{DataType, Field, Schema};
///
/// # fn main() -> Result<(), rust_data_processing::IngestionError> {
/// let schema = Schema::new(vec![
///     Field::new("id", DataType::Int64),
///     Field::new("name", DataType::Utf8),
/// ]);
///
/// let opts = IngestionOptions {
///     format: Some(IngestionFormat::Excel),
///     excel_sheet_selection: ExcelSheetSelection::Sheet("Sheet1".to_string()),
///     ..Default::default()
/// };
///
/// let ds = ingest_from_path("workbook.xlsx", &schema, &opts)?;
/// println!("rows={}", ds.row_count());
/// # Ok(())
/// # }
/// ```
pub fn ingest_from_path(
    path: impl AsRef<Path>,
    schema: &Schema,
    options: &IngestionOptions,
) -> IngestionResult<DataSet> {
    let path = path.as_ref();
    let fmt = match options.format {
        Some(f) => f,
        None => infer_format_from_path(path)?,
    };

    let ctx = IngestionContext {
        path: path.to_path_buf(),
        format: fmt,
    };

    let result = match fmt {
        IngestionFormat::Csv => csv::ingest_csv_from_path(path, schema),
        IngestionFormat::Json => json::ingest_json_from_path(path, schema),
        IngestionFormat::Parquet => parquet::ingest_parquet_from_path(path, schema),
        IngestionFormat::Excel => ingest_excel_dispatch(path, schema, &options.excel_sheet_selection),
    };

    if let Some(obs) = options.observer.as_ref() {
        match &result {
            Ok(ds) => obs.on_success(&ctx, IngestionStats { rows: ds.row_count() }),
            Err(e) => {
                let sev = severity_for_error(e);
                obs.on_failure(&ctx, sev, e);
                if sev >= options.alert_at_or_above {
                    obs.on_alert(&ctx, sev, e);
                }
            }
        }
    }

    result
}

fn severity_for_error(e: &IngestionError) -> IngestionSeverity {
    match e {
        IngestionError::Io(_) => IngestionSeverity::Critical,
        IngestionError::Parquet(err) => {
            // Best-effort: parquet errors often wrap IO, but not always in a structured way.
            // If we can detect IO in the source chain, treat it as Critical.
            if error_chain_contains_io(err) {
                IngestionSeverity::Critical
            } else {
                IngestionSeverity::Error
            }
        }
        IngestionError::Csv(err) => match err.kind() {
            ::csv::ErrorKind::Io(_) => IngestionSeverity::Critical,
            _ => IngestionSeverity::Error,
        },
        #[cfg(feature = "excel")]
        IngestionError::Excel(_) => IngestionSeverity::Error,
        IngestionError::SchemaMismatch { .. } => IngestionSeverity::Error,
        IngestionError::ParseError { .. } => IngestionSeverity::Error,
    }
}

fn error_chain_contains_io(e: &(dyn StdError + 'static)) -> bool {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(e);
    while let Some(err) = cur {
        if err.is::<std::io::Error>() {
            return true;
        }
        cur = err.source();
    }
    false
}

fn infer_format_from_path(path: &Path) -> IngestionResult<IngestionFormat> {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| IngestionError::SchemaMismatch {
            message: format!(
                "cannot infer format: path has no extension ({})",
                path.display()
            ),
        })?;

    IngestionFormat::from_extension(ext).ok_or_else(|| IngestionError::SchemaMismatch {
        message: format!(
            "cannot infer format from extension '{ext}' for path ({})",
            path.display()
        ),
    })
}

fn ingest_excel_dispatch(
    path: &Path,
    schema: &Schema,
    sel: &ExcelSheetSelection,
) -> IngestionResult<DataSet> {
    // Avoid unused warnings when the feature is off.
    let _ = (path, schema, sel);

    #[cfg(feature = "excel")]
    {
        use super::excel;

        match sel {
            ExcelSheetSelection::First => excel::ingest_excel_from_path(path, None, schema),
            ExcelSheetSelection::Sheet(name) => excel::ingest_excel_from_path(path, Some(name.as_str()), schema),
            ExcelSheetSelection::AllSheets => excel::ingest_excel_workbook_from_path(path, None, schema),
            ExcelSheetSelection::Sheets(names) => {
                let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
                excel::ingest_excel_workbook_from_path(path, Some(refs.as_slice()), schema)
            }
        }
    }

    #[cfg(not(feature = "excel"))]
    {
        Err(IngestionError::SchemaMismatch {
            message: "excel ingestion not enabled (enable cargo feature 'excel')".to_string(),
        })
    }
}

/// Convenience helper for callers that want an owned request object.
///
/// This can be useful if you want to enqueue ingestion work in a job system.
#[derive(Clone)]
pub struct IngestionRequest {
    /// Path to the input file.
    pub path: PathBuf,
    /// Schema to validate/parse values into.
    pub schema: Schema,
    /// Options controlling ingestion.
    pub options: IngestionOptions,
}

impl fmt::Debug for IngestionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngestionRequest")
            .field("path", &self.path)
            .field("schema_fields", &self.schema.fields.len())
            .field("options", &self.options)
            .finish()
    }
}

impl IngestionRequest {
    /// Execute the request by calling [`ingest_from_path`].
    pub fn run(&self) -> IngestionResult<DataSet> {
        ingest_from_path(&self.path, &self.schema, &self.options)
    }
}

