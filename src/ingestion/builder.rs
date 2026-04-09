use std::sync::Arc;

use crate::error::IngestionResult;
use crate::types::{DataSet, Schema, Value};

use super::observability::IngestionObserver;
use super::observability::IngestionSeverity;
use super::unified::{ExcelSheetSelection, IngestionFormat, IngestionOptions, ingest_from_path};

/// Builder for [`IngestionOptions`].
///
/// Prefer this over constructing [`IngestionOptions`] directly when you want to:
/// - avoid long struct literals in user code
/// - keep configuration engine-agnostic (no Polars/DataFusion types leak into signatures)
/// - lean on sensible defaults and override only what you need
#[derive(Debug, Clone)]
pub struct IngestionOptionsBuilder {
    options: IngestionOptions,
}

impl Default for IngestionOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IngestionOptionsBuilder {
    /// Create a builder with [`IngestionOptions::default`] values.
    pub fn new() -> Self {
        Self {
            options: IngestionOptions::default(),
        }
    }

    /// Force a specific ingestion format (otherwise inferred from path extension).
    pub fn format(mut self, format: IngestionFormat) -> Self {
        self.options.format = Some(format);
        self
    }

    /// Configure Excel sheet selection.
    pub fn excel_sheet_selection(mut self, sel: ExcelSheetSelection) -> Self {
        self.options.excel_sheet_selection = sel;
        self
    }

    /// Configure an observer for success/failure/alerts.
    pub fn observer(mut self, observer: Arc<dyn IngestionObserver>) -> Self {
        self.options.observer = Some(observer);
        self
    }

    /// Configure the severity threshold at which `on_alert` is invoked.
    pub fn alert_at_or_above(mut self, sev: IngestionSeverity) -> Self {
        self.options.alert_at_or_above = sev;
        self
    }

    /// High-water column for incremental loads (use with [`Self::watermark_exclusive_above`]).
    pub fn watermark_column(mut self, column: impl Into<String>) -> Self {
        self.options.watermark_column = Some(column.into());
        self
    }

    /// Keep rows strictly greater than this value on `watermark_column` (after ingest).
    pub fn watermark_exclusive_above(mut self, v: Value) -> Self {
        self.options.watermark_exclusive_above = Some(v);
        self
    }

    /// Build the configured [`IngestionOptions`].
    pub fn build(self) -> IngestionOptions {
        self.options
    }

    /// Convenience: ingest using the configured options.
    pub fn ingest_from_path(
        self,
        path: impl AsRef<std::path::Path>,
        schema: &Schema,
    ) -> IngestionResult<DataSet> {
        let opts = self.build();
        ingest_from_path(path, schema, &opts)
    }
}

#[cfg(test)]
mod tests {
    use super::IngestionOptionsBuilder;
    use crate::ingestion::{
        ExcelSheetSelection, IngestionFormat, IngestionOptions, IngestionSeverity,
    };

    #[test]
    fn builder_defaults_match_ingestion_options_default() {
        let built = IngestionOptionsBuilder::new().build();
        let direct = IngestionOptions::default();

        assert_eq!(built.format, direct.format);
        assert_eq!(built.excel_sheet_selection, direct.excel_sheet_selection);
        assert_eq!(built.alert_at_or_above, direct.alert_at_or_above);
        assert_eq!(built.observer.is_some(), direct.observer.is_some());
        assert_eq!(built.watermark_column, direct.watermark_column);
        assert_eq!(built.watermark_exclusive_above, direct.watermark_exclusive_above);
    }

    #[test]
    fn builder_sets_fields() {
        use crate::types::Value;

        let built = IngestionOptionsBuilder::new()
            .format(IngestionFormat::Csv)
            .excel_sheet_selection(ExcelSheetSelection::AllSheets)
            .alert_at_or_above(IngestionSeverity::Error)
            .watermark_column("ts")
            .watermark_exclusive_above(Value::Int64(0))
            .build();

        assert_eq!(built.format, Some(IngestionFormat::Csv));
        assert_eq!(built.excel_sheet_selection, ExcelSheetSelection::AllSheets);
        assert_eq!(built.alert_at_or_above, IngestionSeverity::Error);
        assert_eq!(built.watermark_column.as_deref(), Some("ts"));
        assert_eq!(built.watermark_exclusive_above, Some(Value::Int64(0)));
    }
}
