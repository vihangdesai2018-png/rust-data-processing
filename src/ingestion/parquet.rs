//! Parquet ingestion implementation.

use std::path::Path;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Schema};

use polars::prelude::*;

use super::polars_bridge::{dataframe_to_dataset, polars_error_to_ingestion};

/// Ingest a Parquet file into an in-memory `DataSet`.
///
/// Notes:
/// - Validates that all schema fields exist as columns
/// - Delegates Parquet decoding to Polars, then converts into `DataSet`
pub fn ingest_parquet_from_path(path: impl AsRef<Path>, schema: &Schema) -> IngestionResult<DataSet> {
    let path = path.as_ref();

    let df = LazyFrame::scan_parquet(path.to_string_lossy().as_ref().into(), ScanArgsParquet::default())
        .map_err(|e| polars_error_to_ingestion("failed to read parquet with polars", e))?
        .collect()
        .map_err(|e| polars_error_to_ingestion("failed to collect parquet with polars", e))?;

    // Parquet: keep "type mismatch" strictness. If the physical/logical Parquet column type is
    // incompatible with the requested schema type (e.g. UTF8 string column for an Int64 field),
    // we surface this as a ParseError (tests rely on this behavior).
    validate_parquet_column_types(&df, schema)?;

    dataframe_to_dataset(&df, schema, "column", 1)
}

fn validate_parquet_column_types(df: &DataFrame, schema: &Schema) -> IngestionResult<()> {
    for field in &schema.fields {
        let s = df
            .column(&field.name)
            .map_err(|_| IngestionError::SchemaMismatch {
                message: format!("missing required column '{}'", field.name),
            })?
            .as_materialized_series();

        if !dtype_compatible_with_schema(&field.data_type, s.dtype()) {
            return Err(IngestionError::ParseError {
                row: 1,
                column: field.name.clone(),
                raw: s.dtype().to_string(),
                message: "parquet column type mismatch".to_string(),
            });
        }
    }
    Ok(())
}

fn dtype_compatible_with_schema(schema_dtype: &DataType, polars_dtype: &polars::datatypes::DataType) -> bool {
    use polars::datatypes::DataType as P;

    match schema_dtype {
        DataType::Utf8 => matches!(polars_dtype, P::String),
        DataType::Bool => matches!(polars_dtype, P::Boolean),
        DataType::Int64 => matches!(
            polars_dtype,
            P::Int8 | P::Int16 | P::Int32 | P::Int64 | P::UInt8 | P::UInt16 | P::UInt32 | P::UInt64
        ),
        DataType::Float64 => matches!(
            polars_dtype,
            P::Float32
                | P::Float64
                | P::Int8
                | P::Int16
                | P::Int32
                | P::Int64
                | P::UInt8
                | P::UInt16
                | P::UInt32
                | P::UInt64
        ),
    }
}
