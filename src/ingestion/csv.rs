//! CSV ingestion implementation.

use std::path::Path;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Schema, Value};

/// Ingest a CSV file into an in-memory [`DataSet`].
///
/// Rules:
///
/// - CSV must have headers.
/// - Headers must contain all schema fields (order can differ).
/// - Each value is parsed according to the schema field type.
pub fn ingest_csv_from_path(path: impl AsRef<Path>, schema: &Schema) -> IngestionResult<DataSet> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)?;
    ingest_csv_from_reader(&mut rdr, schema)
}

/// Ingest CSV data from an existing CSV reader.
pub fn ingest_csv_from_reader<R: std::io::Read>(
    rdr: &mut csv::Reader<R>,
    schema: &Schema,
) -> IngestionResult<DataSet> {
    let headers = rdr.headers()?.clone();

    // Map schema fields -> CSV column indexes (allows re-ordered CSV columns).
    let mut col_idxs = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        match headers.iter().position(|h| h == field.name) {
            Some(idx) => col_idxs.push(idx),
            None => {
                return Err(IngestionError::SchemaMismatch {
                    message: format!(
                        "missing required column '{field}'. headers={:?}",
                        headers.iter().collect::<Vec<_>>(),
                        field = field.name
                    ),
                });
            }
        }
    }

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for (row_idx0, result) in rdr.records().enumerate() {
        // Report 1-based row number for users; +1 again because header is row 1.
        let user_row = row_idx0 + 2;
        let record = result?;

        let mut row: Vec<Value> = Vec::with_capacity(schema.fields.len());
        for (field, &csv_idx) in schema.fields.iter().zip(col_idxs.iter()) {
            let raw = record.get(csv_idx).unwrap_or("");
            row.push(parse_typed_value(user_row, &field.name, &field.data_type, raw)?);
        }
        rows.push(row);
    }

    Ok(DataSet::new(schema.clone(), rows))
}

fn parse_typed_value(
    row: usize,
    column: &str,
    data_type: &DataType,
    raw: &str,
) -> IngestionResult<Value> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(Value::Null);
    }

    match data_type {
        DataType::Utf8 => Ok(Value::Utf8(trimmed.to_owned())),
        DataType::Int64 => trimmed.parse::<i64>().map(Value::Int64).map_err(|e| {
            IngestionError::ParseError {
                row,
                column: column.to_owned(),
                raw: raw.to_owned(),
                message: e.to_string(),
            }
        }),
        DataType::Float64 => trimmed.parse::<f64>().map(Value::Float64).map_err(|e| {
            IngestionError::ParseError {
                row,
                column: column.to_owned(),
                raw: raw.to_owned(),
                message: e.to_string(),
            }
        }),
        DataType::Bool => parse_bool(trimmed).map(Value::Bool).map_err(|message| {
            IngestionError::ParseError {
                row,
                column: column.to_owned(),
                raw: raw.to_owned(),
                message,
            }
        }),
    }
}

fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        _ => Err("expected bool (true/false/1/0/yes/no)".to_string()),
    }
}
