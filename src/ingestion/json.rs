//! JSON ingestion implementation.
//!
//! Supported inputs:
//! - A JSON array of objects: `[{"a":1}, {"a":2}]`
//! - Newline-delimited JSON (NDJSON): `{"a":1}\n{"a":2}\n`
//!
//! Nested fields are supported using dot paths in schema field names (e.g. `user.name`).

use std::fs::File;
use std::path::Path;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Schema, Value};

use polars::prelude::*;

use super::polars_bridge::{dataframe_to_dataset, polars_error_to_ingestion};

/// Ingest JSON into an in-memory `DataSet`.
pub fn ingest_json_from_path(path: impl AsRef<Path>, schema: &Schema) -> IngestionResult<DataSet> {
    let path = path.as_ref();
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");

    let json_format = if ext.eq_ignore_ascii_case("ndjson") {
        JsonFormat::JsonLines
    } else {
        JsonFormat::Json
    };

    let file = File::open(path)?;
    let df = JsonReader::new(file)
        .with_json_format(json_format)
        .finish()
        .map_err(|e| json_polars_error("failed to read json with polars", e))?;

    // Ensure required top-level fields exist before we build any lazy projections.
    // (Dotted fields like `user.name` are handled via derived columns below.)
    for field in &schema.fields {
        if !field.name.contains('.') && df.column(&field.name).is_err() {
            return Err(IngestionError::SchemaMismatch {
                message: format!("missing required field '{}'", field.name),
            });
        }
    }

    let mut lf = df.lazy();

    // Add derived columns for dotted schema field paths (e.g. `user.name`).
    let mut derived: Vec<Expr> = Vec::new();
    for field in &schema.fields {
        if field.name.contains('.') {
            derived.push(expr_for_dot_path(&field.name));
        }
    }
    if !derived.is_empty() {
        lf = lf.with_columns(derived);
    }

    // Select only the schema columns in schema order, then convert to DataSet.
    let projection: Vec<Expr> = schema.fields.iter().map(|f| col(&f.name)).collect();
    let projected = lf
        .select(projection)
        .collect()
        .map_err(|e| json_polars_error("failed to project json fields", e))?;

    dataframe_to_dataset(&projected, schema, "field", 1)
}

fn json_polars_error(action: &str, err: PolarsError) -> IngestionError {
    match err {
        PolarsError::ColumnNotFound(name) => IngestionError::SchemaMismatch {
            message: format!("missing required field '{name}'"),
        },
        other => polars_error_to_ingestion(action, other),
    }
}

fn expr_for_dot_path(path: &str) -> Expr {
    let mut iter = path.split('.');
    let root = iter.next().unwrap_or(path);
    let mut expr = col(root);
    for seg in iter {
        expr = expr.struct_().field_by_name(seg);
    }
    expr.alias(path)
}

/// Ingest JSON from an in-memory string into a [`DataSet`].
pub fn ingest_json_from_str(input: &str, schema: &Schema) -> IngestionResult<DataSet> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(IngestionError::SchemaMismatch {
            message: "json input is empty".to_string(),
        });
    }

    // First try parsing as a single JSON value (array or object).
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
        match v {
            serde_json::Value::Array(items) => ingest_json_values(&items, schema),
            serde_json::Value::Object(_) => ingest_json_values(&vec![v], schema),
            _ => Err(IngestionError::SchemaMismatch {
                message: "json must be an object, an array of objects, or NDJSON".to_string(),
            }),
        }
    } else {
        // Fall back to NDJSON.
        let mut values = Vec::new();
        for (i, line) in trimmed.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v = serde_json::from_str::<serde_json::Value>(line).map_err(|e| {
                IngestionError::SchemaMismatch {
                    message: format!("invalid ndjson at line {}: {}", i + 1, e),
                }
            })?;
            values.push(v);
        }
        ingest_json_values(&values, schema)
    }
}

fn ingest_json_values(values: &[serde_json::Value], schema: &Schema) -> IngestionResult<DataSet> {
    let mut rows: Vec<Vec<Value>> = Vec::with_capacity(values.len());

    for (idx0, v) in values.iter().enumerate() {
        let row_num = idx0 + 1;
        let obj = v.as_object().ok_or_else(|| IngestionError::SchemaMismatch {
            message: format!("row {row_num} is not a json object"),
        })?;

        let mut row: Vec<Value> = Vec::with_capacity(schema.fields.len());
        for field in &schema.fields {
            let jv = get_by_dot_path(obj, &field.name).ok_or_else(|| IngestionError::SchemaMismatch {
                message: format!("row {row_num} missing required field '{}'", field.name),
            })?;
            row.push(convert_json_value(row_num, &field.name, &field.data_type, jv)?);
        }
        rows.push(row);
    }

    Ok(DataSet::new(schema.clone(), rows))
}

fn get_by_dot_path<'a>(
    root: &'a serde_json::Map<String, serde_json::Value>,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let mut current: &serde_json::Value = root.get(path.split('.').next().unwrap_or(path))?;

    // If there are no dots, short-circuit.
    if !path.contains('.') {
        return Some(current);
    }

    for segment in path.split('.').skip(1) {
        match current {
            serde_json::Value::Object(map) => current = map.get(segment)?,
            _ => return None,
        }
    }
    Some(current)
}

fn convert_json_value(
    row: usize,
    column: &str,
    data_type: &DataType,
    v: &serde_json::Value,
) -> IngestionResult<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }

    match data_type {
        DataType::Utf8 => v.as_str().map(|s| Value::Utf8(s.to_string())).ok_or_else(|| {
            IngestionError::ParseError {
                row,
                column: column.to_string(),
                raw: v.to_string(),
                message: "expected string".to_string(),
            }
        }),
        DataType::Bool => v.as_bool().map(Value::Bool).ok_or_else(|| IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: v.to_string(),
            message: "expected bool".to_string(),
        }),
        DataType::Int64 => {
            if let Some(n) = v.as_i64() {
                Ok(Value::Int64(n))
            } else if let Some(n) = v.as_u64() {
                i64::try_from(n).map(Value::Int64).map_err(|_| IngestionError::ParseError {
                    row,
                    column: column.to_string(),
                    raw: v.to_string(),
                    message: "u64 out of range for i64".to_string(),
                })
            } else {
                Err(IngestionError::ParseError {
                    row,
                    column: column.to_string(),
                    raw: v.to_string(),
                    message: "expected integer number".to_string(),
                })
            }
        }
        DataType::Float64 => v.as_f64().map(Value::Float64).ok_or_else(|| IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: v.to_string(),
            message: "expected number".to_string(),
        }),
    }
}
