//! Direct DB ingestion via ConnectorX (feature-gated).
//!
//! Feature: `db_connectorx`
//!
//! Phase 1 intent:
//! - Keep this minimal and read-only
//! - Prefer DB → Arrow (ConnectorX) → `DataSet`
//! - Keep type mapping rules explicit and lossy when needed

use std::convert::TryFrom;

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;

use connectorx::destinations::arrow::ArrowDestination;
use connectorx::get_arrow::get_arrow;
use connectorx::prelude::CXQuery;
use connectorx::source_router::SourceConn;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Field, Schema, Value};

/// Ingest a SQL query from a database into a [`DataSet`], validating/casting into `schema`.
///
/// Connection strings follow ConnectorX conventions, e.g.:
/// - `postgresql://user:pass@host:5432/db?cxprotocol=binary`
/// - `mysql://user:pass@host:3306/db`
pub fn ingest_from_db(conn: &str, query: &str, schema: &Schema) -> IngestionResult<DataSet> {
    let batches = run_connectorx(conn, &[query.to_string()])?;
    record_batches_to_dataset(&batches, schema)
}

/// Convenience: infer a best-effort schema from the query result, then ingest.
///
/// This uses a lossy mapping into `DataType::{Int64, Float64, Bool, Utf8}`.
pub fn ingest_from_db_infer(conn: &str, query: &str) -> IngestionResult<DataSet> {
    let batches = run_connectorx(conn, &[query.to_string()])?;
    let schema = infer_schema_from_record_batches_lossy(&batches)?;
    record_batches_to_dataset(&batches, &schema)
}

fn run_connectorx(conn: &str, queries: &[String]) -> IngestionResult<Vec<RecordBatch>> {
    let source_conn = SourceConn::try_from(conn).map_err(|e| IngestionError::SchemaMismatch {
        message: format!("invalid db connection string: {e}"),
    })?;

    let cx_queries: Vec<CXQuery<String>> = queries.iter().map(|q| CXQuery::Naked(q.clone())).collect();

    let dest: ArrowDestination = get_arrow(&source_conn, None, &cx_queries, None).map_err(|e| IngestionError::Engine {
        message: "failed to ingest from db via connectorx".to_string(),
        source: Box::new(e),
    })?;

    dest.arrow().map_err(|e| IngestionError::Engine {
        message: "failed to extract Arrow record batches from connectorx destination".to_string(),
        source: Box::new(e),
    })
}

fn infer_schema_from_record_batches_lossy(batches: &[RecordBatch]) -> IngestionResult<Schema> {
    let first = batches.first().ok_or_else(|| IngestionError::SchemaMismatch {
        message: "db query returned zero record batches".to_string(),
    })?;

    let mut fields = Vec::with_capacity(first.schema().fields().len());
    for f in first.schema().fields() {
        let dt = match f.data_type() {
            ArrowDataType::Boolean => DataType::Bool,
            ArrowDataType::Float16 | ArrowDataType::Float32 | ArrowDataType::Float64 => DataType::Float64,
            ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::UInt8
            | ArrowDataType::UInt16
            | ArrowDataType::UInt32
            | ArrowDataType::UInt64 => DataType::Int64,
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::Utf8,
            // Phase 1: map everything else to Utf8 (stringify at conversion time when possible).
            _ => DataType::Utf8,
        };
        fields.push(Field::new(f.name().to_string(), dt));
    }

    Ok(Schema::new(fields))
}

fn record_batches_to_dataset(batches: &[RecordBatch], schema: &Schema) -> IngestionResult<DataSet> {
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    for batch in batches {
        let ds = record_batch_to_dataset(batch, schema)?;
        all_rows.extend(ds.rows);
    }
    Ok(DataSet::new(schema.clone(), all_rows))
}

fn record_batch_to_dataset(batch: &RecordBatch, schema: &Schema) -> IngestionResult<DataSet> {
    let mut col_idx = Vec::with_capacity(schema.fields.len());
    for f in &schema.fields {
        let idx = batch
            .schema()
            .index_of(&f.name)
            .map_err(|_| IngestionError::SchemaMismatch {
                message: format!("missing required column '{}'", f.name),
            })?;
        col_idx.push(idx);
    }

    let nrows = batch.num_rows();
    let mut out_rows = Vec::with_capacity(nrows);
    for row_i in 0..nrows {
        let mut row = Vec::with_capacity(schema.fields.len());
        for (field, idx) in schema.fields.iter().zip(col_idx.iter().copied()) {
            let arr = batch.column(idx);
            row.push(arrow_value_to_value(arr.as_ref(), row_i, &field.data_type, &field.name)?);
        }
        out_rows.push(row);
    }
    Ok(DataSet::new(schema.clone(), out_rows))
}

fn arrow_value_to_value(arr: &dyn Array, row: usize, to: &DataType, name: &str) -> IngestionResult<Value> {
    if arr.is_null(row) {
        return Ok(Value::Null);
    }

    match to {
        DataType::Bool => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|a| Value::Bool(a.value(row)))
            .ok_or_else(|| IngestionError::SchemaMismatch {
                message: format!("column '{name}' is not boolean"),
            }),

        DataType::Int64 => {
            if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                Ok(Value::Int64(a.value(row)))
            } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
                Ok(Value::Int64(a.value(row) as i64))
            } else if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
                Ok(Value::Int64(a.value(row) as i64))
            } else if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
                Ok(Value::Int64(a.value(row) as i64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
                Ok(Value::Int64(a.value(row) as i64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
                Ok(Value::Int64(a.value(row) as i64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
                Ok(Value::Int64(a.value(row) as i64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
                Ok(Value::Int64(a.value(row) as i64))
            } else if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                let v = a.value(row);
                if v.fract() == 0.0 {
                    Ok(Value::Int64(v as i64))
                } else {
                    Err(IngestionError::ParseError {
                        row: row + 1,
                        column: name.to_string(),
                        raw: v.to_string(),
                        message: "expected integer (got non-integer float)".to_string(),
                    })
                }
            } else if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
                let v = a.value(row) as f64;
                if v.fract() == 0.0 {
                    Ok(Value::Int64(v as i64))
                } else {
                    Err(IngestionError::ParseError {
                        row: row + 1,
                        column: name.to_string(),
                        raw: v.to_string(),
                        message: "expected integer (got non-integer float)".to_string(),
                    })
                }
            } else {
                Err(IngestionError::SchemaMismatch {
                    message: format!("column '{name}' is not an integer-compatible Arrow array"),
                })
            }
        }

        DataType::Float64 => {
            if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
                Ok(Value::Float64(a.value(row)))
            } else if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<Int16Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<Int8Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt32Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt16Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else if let Some(a) = arr.as_any().downcast_ref::<UInt8Array>() {
                Ok(Value::Float64(a.value(row) as f64))
            } else {
                Err(IngestionError::SchemaMismatch {
                    message: format!("column '{name}' is not a float-compatible Arrow array"),
                })
            }
        }

        DataType::Utf8 => {
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                Ok(Value::Utf8(a.value(row).to_string()))
            } else {
                // Phase 1: stringify other Arrow array types.
                Ok(Value::Utf8(format!("{arr:?}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ingest_from_db_infer;

    #[test]
    fn db_ingest_returns_error_for_invalid_connection_string() {
        let err = ingest_from_db_infer("not a url", "SELECT 1").unwrap_err();
        assert!(err.to_string().contains("invalid db connection string"));
    }
}
