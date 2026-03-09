use polars::prelude::*;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Schema, Value};

pub(crate) fn polars_error_to_ingestion(action: &str, err: PolarsError) -> IngestionError {
    match err {
        PolarsError::IO { error, .. } => {
            // `PolarsError::IO` stores an `Arc<io::Error>`, but our public error type stores a
            // concrete `io::Error`. Reconstruct one with the same kind/message.
            IngestionError::Io(std::io::Error::new(error.kind(), error.to_string()))
        }
        other => IngestionError::Engine {
            message: action.to_string(),
            source: Box::new(other),
        },
    }
}

pub(crate) fn infer_schema_from_dataframe(df: &DataFrame) -> IngestionResult<Schema> {
    use polars::datatypes::DataType as P;

    let mut fields = Vec::with_capacity(df.width());
    for col in df.columns() {
        let s = col.as_materialized_series();
        let dt = match s.dtype() {
            P::Int64 => DataType::Int64,
            P::Float64 => DataType::Float64,
            P::Boolean => DataType::Bool,
            P::String => DataType::Utf8,
            other => {
                return Err(IngestionError::SchemaMismatch {
                    message: format!("unsupported polars dtype for output schema: {other}"),
                });
            }
        };
        fields.push(crate::types::Field::new(s.name().to_string(), dt));
    }
    Ok(Schema::new(fields))
}

/// Infer an output schema from a Polars [`DataFrame`], using a **lossy** mapping into our limited
/// logical type system.
///
/// - Supported mappings: Int64/Float64/Boolean/String
/// - Other Polars dtypes are mapped to `DataType::Utf8` (stringified during conversion)
pub(crate) fn infer_schema_from_dataframe_lossy(df: &DataFrame) -> IngestionResult<Schema> {
    use polars::datatypes::DataType as P;

    let mut fields = Vec::with_capacity(df.width());
    for col in df.columns() {
        let s = col.as_materialized_series();
        let dt = match s.dtype() {
            P::Int8
            | P::Int16
            | P::Int32
            | P::Int64
            | P::UInt8
            | P::UInt16
            | P::UInt32
            | P::UInt64 => DataType::Int64,
            P::Float32 | P::Float64 => DataType::Float64,
            P::Boolean => DataType::Bool,
            P::String => DataType::Utf8,
            _ => DataType::Utf8,
        };
        fields.push(crate::types::Field::new(s.name().to_string(), dt));
    }
    Ok(Schema::new(fields))
}

/// Convert an in-memory [`DataSet`] into a Polars [`DataFrame`].
///
/// This validates that each cell in the dataset matches the declared schema data type.
pub(crate) fn dataset_to_dataframe(ds: &DataSet) -> IngestionResult<DataFrame> {
    let nrows = ds.row_count();
    let mut cols: Vec<Column> = Vec::with_capacity(ds.schema.fields.len());

    for (col_idx, field) in ds.schema.fields.iter().enumerate() {
        match field.data_type {
            DataType::Int64 => {
                let mut v: Vec<Option<i64>> = Vec::with_capacity(nrows);
                for (row_idx0, row) in ds.rows.iter().enumerate() {
                    match row.get(col_idx) {
                        Some(Value::Null) | None => v.push(None),
                        Some(Value::Int64(x)) => v.push(Some(*x)),
                        Some(other) => {
                            return Err(IngestionError::ParseError {
                                row: row_idx0 + 1,
                                column: field.name.clone(),
                                raw: format!("{other:?}"),
                                message: "value does not match schema type Int64".to_string(),
                            });
                        }
                    }
                }
                cols.push(Series::new((&field.name).into(), v).into());
            }
            DataType::Float64 => {
                let mut v: Vec<Option<f64>> = Vec::with_capacity(nrows);
                for (row_idx0, row) in ds.rows.iter().enumerate() {
                    match row.get(col_idx) {
                        Some(Value::Null) | None => v.push(None),
                        Some(Value::Float64(x)) => v.push(Some(*x)),
                        Some(other) => {
                            return Err(IngestionError::ParseError {
                                row: row_idx0 + 1,
                                column: field.name.clone(),
                                raw: format!("{other:?}"),
                                message: "value does not match schema type Float64".to_string(),
                            });
                        }
                    }
                }
                cols.push(Series::new((&field.name).into(), v).into());
            }
            DataType::Bool => {
                let mut v: Vec<Option<bool>> = Vec::with_capacity(nrows);
                for (row_idx0, row) in ds.rows.iter().enumerate() {
                    match row.get(col_idx) {
                        Some(Value::Null) | None => v.push(None),
                        Some(Value::Bool(x)) => v.push(Some(*x)),
                        Some(other) => {
                            return Err(IngestionError::ParseError {
                                row: row_idx0 + 1,
                                column: field.name.clone(),
                                raw: format!("{other:?}"),
                                message: "value does not match schema type Bool".to_string(),
                            });
                        }
                    }
                }
                cols.push(Series::new((&field.name).into(), v).into());
            }
            DataType::Utf8 => {
                let mut v: Vec<Option<String>> = Vec::with_capacity(nrows);
                for (row_idx0, row) in ds.rows.iter().enumerate() {
                    match row.get(col_idx) {
                        Some(Value::Null) | None => v.push(None),
                        Some(Value::Utf8(x)) => v.push(Some(x.clone())),
                        Some(other) => {
                            return Err(IngestionError::ParseError {
                                row: row_idx0 + 1,
                                column: field.name.clone(),
                                raw: format!("{other:?}"),
                                message: "value does not match schema type Utf8".to_string(),
                            });
                        }
                    }
                }
                cols.push(Series::new((&field.name).into(), v).into());
            }
        }
    }

    DataFrame::new(nrows, cols).map_err(|e| polars_error_to_ingestion("failed to build polars DataFrame", e))
}

pub(crate) fn dataframe_to_dataset(
    df: &DataFrame,
    schema: &Schema,
    missing_kind: &'static str,
    user_row_start: usize,
) -> IngestionResult<DataSet> {
    for field in &schema.fields {
        if df.column(&field.name).is_err() {
            return Err(IngestionError::SchemaMismatch {
                message: format!("missing required {missing_kind} '{}'", field.name),
            });
        }
    }

    let mut cols: Vec<Series> = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let s = df
            .column(&field.name)
            .map_err(|_| IngestionError::SchemaMismatch {
                message: format!("missing required {missing_kind} '{}'", field.name),
            })?
            .as_materialized_series()
            .clone();

        let target = match field.data_type {
            DataType::Int64 => polars::datatypes::DataType::Int64,
            DataType::Float64 => polars::datatypes::DataType::Float64,
            DataType::Bool => polars::datatypes::DataType::Boolean,
            DataType::Utf8 => polars::datatypes::DataType::String,
        };

        let casted = s.cast(&target).map_err(|e| IngestionError::ParseError {
            row: user_row_start,
            column: field.name.clone(),
            raw: "".to_string(),
            message: e.to_string(),
        })?;

        cols.push(casted);
    }

    let nrows = df.height();
    let mut out_rows: Vec<Vec<Value>> = Vec::with_capacity(nrows);
    for row_idx0 in 0..nrows {
        let user_row = row_idx0 + user_row_start;
        let mut out: Vec<Value> = Vec::with_capacity(schema.fields.len());
        for (field, s) in schema.fields.iter().zip(cols.iter()) {
            let av = s.get(row_idx0).map_err(|e| IngestionError::ParseError {
                row: user_row,
                column: field.name.clone(),
                raw: "".to_string(),
                message: e.to_string(),
            })?;

            let v = match (field.data_type.clone(), av) {
                (_, AnyValue::Null) => Value::Null,
                (DataType::Int64, AnyValue::Int64(v)) => Value::Int64(v),
                (DataType::Float64, AnyValue::Float64(v)) => Value::Float64(v),
                (DataType::Bool, AnyValue::Boolean(v)) => Value::Bool(v),
                (DataType::Utf8, AnyValue::String(v)) => Value::Utf8(v.to_string()),
                (DataType::Utf8, AnyValue::StringOwned(v)) => Value::Utf8(v.to_string()),
                (dt, other) => {
                    return Err(IngestionError::ParseError {
                        row: user_row,
                        column: field.name.clone(),
                        raw: other.to_string(),
                        message: format!("unexpected value for {dt:?}"),
                    });
                }
            };
            out.push(v);
        }
        out_rows.push(out);
    }

    Ok(DataSet::new(schema.clone(), out_rows))
}

