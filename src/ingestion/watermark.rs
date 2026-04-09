//! High-water / incremental row filter applied **after** ingest (file or DB).
//!
//! When [`super::IngestionOptions::watermark_column`] and
//! [`super::IngestionOptions::watermark_exclusive_above`] are both set, only rows where the
//! watermark column is **strictly greater than** the high-water value are kept. Rows with a null
//! in that column are dropped.

use std::cmp::Ordering;

use crate::error::{IngestionError, IngestionResult};
use crate::types::{DataSet, DataType, Schema, Value};

use super::IngestionOptions;

/// Ensure watermark options are consistent with `schema` and with each other.
pub fn validate_watermark_config(schema: &Schema, options: &IngestionOptions) -> IngestionResult<()> {
    let col = &options.watermark_column;
    let floor = &options.watermark_exclusive_above;
    match (col.as_ref(), floor.as_ref()) {
        (None, None) => Ok(()),
        (Some(_), None) | (None, Some(_)) => Err(IngestionError::SchemaMismatch {
            message: "watermark_column and watermark_exclusive_above must both be set or both omitted"
                .to_string(),
        }),
        (Some(name), Some(floor_val)) => {
            if matches!(floor_val, Value::Null) {
                return Err(IngestionError::SchemaMismatch {
                    message: "watermark_exclusive_above must not be Null".to_string(),
                });
            }
            let idx = schema.index_of(name).ok_or_else(|| IngestionError::SchemaMismatch {
                message: format!("watermark column '{name}' not found in schema"),
            })?;
            let field = &schema.fields[idx];
            ensure_value_matches_type(floor_val, &field.data_type, "watermark_exclusive_above")?;
            Ok(())
        }
    }
}

fn ensure_value_matches_type(v: &Value, dt: &DataType, ctx: &str) -> IngestionResult<()> {
    let ok = match dt {
        DataType::Int64 => matches!(v, Value::Int64(_)),
        DataType::Float64 => matches!(v, Value::Float64(_)),
        DataType::Bool => matches!(v, Value::Bool(_)),
        DataType::Utf8 => matches!(v, Value::Utf8(_)),
    };
    if ok {
        Ok(())
    } else {
        Err(IngestionError::SchemaMismatch {
            message: format!(
                "{ctx} does not match the watermark column type ({dt:?})",
                dt = dt
            ),
        })
    }
}

/// Keep only rows where `column` compares **strictly greater than** `floor` (per column [`DataType`]).
pub fn apply_watermark_filter(
    ds: DataSet,
    schema: &Schema,
    column: &str,
    floor: &Value,
) -> IngestionResult<DataSet> {
    let idx = schema.index_of(column).ok_or_else(|| IngestionError::SchemaMismatch {
        message: format!("watermark column '{column}' not found in schema"),
    })?;
    let dt = &schema.fields[idx].data_type;

    let mut kept = Vec::with_capacity(ds.rows.len());
    for (row_i0, row) in ds.rows.iter().enumerate() {
        let user_row = row_i0 + 1;
        let cell = &row[idx];
        if row_is_above_watermark(cell, floor, dt, user_row, column)? {
            kept.push(row.clone());
        }
    }

    Ok(DataSet::new(ds.schema, kept))
}

/// Apply watermark filtering when options request it (call after [`validate_watermark_config`]).
pub fn apply_watermark_after_ingest(
    ds: DataSet,
    schema: &Schema,
    options: &IngestionOptions,
) -> IngestionResult<DataSet> {
    match (&options.watermark_column, &options.watermark_exclusive_above) {
        (None, None) => Ok(ds),
        (Some(col), Some(floor)) => apply_watermark_filter(ds, schema, col, floor),
        _ => Err(IngestionError::SchemaMismatch {
            message: "invalid watermark options state".to_string(),
        }),
    }
}

fn row_is_above_watermark(
    cell: &Value,
    floor: &Value,
    dt: &DataType,
    row: usize,
    column: &str,
) -> IngestionResult<bool> {
    if matches!(cell, Value::Null) {
        return Ok(false);
    }
    let ord = compare_cell_to_floor(cell, floor, dt, row, column)?;
    Ok(ord == Ordering::Greater)
}

fn compare_cell_to_floor(
    cell: &Value,
    floor: &Value,
    dt: &DataType,
    row: usize,
    column: &str,
) -> IngestionResult<Ordering> {
    match dt {
        DataType::Int64 => {
            let a = expect_int64(cell, row, column)?;
            let b = match floor {
                Value::Int64(v) => *v,
                _ => {
                    return Err(IngestionError::SchemaMismatch {
                        message: "watermark value type mismatch (expected int64)".to_string(),
                    })
                }
            };
            Ok(a.cmp(&b))
        }
        DataType::Float64 => {
            let a = expect_float64(cell, row, column)?;
            let b = match floor {
                Value::Float64(v) => *v,
                _ => {
                    return Err(IngestionError::SchemaMismatch {
                        message: "watermark value type mismatch (expected float64)".to_string(),
                    })
                }
            };
            Ok(a.total_cmp(&b))
        }
        DataType::Bool => {
            let a = expect_bool(cell, row, column)?;
            let b = match floor {
                Value::Bool(v) => *v,
                _ => {
                    return Err(IngestionError::SchemaMismatch {
                        message: "watermark value type mismatch (expected bool)".to_string(),
                    })
                }
            };
            Ok(a.cmp(&b))
        }
        DataType::Utf8 => {
            let a = match cell {
                Value::Utf8(s) => s.as_str(),
                _ => {
                    return Err(IngestionError::ParseError {
                        row,
                        column: column.to_string(),
                        raw: format!("{cell:?}"),
                        message: "expected utf8 for watermark column".to_string(),
                    })
                }
            };
            let b = match floor {
                Value::Utf8(s) => s.as_str(),
                _ => {
                    return Err(IngestionError::SchemaMismatch {
                        message: "watermark value type mismatch (expected utf8)".to_string(),
                    })
                }
            };
            Ok(a.cmp(b))
        }
    }
}

fn expect_int64(v: &Value, row: usize, column: &str) -> IngestionResult<i64> {
    match v {
        Value::Int64(i) => Ok(*i),
        _ => Err(IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: format!("{v:?}"),
            message: "expected int64 for watermark column".to_string(),
        }),
    }
}

fn expect_float64(v: &Value, row: usize, column: &str) -> IngestionResult<f64> {
    match v {
        Value::Float64(f) => Ok(*f),
        _ => Err(IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: format!("{v:?}"),
            message: "expected float64 for watermark column".to_string(),
        }),
    }
}

fn expect_bool(v: &Value, row: usize, column: &str) -> IngestionResult<bool> {
    match v {
        Value::Bool(b) => Ok(*b),
        _ => Err(IngestionError::ParseError {
            row,
            column: column.to_string(),
            raw: format!("{v:?}"),
            message: "expected bool for watermark column".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Field;

    fn ts_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("ts", DataType::Int64),
        ])
    }

    #[test]
    fn filter_keeps_strictly_greater() {
        let schema = ts_schema();
        let ds = DataSet::new(
            schema.clone(),
            vec![
                vec![Value::Int64(1), Value::Int64(100)],
                vec![Value::Int64(2), Value::Int64(101)],
            ],
        );
        let out = apply_watermark_filter(ds, &schema, "ts", &Value::Int64(100)).unwrap();
        assert_eq!(out.row_count(), 1);
        assert_eq!(out.rows[0][0], Value::Int64(2));
    }

    #[test]
    fn filter_empty_when_none_above() {
        let schema = ts_schema();
        let ds = DataSet::new(
            schema.clone(),
            vec![vec![Value::Int64(1), Value::Int64(10)]],
        );
        let out = apply_watermark_filter(ds, &schema, "ts", &Value::Int64(99)).unwrap();
        assert_eq!(out.row_count(), 0);
    }
}
