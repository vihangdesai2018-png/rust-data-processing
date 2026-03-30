//! Transformation specifications and helpers.
//!
//! This module defines **engine-agnostic** transformation specs in crate-owned types that can be
//! applied to an in-memory [`crate::types::DataSet`].
//!
//! Phase 1 intent:
//! - Keep public API free of Polars types
//! - Implement by compiling to the Polars-backed [`crate::pipeline::DataFrame`] where possible
//! - Reserve room for additional backends later
//!
//! ## Example
//!
//! ```rust
//! use rust_data_processing::pipeline::CastMode;
//! use rust_data_processing::transform::{TransformSpec, TransformStep};
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! let ds = DataSet::new(
//!     Schema::new(vec![
//!         Field::new("id", DataType::Int64),
//!         Field::new("score", DataType::Int64),
//!         Field::new("weather", DataType::Utf8),
//!     ]),
//!     vec![
//!         vec![Value::Int64(1), Value::Int64(10), Value::Utf8("drizzle".to_string())],
//!         vec![Value::Int64(2), Value::Null, Value::Utf8("rain".to_string())],
//!     ],
//! );
//!
//! let out_schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64),
//!     Field::new("score_f", DataType::Float64),
//!     Field::new("wx", DataType::Utf8),
//! ]);
//!
//! let spec = TransformSpec::new(out_schema.clone())
//!     .with_step(TransformStep::Rename {
//!         pairs: vec![("weather".to_string(), "wx".to_string())],
//!     })
//!     .with_step(TransformStep::Rename {
//!         pairs: vec![("score".to_string(), "score_f".to_string())],
//!     })
//!     .with_step(TransformStep::Cast {
//!         column: "score_f".to_string(),
//!         to: DataType::Float64,
//!         mode: CastMode::Lossy,
//!     })
//!     .with_step(TransformStep::FillNull {
//!         column: "score_f".to_string(),
//!         value: Value::Float64(0.0),
//!     })
//!     .with_step(TransformStep::Select {
//!         columns: vec!["id".to_string(), "score_f".to_string(), "wx".to_string()],
//!     });
//!
//! let out = spec.apply(&ds)?;
//! assert_eq!(out.schema, out_schema);
//! # Ok(())
//! # }
//! ```

use crate::error::IngestionResult;
use crate::pipeline::{CastMode, DataFrame};
use crate::types::{DataSet, DataType, Schema, Value};
use serde::{Deserialize, Serialize};

/// A transformation step in a [`TransformSpec`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransformStep {
    /// Select/reorder columns (in the provided order).
    Select { columns: Vec<String> },
    /// Drop columns.
    Drop { columns: Vec<String> },
    /// Rename columns (strict: source columns must exist).
    Rename { pairs: Vec<(String, String)> },
    /// Cast a column to a target type.
    Cast {
        column: String,
        to: DataType,
        #[serde(default)]
        mode: CastMode,
    },
    /// Fill nulls in a column with a literal.
    FillNull { column: String, value: Value },
    /// Add a derived column with a literal value.
    WithLiteral { name: String, value: Value },
    /// Add a derived Float64 column: `name = source * factor` (nulls propagate).
    DeriveMulF64 {
        name: String,
        source: String,
        factor: f64,
    },
    /// Add a derived Float64 column: `name = source + delta` (nulls propagate).
    DeriveAddF64 {
        name: String,
        source: String,
        delta: f64,
    },
}

/// A user-provided transformation specification with an explicit output schema.
///
/// The output schema is used to:
/// - enforce required output columns exist
/// - enforce output types (via casting) when collecting back into a [`DataSet`]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransformSpec {
    pub output_schema: Schema,
    pub steps: Vec<TransformStep>,
}

impl TransformSpec {
    pub fn new(output_schema: Schema) -> Self {
        Self {
            output_schema,
            steps: Vec::new(),
        }
    }

    pub fn with_step(mut self, step: TransformStep) -> Self {
        self.steps.push(step);
        self
    }

    /// Apply this spec to an input dataset.
    pub fn apply(&self, input: &DataSet) -> IngestionResult<DataSet> {
        let mut df = DataFrame::from_dataset(input)?;

        for step in &self.steps {
            df = match step {
                TransformStep::Select { columns } => {
                    let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    df.select(&cols)?
                }
                TransformStep::Drop { columns } => {
                    let cols: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
                    df.drop(&cols)?
                }
                TransformStep::Rename { pairs } => {
                    let pairs_ref: Vec<(&str, &str)> = pairs
                        .iter()
                        .map(|(a, b)| (a.as_str(), b.as_str()))
                        .collect();
                    df.rename(&pairs_ref)?
                }
                TransformStep::Cast { column, to, mode } => {
                    df.cast_with_mode(column, to.clone(), *mode)?
                }
                TransformStep::FillNull { column, value } => df.fill_null(column, value.clone())?,
                TransformStep::WithLiteral { name, value } => {
                    df.with_literal(name, value.clone())?
                }
                TransformStep::DeriveMulF64 {
                    name,
                    source,
                    factor,
                } => df.with_mul_f64(name, source, *factor)?,
                TransformStep::DeriveAddF64 {
                    name,
                    source,
                    delta,
                } => df.with_add_f64(name, source, *delta)?,
            };
        }

        df.collect_with_schema(&self.output_schema)
    }
}

/// Arrow interop helpers (feature-gated).
#[cfg(feature = "arrow")]
pub mod arrow {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use arrow::record_batch::RecordBatch;

    use crate::error::{IngestionError, IngestionResult};
    use crate::types::{DataSet, DataType, Field as DsField, Schema, Value};

    pub fn schema_from_record_batch(batch: &RecordBatch) -> IngestionResult<Schema> {
        let mut fields = Vec::with_capacity(batch.schema().fields().len());
        for f in batch.schema().fields() {
            let dt = match f.data_type() {
                ArrowDataType::Int64 => DataType::Int64,
                ArrowDataType::Float64 => DataType::Float64,
                ArrowDataType::Boolean => DataType::Bool,
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataType::Utf8,
                other => {
                    return Err(IngestionError::SchemaMismatch {
                        message: format!("unsupported Arrow dtype in schema: {other:?}"),
                    });
                }
            };
            fields.push(DsField::new(f.name().to_string(), dt));
        }
        Ok(Schema::new(fields))
    }

    pub fn dataset_to_record_batch(ds: &DataSet) -> IngestionResult<RecordBatch> {
        let mut arrow_fields = Vec::with_capacity(ds.schema.fields.len());
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(ds.schema.fields.len());

        for (col_idx, field) in ds.schema.fields.iter().enumerate() {
            match field.data_type {
                DataType::Int64 => {
                    let mut v = Vec::with_capacity(ds.row_count());
                    for row in &ds.rows {
                        match row.get(col_idx) {
                            Some(Value::Null) | None => v.push(None),
                            Some(Value::Int64(x)) => v.push(Some(*x)),
                            Some(other) => {
                                return Err(IngestionError::ParseError {
                                    row: 1,
                                    column: field.name.clone(),
                                    raw: format!("{other:?}"),
                                    message: "value does not match schema type Int64".to_string(),
                                });
                            }
                        }
                    }
                    cols.push(Arc::new(Int64Array::from(v)) as ArrayRef);
                    arrow_fields.push(Field::new(&field.name, ArrowDataType::Int64, true));
                }
                DataType::Float64 => {
                    let mut v = Vec::with_capacity(ds.row_count());
                    for row in &ds.rows {
                        match row.get(col_idx) {
                            Some(Value::Null) | None => v.push(None),
                            Some(Value::Float64(x)) => v.push(Some(*x)),
                            Some(other) => {
                                return Err(IngestionError::ParseError {
                                    row: 1,
                                    column: field.name.clone(),
                                    raw: format!("{other:?}"),
                                    message: "value does not match schema type Float64".to_string(),
                                });
                            }
                        }
                    }
                    cols.push(Arc::new(Float64Array::from(v)) as ArrayRef);
                    arrow_fields.push(Field::new(&field.name, ArrowDataType::Float64, true));
                }
                DataType::Bool => {
                    let mut v = Vec::with_capacity(ds.row_count());
                    for row in &ds.rows {
                        match row.get(col_idx) {
                            Some(Value::Null) | None => v.push(None),
                            Some(Value::Bool(x)) => v.push(Some(*x)),
                            Some(other) => {
                                return Err(IngestionError::ParseError {
                                    row: 1,
                                    column: field.name.clone(),
                                    raw: format!("{other:?}"),
                                    message: "value does not match schema type Bool".to_string(),
                                });
                            }
                        }
                    }
                    cols.push(Arc::new(BooleanArray::from(v)) as ArrayRef);
                    arrow_fields.push(Field::new(&field.name, ArrowDataType::Boolean, true));
                }
                DataType::Utf8 => {
                    let mut v = Vec::with_capacity(ds.row_count());
                    for row in &ds.rows {
                        match row.get(col_idx) {
                            Some(Value::Null) | None => v.push(None),
                            Some(Value::Utf8(x)) => v.push(Some(x.as_str())),
                            Some(other) => {
                                return Err(IngestionError::ParseError {
                                    row: 1,
                                    column: field.name.clone(),
                                    raw: format!("{other:?}"),
                                    message: "value does not match schema type Utf8".to_string(),
                                });
                            }
                        }
                    }
                    cols.push(Arc::new(StringArray::from(v)) as ArrayRef);
                    arrow_fields.push(Field::new(&field.name, ArrowDataType::Utf8, true));
                }
            }
        }

        let schema = Arc::new(ArrowSchema::new(arrow_fields));
        RecordBatch::try_new(schema, cols).map_err(|e| IngestionError::Engine {
            message: "failed to build Arrow RecordBatch".to_string(),
            source: Box::new(e),
        })
    }

    pub fn record_batch_to_dataset(
        batch: &RecordBatch,
        schema: &Schema,
    ) -> IngestionResult<DataSet> {
        // Map schema fields to column indices by name.
        let mut col_idx = Vec::with_capacity(schema.fields.len());
        for f in &schema.fields {
            let idx =
                batch
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
                let v = match field.data_type {
                    DataType::Int64 => {
                        let a = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                            IngestionError::SchemaMismatch {
                                message: format!("arrow column '{}' is not Int64", field.name),
                            }
                        })?;
                        if a.is_null(row_i) {
                            Value::Null
                        } else {
                            Value::Int64(a.value(row_i))
                        }
                    }
                    DataType::Float64 => {
                        let a = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                            IngestionError::SchemaMismatch {
                                message: format!("arrow column '{}' is not Float64", field.name),
                            }
                        })?;
                        if a.is_null(row_i) {
                            Value::Null
                        } else {
                            Value::Float64(a.value(row_i))
                        }
                    }
                    DataType::Bool => {
                        let a = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                            IngestionError::SchemaMismatch {
                                message: format!("arrow column '{}' is not Boolean", field.name),
                            }
                        })?;
                        if a.is_null(row_i) {
                            Value::Null
                        } else {
                            Value::Bool(a.value(row_i))
                        }
                    }
                    DataType::Utf8 => {
                        // Accept both Utf8 and LargeUtf8 arrays.
                        if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                            if a.is_null(row_i) {
                                Value::Null
                            } else {
                                Value::Utf8(a.value(row_i).to_string())
                            }
                        } else {
                            return Err(IngestionError::SchemaMismatch {
                                message: format!("arrow column '{}' is not Utf8", field.name),
                            });
                        }
                    }
                };
                row.push(v);
            }
            out_rows.push(row);
        }
        Ok(DataSet::new(schema.clone(), out_rows))
    }
}

/// Serde-based interop helpers (feature-gated).
///
/// This uses `serde_arrow` to reduce boilerplate when turning a Rust record type into columnar data.
#[cfg(feature = "serde_arrow")]
pub mod serde_interop {
    use arrow::datatypes::FieldRef;
    use arrow::record_batch::RecordBatch;
    use serde_arrow::schema::{SchemaLike, TracingOptions};

    use crate::error::{IngestionError, IngestionResult};

    /// Build a `RecordBatch` from Rust records using schema tracing.
    pub fn to_record_batch<T>(records: &Vec<T>) -> IngestionResult<RecordBatch>
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
        let fields = Vec::<FieldRef>::from_type::<T>(TracingOptions::default()).map_err(|e| {
            IngestionError::Engine {
                message: "failed to trace Arrow schema from type".to_string(),
                source: Box::new(e),
            }
        })?;

        serde_arrow::to_record_batch(&fields, records).map_err(|e| IngestionError::Engine {
            message: "failed to convert records to Arrow RecordBatch".to_string(),
            source: Box::new(e),
        })
    }

    /// Deserialize Rust records from a `RecordBatch`.
    pub fn from_record_batch<T>(batch: &RecordBatch) -> IngestionResult<Vec<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_arrow::from_record_batch(batch).map_err(|e| IngestionError::Engine {
            message: "failed to deserialize records from Arrow RecordBatch".to_string(),
            source: Box::new(e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{TransformSpec, TransformStep};
    use crate::pipeline::CastMode;
    use crate::types::{DataSet, DataType, Field, Schema, Value};

    fn sample_dataset() -> DataSet {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("score", DataType::Int64),
        ]);
        let rows = vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Null],
        ];
        DataSet::new(schema, rows)
    }

    #[test]
    fn transform_spec_can_rename_cast_fill_and_derive() {
        let ds = sample_dataset();

        let out_schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("score_x2", DataType::Float64),
            Field::new("score_f", DataType::Float64),
            Field::new("tag", DataType::Utf8),
        ]);

        let spec = TransformSpec::new(out_schema.clone())
            .with_step(TransformStep::Rename {
                pairs: vec![("score".to_string(), "score_f".to_string())],
            })
            .with_step(TransformStep::Cast {
                column: "score_f".to_string(),
                to: DataType::Float64,
                mode: CastMode::Strict,
            })
            .with_step(TransformStep::FillNull {
                column: "score_f".to_string(),
                value: Value::Float64(0.0),
            })
            .with_step(TransformStep::DeriveMulF64 {
                name: "score_x2".to_string(),
                source: "score_f".to_string(),
                factor: 2.0,
            })
            .with_step(TransformStep::WithLiteral {
                name: "tag".to_string(),
                value: Value::Utf8("A".to_string()),
            })
            .with_step(TransformStep::Select {
                columns: vec![
                    "id".to_string(),
                    "score_x2".to_string(),
                    "score_f".to_string(),
                    "tag".to_string(),
                ],
            });

        let out = spec.apply(&ds).unwrap();
        assert_eq!(out.schema, out_schema);
        assert_eq!(out.row_count(), 2);
        assert_eq!(out.rows[0][0], Value::Int64(1));
        assert_eq!(out.rows[0][1], Value::Float64(20.0));
        assert_eq!(out.rows[0][2], Value::Float64(10.0));
        assert_eq!(out.rows[0][3], Value::Utf8("A".to_string()));

        assert_eq!(out.rows[1][0], Value::Int64(2));
        assert_eq!(out.rows[1][1], Value::Float64(0.0));
        assert_eq!(out.rows[1][2], Value::Float64(0.0));
        assert_eq!(out.rows[1][3], Value::Utf8("A".to_string()));
    }
}
