//! DataFrame-centric pipeline/transforms backed by a Polars lazy plan.
//!
//! This module provides a small, engine-delegated pipeline API that compiles to a Polars
//! [`polars::prelude::LazyFrame`] and then collects results back into our in-memory [`crate::types::DataSet`].
//!
//! Design goals for Phase 1:
//! - Keep the public API in our own types (no Polars types in signatures)
//! - Support a minimal set of transformation primitives needed for parity/benchmarks
//! - Provide deterministic, testable behavior (null handling, missing column errors)

use crate::error::{IngestionError, IngestionResult};
use crate::ingestion::polars_bridge::{
    dataframe_to_dataset, dataset_to_dataframe, infer_schema_from_dataframe, polars_error_to_ingestion,
};
use crate::types::{DataSet, Value};

use polars::prelude::*;

/// A predicate used by [`DataFrame::filter`].
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Keep rows where `column == value`.
    Eq { column: String, value: Value },
    /// Keep rows where `column` is not null.
    NotNull { column: String },
    /// Keep rows where `column % modulus == equals` (Int64 only).
    ModEqInt64 {
        column: String,
        modulus: i64,
        equals: i64,
    },
}

/// A DataFrame-centric pipeline compiled into a lazy plan.
///
/// The public API stays in this crate's own types. The current engine implementation is Polars,
/// but callers do not need to depend on Polars types.
#[derive(Clone)]
pub struct DataFrame {
    lf: LazyFrame,
}

impl DataFrame {
    /// Build a pipeline starting from an in-memory [`DataSet`].
    ///
    /// Note: this converts the dataset into a Polars `DataFrame` first. The transformations after
    /// that are planned lazily.
    pub fn from_dataset(ds: &DataSet) -> IngestionResult<Self> {
        let df = dataset_to_dataframe(ds)?;
        Ok(Self { lf: df.lazy() })
    }

    /// Add a filter predicate.
    pub fn filter(mut self, predicate: Predicate) -> IngestionResult<Self> {
        let expr = match predicate {
            Predicate::Eq { column, value } => match value {
                Value::Null => col(&column).is_null(),
                Value::Int64(x) => col(&column).eq(lit(x)),
                Value::Float64(x) => col(&column).eq(lit(x)),
                Value::Bool(x) => col(&column).eq(lit(x)),
                Value::Utf8(s) => col(&column).eq(lit(s)),
            },
            Predicate::NotNull { column } => col(&column).is_not_null(),
            Predicate::ModEqInt64 {
                column,
                modulus,
                equals,
            } => (col(&column) % lit(modulus)).eq(lit(equals)),
        };
        // Planning ops are infallible; errors surface at `collect` time.
        self.lf = self.lf.filter(expr);
        Ok(self)
    }

    /// Multiply a Float64 column by a constant factor (nulls remain null).
    pub fn multiply_f64(mut self, column: &str, factor: f64) -> IngestionResult<Self> {
        // Planning ops are infallible; errors surface at `collect` time.
        self.lf = self.lf.with_columns([(col(column) * lit(factor)).alias(column)]);
        Ok(self)
    }

    /// Select a subset of columns (in the provided order).
    pub fn select(mut self, columns: &[&str]) -> IngestionResult<Self> {
        let exprs: Vec<Expr> = columns.iter().map(|c| col(*c)).collect();
        // Planning ops are infallible; errors surface at `collect` time.
        self.lf = self.lf.select(exprs);
        Ok(self)
    }

    /// Collect the pipeline into an in-memory [`DataSet`].
    pub fn collect(self) -> IngestionResult<DataSet> {
        let df = self
            .lf
            .collect()
            .map_err(|e| polars_error_to_ingestion("failed to collect polars lazy plan", e))?;
        let out_schema = infer_schema_from_dataframe(&df)?;
        dataframe_to_dataset(&df, &out_schema, "column", 1)
    }

    /// Reduce a numeric column by summing values (nulls ignored; all-null -> null).
    ///
    /// Returns `None` if `column` does not exist (aligned with `processing::reduce`).
    pub fn sum(mut self, column: &str) -> IngestionResult<Option<Value>> {
        // We intentionally keep the `None` behavior aligned with `processing::reduce`.
        let df_schema = self
            .lf
            .collect_schema()
            .map_err(|e| polars_error_to_ingestion("failed to collect polars schema", e))?;
        if df_schema.get(column).is_none() {
            return Ok(None);
        }

        let df = self
            .lf
            .select([col(column).sum().alias(column)])
            .collect()
            .map_err(|e| polars_error_to_ingestion("failed to collect polars sum", e))?;

        // Single-row, single-column output.
        let s = df
            .column(column)
            .map_err(|_| IngestionError::SchemaMismatch {
                message: format!("missing required column '{column}'"),
            })?
            .as_materialized_series();
        if s.len() == 0 {
            return Ok(Some(Value::Null));
        }
        let av = s.get(0).map_err(|e| IngestionError::SchemaMismatch {
            message: format!("polars sum output error: {e}"),
        })?;
        Ok(Some(anyvalue_to_value(av)))
    }
}

fn anyvalue_to_value(av: AnyValue) -> Value {
    match av {
        AnyValue::Null => Value::Null,
        AnyValue::Int64(v) => Value::Int64(v),
        AnyValue::Float64(v) => Value::Float64(v),
        AnyValue::Boolean(v) => Value::Bool(v),
        AnyValue::String(v) => Value::Utf8(v.to_string()),
        AnyValue::StringOwned(v) => Value::Utf8(v.to_string()),
        other => Value::Utf8(other.to_string()),
    }
}

/// Backwards-compatible alias for earlier naming.
pub type PolarsPipeline = DataFrame;

#[cfg(test)]
mod tests {
    use super::{DataFrame, PolarsPipeline, Predicate};
    use crate::processing::{filter, map, reduce, ReduceOp};
    use crate::types::{DataSet, DataType, Field, Schema, Value};

    fn sample_dataset() -> DataSet {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("active", DataType::Bool),
            Field::new("score", DataType::Float64),
        ]);
        let rows = vec![
            vec![Value::Int64(1), Value::Bool(true), Value::Float64(10.0)],
            vec![Value::Int64(2), Value::Bool(true), Value::Float64(20.0)],
            vec![Value::Int64(3), Value::Bool(false), Value::Float64(30.0)],
            vec![Value::Int64(4), Value::Bool(true), Value::Null],
        ];
        DataSet::new(schema, rows)
    }

    #[test]
    fn polars_pipeline_filter_map_reduce_parity_with_in_memory() {
        let ds = sample_dataset();

        // In-memory baseline: active && even id, score *= 2.0, then sum(score)
        let active_idx = ds.schema.index_of("active").unwrap();
        let id_idx = ds.schema.index_of("id").unwrap();
        let filtered = filter(&ds, |row| {
            let is_active = matches!(row.get(active_idx), Some(Value::Bool(true)));
            let even_id = matches!(row.get(id_idx), Some(Value::Int64(v)) if *v % 2 == 0);
            is_active && even_id
        });
        let mapped = map(&filtered, |row| {
            let mut out = row.to_vec();
            if let Some(Value::Float64(v)) = out.get(2) {
                out[2] = Value::Float64(v * 2.0);
            }
            out
        });
        let expected = reduce(&mapped, "score", ReduceOp::Sum).unwrap();

        // Polars-delegated pipeline.
        let got = DataFrame::from_dataset(&ds)
            .unwrap()
            .filter(Predicate::Eq {
                column: "active".to_string(),
                value: Value::Bool(true),
            })
            .unwrap()
            .filter(Predicate::ModEqInt64 {
                column: "id".to_string(),
                modulus: 2,
                equals: 0,
            })
            .unwrap()
            .multiply_f64("score", 2.0)
            .unwrap()
            .sum("score")
            .unwrap()
            .unwrap();

        assert_eq!(got, expected);
    }

    #[test]
    fn polars_pipeline_collect_select_works() {
        let ds = sample_dataset();
        let out = DataFrame::from_dataset(&ds)
            .unwrap()
            .select(&["score", "id"])
            .unwrap()
            .collect()
            .unwrap();

        assert_eq!(out.schema.field_names().collect::<Vec<_>>(), vec!["score", "id"]);
        assert_eq!(out.row_count(), ds.row_count());
        assert_eq!(out.rows[0][0], Value::Float64(10.0));
        assert_eq!(out.rows[0][1], Value::Int64(1));
    }

    #[test]
    fn polars_pipeline_sum_returns_none_for_missing_column() {
        let ds = sample_dataset();
        let out = DataFrame::from_dataset(&ds).unwrap().sum("missing").unwrap();
        assert_eq!(out, None);
    }

    #[test]
    fn polars_errors_are_preserved_as_engine_error_sources() {
        // Trigger a Polars execution error by applying a numeric multiply to a Utf8 column.
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8)]);
        let ds = DataSet::new(schema, vec![vec![Value::Utf8("x".to_string())]]);

        let err = DataFrame::from_dataset(&ds)
            .unwrap()
            .multiply_f64("name", 2.0)
            .unwrap()
            .collect()
            .unwrap_err();

        // This should not be stringified into SchemaMismatch; it should preserve a source() chain.
        match err {
            crate::error::IngestionError::Engine { source, .. } => {
                assert!(!source.to_string().is_empty());
            }
            other => panic!("expected Engine error, got: {other:?}"),
        }
    }

    #[test]
    fn backwards_compatible_polars_pipeline_alias_exists() {
        let ds = sample_dataset();
        let _ = PolarsPipeline::from_dataset(&ds).unwrap().select(&["id"]).unwrap();
    }
}

