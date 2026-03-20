//! DataFrame-centric pipeline/transforms backed by a Polars lazy plan.
//!
//! This module provides a small, engine-delegated pipeline API that compiles to a Polars
//! [`polars::prelude::LazyFrame`] and then collects results back into our in-memory [`crate::types::DataSet`].
//!
//! Design goals for Phase 1:
//! - Keep the public API in our own types (no Polars types in signatures)
//! - Support a minimal set of transformation primitives needed for parity/benchmarks
//! - Provide deterministic, testable behavior (null handling, missing column errors)
//!
//! # Examples
//!
//! ```no_run
//! use rust_data_processing::pipeline::{Agg, DataFrame, JoinKind, Predicate};
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! let ds = DataSet::new(
//!     Schema::new(vec![
//!         Field::new("id", DataType::Int64),
//!         Field::new("active", DataType::Bool),
//!         Field::new("score", DataType::Int64),
//!         Field::new("grp", DataType::Utf8),
//!     ]),
//!     vec![
//!         vec![Value::Int64(1), Value::Bool(true), Value::Int64(10), Value::Utf8("A".to_string())],
//!         vec![Value::Int64(2), Value::Bool(true), Value::Null, Value::Utf8("A".to_string())],
//!     ],
//! );
//!
//! // Rename + cast + fill nulls.
//! let cleaned = DataFrame::from_dataset(&ds)?
//!     .rename(&[("score", "score_i")])?
//!     .cast("score_i", DataType::Float64)?
//!     .fill_null("score_i", Value::Float64(0.0))?;
//!
//! // Filter + group_by.
//! let _out = cleaned
//!     .filter(Predicate::Eq {
//!         column: "active".to_string(),
//!         value: Value::Bool(true),
//!     })?
//!     .group_by(
//!         &["grp"],
//!         &[Agg::Sum {
//!             column: "score_i".to_string(),
//!             alias: "sum_score".to_string(),
//!         }],
//!     )?
//!     .collect()?;
//!
//! // Join two DataFrames.
//! let left = DataFrame::from_dataset(&ds)?;
//! let right = DataFrame::from_dataset(&ds)?;
//! let _joined = left.join(right, &["id"], &["id"], JoinKind::Inner)?;
//! # Ok(())
//! # }
//! ```

use crate::error::{IngestionError, IngestionResult};
use crate::ingestion::polars_bridge::{
    dataframe_to_dataset, dataset_to_dataframe, infer_schema_from_dataframe, polars_error_to_ingestion,
};
use crate::processing::{FeatureMeanStd, ReduceOp, VarianceKind};
use crate::types::{DataSet, DataType, Schema, Value};

use polars::prelude::*;
use polars::chunked_array::cast::CastOptions;
use serde::{Deserialize, Serialize};

const REDUCE_SCALAR_COL: &str = "__rust_dp_reduce_scalar";

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

/// Join behavior for [`DataFrame::join`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

/// Aggregations for [`DataFrame::group_by`].
#[derive(Debug, Clone, PartialEq)]
pub enum Agg {
    /// Count rows in each group (includes nulls).
    CountRows { alias: String },
    /// Count non-null values of a column in each group.
    CountNotNull { column: String, alias: String },
    Sum { column: String, alias: String },
    Min { column: String, alias: String },
    Max { column: String, alias: String },
    /// Mean of numeric values (cast to `Float64` first), nulls ignored.
    Mean { column: String, alias: String },
    Variance {
        column: String,
        alias: String,
        kind: VarianceKind,
    },
    StdDev {
        column: String,
        alias: String,
        kind: VarianceKind,
    },
    SumSquares { column: String, alias: String },
    L2Norm { column: String, alias: String },
    /// Distinct count of non-null values in each group.
    CountDistinctNonNull { column: String, alias: String },
}

/// Casting behavior for [`DataFrame::cast_with_mode`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CastMode {
    /// Casting errors fail the pipeline at `collect()` time.
    Strict,
    /// Casting errors yield nulls instead of failing.
    Lossy,
}

impl Default for CastMode {
    fn default() -> Self {
        Self::Strict
    }
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

    /// Add a constant Float64 value to a column (nulls remain null).
    pub fn add_f64(mut self, column: &str, delta: f64) -> IngestionResult<Self> {
        self.lf = self.lf.with_columns([(col(column) + lit(delta)).alias(column)]);
        Ok(self)
    }

    /// Add a derived Float64 column: `name = source * factor` (nulls remain null).
    pub fn with_mul_f64(mut self, name: &str, source: &str, factor: f64) -> IngestionResult<Self> {
        self.lf = self
            .lf
            .with_columns([(col(source) * lit(factor)).alias(name)]);
        Ok(self)
    }

    /// Add a derived Float64 column: `name = source + delta` (nulls remain null).
    pub fn with_add_f64(mut self, name: &str, source: &str, delta: f64) -> IngestionResult<Self> {
        self.lf = self
            .lf
            .with_columns([(col(source) + lit(delta)).alias(name)]);
        Ok(self)
    }

    /// Select a subset of columns (in the provided order).
    pub fn select(mut self, columns: &[&str]) -> IngestionResult<Self> {
        let exprs: Vec<Expr> = columns.iter().map(|c| col(*c)).collect();
        // Planning ops are infallible; errors surface at `collect` time.
        self.lf = self.lf.select(exprs);
        Ok(self)
    }

    /// Rename columns.
    ///
    /// This uses Polars' `rename(..., strict=true)` behavior: all `from` columns must exist.
    pub fn rename(mut self, pairs: &[(&str, &str)]) -> IngestionResult<Self> {
        let (existing, new): (Vec<&str>, Vec<&str>) = pairs.iter().copied().unzip();
        self.lf = self.lf.rename(existing, new, true);
        Ok(self)
    }

    /// Cast a column to a target type.
    ///
    /// Note: cast errors (e.g. invalid parses) surface at `collect()` time.
    pub fn cast(self, column: &str, to: DataType) -> IngestionResult<Self> {
        self.cast_with_mode(column, to, CastMode::Strict)
    }

    /// Cast a column with an explicit mode (strict vs lossy).
    pub fn cast_with_mode(mut self, column: &str, to: DataType, mode: CastMode) -> IngestionResult<Self> {
        let dt = to_polars_dtype(&to);
        let expr = match mode {
            CastMode::Strict => col(column).strict_cast(dt),
            CastMode::Lossy => col(column).cast_with_options(dt, CastOptions::NonStrict),
        }
        .alias(column);
        self.lf = self.lf.with_columns([expr]);
        Ok(self)
    }

    /// Drop columns by name.
    pub fn drop(mut self, columns: &[&str]) -> IngestionResult<Self> {
        let names: Vec<PlSmallStr> = columns.iter().map(|c| (*c).into()).collect();
        let sel = Selector::ByName {
            names: names.into(),
            strict: true,
        };
        self.lf = self.lf.drop(sel);
        Ok(self)
    }

    /// Fill nulls in a column with a literal.
    pub fn fill_null(mut self, column: &str, value: Value) -> IngestionResult<Self> {
        let lit_expr = value_to_lit_expr(value)?;
        self.lf = self
            .lf
            .with_columns([col(column).fill_null(lit_expr).alias(column)]);
        Ok(self)
    }

    /// Add a derived column with a literal value.
    pub fn with_literal(mut self, name: &str, value: Value) -> IngestionResult<Self> {
        let lit_expr = value_to_lit_expr(value)?;
        self.lf = self.lf.with_columns([lit_expr.alias(name)]);
        Ok(self)
    }

    /// Group rows by `keys` and compute aggregations.
    pub fn group_by(mut self, keys: &[&str], aggs: &[Agg]) -> IngestionResult<Self> {
        if keys.is_empty() {
            return Err(IngestionError::SchemaMismatch {
                message: "group_by requires at least one key column".to_string(),
            });
        }
        if aggs.is_empty() {
            return Err(IngestionError::SchemaMismatch {
                message: "group_by requires at least one aggregation".to_string(),
            });
        }

        let key_exprs: Vec<Expr> = keys.iter().map(|k| col(*k)).collect();
        let agg_exprs: Vec<Expr> = aggs.iter().map(agg_to_expr).collect();
        self.lf = self.lf.group_by(key_exprs).agg(agg_exprs);
        Ok(self)
    }

    /// Join this pipeline with another [`DataFrame`] on key columns.
    ///
    /// Note: join planning is infallible; missing-column errors surface at `collect()` time.
    pub fn join(mut self, other: DataFrame, left_on: &[&str], right_on: &[&str], how: JoinKind) -> IngestionResult<Self> {
        if left_on.is_empty() || right_on.is_empty() {
            return Err(IngestionError::SchemaMismatch {
                message: "join requires at least one join key on each side".to_string(),
            });
        }
        if left_on.len() != right_on.len() {
            return Err(IngestionError::SchemaMismatch {
                message: format!(
                    "join requires left_on and right_on to have same length (left_on={}, right_on={})",
                    left_on.len(),
                    right_on.len()
                ),
            });
        }

        let left_exprs: Vec<Expr> = left_on.iter().map(|c| col(*c)).collect();
        let right_exprs: Vec<Expr> = right_on.iter().map(|c| col(*c)).collect();

        let how = match how {
            JoinKind::Inner => JoinType::Inner,
            JoinKind::Left => JoinType::Left,
            JoinKind::Right => JoinType::Right,
            JoinKind::Full => JoinType::Full,
        };

        self.lf = self
            .lf
            .join(other.lf, left_exprs, right_exprs, JoinArgs::new(how));
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

    /// Collect the pipeline into an in-memory [`DataSet`], enforcing an explicit output schema.
    pub fn collect_with_schema(self, schema: &Schema) -> IngestionResult<DataSet> {
        let df = self
            .lf
            .collect()
            .map_err(|e| polars_error_to_ingestion("failed to collect polars lazy plan", e))?;
        dataframe_to_dataset(&df, schema, "column", 1)
    }

    /// Reduce a column using a built-in [`ReduceOp`] (Polars-backed).
    ///
    /// Returns `None` if `column` does not exist (aligned with [`crate::processing::reduce`]).
    pub fn reduce(mut self, column: &str, op: ReduceOp) -> IngestionResult<Option<Value>> {
        let df_schema = self
            .lf
            .collect_schema()
            .map_err(|e| polars_error_to_ingestion("failed to collect polars schema", e))?;
        if df_schema.get(column).is_none() {
            return Ok(None);
        }

        let expr = polars_reduce_expr(column, op);
        let df = self
            .lf
            .select([expr.alias(REDUCE_SCALAR_COL)])
            .collect()
            .map_err(|e| polars_error_to_ingestion("failed to collect polars reduce", e))?;

        let s = df
            .column(REDUCE_SCALAR_COL)
            .map_err(|_| IngestionError::SchemaMismatch {
                message: format!("missing reduce output column '{REDUCE_SCALAR_COL}'"),
            })?
            .as_materialized_series();
        if s.len() == 0 {
            return Ok(Some(Value::Null));
        }
        let av = s.get(0).map_err(|e| IngestionError::SchemaMismatch {
            message: format!("polars reduce output error: {e}"),
        })?;
        Ok(Some(anyvalue_to_value(av)))
    }

    /// Reduce a numeric column by summing values (nulls ignored; all-null -> null).
    ///
    /// Returns `None` if `column` does not exist (aligned with `processing::reduce`).
    pub fn sum(self, column: &str) -> IngestionResult<Option<Value>> {
        self.reduce(column, ReduceOp::Sum)
    }

    /// Single Polars collect: for each column, mean and standard deviation (`std_kind` maps to
    /// Polars `ddof`). Columns are cast to `Float64` first (aligned with scalar reduces).
    ///
    /// Returns an error if any column name is missing from the lazy schema.
    pub fn feature_wise_mean_std(
        mut self,
        columns: &[&str],
        std_kind: VarianceKind,
    ) -> IngestionResult<Vec<(String, FeatureMeanStd)>> {
        let df_schema = self
            .lf
            .collect_schema()
            .map_err(|e| polars_error_to_ingestion("failed to collect polars schema", e))?;
        for c in columns {
            if df_schema.get(*c).is_none() {
                return Err(IngestionError::SchemaMismatch {
                    message: format!("feature_wise_mean_std: unknown column '{c}'"),
                });
            }
        }
        let ddof = match std_kind {
            VarianceKind::Population => 0u8,
            VarianceKind::Sample => 1u8,
        };
        use polars::datatypes::DataType as P;
        let mut exprs: Vec<Expr> = Vec::with_capacity(columns.len() * 2);
        for (i, c) in columns.iter().enumerate() {
            let cf = col(*c).strict_cast(P::Float64);
            exprs.push(cf.clone().mean().alias(format!("__fwm_{i}_mean").as_str()));
            exprs.push(cf.std(ddof).alias(format!("__fwm_{i}_std").as_str()));
        }
        let df = self
            .lf
            .select(exprs)
            .collect()
            .map_err(|e| polars_error_to_ingestion("failed to collect feature_wise_mean_std", e))?;

        if df.height() == 0 {
            return Ok(columns
                .iter()
                .map(|c| {
                    (
                        (*c).to_string(),
                        FeatureMeanStd {
                            mean: Value::Null,
                            std_dev: Value::Null,
                        },
                    )
                })
                .collect());
        }

        let mut out = Vec::with_capacity(columns.len());
        for i in 0..columns.len() {
            let mean_s = df
                .column(&format!("__fwm_{i}_mean"))
                .map_err(|_| IngestionError::SchemaMismatch {
                    message: format!("missing __fwm_{i}_mean"),
                })?
                .as_materialized_series();
            let std_s = df
                .column(&format!("__fwm_{i}_std"))
                .map_err(|_| IngestionError::SchemaMismatch {
                    message: format!("missing __fwm_{i}_std"),
                })?
                .as_materialized_series();
            let mean_av = mean_s.get(0).map_err(|e| IngestionError::SchemaMismatch {
                message: format!("feature_wise mean get: {e}"),
            })?;
            let std_av = std_s.get(0).map_err(|e| IngestionError::SchemaMismatch {
                message: format!("feature_wise std get: {e}"),
            })?;
            out.push((
                columns[i].to_string(),
                FeatureMeanStd {
                    mean: anyvalue_to_value(mean_av),
                    std_dev: anyvalue_to_value(std_av),
                },
            ));
        }
        Ok(out)
    }

    pub(crate) fn lazy_clone(&self) -> LazyFrame {
        self.lf.clone()
    }

    pub(crate) fn from_lazyframe(lf: LazyFrame) -> Self {
        Self { lf }
    }
}

fn polars_reduce_expr(column: &str, op: ReduceOp) -> Expr {
    use polars::datatypes::DataType as P;
    let c = col(column);
    match op {
        ReduceOp::Count => len(),
        ReduceOp::Sum => c.sum(),
        ReduceOp::Min => c.min(),
        ReduceOp::Max => c.max(),
        ReduceOp::Mean => c.clone().strict_cast(P::Float64).mean(),
        ReduceOp::Variance(kind) => {
            let ddof = match kind {
                VarianceKind::Population => 0u8,
                VarianceKind::Sample => 1u8,
            };
            c.clone().strict_cast(P::Float64).var(ddof)
        }
        ReduceOp::StdDev(kind) => {
            let ddof = match kind {
                VarianceKind::Population => 0u8,
                VarianceKind::Sample => 1u8,
            };
            c.clone().strict_cast(P::Float64).std(ddof)
        }
        ReduceOp::SumSquares => c
            .clone()
            .strict_cast(P::Float64)
            .pow(lit(2.0))
            .sum(),
        ReduceOp::L2Norm => c
            .clone()
            .strict_cast(P::Float64)
            .pow(lit(2.0))
            .sum()
            .sqrt(),
        ReduceOp::CountDistinctNonNull => c.drop_nulls().n_unique(),
    }
}

fn agg_to_expr(agg: &Agg) -> Expr {
    use polars::datatypes::DataType as P;
    match agg {
        Agg::CountRows { alias } => len().alias(alias.as_str()),
        Agg::CountNotNull { column, alias } => col(column.as_str()).count().alias(alias.as_str()),
        Agg::Sum { column, alias } => col(column.as_str()).sum().alias(alias.as_str()),
        Agg::Min { column, alias } => col(column.as_str()).min().alias(alias.as_str()),
        Agg::Max { column, alias } => col(column.as_str()).max().alias(alias.as_str()),
        Agg::Mean { column, alias } => col(column.as_str())
            .strict_cast(P::Float64)
            .mean()
            .alias(alias.as_str()),
        Agg::Variance {
            column,
            alias,
            kind,
        } => {
            let ddof = match kind {
                VarianceKind::Population => 0u8,
                VarianceKind::Sample => 1u8,
            };
            col(column.as_str())
                .strict_cast(P::Float64)
                .var(ddof)
                .alias(alias.as_str())
        }
        Agg::StdDev {
            column,
            alias,
            kind,
        } => {
            let ddof = match kind {
                VarianceKind::Population => 0u8,
                VarianceKind::Sample => 1u8,
            };
            col(column.as_str())
                .strict_cast(P::Float64)
                .std(ddof)
                .alias(alias.as_str())
        }
        Agg::SumSquares { column, alias } => col(column.as_str())
            .strict_cast(P::Float64)
            .pow(lit(2.0))
            .sum()
            .alias(alias.as_str()),
        Agg::L2Norm { column, alias } => col(column.as_str())
            .strict_cast(P::Float64)
            .pow(lit(2.0))
            .sum()
            .sqrt()
            .alias(alias.as_str()),
        Agg::CountDistinctNonNull { column, alias } => col(column.as_str())
            .drop_nulls()
            .n_unique()
            .alias(alias.as_str()),
    }
}

fn to_polars_dtype(dt: &DataType) -> polars::datatypes::DataType {
    match dt {
        DataType::Int64 => polars::datatypes::DataType::Int64,
        DataType::Float64 => polars::datatypes::DataType::Float64,
        DataType::Bool => polars::datatypes::DataType::Boolean,
        DataType::Utf8 => polars::datatypes::DataType::String,
    }
}

fn value_to_lit_expr(value: Value) -> IngestionResult<Expr> {
    match value {
        Value::Null => Err(IngestionError::SchemaMismatch {
            message: "Value::Null is not supported as a literal expression; use fill_null or cast/collect to materialize".to_string(),
        }),
        Value::Int64(v) => Ok(lit(v)),
        Value::Float64(v) => Ok(lit(v)),
        Value::Bool(v) => Ok(lit(v)),
        Value::Utf8(v) => Ok(lit(v)),
    }
}

fn anyvalue_to_value(av: AnyValue) -> Value {
    match av {
        AnyValue::Null => Value::Null,
        AnyValue::Int8(v) => Value::Int64(v as i64),
        AnyValue::Int16(v) => Value::Int64(v as i64),
        AnyValue::Int32(v) => Value::Int64(v as i64),
        AnyValue::Int64(v) => Value::Int64(v),
        AnyValue::UInt8(v) => Value::Int64(v as i64),
        AnyValue::UInt16(v) => Value::Int64(v as i64),
        AnyValue::UInt32(v) => Value::Int64(v as i64),
        AnyValue::UInt64(v) => Value::Int64(v as i64),
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
    use super::{Agg, DataFrame, JoinKind, PolarsPipeline, Predicate};
    use crate::processing::{feature_wise_mean_std, filter, map, reduce, ReduceOp, VarianceKind};
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
    fn polars_pipeline_reduce_parity_mean_variance_l2_distinct() {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Float64),
            Field::new("tag", DataType::Utf8),
        ]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Float64(1.0), Value::Utf8("a".to_string())],
                vec![Value::Float64(2.0), Value::Utf8("b".to_string())],
                vec![Value::Null, Value::Utf8("a".to_string())],
            ],
        );

        let mean = reduce(&ds, "x", ReduceOp::Mean).unwrap();
        let var_pop = reduce(&ds, "x", ReduceOp::Variance(VarianceKind::Population)).unwrap();
        let l2 = reduce(&ds, "x", ReduceOp::L2Norm).unwrap();
        let dcnt = reduce(&ds, "tag", ReduceOp::CountDistinctNonNull).unwrap();

        assert_eq!(
            DataFrame::from_dataset(&ds)
                .unwrap()
                .reduce("x", ReduceOp::Mean)
                .unwrap()
                .unwrap(),
            mean
        );
        assert_eq!(
            DataFrame::from_dataset(&ds)
                .unwrap()
                .reduce("x", ReduceOp::Variance(VarianceKind::Population))
                .unwrap()
                .unwrap(),
            var_pop
        );
        assert_eq!(
            DataFrame::from_dataset(&ds)
                .unwrap()
                .reduce("x", ReduceOp::L2Norm)
                .unwrap()
                .unwrap(),
            l2
        );
        assert_eq!(
            DataFrame::from_dataset(&ds)
                .unwrap()
                .reduce("tag", ReduceOp::CountDistinctNonNull)
                .unwrap()
                .unwrap(),
            dcnt
        );
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

    #[test]
    fn rename_cast_fill_null_group_by_and_join_work() {
        // rename + cast + fill_null
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("score", DataType::Int64),
        ]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Int64(1), Value::Int64(10)],
                vec![Value::Int64(2), Value::Null],
            ],
        );

        let out = DataFrame::from_dataset(&ds)
            .unwrap()
            .rename(&[("score", "score_i")])
            .unwrap()
            .cast("score_i", DataType::Float64)
            .unwrap()
            .fill_null("score_i", Value::Float64(0.0))
            .unwrap()
            .collect()
            .unwrap();

        assert_eq!(out.schema.field_names().collect::<Vec<_>>(), vec!["id", "score_i"]);
        assert_eq!(out.rows[0][1], Value::Float64(10.0));
        assert_eq!(out.rows[1][1], Value::Float64(0.0));

        // group_by
        let schema = Schema::new(vec![
            Field::new("grp", DataType::Utf8),
            Field::new("score", DataType::Float64),
        ]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Utf8("A".to_string()), Value::Float64(1.0)],
                vec![Value::Utf8("A".to_string()), Value::Float64(2.0)],
                vec![Value::Utf8("B".to_string()), Value::Null],
            ],
        );

        let out = DataFrame::from_dataset(&ds)
            .unwrap()
            .group_by(
                &["grp"],
                &[
                    Agg::Sum {
                        column: "score".to_string(),
                        alias: "sum_score".to_string(),
                    },
                    Agg::CountRows {
                        alias: "cnt".to_string(),
                    },
                ],
            )
            .unwrap()
            .collect()
            .unwrap();

        // Order is not guaranteed; validate via a lookup.
        let mut sums: std::collections::HashMap<String, (Value, Value)> = std::collections::HashMap::new();
        for row in &out.rows {
            if let Value::Utf8(g) = &row[0] {
                sums.insert(g.clone(), (row[1].clone(), row[2].clone()));
            }
        }
        assert_eq!(
            sums.get("A"),
            Some(&(Value::Float64(3.0), Value::Int64(2)))
        );
        assert_eq!(
            sums.get("B"),
            // Polars `sum` ignores nulls and returns 0.0 for all-null groups.
            Some(&(Value::Float64(0.0), Value::Int64(1)))
        );

        // join
        let left = DataSet::new(
            Schema::new(vec![Field::new("id", DataType::Int64), Field::new("name", DataType::Utf8)]),
            vec![
                vec![Value::Int64(1), Value::Utf8("Ada".to_string())],
                vec![Value::Int64(2), Value::Utf8("Grace".to_string())],
            ],
        );
        let right = DataSet::new(
            Schema::new(vec![Field::new("id", DataType::Int64), Field::new("score", DataType::Float64)]),
            vec![
                vec![Value::Int64(1), Value::Float64(9.0)],
                vec![Value::Int64(3), Value::Float64(7.0)],
            ],
        );

        let out = DataFrame::from_dataset(&left)
            .unwrap()
            .join(
                DataFrame::from_dataset(&right).unwrap(),
                &["id"],
                &["id"],
                JoinKind::Inner,
            )
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(out.row_count(), 1);
        // One matched row with id=1.
        assert_eq!(out.rows[0][0], Value::Int64(1));
    }

    #[test]
    fn polars_feature_wise_mean_std_matches_in_memory() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64),
            Field::new("b", DataType::Float64),
        ]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Int64(1), Value::Float64(10.0)],
                vec![Value::Int64(3), Value::Float64(20.0)],
            ],
        );
        let mem = feature_wise_mean_std(&ds, &["a", "b"], VarianceKind::Sample).unwrap();
        let pol = DataFrame::from_dataset(&ds)
            .unwrap()
            .feature_wise_mean_std(&["a", "b"], VarianceKind::Sample)
            .unwrap();
        assert_eq!(mem.len(), pol.len());
        for i in 0..mem.len() {
            assert_eq!(mem[i].0, pol[i].0);
            assert_eq!(mem[i].1.mean, pol[i].1.mean);
            match (&mem[i].1.std_dev, &pol[i].1.std_dev) {
                (Value::Float64(m), Value::Float64(p)) => assert!((m - p).abs() < 1e-9),
                (a, b) => assert_eq!(a, b),
            }
        }
    }

    #[test]
    fn group_by_mean_std_count_distinct_all_null_numeric_is_null() {
        let schema = Schema::new(vec![
            Field::new("g", DataType::Utf8),
            Field::new("x", DataType::Float64),
            Field::new("tag", DataType::Utf8),
        ]);
        let ds = DataSet::new(
            schema,
            vec![
                vec![Value::Utf8("A".to_string()), Value::Null, Value::Utf8("p".to_string())],
                vec![Value::Utf8("A".to_string()), Value::Null, Value::Utf8("q".to_string())],
            ],
        );
        let out = DataFrame::from_dataset(&ds)
            .unwrap()
            .group_by(
                &["g"],
                &[
                    Agg::Mean {
                        column: "x".to_string(),
                        alias: "mx".to_string(),
                    },
                    Agg::StdDev {
                        column: "x".to_string(),
                        alias: "sx".to_string(),
                        kind: VarianceKind::Sample,
                    },
                    Agg::CountDistinctNonNull {
                        column: "tag".to_string(),
                        alias: "dt".to_string(),
                    },
                ],
            )
            .unwrap()
            .collect()
            .unwrap();
        assert_eq!(out.row_count(), 1);
        assert_eq!(out.rows[0][0], Value::Utf8("A".to_string()));
        assert_eq!(out.rows[0][1], Value::Null);
        assert_eq!(out.rows[0][2], Value::Null);
        assert_eq!(out.rows[0][3], Value::Int64(2));
    }
}

