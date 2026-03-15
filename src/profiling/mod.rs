//! Profiling (Phase 1).
//!
//! A small, engine-delegated profiler that computes common column metrics using Polars under the hood,
//! while keeping the public API in crate-owned types.
//!
//! ## Example
//!
//! ```rust
//! use rust_data_processing::profiling::{profile_dataset, ProfileOptions, SamplingMode};
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! let ds = DataSet::new(
//!     Schema::new(vec![Field::new("score", DataType::Float64)]),
//!     vec![vec![Value::Float64(1.0)], vec![Value::Null], vec![Value::Float64(3.0)]],
//! );
//!
//! let rep = profile_dataset(
//!     &ds,
//!     &ProfileOptions {
//!         sampling: SamplingMode::Head(2),
//!         quantiles: vec![0.5],
//!     },
//! )?;
//!
//! assert_eq!(rep.row_count, 2);
//! assert_eq!(rep.columns[0].null_count, 1);
//! # Ok(())
//! # }
//! ```

use crate::error::{IngestionError, IngestionResult};
use crate::pipeline::DataFrame;
use crate::types::{DataSet, DataType};

use polars::prelude::*;

/// How profiling should sample rows before computing metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SamplingMode {
    /// Profile the full dataset.
    Full,
    /// Profile only the first N rows.
    Head(usize),
}

impl Default for SamplingMode {
    fn default() -> Self {
        Self::Full
    }
}

/// Options for profiling.
#[derive(Debug, Clone, PartialEq)]
pub struct ProfileOptions {
    pub sampling: SamplingMode,
    /// Quantiles to compute for numeric columns (values in [0.0, 1.0]).
    pub quantiles: Vec<f64>,
}

impl Default for ProfileOptions {
    fn default() -> Self {
        Self {
            sampling: SamplingMode::Full,
            quantiles: vec![0.5, 0.95],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NumericProfile {
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub mean: Option<f64>,
    pub quantiles: Vec<(f64, Option<f64>)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub distinct_count: usize,
    pub numeric: Option<NumericProfile>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProfileReport {
    pub sampling: SamplingMode,
    /// Row count of the profiled (possibly sampled) data.
    pub row_count: usize,
    pub columns: Vec<ColumnProfile>,
}

/// Render a profile report to a stable JSON string.
pub fn render_profile_report_json(report: &ProfileReport) -> IngestionResult<String> {
    let sampling = match report.sampling {
        SamplingMode::Full => "full",
        SamplingMode::Head(_) => "head",
    };

    let cols: Vec<serde_json::Value> = report
        .columns
        .iter()
        .map(|c| {
            let dtype = match c.data_type {
                DataType::Int64 => "int64",
                DataType::Float64 => "float64",
                DataType::Bool => "bool",
                DataType::Utf8 => "utf8",
            };
            let numeric = c.numeric.as_ref().map(|n| {
                serde_json::json!({
                    "min": n.min,
                    "max": n.max,
                    "mean": n.mean,
                    "quantiles": n.quantiles.iter().map(|(q, v)| serde_json::json!({"q": q, "value": v})).collect::<Vec<_>>(),
                })
            });

            serde_json::json!({
                "name": c.name,
                "data_type": dtype,
                "null_count": c.null_count,
                "distinct_count": c.distinct_count,
                "numeric": numeric,
            })
        })
        .collect();

    serde_json::to_string_pretty(&serde_json::json!({
        "sampling": sampling,
        "row_count": report.row_count,
        "columns": cols,
    }))
    .map_err(|e| IngestionError::SchemaMismatch {
        message: format!("failed to serialize profile report json: {e}"),
    })
}

/// Render a profile report to a human-readable Markdown string.
pub fn render_profile_report_markdown(report: &ProfileReport) -> String {
    let sampling = match report.sampling {
        SamplingMode::Full => "Full",
        SamplingMode::Head(n) => {
            return format!(
                "## Profile report\n\n- Sampling: **Head({n})**\n- Rows profiled: **{}**\n\n{}",
                report.row_count,
                render_columns_markdown(&report.columns)
            );
        }
    };

    format!(
        "## Profile report\n\n- Sampling: **{sampling}**\n- Rows profiled: **{}**\n\n{}",
        report.row_count,
        render_columns_markdown(&report.columns)
    )
}

fn render_columns_markdown(cols: &[ColumnProfile]) -> String {
    let mut out = String::new();
    out.push_str("### Columns\n\n");
    out.push_str("| column | type | nulls | distinct (non-null) | min | max | mean |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    for c in cols {
        let dtype = match c.data_type {
            DataType::Int64 => "Int64",
            DataType::Float64 => "Float64",
            DataType::Bool => "Bool",
            DataType::Utf8 => "Utf8",
        };
        let (min, max, mean) = match &c.numeric {
            Some(n) => (
                n.min.map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".to_string()),
                n.max.map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".to_string()),
                n.mean.map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".to_string()),
            ),
            None => ("—".to_string(), "—".to_string(), "—".to_string()),
        };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} |\n",
            c.name, dtype, c.null_count, c.distinct_count, min, max, mean
        ));
    }
    out
}

/// Profile an in-memory dataset.
pub fn profile_dataset(ds: &DataSet, options: &ProfileOptions) -> IngestionResult<ProfileReport> {
    let df = DataFrame::from_dataset(ds)?;
    profile_frame(&df, options)
}

/// Profile a pipeline frame (computed lazily).
pub fn profile_frame(df: &DataFrame, options: &ProfileOptions) -> IngestionResult<ProfileReport> {
    let mut lf = df.lazy_clone();

    lf = match options.sampling {
        SamplingMode::Full => lf,
        SamplingMode::Head(n) => lf.limit(n as IdxSize),
    };

    let schema = lf
        .clone()
        .collect_schema()
        .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("failed to collect schema", e))?;

    let cols: Vec<(String, DataType, bool)> = schema
        .iter_fields()
        .map(|f| {
            let (dt, is_numeric) = polars_dtype_to_profile_dtype(f.dtype());
            (f.name().to_string(), dt, is_numeric)
        })
        .collect();

    if cols.is_empty() {
        return Ok(ProfileReport {
            sampling: options.sampling,
            row_count: 0,
            columns: Vec::new(),
        });
    }

    // Build a single-row aggregation over the (optionally sampled) LazyFrame.
    let mut exprs: Vec<Expr> = Vec::new();
    exprs.push(len().alias("__rows"));

    for (name, _dt, is_numeric) in &cols {
        exprs.push(col(name).null_count().alias(&format!("{name}__nulls")));
        // Distinct count excluding nulls (common profiling expectation).
        exprs.push(col(name).drop_nulls().n_unique().alias(&format!("{name}__distinct")));
        if *is_numeric {
            exprs.push(col(name).min().alias(&format!("{name}__min")));
            exprs.push(col(name).max().alias(&format!("{name}__max")));
            exprs.push(col(name).mean().alias(&format!("{name}__mean")));
            for q in &options.quantiles {
                if !(0.0..=1.0).contains(q) {
                    return Err(IngestionError::SchemaMismatch {
                        message: format!("invalid quantile {q}; expected value in [0.0, 1.0]"),
                    });
                }
                let pct = (q * 100.0).round() as i64;
                exprs.push(
                    col(name)
                        .quantile(lit(*q), QuantileMethod::Nearest)
                        .alias(&format!("{name}__p{pct}")),
                );
            }
        }
    }

    let agg = lf
        .select(exprs)
        .collect()
        .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("failed to compute profile", e))?;

    let row_count_col = agg.column("__rows").map_err(|e| {
        crate::ingestion::polars_bridge::polars_error_to_ingestion("profiling missing __rows column", e)
    })?;
    let row_count = any_to_usize(row_count_col.as_materialized_series(), 0)?.unwrap_or(0);

    let mut out_cols: Vec<ColumnProfile> = Vec::with_capacity(cols.len());
    for (name, dt, is_numeric) in cols {
        let nulls_col = agg.column(&format!("{name}__nulls")).map_err(|e| {
            crate::ingestion::polars_bridge::polars_error_to_ingestion(
                &format!("profiling missing null_count for '{name}'"),
                e,
            )
        })?;
        let null_count = any_to_usize(nulls_col.as_materialized_series(), 0)?.unwrap_or(0);

        let distinct_col = agg.column(&format!("{name}__distinct")).map_err(|e| {
            crate::ingestion::polars_bridge::polars_error_to_ingestion(
                &format!("profiling missing distinct_count for '{name}'"),
                e,
            )
        })?;
        let distinct_count = any_to_usize(distinct_col.as_materialized_series(), 0)?.unwrap_or(0);

        let numeric = if is_numeric {
            let min = any_to_f64(
                agg.column(&format!("{name}__min"))
                    .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("profiling missing min", e))?
                    .as_materialized_series(),
                0,
            )?;
            let max = any_to_f64(
                agg.column(&format!("{name}__max"))
                    .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("profiling missing max", e))?
                    .as_materialized_series(),
                0,
            )?;
            let mean = any_to_f64(
                agg.column(&format!("{name}__mean"))
                    .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("profiling missing mean", e))?
                    .as_materialized_series(),
                0,
            )?;
            let mut qs = Vec::with_capacity(options.quantiles.len());
            for q in &options.quantiles {
                let pct = (q * 100.0).round() as i64;
                let v = any_to_f64(
                    agg.column(&format!("{name}__p{pct}"))
                        .map_err(|e| {
                            crate::ingestion::polars_bridge::polars_error_to_ingestion(
                                "profiling missing quantile",
                                e,
                            )
                        })?
                        .as_materialized_series(),
                    0,
                )?;
                qs.push((*q, v));
            }
            Some(NumericProfile {
                min,
                max,
                mean,
                quantiles: qs,
            })
        } else {
            None
        };

        out_cols.push(ColumnProfile {
            name,
            data_type: dt,
            null_count,
            distinct_count,
            numeric,
        });
    }

    Ok(ProfileReport {
        sampling: options.sampling,
        row_count,
        columns: out_cols,
    })
}

fn polars_dtype_to_profile_dtype(dt: &polars::datatypes::DataType) -> (DataType, bool) {
    use polars::datatypes::DataType as P;
    match dt {
        P::Boolean => (DataType::Bool, false),
        P::String => (DataType::Utf8, false),
        P::Int8 | P::Int16 | P::Int32 | P::Int64 | P::UInt8 | P::UInt16 | P::UInt32 | P::UInt64 => {
            (DataType::Int64, true)
        }
        P::Float32 | P::Float64 => (DataType::Float64, true),
        _ => (DataType::Utf8, false),
    }
}

fn any_to_usize(s: &Series, idx: usize) -> IngestionResult<Option<usize>> {
    let av = s
        .get(idx)
        .map_err(|e| IngestionError::Engine {
            message: "failed to read profile value".to_string(),
            source: Box::new(e),
        })?;
    Ok(match av {
        AnyValue::Null => None,
        AnyValue::Int64(v) => Some(v.max(0) as usize),
        AnyValue::UInt64(v) => Some(v as usize),
        AnyValue::Int32(v) => Some((v as i64).max(0) as usize),
        AnyValue::UInt32(v) => Some(v as usize),
        AnyValue::Int16(v) => Some((v as i64).max(0) as usize),
        AnyValue::UInt16(v) => Some(v as usize),
        AnyValue::Int8(v) => Some((v as i64).max(0) as usize),
        AnyValue::UInt8(v) => Some(v as usize),
        other => {
            return Err(IngestionError::SchemaMismatch {
                message: format!("expected integer-like profile value, got {other}"),
            })
        }
    })
}

fn any_to_f64(s: &Series, idx: usize) -> IngestionResult<Option<f64>> {
    let av = s
        .get(idx)
        .map_err(|e| IngestionError::Engine {
            message: "failed to read profile value".to_string(),
            source: Box::new(e),
        })?;
    Ok(match av {
        AnyValue::Null => None,
        AnyValue::Float64(v) => Some(v),
        AnyValue::Float32(v) => Some(v as f64),
        AnyValue::Int64(v) => Some(v as f64),
        AnyValue::UInt64(v) => Some(v as f64),
        AnyValue::Int32(v) => Some(v as f64),
        AnyValue::UInt32(v) => Some(v as f64),
        other => {
            return Err(IngestionError::SchemaMismatch {
                message: format!("expected numeric profile value, got {other}"),
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Field, Schema};
    use crate::types::Value;

    fn tiny() -> DataSet {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64),
            Field::new("score", DataType::Float64),
            Field::new("name", DataType::Utf8),
        ]);
        DataSet::new(
            schema,
            vec![
                vec![Value::Int64(1), Value::Float64(10.0), Value::Utf8("A".to_string())],
                vec![Value::Int64(2), Value::Null, Value::Utf8("A".to_string())],
                vec![Value::Int64(3), Value::Float64(30.0), Value::Utf8("B".to_string())],
            ],
        )
    }

    #[test]
    fn profiling_counts_rows_nulls_and_distinct() {
        let ds = tiny();
        let rep = profile_dataset(&ds, &ProfileOptions::default()).unwrap();
        assert_eq!(rep.row_count, 3);

        let score = rep.columns.iter().find(|c| c.name == "score").unwrap();
        assert_eq!(score.null_count, 1);
        assert_eq!(score.distinct_count, 2);
        assert!(score.numeric.is_some());

        let name = rep.columns.iter().find(|c| c.name == "name").unwrap();
        assert_eq!(name.null_count, 0);
        assert_eq!(name.distinct_count, 2);
        assert!(name.numeric.is_none());
    }

    #[test]
    fn profiling_supports_head_sampling() {
        let ds = tiny();
        let rep = profile_dataset(
            &ds,
            &ProfileOptions {
                sampling: SamplingMode::Head(2),
                quantiles: vec![0.5],
            },
        )
        .unwrap();
        assert_eq!(rep.row_count, 2);
    }

    #[test]
    fn profile_report_renders_json_and_markdown() {
        let ds = tiny();
        let rep = profile_dataset(&ds, &ProfileOptions::default()).unwrap();
        let json = render_profile_report_json(&rep).unwrap();
        assert!(json.contains("\"row_count\""));
        assert!(json.contains("\"columns\""));

        let md = render_profile_report_markdown(&rep);
        assert!(md.contains("## Profile report"));
        assert!(md.contains("### Columns"));
    }
}

