//! Outlier detection (Phase 1).
//!
//! Provides a few common numeric outlier detection primitives backed by Polars expressions.
//!
//! ## Example
//!
//! ```rust
//! use rust_data_processing::outliers::{detect_outliers_dataset, OutlierMethod, OutlierOptions};
//! use rust_data_processing::profiling::SamplingMode;
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! let ds = DataSet::new(
//!     Schema::new(vec![Field::new("x", DataType::Float64)]),
//!     vec![
//!         vec![Value::Float64(1.0)],
//!         vec![Value::Float64(1.0)],
//!         vec![Value::Float64(1.0)],
//!         vec![Value::Float64(1.0)],
//!         vec![Value::Float64(1000.0)],
//!     ],
//! );
//!
//! let rep = detect_outliers_dataset(
//!     &ds,
//!     "x",
//!     OutlierMethod::Iqr { k: 1.5 },
//!     &OutlierOptions { sampling: SamplingMode::Full, max_examples: 3 },
//! )?;
//!
//! assert!(rep.outlier_count >= 1);
//! # Ok(())
//! # }
//! ```

use crate::error::{IngestionError, IngestionResult};
use crate::pipeline::DataFrame;
use crate::profiling::SamplingMode;
use crate::types::DataSet;

use polars::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutlierMethod {
    /// Standard z-score outliers: \(|x - mean| / std > threshold\).
    ZScore { threshold: f64 },
    /// Tukey fences using IQR: \(x < Q1 - k·IQR\) or \(x > Q3 + k·IQR\).
    Iqr { k: f64 },
    /// Median absolute deviation (MAD) based score: \(0.6745·|x - median| / MAD > threshold\).
    Mad { threshold: f64 },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutlierOptions {
    pub sampling: SamplingMode,
    pub max_examples: usize,
}

impl Default for OutlierOptions {
    fn default() -> Self {
        Self {
            sampling: SamplingMode::Full,
            max_examples: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutlierStats {
    pub method: OutlierMethod,
    pub mean: Option<f64>,
    pub std: Option<f64>,
    pub median: Option<f64>,
    pub mad: Option<f64>,
    pub q1: Option<f64>,
    pub q3: Option<f64>,
    pub lower_fence: Option<f64>,
    pub upper_fence: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutlierReport {
    pub column: String,
    pub sampling: SamplingMode,
    pub row_count: usize,
    pub outlier_count: usize,
    pub stats: OutlierStats,
    pub examples: Vec<f64>,
}

pub fn detect_outliers_dataset(
    ds: &DataSet,
    column: &str,
    method: OutlierMethod,
    options: &OutlierOptions,
) -> IngestionResult<OutlierReport> {
    let df = DataFrame::from_dataset(ds)?;
    detect_outliers_frame(&df, column, method, options)
}

pub fn detect_outliers_frame(
    df: &DataFrame,
    column: &str,
    method: OutlierMethod,
    options: &OutlierOptions,
) -> IngestionResult<OutlierReport> {
    let mut lf = df.lazy_clone();
    lf = match options.sampling {
        SamplingMode::Full => lf,
        SamplingMode::Head(n) => lf.limit(n as IdxSize),
    };

    // Compute stats in one collect.
    let stats_df = match method {
        OutlierMethod::ZScore { .. } => lf
            .clone()
            .select([
                len().alias("__rows"),
                col(column).mean().alias("__mean"),
                col(column).std(1).alias("__std"),
            ])
            .collect(),
        OutlierMethod::Iqr { .. } => lf
            .clone()
            .select([
                len().alias("__rows"),
                col(column)
                    .quantile(lit(0.25), QuantileMethod::Nearest)
                    .alias("__q1"),
                col(column)
                    .quantile(lit(0.75), QuantileMethod::Nearest)
                    .alias("__q3"),
            ])
            .collect(),
        OutlierMethod::Mad { .. } => lf
            .clone()
            .select([
                len().alias("__rows"),
                col(column).median().alias("__median"),
                // MAD requires a second pass; we compute median first, then derive MAD below.
            ])
            .collect(),
    }
    .map_err(|e| {
        crate::ingestion::polars_bridge::polars_error_to_ingestion(
            "failed to compute outlier stats",
            e,
        )
    })?;

    let row_count = read_f64(&stats_df, "__rows")?.unwrap_or(0.0) as usize;

    let (stats, predicate) = match method {
        OutlierMethod::ZScore { threshold } => {
            let mean = read_f64(&stats_df, "__mean")?;
            let std = read_f64(&stats_df, "__std")?;
            let pred = match (mean, std) {
                (Some(m), Some(s)) if s > 0.0 => {
                    ((col(column) - lit(m)) / lit(s)).abs().gt(lit(threshold))
                }
                _ => lit(false),
            };
            (
                OutlierStats {
                    method,
                    mean,
                    std,
                    median: None,
                    mad: None,
                    q1: None,
                    q3: None,
                    lower_fence: None,
                    upper_fence: None,
                },
                pred,
            )
        }
        OutlierMethod::Iqr { k } => {
            let q1 = read_f64(&stats_df, "__q1")?;
            let q3 = read_f64(&stats_df, "__q3")?;
            let (lower, upper, pred) = match (q1, q3) {
                (Some(a), Some(b)) => {
                    let iqr = b - a;
                    let lo = a - k * iqr;
                    let hi = b + k * iqr;
                    (
                        Some(lo),
                        Some(hi),
                        col(column).lt(lit(lo)).or(col(column).gt(lit(hi))),
                    )
                }
                _ => (None, None, lit(false)),
            };
            (
                OutlierStats {
                    method,
                    mean: None,
                    std: None,
                    median: None,
                    mad: None,
                    q1,
                    q3,
                    lower_fence: lower,
                    upper_fence: upper,
                },
                pred,
            )
        }
        OutlierMethod::Mad { threshold } => {
            let median = read_f64(&stats_df, "__median")?;
            // Compute MAD on the same sampled lf.
            let mad = if let Some(m) = median {
                let mad_df = lf
                    .clone()
                    .select([(col(column) - lit(m)).abs().median().alias("__mad")])
                    .collect()
                    .map_err(|e| {
                        crate::ingestion::polars_bridge::polars_error_to_ingestion(
                            "failed to compute MAD",
                            e,
                        )
                    })?;
                read_f64(&mad_df, "__mad")?
            } else {
                None
            };
            let pred = match (median, mad) {
                (Some(m), Some(d)) if d > 0.0 => {
                    (lit(0.6745) * (col(column) - lit(m)).abs() / lit(d)).gt(lit(threshold))
                }
                _ => lit(false),
            };
            (
                OutlierStats {
                    method,
                    mean: None,
                    std: None,
                    median,
                    mad,
                    q1: None,
                    q3: None,
                    lower_fence: None,
                    upper_fence: None,
                },
                pred,
            )
        }
    };

    // Count outliers and collect examples.
    let outlier_count_df = lf
        .clone()
        .filter(predicate.clone())
        .select([len().alias("__outliers")])
        .collect()
        .map_err(|e| {
            crate::ingestion::polars_bridge::polars_error_to_ingestion(
                "failed to count outliers",
                e,
            )
        })?;
    let outlier_count = read_f64(&outlier_count_df, "__outliers")?.unwrap_or(0.0) as usize;

    let examples = if outlier_count > 0 && options.max_examples > 0 {
        let ex_df = lf
            .clone()
            .filter(predicate)
            .select([col(column)])
            .limit(options.max_examples as IdxSize)
            .collect()
            .map_err(|e| {
                crate::ingestion::polars_bridge::polars_error_to_ingestion(
                    "failed to collect outlier examples",
                    e,
                )
            })?;
        let s = ex_df
            .column(column)
            .map_err(|e| {
                crate::ingestion::polars_bridge::polars_error_to_ingestion(
                    "missing outlier column",
                    e,
                )
            })?
            .as_materialized_series();
        series_to_f64_vec(s, options.max_examples)?
    } else {
        Vec::new()
    };

    Ok(OutlierReport {
        column: column.to_string(),
        sampling: options.sampling,
        row_count,
        outlier_count,
        stats,
        examples,
    })
}

pub fn render_outlier_report_json(rep: &OutlierReport) -> IngestionResult<String> {
    serde_json::to_string_pretty(&serde_json::json!({
        "column": rep.column,
        "sampling": format!("{:?}", rep.sampling),
        "row_count": rep.row_count,
        "outlier_count": rep.outlier_count,
        "stats": {
            "method": format!("{:?}", rep.stats.method),
            "mean": rep.stats.mean,
            "std": rep.stats.std,
            "median": rep.stats.median,
            "mad": rep.stats.mad,
            "q1": rep.stats.q1,
            "q3": rep.stats.q3,
            "lower_fence": rep.stats.lower_fence,
            "upper_fence": rep.stats.upper_fence,
        },
        "examples": rep.examples,
    }))
    .map_err(|e| IngestionError::SchemaMismatch {
        message: format!("failed to serialize outlier report json: {e}"),
    })
}

pub fn render_outlier_report_markdown(rep: &OutlierReport) -> String {
    let mut out = String::new();
    out.push_str("## Outlier report\n\n");
    out.push_str(&format!("- Column: `{}`\n", rep.column));
    out.push_str(&format!("- Rows profiled: **{}**\n", rep.row_count));
    out.push_str(&format!("- Outliers: **{}**\n\n", rep.outlier_count));
    out.push_str("### Stats\n\n");
    out.push_str(&format!(
        "- Method: `{}`\n",
        format!("{:?}", rep.stats.method)
    ));
    if let Some(v) = rep.stats.mean {
        out.push_str(&format!("- mean: `{v:.6}`\n"));
    }
    if let Some(v) = rep.stats.std {
        out.push_str(&format!("- std: `{v:.6}`\n"));
    }
    if let Some(v) = rep.stats.median {
        out.push_str(&format!("- median: `{v:.6}`\n"));
    }
    if let Some(v) = rep.stats.mad {
        out.push_str(&format!("- mad: `{v:.6}`\n"));
    }
    if let (Some(a), Some(b)) = (rep.stats.lower_fence, rep.stats.upper_fence) {
        out.push_str(&format!("- fences: `[{a:.6}, {b:.6}]`\n"));
    }
    if !rep.examples.is_empty() {
        out.push_str("\n### Examples\n\n");
        for v in &rep.examples {
            out.push_str(&format!("- `{v}`\n"));
        }
    }
    out
}

fn read_f64(df: &polars::prelude::DataFrame, name: &str) -> IngestionResult<Option<f64>> {
    let col = df
        .column(name)
        .map_err(|e| {
            crate::ingestion::polars_bridge::polars_error_to_ingestion("missing stats column", e)
        })?
        .as_materialized_series();
    let av = col.get(0).map_err(|e| IngestionError::Engine {
        message: "failed to read outlier stat".to_string(),
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
                message: format!("expected numeric stat value, got {other}"),
            });
        }
    })
}

fn series_to_f64_vec(s: &Series, max: usize) -> IngestionResult<Vec<f64>> {
    let n = usize::min(max, s.len());
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let av = s.get(i).map_err(|e| IngestionError::Engine {
            message: "failed to read outlier example".to_string(),
            source: Box::new(e),
        })?;
        match av {
            AnyValue::Null => {}
            AnyValue::Float64(v) => out.push(v),
            AnyValue::Float32(v) => out.push(v as f64),
            AnyValue::Int64(v) => out.push(v as f64),
            AnyValue::Int32(v) => out.push(v as f64),
            other => {
                return Err(IngestionError::SchemaMismatch {
                    message: format!("expected numeric outlier example, got {other}"),
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use crate::types::{DataType, Field, Schema};

    fn ds() -> DataSet {
        DataSet::new(
            Schema::new(vec![Field::new("x", DataType::Float64)]),
            vec![
                vec![Value::Float64(1.0)],
                vec![Value::Float64(2.0)],
                vec![Value::Float64(3.0)],
                vec![Value::Float64(1000.0)],
            ],
        )
    }

    #[test]
    fn outlier_iqr_finds_extreme_value_and_renders() {
        let rep = detect_outliers_dataset(
            &ds(),
            "x",
            OutlierMethod::Iqr { k: 1.5 },
            &OutlierOptions {
                sampling: SamplingMode::Full,
                max_examples: 3,
            },
        )
        .unwrap();
        assert!(rep.outlier_count >= 1);
        let json = render_outlier_report_json(&rep).unwrap();
        assert!(json.contains("\"outlier_count\""));
        let md = render_outlier_report_markdown(&rep);
        assert!(md.contains("## Outlier report"));
    }
}
