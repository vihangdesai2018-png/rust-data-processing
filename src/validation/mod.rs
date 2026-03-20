//! Validation (Phase 1).
//!
//! A small validation DSL that compiles checks to Polars expressions (via our pipeline) while keeping
//! the public API in crate-owned types.
//!
//! ## Example
//!
//! ```rust
//! use rust_data_processing::validation::{validate_dataset, Check, Severity, ValidationSpec};
//! use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
//!
//! # fn main() -> Result<(), rust_data_processing::IngestionError> {
//! let ds = DataSet::new(
//!     Schema::new(vec![
//!         Field::new("id", DataType::Int64),
//!         Field::new("name", DataType::Utf8),
//!     ]),
//!     vec![
//!         vec![Value::Int64(1), Value::Utf8("Ada".to_string())],
//!         vec![Value::Int64(2), Value::Null],
//!     ],
//! );
//!
//! let spec = ValidationSpec::new(vec![
//!     Check::NotNull { column: "name".to_string(), severity: Severity::Error },
//! ]);
//! let rep = validate_dataset(&ds, &spec)?;
//! assert_eq!(rep.summary.failed_checks, 1);
//! # Ok(())
//! # }
//! ```

use crate::error::{IngestionError, IngestionResult};
use crate::pipeline::DataFrame;
use crate::types::{DataSet, Value};

use polars::prelude::*;

/// Severity for a validation check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Info,
    Warn,
    Error,
}

/// A single validation check.
#[derive(Debug, Clone, PartialEq)]
pub enum Check {
    NotNull { column: String, severity: Severity },
    RangeF64 {
        column: String,
        min: f64,
        max: f64,
        severity: Severity,
    },
    RegexMatch {
        column: String,
        pattern: String,
        severity: Severity,
        /// If true, invalid regex patterns become errors; if false, invalid regex evaluates to false.
        strict: bool,
    },
    InSet {
        column: String,
        values: Vec<Value>,
        severity: Severity,
    },
    Unique { column: String, severity: Severity },
}

/// A collection of checks.
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationSpec {
    pub checks: Vec<Check>,
    /// Maximum number of example values to include for failing checks.
    pub max_examples: usize,
}

impl ValidationSpec {
    pub fn new(checks: Vec<Check>) -> Self {
        Self {
            checks,
            max_examples: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationSummary {
    pub total_checks: usize,
    pub failed_checks: usize,
    pub max_severity: Option<Severity>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckResult {
    pub check: Check,
    pub failed_count: usize,
    pub examples: Vec<Value>,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValidationReport {
    pub results: Vec<CheckResult>,
    pub summary: ValidationSummary,
}

pub fn validate_dataset(ds: &DataSet, spec: &ValidationSpec) -> IngestionResult<ValidationReport> {
    let df = DataFrame::from_dataset(ds)?;
    validate_frame(&df, spec)
}

pub fn validate_frame(df: &DataFrame, spec: &ValidationSpec) -> IngestionResult<ValidationReport> {
    if spec.checks.is_empty() {
        return Ok(ValidationReport {
            results: Vec::new(),
            summary: ValidationSummary {
                total_checks: 0,
                failed_checks: 0,
                max_severity: None,
            },
        });
    }

    // One-shot aggregation to compute failed counts.
    let lf = df.lazy_clone();
    let mut exprs: Vec<Expr> = Vec::with_capacity(spec.checks.len());

    for (i, chk) in spec.checks.iter().enumerate() {
        exprs.push(fail_count_expr(chk).alias(&fail_count_col_name(i)));
    }

    let agg = lf
        .select(exprs)
        .collect()
        .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("failed to compute validation counts", e))?;

    let mut results: Vec<CheckResult> = Vec::with_capacity(spec.checks.len());
    let mut failed_checks = 0usize;
    let mut max_sev: Option<Severity> = None;

    for (i, chk) in spec.checks.iter().cloned().enumerate() {
        let col = agg
            .column(&fail_count_col_name(i))
            .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("validation missing agg column", e))?;
        let failed_count = series_to_usize(col.as_materialized_series())?.unwrap_or(0);

        if failed_count > 0 {
            failed_checks += 1;
            let sev = severity_of(&chk);
            max_sev = Some(max_sev.map(|s| s.max(sev)).unwrap_or(sev));
        }

        let examples = if failed_count > 0 && spec.max_examples > 0 {
            collect_examples(df, &chk, spec.max_examples).unwrap_or_default()
        } else {
            Vec::new()
        };

        results.push(CheckResult {
            message: default_message(&chk, failed_count),
            check: chk,
            failed_count,
            examples,
        });
    }

    Ok(ValidationReport {
        summary: ValidationSummary {
            total_checks: spec.checks.len(),
            failed_checks,
            max_severity: max_sev,
        },
        results,
    })
}

pub fn render_validation_report_json(rep: &ValidationReport) -> IngestionResult<String> {
    let results: Vec<serde_json::Value> = rep
        .results
        .iter()
        .map(|r| serde_json::json!({
            "check": format!("{:?}", r.check),
            "failed_count": r.failed_count,
            "examples": r.examples.iter().map(value_to_json).collect::<Vec<_>>(),
            "message": r.message,
        }))
        .collect();

    serde_json::to_string_pretty(&serde_json::json!({
        "summary": {
            "total_checks": rep.summary.total_checks,
            "failed_checks": rep.summary.failed_checks,
            "max_severity": rep.summary.max_severity.map(|s| format!("{s:?}")),
        },
        "results": results,
    }))
    .map_err(|e| IngestionError::SchemaMismatch {
        message: format!("failed to serialize validation report json: {e}"),
    })
}

pub fn render_validation_report_markdown(rep: &ValidationReport) -> String {
    let mut out = String::new();
    out.push_str("## Validation report\n\n");
    out.push_str(&format!(
        "- Total checks: **{}**\n- Failed checks: **{}**\n\n",
        rep.summary.total_checks, rep.summary.failed_checks
    ));

    out.push_str("### Results\n\n");
    for r in &rep.results {
        let status = if r.failed_count == 0 { "PASS" } else { "FAIL" };
        out.push_str(&format!("- **{status}**: `{}`\n", format!("{:?}", r.check)));
        out.push_str(&format!("  - Failed: **{}**\n", r.failed_count));
        out.push_str(&format!("  - Message: {}\n", r.message));
        if !r.examples.is_empty() {
            out.push_str("  - Examples:\n");
            for ex in &r.examples {
                out.push_str(&format!("    - `{}`\n", format!("{ex:?}")));
            }
        }
    }
    out
}

fn fail_count_col_name(i: usize) -> String {
    format!("__fail_{i}")
}

fn severity_of(chk: &Check) -> Severity {
    match chk {
        Check::NotNull { severity, .. }
        | Check::RangeF64 { severity, .. }
        | Check::RegexMatch { severity, .. }
        | Check::InSet { severity, .. }
        | Check::Unique { severity, .. } => *severity,
    }
}

fn default_message(chk: &Check, failed: usize) -> String {
    match chk {
        Check::NotNull { column, .. } => format!("column '{column}' has {failed} null(s)"),
        Check::RangeF64 { column, min, max, .. } => {
            format!("column '{column}' has {failed} value(s) outside [{min}, {max}]")
        }
        Check::RegexMatch { column, pattern, .. } => {
            format!("column '{column}' has {failed} value(s) not matching /{pattern}/")
        }
        Check::InSet { column, .. } => format!("column '{column}' has {failed} value(s) not in set"),
        Check::Unique { column, .. } => format!("column '{column}' has {failed} duplicate(s) among non-null values"),
    }
}

fn fail_count_expr(chk: &Check) -> Expr {
    match chk {
        Check::NotNull { column, .. } => col(column).is_null().sum(),
        Check::RangeF64 { column, min, max, .. } => {
            (col(column).lt(lit(*min)).or(col(column).gt(lit(*max)))).sum()
        }
        Check::RegexMatch {
            column,
            pattern,
            strict,
            ..
        } => col(column)
            .cast(DataType::String)
            .str()
            .contains(lit(pattern.clone()), *strict)
            .not()
            .sum(),
        Check::InSet { column, values, .. } => {
            let set_expr = lit(values_to_series(values));
            col(column).is_in(set_expr, false).not().sum()
        }
        Check::Unique { column, .. } => {
            // duplicates among non-null: non_null_count - unique_count
            let non_null = col(column).is_not_null().sum();
            let unique = col(column).drop_nulls().n_unique();
            (non_null - unique).alias("__dup")
        }
    }
}

fn values_to_series(values: &[Value]) -> Series {
    // We deliberately keep this minimal: enforce all values are same primitive type.
    if values.is_empty() {
        return Series::new("set".into(), Vec::<i64>::new());
    }
    match &values[0] {
        Value::Int64(_) => {
            let mut v: Vec<i64> = Vec::with_capacity(values.len());
            for x in values {
                if let Value::Int64(i) = x {
                    v.push(*i);
                }
            }
            Series::new("set".into(), v)
        }
        Value::Bool(_) => {
            let mut v: Vec<bool> = Vec::with_capacity(values.len());
            for x in values {
                if let Value::Bool(b) = x {
                    v.push(*b);
                }
            }
            Series::new("set".into(), v)
        }
        Value::Utf8(_) => {
            let mut v: Vec<String> = Vec::with_capacity(values.len());
            for x in values {
                if let Value::Utf8(s) = x {
                    v.push(s.clone());
                }
            }
            Series::new("set".into(), v)
        }
        Value::Float64(_) | Value::Null => Series::new("set".into(), Vec::<String>::new()),
    }
}

fn series_to_usize(s: &Series) -> IngestionResult<Option<usize>> {
    let av = s
        .get(0)
        .map_err(|e| IngestionError::Engine {
            message: "failed to read validation value".to_string(),
            source: Box::new(e),
        })?;
    Ok(match av {
        AnyValue::Null => None,
        AnyValue::Int64(v) => Some(v.max(0) as usize),
        AnyValue::UInt64(v) => Some(v as usize),
        AnyValue::Int32(v) => Some((v as i64).max(0) as usize),
        AnyValue::UInt32(v) => Some(v as usize),
        other => {
            return Err(IngestionError::SchemaMismatch {
                message: format!("expected integer-like validation value, got {other}"),
            })
        }
    })
}

fn collect_examples(df: &DataFrame, chk: &Check, max_examples: usize) -> IngestionResult<Vec<Value>> {
    let mut lf = df.lazy_clone();
    let (col_name, predicate) = match chk {
        Check::NotNull { column, .. } => (column.as_str(), col(column).is_null()),
        Check::RangeF64 { column, min, max, .. } => {
            (column.as_str(), col(column).lt(lit(*min)).or(col(column).gt(lit(*max))))
        }
        Check::RegexMatch { column, pattern, strict, .. } => (
            column.as_str(),
            col(column)
                .cast(DataType::String)
                .str()
                .contains(lit(pattern.clone()), *strict)
                .not(),
        ),
        Check::InSet { column, values, .. } => {
            (column.as_str(), col(column).is_in(lit(values_to_series(values)), false).not())
        }
        Check::Unique { .. } => return Ok(Vec::new()), // examples for duplicates would require group-by; skip in Phase 1
    };

    lf = lf.filter(predicate).select([col(col_name)]).limit(max_examples as IdxSize);
    let out = lf
        .collect()
        .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("failed to collect validation examples", e))?;

    let s = out
        .column(col_name)
        .map_err(|e| crate::ingestion::polars_bridge::polars_error_to_ingestion("missing validation example column", e))?
        .as_materialized_series()
        .clone();

    let mut ex = Vec::new();
    for i in 0..usize::min(max_examples, s.len()) {
        let v = s.get(i).map_err(|e| IngestionError::Engine {
            message: "failed to read validation example".to_string(),
            source: Box::new(e),
        })?;
        ex.push(any_to_value(v));
    }
    Ok(ex)
}

fn any_to_value(v: AnyValue) -> Value {
    match v {
        AnyValue::Null => Value::Null,
        AnyValue::Boolean(b) => Value::Bool(b),
        AnyValue::Int64(i) => Value::Int64(i),
        AnyValue::Float64(x) => Value::Float64(x),
        AnyValue::String(s) => Value::Utf8(s.to_string()),
        AnyValue::StringOwned(s) => Value::Utf8(s.to_string()),
        other => Value::Utf8(other.to_string()),
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Int64(i) => serde_json::json!(i),
        Value::Float64(x) => serde_json::json!(x),
        Value::Bool(b) => serde_json::json!(b),
        Value::Utf8(s) => serde_json::json!(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field, Schema};

    fn sample() -> DataSet {
        DataSet::new(
            Schema::new(vec![
                Field::new("id", DataType::Int64),
                Field::new("name", DataType::Utf8),
                Field::new("score", DataType::Float64),
            ]),
            vec![
                vec![Value::Int64(1), Value::Utf8("Ada".to_string()), Value::Float64(10.0)],
                vec![Value::Int64(2), Value::Null, Value::Float64(200.0)],
                vec![Value::Int64(2), Value::Utf8("Bob".to_string()), Value::Float64(5.0)],
            ],
        )
    }

    #[test]
    fn validation_counts_failures_and_renders_reports() {
        let ds = sample();
        let spec = ValidationSpec::new(vec![
            Check::NotNull {
                column: "name".to_string(),
                severity: Severity::Error,
            },
            Check::RangeF64 {
                column: "score".to_string(),
                min: 0.0,
                max: 100.0,
                severity: Severity::Warn,
            },
            Check::Unique {
                column: "id".to_string(),
                severity: Severity::Error,
            },
        ]);

        let rep = validate_dataset(&ds, &spec).unwrap();
        assert_eq!(rep.summary.total_checks, 3);
        assert!(rep.summary.failed_checks >= 1);

        let json = render_validation_report_json(&rep).unwrap();
        assert!(json.contains("\"results\""));

        let md = render_validation_report_markdown(&rep);
        assert!(md.contains("## Validation report"));
    }
}

