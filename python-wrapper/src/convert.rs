//! Shared Python ↔ Rust conversions for the PyO3 extension.

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyList};

use rust_data_processing::IngestionError;
use rust_data_processing::execution::ExecutionOptions;
use rust_data_processing::ingestion::{
    ExcelSheetSelection, IngestionFormat, IngestionOptions, IngestionSeverity,
};
use rust_data_processing::outliers::{OutlierMethod, OutlierOptions};
use rust_data_processing::processing::VarianceKind;
use rust_data_processing::profiling::{ProfileOptions, SamplingMode};
use rust_data_processing::types::{DataType, Field, Schema, Value};
use rust_data_processing::validation::{Check, Severity, ValidationSpec};

pub(crate) fn ingestion_err_to_py(e: IngestionError) -> PyErr {
    match e {
        IngestionError::Io(err) => PyIOError::new_err(err.to_string()),
        _ => PyValueError::new_err(e.to_string()),
    }
}

pub(crate) fn parse_ingestion_severity(s: &str) -> PyResult<IngestionSeverity> {
    match s.to_ascii_lowercase().as_str() {
        "info" => Ok(IngestionSeverity::Info),
        "warning" | "warn" => Ok(IngestionSeverity::Warning),
        "error" => Ok(IngestionSeverity::Error),
        "critical" => Ok(IngestionSeverity::Critical),
        _ => Err(PyValueError::new_err(
            "alert_at_or_above must be 'info', 'warning', 'error', or 'critical'",
        )),
    }
}

pub(crate) fn parse_data_type(s: &str) -> PyResult<DataType> {
    match s.to_ascii_lowercase().as_str() {
        "int64" | "i64" => Ok(DataType::Int64),
        "float64" | "f64" | "double" => Ok(DataType::Float64),
        "bool" | "boolean" => Ok(DataType::Bool),
        "utf8" | "string" | "str" | "text" => Ok(DataType::Utf8),
        _ => Err(PyValueError::new_err(format!(
            "unknown data_type '{s}'; expected int64, float64, bool, or utf8"
        ))),
    }
}

pub(crate) fn schema_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Schema> {
    let list = obj.downcast::<PyList>()?;
    let mut fields = Vec::with_capacity(list.len());
    for item in list.iter() {
        let d = item.downcast::<PyDict>()?;
        let name: String = d
            .get_item("name")?
            .ok_or_else(|| PyValueError::new_err("schema field missing 'name'"))?
            .extract()?;
        let dt: String = d
            .get_item("data_type")?
            .ok_or_else(|| PyValueError::new_err("schema field missing 'data_type'"))?
            .extract()?;
        fields.push(Field::new(name, parse_data_type(dt.trim())?));
    }
    Ok(Schema::new(fields))
}

pub(crate) fn schema_to_py_list(py: Python<'_>, schema: &Schema) -> PyResult<PyObject> {
    let list = PyList::empty(py);
    for f in &schema.fields {
        let d = PyDict::new(py);
        d.set_item("name", &f.name)?;
        let dt = match f.data_type {
            DataType::Int64 => "int64",
            DataType::Float64 => "float64",
            DataType::Bool => "bool",
            DataType::Utf8 => "utf8",
        };
        d.set_item("data_type", dt)?;
        list.append(d)?;
    }
    Ok(list.into())
}

pub(crate) fn parse_format(s: &str) -> PyResult<IngestionFormat> {
    match s.to_ascii_lowercase().as_str() {
        "csv" => Ok(IngestionFormat::Csv),
        "json" | "ndjson" => Ok(IngestionFormat::Json),
        "parquet" | "pq" => Ok(IngestionFormat::Parquet),
        "excel" | "xlsx" | "xls" | "ods" => Ok(IngestionFormat::Excel),
        _ => Err(PyValueError::new_err(format!(
            "unknown format '{s}'; expected csv, json, parquet, or excel"
        ))),
    }
}

fn excel_selection_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ExcelSheetSelection> {
    let d = obj.downcast::<PyDict>()?;
    let mode: String = d
        .get_item("mode")?
        .ok_or_else(|| PyValueError::new_err("excel_sheet_selection missing 'mode'"))?
        .extract()?;
    match mode.to_ascii_lowercase().as_str() {
        "first" => Ok(ExcelSheetSelection::First),
        "all" | "all_sheets" => Ok(ExcelSheetSelection::AllSheets),
        "sheet" => {
            let name: String = d
                .get_item("name")?
                .ok_or_else(|| PyValueError::new_err("mode 'sheet' requires 'name'"))?
                .extract()?;
            Ok(ExcelSheetSelection::Sheet(name))
        }
        "sheets" => {
            let names_any = d
                .get_item("names")?
                .ok_or_else(|| PyValueError::new_err("mode 'sheets' requires 'names' (list)"))?;
            let names_list = names_any.downcast::<PyList>()?;
            let mut names = Vec::with_capacity(names_list.len());
            for x in names_list.iter() {
                names.push(x.extract::<String>()?);
            }
            Ok(ExcelSheetSelection::Sheets(names))
        }
        _ => Err(PyValueError::new_err(
            "excel_sheet_selection.mode must be 'first', 'sheet', 'all', or 'sheets'",
        )),
    }
}

pub(crate) fn ingestion_options_from_py(
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<IngestionOptions> {
    let Some(obj) = obj else {
        return Ok(IngestionOptions::default());
    };
    let d = obj.downcast::<PyDict>()?;
    let mut o = IngestionOptions::default();
    if let Some(v) = d.get_item("format")? {
        let s: String = v.extract()?;
        o.format = Some(parse_format(s.trim())?);
    }
    if let Some(v) = d.get_item("excel_sheet_selection")? {
        o.excel_sheet_selection = excel_selection_from_py(&v)?;
    }
    Ok(o)
}

pub(crate) fn value_to_py(py: Python<'_>, v: &Value) -> PyObject {
    match v {
        Value::Null => py.None().into(),
        Value::Int64(i) => i
            .into_pyobject(py)
            .expect("int converts")
            .into_any()
            .unbind(),
        Value::Float64(f) => f
            .into_pyobject(py)
            .expect("float converts")
            .into_any()
            .unbind(),
        Value::Bool(b) => PyBool::new(py, *b).to_owned().into(),
        Value::Utf8(s) => s
            .as_str()
            .into_pyobject(py)
            .expect("str converts")
            .into_any()
            .unbind(),
    }
}

pub(crate) fn value_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    // Python `bool` subclasses `int`; try bool before integer extraction.
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Int64(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Value::Float64(f));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::Utf8(s));
    }
    Err(PyValueError::new_err(
        "value must be None, int, float, bool, or str",
    ))
}

pub(crate) fn dataset_from_rows_py(
    schema: &Bound<'_, PyAny>,
    rows: &Bound<'_, PyAny>,
) -> PyResult<rust_data_processing::types::DataSet> {
    use rust_data_processing::types::DataSet;
    let schema = schema_from_py(schema)?;
    let list = rows.downcast::<PyList>()?;
    let ncols = schema.fields.len();
    let mut out_rows = Vec::with_capacity(list.len());
    for row_any in list.iter() {
        let row_list = row_any.downcast::<PyList>()?;
        if row_list.len() != ncols {
            return Err(PyValueError::new_err(format!(
                "row length {} does not match schema length {}",
                row_list.len(),
                ncols
            )));
        }
        let mut row = Vec::with_capacity(ncols);
        for c in row_list.iter() {
            row.push(value_from_py(&c)?);
        }
        out_rows.push(row);
    }
    Ok(DataSet::new(schema, out_rows))
}

pub(crate) fn parse_reduce_op(s: &str) -> PyResult<rust_data_processing::processing::ReduceOp> {
    use rust_data_processing::processing::ReduceOp;
    match s.to_ascii_lowercase().replace('-', "_").as_str() {
        "count" => Ok(ReduceOp::Count),
        "sum" => Ok(ReduceOp::Sum),
        "min" => Ok(ReduceOp::Min),
        "max" => Ok(ReduceOp::Max),
        "mean" => Ok(ReduceOp::Mean),
        "variance_population" | "var_pop" => Ok(ReduceOp::Variance(VarianceKind::Population)),
        "variance_sample" | "var_sample" => Ok(ReduceOp::Variance(VarianceKind::Sample)),
        "stddev_population" | "std_pop" | "std_dev_population" => {
            Ok(ReduceOp::StdDev(VarianceKind::Population))
        }
        "stddev_sample" | "std_sample" | "std_dev_sample" => {
            Ok(ReduceOp::StdDev(VarianceKind::Sample))
        }
        "sum_squares" | "sumsq" => Ok(ReduceOp::SumSquares),
        "l2_norm" | "l2" => Ok(ReduceOp::L2Norm),
        "count_distinct_non_null" | "count_distinct" => Ok(ReduceOp::CountDistinctNonNull),
        _ => Err(PyValueError::new_err(format!(
            "unknown reduce op '{s}'; see API.md for supported names"
        ))),
    }
}

pub(crate) fn parse_variance_kind(s: &str) -> PyResult<VarianceKind> {
    match s.to_ascii_lowercase().as_str() {
        "population" | "pop" => Ok(VarianceKind::Population),
        "sample" => Ok(VarianceKind::Sample),
        _ => Err(PyValueError::new_err(
            "std_kind must be 'population' or 'sample'",
        )),
    }
}

fn severity_from_py(s: &str) -> PyResult<Severity> {
    match s.to_ascii_lowercase().as_str() {
        "info" => Ok(Severity::Info),
        "warn" | "warning" => Ok(Severity::Warn),
        "error" => Ok(Severity::Error),
        _ => Err(PyValueError::new_err(
            "severity must be 'info', 'warn', or 'error'",
        )),
    }
}

pub(crate) fn validation_spec_from_py(obj: &Bound<'_, PyAny>) -> PyResult<ValidationSpec> {
    let d = obj.downcast::<PyDict>()?;
    let checks_any = d
        .get_item("checks")?
        .ok_or_else(|| PyValueError::new_err("validation spec missing 'checks' list"))?;
    let checks_list = checks_any.downcast::<PyList>()?;
    let mut checks = Vec::with_capacity(checks_list.len());
    for item in checks_list.iter() {
        let c = item.downcast::<PyDict>()?;
        let kind: String = c
            .get_item("kind")?
            .ok_or_else(|| PyValueError::new_err("check missing 'kind'"))?
            .extract()?;
        let check = match kind.to_ascii_lowercase().as_str() {
            "not_null" => {
                let column: String = c
                    .get_item("column")?
                    .ok_or_else(|| PyValueError::new_err("not_null check needs 'column'"))?
                    .extract()?;
                let sev: String = c
                    .get_item("severity")?
                    .ok_or_else(|| PyValueError::new_err("not_null check needs 'severity'"))?
                    .extract()?;
                Check::NotNull {
                    column,
                    severity: severity_from_py(&sev)?,
                }
            }
            "range_f64" => {
                let column: String = c
                    .get_item("column")?
                    .ok_or_else(|| PyValueError::new_err("range_f64 needs 'column'"))?
                    .extract()?;
                let min: f64 = c
                    .get_item("min")?
                    .ok_or_else(|| PyValueError::new_err("range_f64 needs 'min'"))?
                    .extract()?;
                let max: f64 = c
                    .get_item("max")?
                    .ok_or_else(|| PyValueError::new_err("range_f64 needs 'max'"))?
                    .extract()?;
                let sev: String = c
                    .get_item("severity")?
                    .ok_or_else(|| PyValueError::new_err("range_f64 needs 'severity'"))?
                    .extract()?;
                Check::RangeF64 {
                    column,
                    min,
                    max,
                    severity: severity_from_py(&sev)?,
                }
            }
            "regex_match" => {
                let column: String = c
                    .get_item("column")?
                    .ok_or_else(|| PyValueError::new_err("regex_match needs 'column'"))?
                    .extract()?;
                let pattern: String = c
                    .get_item("pattern")?
                    .ok_or_else(|| PyValueError::new_err("regex_match needs 'pattern'"))?
                    .extract()?;
                let sev: String = c
                    .get_item("severity")?
                    .ok_or_else(|| PyValueError::new_err("regex_match needs 'severity'"))?
                    .extract()?;
                let strict = c
                    .get_item("strict")?
                    .map(|v| v.extract::<bool>())
                    .transpose()?
                    .unwrap_or(true);
                Check::RegexMatch {
                    column,
                    pattern,
                    severity: severity_from_py(&sev)?,
                    strict,
                }
            }
            "in_set" => {
                let column: String = c
                    .get_item("column")?
                    .ok_or_else(|| PyValueError::new_err("in_set needs 'column'"))?
                    .extract()?;
                let vals_any = c
                    .get_item("values")?
                    .ok_or_else(|| PyValueError::new_err("in_set needs 'values' list"))?;
                let vals_list = vals_any.downcast::<PyList>()?;
                let mut values = Vec::with_capacity(vals_list.len());
                for v in vals_list.iter() {
                    values.push(value_from_py(&v)?);
                }
                let sev: String = c
                    .get_item("severity")?
                    .ok_or_else(|| PyValueError::new_err("in_set needs 'severity'"))?
                    .extract()?;
                Check::InSet {
                    column,
                    values,
                    severity: severity_from_py(&sev)?,
                }
            }
            "unique" => {
                let column: String = c
                    .get_item("column")?
                    .ok_or_else(|| PyValueError::new_err("unique needs 'column'"))?
                    .extract()?;
                let sev: String = c
                    .get_item("severity")?
                    .ok_or_else(|| PyValueError::new_err("unique needs 'severity'"))?
                    .extract()?;
                Check::Unique {
                    column,
                    severity: severity_from_py(&sev)?,
                }
            }
            _ => {
                return Err(PyValueError::new_err(format!(
                    "unknown validation check kind '{kind}'"
                )));
            }
        };
        checks.push(check);
    }
    let mut spec = ValidationSpec::new(checks);
    if let Some(v) = d.get_item("max_examples")? {
        spec.max_examples = v.extract::<usize>()?;
    }
    Ok(spec)
}

pub(crate) fn profile_options_from_py(obj: Option<&Bound<'_, PyAny>>) -> PyResult<ProfileOptions> {
    let Some(obj) = obj else {
        return Ok(ProfileOptions::default());
    };
    let d = obj.downcast::<PyDict>()?;
    let mut o = ProfileOptions::default();
    if let Some(v) = d.get_item("sampling")? {
        if let Ok(s) = v.extract::<String>() {
            match s.to_ascii_lowercase().as_str() {
                "full" => o.sampling = SamplingMode::Full,
                _ => {
                    return Err(PyValueError::new_err(
                        "when sampling is a string, only 'full' is supported; use head_rows for head sampling",
                    ));
                }
            }
        } else if let Ok(sub) = v.downcast::<PyDict>() {
            if let Some(n) = sub.get_item("head")? {
                o.sampling = SamplingMode::Head(n.extract::<usize>()?);
            }
        }
    }
    if let Some(v) = d.get_item("head_rows")? {
        o.sampling = SamplingMode::Head(v.extract::<usize>()?);
    }
    if let Some(v) = d.get_item("quantiles")? {
        let lst = v.downcast::<PyList>()?;
        let mut qs = Vec::with_capacity(lst.len());
        for x in lst.iter() {
            qs.push(x.extract::<f64>()?);
        }
        o.quantiles = qs;
    }
    Ok(o)
}

pub(crate) fn outlier_method_from_py(obj: &Bound<'_, PyAny>) -> PyResult<OutlierMethod> {
    let d = obj.downcast::<PyDict>()?;
    let kind: String = d
        .get_item("kind")?
        .ok_or_else(|| PyValueError::new_err("outlier method missing 'kind'"))?
        .extract()?;
    match kind.to_ascii_lowercase().as_str() {
        "z_score" | "zscore" => {
            let threshold = match d.get_item("threshold")? {
                None => 3.0,
                Some(v) => v.extract::<f64>()?,
            };
            Ok(OutlierMethod::ZScore { threshold })
        }
        "iqr" | "tukey" => {
            let k = match d.get_item("k")? {
                None => 1.5,
                Some(v) => v.extract::<f64>()?,
            };
            Ok(OutlierMethod::Iqr { k })
        }
        "mad" => {
            let threshold = match d.get_item("threshold")? {
                None => 3.5,
                Some(v) => v.extract::<f64>()?,
            };
            Ok(OutlierMethod::Mad { threshold })
        }
        _ => Err(PyValueError::new_err(
            "outlier kind must be 'z_score', 'iqr', or 'mad'",
        )),
    }
}

pub(crate) fn outlier_options_from_py(obj: Option<&Bound<'_, PyAny>>) -> PyResult<OutlierOptions> {
    let Some(obj) = obj else {
        return Ok(OutlierOptions::default());
    };
    let d = obj.downcast::<PyDict>()?;
    let mut o = OutlierOptions::default();
    if let Some(v) = d.get_item("sampling")? {
        if let Ok(s) = v.extract::<String>() {
            if s.to_ascii_lowercase() == "full" {
                o.sampling = SamplingMode::Full;
            }
        } else if let Ok(sub) = v.downcast::<PyDict>() {
            if let Some(n) = sub.get_item("head")? {
                o.sampling = SamplingMode::Head(n.extract::<usize>()?);
            }
        }
    }
    if let Some(v) = d.get_item("head_rows")? {
        o.sampling = SamplingMode::Head(v.extract::<usize>()?);
    }
    if let Some(v) = d.get_item("max_examples")? {
        o.max_examples = v.extract::<usize>()?;
    }
    Ok(o)
}

pub(crate) fn execution_options_from_py(
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<ExecutionOptions> {
    let Some(obj) = obj else {
        return Ok(ExecutionOptions::default());
    };
    let d = obj.downcast::<PyDict>()?;
    let mut o = ExecutionOptions::default();
    if let Some(v) = d.get_item("num_threads")? {
        if v.is_none() {
            o.num_threads = None;
        } else {
            o.num_threads = Some(v.extract::<usize>()?);
        }
    }
    if let Some(v) = d.get_item("chunk_size")? {
        o.chunk_size = v.extract::<usize>()?;
    }
    if let Some(v) = d.get_item("max_in_flight_chunks")? {
        o.max_in_flight_chunks = v.extract::<usize>()?;
    }
    Ok(o)
}

pub(crate) fn metrics_snapshot_to_py(
    py: Python<'_>,
    s: &rust_data_processing::execution::ExecutionMetricsSnapshot,
) -> PyResult<PyObject> {
    let d = PyDict::new(py);
    d.set_item("run_id", s.run_id)?;
    d.set_item("rows_processed", s.rows_processed)?;
    d.set_item("chunks_started", s.chunks_started)?;
    d.set_item("chunks_finished", s.chunks_finished)?;
    d.set_item("max_active_chunks", s.max_active_chunks)?;
    d.set_item("throttle_wait_seconds", s.throttle_wait.as_secs_f64())?;
    match s.elapsed {
        Some(e) => d.set_item("elapsed_seconds", e.as_secs_f64())?,
        None => d.set_item("elapsed_seconds", py.None())?,
    }
    Ok(d.into())
}
