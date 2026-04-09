//! PyO3 extension: bindings over the `rust-data-processing` crate.
//!
//! Python imports the native module as `rust_data_processing._rust_data_processing`; use the
//! stable surface in `rust_data_processing/__init__.py`.

mod convert;
mod observer_bridge;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use convert::*;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use rust_data_processing::execution::ExecutionEngine;
use rust_data_processing::ingestion::{
    IngestionOptions, discover_hive_partitioned_files as discover_hive_partitioned_files_rs,
    infer_schema_from_path, ingest_from_db, ingest_from_db_infer, ingest_from_path,
    ingest_from_path_infer, parse_partition_segment as parse_partition_segment_rs,
    paths_from_explicit_list as paths_from_explicit_list_rs, paths_from_glob as paths_from_glob_rs,
};
use rust_data_processing::outliers::{
    detect_outliers_dataset, render_outlier_report_json, render_outlier_report_markdown,
};
use rust_data_processing::pipeline::{Agg, CastMode, DataFrame, JoinKind, Predicate};
use rust_data_processing::processing::{
    VarianceKind, arg_max_row, arg_min_row, feature_wise_mean_std, reduce, top_k_by_frequency,
};
use rust_data_processing::profiling::{
    profile_dataset, render_profile_report_json, render_profile_report_markdown,
};
use rust_data_processing::sql;
use rust_data_processing::transform::TransformSpec;
use rust_data_processing::types::{DataSet, Value};
use rust_data_processing::validation::{
    render_validation_report_json, render_validation_report_markdown, validate_dataset,
};

fn dict_req_str(d: &Bound<'_, PyDict>, key: &str) -> PyResult<String> {
    d.get_item(key)?
        .ok_or_else(|| PyValueError::new_err(format!("aggregation dict missing '{key}'")))?
        .extract()
}

fn agg_from_py(d: &Bound<'_, PyDict>) -> PyResult<Agg> {
    let typ: String = dict_req_str(d, "type")?;
    match typ.to_ascii_lowercase().replace('-', "_").as_str() {
        "count_rows" => Ok(Agg::CountRows {
            alias: dict_req_str(d, "alias")?,
        }),
        "count_not_null" => Ok(Agg::CountNotNull {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        "sum" => Ok(Agg::Sum {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        "min" => Ok(Agg::Min {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        "max" => Ok(Agg::Max {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        "mean" => Ok(Agg::Mean {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        "variance" => {
            let kind = match d.get_item("kind")? {
                None => VarianceKind::Sample,
                Some(v) => parse_variance_kind(&v.extract::<String>()?)?,
            };
            Ok(Agg::Variance {
                column: dict_req_str(d, "column")?,
                alias: dict_req_str(d, "alias")?,
                kind,
            })
        }
        "std_dev" | "stddev" => {
            let kind = match d.get_item("kind")? {
                None => VarianceKind::Sample,
                Some(v) => parse_variance_kind(&v.extract::<String>()?)?,
            };
            Ok(Agg::StdDev {
                column: dict_req_str(d, "column")?,
                alias: dict_req_str(d, "alias")?,
                kind,
            })
        }
        "sum_squares" => Ok(Agg::SumSquares {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        "l2_norm" | "l2" => Ok(Agg::L2Norm {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        "count_distinct_non_null" | "count_distinct" => Ok(Agg::CountDistinctNonNull {
            column: dict_req_str(d, "column")?,
            alias: dict_req_str(d, "alias")?,
        }),
        _ => Err(PyValueError::new_err(format!(
            "unknown aggregation type '{typ}'"
        ))),
    }
}

fn join_kind_from_str(s: &str) -> PyResult<JoinKind> {
    match s.to_ascii_lowercase().as_str() {
        "inner" => Ok(JoinKind::Inner),
        "left" => Ok(JoinKind::Left),
        "right" => Ok(JoinKind::Right),
        "full" | "outer" => Ok(JoinKind::Full),
        _ => Err(PyValueError::new_err(
            "join kind must be 'inner', 'left', 'right', or 'full'",
        )),
    }
}

fn cast_mode_from_str(s: &str) -> PyResult<CastMode> {
    match s.to_ascii_lowercase().as_str() {
        "strict" => Ok(CastMode::Strict),
        "lossy" => Ok(CastMode::Lossy),
        _ => Err(PyValueError::new_err(
            "cast mode must be 'strict' or 'lossy'",
        )),
    }
}

fn merge_ingestion_options(
    py: Python<'_>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<IngestionOptions> {
    let mut opts = ingestion_options_from_py(options)?;
    if let Some(o) = options {
        if let Ok(d) = o.downcast::<PyDict>() {
            observer_bridge::apply_ingestion_observer_options(py, d, &mut opts)?;
        }
    }
    Ok(opts)
}

/// In-memory tabular dataset (mirrors `rust_data_processing::types::DataSet`).
#[pyclass(name = "DataSet")]
#[derive(Clone)]
pub struct PyDataSet {
    pub(crate) inner: DataSet,
}

#[pymethods]
impl PyDataSet {
    #[new]
    #[pyo3(signature = (schema, rows))]
    fn new(schema: &Bound<'_, PyAny>, rows: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: dataset_from_rows_py(schema, rows)?,
        })
    }

    fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    fn column_names(&self) -> Vec<String> {
        self.inner
            .schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        schema_to_py_list(py, &self.inner.schema)
    }

    fn to_rows(&self, py: Python<'_>) -> PyResult<PyObject> {
        let outer = PyList::empty(py);
        for row in &self.inner.rows {
            let inner = PyList::empty(py);
            for v in row {
                inner.append(value_to_py(py, v))?;
            }
            outer.append(inner)?;
        }
        Ok(outer.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "DataSet(rows={}, columns={})",
            self.inner.row_count(),
            self.inner.schema.fields.len()
        )
    }
}

impl PyDataSet {
    pub(crate) fn from_inner(inner: DataSet) -> Self {
        Self { inner }
    }
}

/// Polars-backed lazy pipeline; collect to [`DataSet`] when ready.
#[pyclass(name = "DataFrame")]
pub struct PyDataFrame {
    pub(crate) inner: DataFrame,
}

#[pymethods]
impl PyDataFrame {
    #[staticmethod]
    fn from_dataset(ds: &PyDataSet) -> PyResult<Self> {
        DataFrame::from_dataset(&ds.inner)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn filter_eq(&self, column: String, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let v = value_from_py(value)?;
        self.inner
            .clone()
            .filter(Predicate::Eq { column, value: v })
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn filter_not_null(&self, column: String) -> PyResult<Self> {
        self.inner
            .clone()
            .filter(Predicate::NotNull { column })
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    #[pyo3(signature = (column, modulus, equals))]
    fn filter_mod_eq_int64(&self, column: String, modulus: i64, equals: i64) -> PyResult<Self> {
        self.inner
            .clone()
            .filter(Predicate::ModEqInt64 {
                column,
                modulus,
                equals,
            })
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn select(&self, columns: Vec<String>) -> PyResult<Self> {
        let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.inner
            .clone()
            .select(&refs)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn rename(&self, pairs: Vec<(String, String)>) -> PyResult<Self> {
        let refs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(a, b)| (a.as_str(), b.as_str()))
            .collect();
        self.inner
            .clone()
            .rename(&refs)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn drop(&self, columns: Vec<String>) -> PyResult<Self> {
        let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        self.inner
            .clone()
            .drop(&refs)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn cast(&self, column: &str, to: &str) -> PyResult<Self> {
        let dt = parse_data_type(to)?;
        self.inner
            .clone()
            .cast(column, dt)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    #[pyo3(signature = (column, to, mode))]
    fn cast_with_mode(&self, column: &str, to: &str, mode: &str) -> PyResult<Self> {
        let dt = parse_data_type(to)?;
        let m = cast_mode_from_str(mode)?;
        self.inner
            .clone()
            .cast_with_mode(column, dt, m)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn fill_null(&self, column: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let v = value_from_py(value)?;
        self.inner
            .clone()
            .fill_null(column, v)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn with_literal(&self, name: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let v = value_from_py(value)?;
        self.inner
            .clone()
            .with_literal(name, v)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn multiply_f64(&self, column: &str, factor: f64) -> PyResult<Self> {
        self.inner
            .clone()
            .multiply_f64(column, factor)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn add_f64(&self, column: &str, delta: f64) -> PyResult<Self> {
        self.inner
            .clone()
            .add_f64(column, delta)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn with_mul_f64(&self, name: &str, source: &str, factor: f64) -> PyResult<Self> {
        self.inner
            .clone()
            .with_mul_f64(name, source, factor)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn with_add_f64(&self, name: &str, source: &str, delta: f64) -> PyResult<Self> {
        self.inner
            .clone()
            .with_add_f64(name, source, delta)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn group_by(&self, keys: Vec<String>, aggs: &Bound<'_, PyList>) -> PyResult<Self> {
        let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let mut out_aggs = Vec::with_capacity(aggs.len());
        for item in aggs.iter() {
            let d = item.downcast::<PyDict>()?;
            out_aggs.push(agg_from_py(&d)?);
        }
        self.inner
            .clone()
            .group_by(&key_refs, &out_aggs)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn join(
        &self,
        other: &PyDataFrame,
        left_on: Vec<String>,
        right_on: Vec<String>,
        how: &str,
    ) -> PyResult<Self> {
        let l: Vec<&str> = left_on.iter().map(|s| s.as_str()).collect();
        let r: Vec<&str> = right_on.iter().map(|s| s.as_str()).collect();
        self.inner
            .clone()
            .join(other.inner.clone(), &l, &r, join_kind_from_str(how)?)
            .map(|inner| Self { inner })
            .map_err(ingestion_err_to_py)
    }

    fn collect(&self) -> PyResult<PyDataSet> {
        self.inner
            .clone()
            .collect()
            .map(PyDataSet::from_inner)
            .map_err(ingestion_err_to_py)
    }

    fn collect_with_schema(&self, schema: &Bound<'_, PyAny>) -> PyResult<PyDataSet> {
        let sch = schema_from_py(schema)?;
        self.inner
            .clone()
            .collect_with_schema(&sch)
            .map(PyDataSet::from_inner)
            .map_err(ingestion_err_to_py)
    }

    fn reduce(&self, column: &str, op: &str) -> PyResult<Option<PyObject>> {
        let rop = parse_reduce_op(op)?;
        let v = self
            .inner
            .clone()
            .reduce(column, rop)
            .map_err(ingestion_err_to_py)?;
        Python::with_gil(|py| match v {
            None => Ok(None),
            Some(val) => Ok(Some(value_to_py(py, &val))),
        })
    }

    fn sum(&self, column: &str) -> PyResult<Option<PyObject>> {
        let v = self
            .inner
            .clone()
            .sum(column)
            .map_err(ingestion_err_to_py)?;
        Python::with_gil(|py| match v {
            None => Ok(None),
            Some(val) => Ok(Some(value_to_py(py, &val))),
        })
    }

    #[pyo3(signature = (columns, std_kind=None))]
    fn feature_wise_mean_std(
        &self,
        py: Python<'_>,
        columns: Vec<String>,
        std_kind: Option<&str>,
    ) -> PyResult<PyObject> {
        let kind = match std_kind {
            None => VarianceKind::Sample,
            Some(s) => parse_variance_kind(s)?,
        };
        let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        let rows = self
            .inner
            .clone()
            .feature_wise_mean_std(&refs, kind)
            .map_err(ingestion_err_to_py)?;
        let out = PyList::empty(py);
        for (name, m) in rows {
            let d = PyDict::new(py);
            d.set_item("column", name)?;
            d.set_item("mean", value_to_py(py, &m.mean))?;
            d.set_item("std_dev", value_to_py(py, &m.std_dev))?;
            out.append(d)?;
        }
        Ok(out.into())
    }

    fn __repr__(&self) -> String {
        "DataFrame(lazy)".to_string()
    }
}

/// Multi-table SQL context (register several pipeline frames, then `execute`).
#[pyclass(name = "SqlContext")]
pub struct PySqlContext {
    inner: Mutex<sql::Context>,
}

#[pymethods]
impl PySqlContext {
    #[new]
    fn new() -> Self {
        Self {
            inner: Mutex::new(sql::Context::new()),
        }
    }

    fn register(&self, name: &str, df: &PyDataFrame) -> PyResult<()> {
        self.inner
            .lock()
            .expect("sql context mutex poisoned")
            .register(name, &df.inner)
            .map_err(ingestion_err_to_py)
    }

    fn execute(&self, sql: &str) -> PyResult<PyDataFrame> {
        let mut g = self.inner.lock().expect("sql context mutex poisoned");
        let out = g.execute(sql).map_err(ingestion_err_to_py)?;
        Ok(PyDataFrame { inner: out })
    }
}

/// Configurable Rayon-backed engine: parallel filter/map (Python row callbacks acquire the GIL per row),
/// sequential `reduce`, and optional `on_execution_event` hook.
#[pyclass(name = "ExecutionEngine")]
pub struct PyExecutionEngine {
    inner: ExecutionEngine,
}

#[pymethods]
impl PyExecutionEngine {
    #[new]
    #[pyo3(signature = (options=None, on_execution_event=None))]
    fn new(
        options: Option<&Bound<'_, PyAny>>,
        on_execution_event: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let opts = execution_options_from_py(options)?;
        let engine = ExecutionEngine::new(opts);
        let inner = if let Some(cb) = on_execution_event {
            engine.with_observer(Arc::new(observer_bridge::PyExecutionObserver {
                callback: cb,
            }))
        } else {
            engine
        };
        Ok(Self { inner })
    }

    fn filter_parallel(
        &self,
        py: Python<'_>,
        ds: &PyDataSet,
        predicate: Py<PyAny>,
    ) -> PyResult<PyDataSet> {
        let data = ds.inner.clone();
        let pred = predicate.clone_ref(py);
        let err: Mutex<Option<PyErr>> = Mutex::new(None);
        let err_ref = &err;
        let out = py.allow_threads(|| {
            self.inner.filter_parallel(&data, move |row| {
                if err_ref.lock().unwrap().is_some() {
                    return false;
                }
                Python::with_gil(|py| {
                    let list = PyList::empty(py);
                    for v in row {
                        if list.append(value_to_py(py, v)).is_err() {
                            *err_ref.lock().unwrap() =
                                Some(PyValueError::new_err("failed to build row list"));
                            return false;
                        }
                    }
                    match pred.bind(py).call1((list,)) {
                        Ok(o) => {
                            let got: PyResult<bool> = o.extract();
                            match got {
                                Ok(b) => b,
                                Err(e) => {
                                    *err_ref.lock().unwrap() = Some(e);
                                    false
                                }
                            }
                        }
                        Err(e) => {
                            *err_ref.lock().unwrap() = Some(e);
                            false
                        }
                    }
                })
            })
        });
        if let Some(e) = err.into_inner().unwrap() {
            return Err(e);
        }
        Ok(PyDataSet::from_inner(out))
    }

    fn map_parallel(
        &self,
        py: Python<'_>,
        ds: &PyDataSet,
        mapper: Py<PyAny>,
    ) -> PyResult<PyDataSet> {
        let ncols = ds.inner.schema.fields.len();
        let data = ds.inner.clone();
        let mapper = mapper.clone_ref(py);
        let err: Mutex<Option<PyErr>> = Mutex::new(None);
        let err_ref = &err;
        let null_row = vec![Value::Null; ncols];
        let out = py.allow_threads(|| {
            self.inner.map_parallel(&data, move |row| {
                if err_ref.lock().unwrap().is_some() {
                    return null_row.clone();
                }
                Python::with_gil(|py| {
                    let list = PyList::empty(py);
                    for v in row {
                        if list.append(value_to_py(py, v)).is_err() {
                            *err_ref.lock().unwrap() =
                                Some(PyValueError::new_err("failed to build row list"));
                            return null_row.clone();
                        }
                    }
                    match mapper.bind(py).call1((list,)) {
                        Ok(new_row) => match new_row.downcast::<PyList>() {
                            Ok(py_row) => {
                                if py_row.len() != ncols {
                                    *err_ref.lock().unwrap() =
                                        Some(PyValueError::new_err(format!(
                                            "mapper returned length {}, expected {ncols}",
                                            py_row.len()
                                        )));
                                    return null_row.clone();
                                }
                                let mut v = Vec::with_capacity(ncols);
                                for i in 0..ncols {
                                    match py_row.get_item(i) {
                                        Ok(cell) => match value_from_py(&cell) {
                                            Ok(val) => v.push(val),
                                            Err(e) => {
                                                *err_ref.lock().unwrap() = Some(e);
                                                return null_row.clone();
                                            }
                                        },
                                        Err(e) => {
                                            *err_ref.lock().unwrap() = Some(e);
                                            return null_row.clone();
                                        }
                                    }
                                }
                                v
                            }
                            Err(_) => {
                                *err_ref.lock().unwrap() = Some(PyValueError::new_err(
                                    "mapper must return a list of cell values",
                                ));
                                null_row.clone()
                            }
                        },
                        Err(e) => {
                            *err_ref.lock().unwrap() = Some(e);
                            null_row.clone()
                        }
                    }
                })
            })
        });
        if let Some(e) = err.into_inner().unwrap() {
            return Err(e);
        }
        Ok(PyDataSet::from_inner(out))
    }

    fn reduce(
        &self,
        py: Python<'_>,
        ds: &PyDataSet,
        column: &str,
        op: &str,
    ) -> PyResult<Option<PyObject>> {
        let rop = parse_reduce_op(op)?;
        let v = self.inner.reduce(&ds.inner, column, rop);
        Ok(v.map(|val| value_to_py(py, &val)))
    }

    fn metrics_snapshot(&self, py: Python<'_>) -> PyResult<PyObject> {
        let s = self.inner.metrics().snapshot();
        metrics_snapshot_to_py(py, &s)
    }
}

#[pyfunction(name = "ingest_from_path")]
#[pyo3(signature = (path, schema, options=None))]
fn ingest_from_path_py(
    py: Python<'_>,
    path: &str,
    schema: &Bound<'_, PyAny>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyDataSet> {
    let schema = schema_from_py(schema)?;
    let opts = merge_ingestion_options(py, options)?;
    ingest_from_path(path, &schema, &opts)
        .map(PyDataSet::from_inner)
        .map_err(ingestion_err_to_py)
}

#[pyfunction(name = "infer_schema_from_path")]
#[pyo3(signature = (path, options=None))]
fn infer_schema_from_path_py(
    py: Python<'_>,
    path: &str,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyObject> {
    let opts = merge_ingestion_options(py, options)?;
    let s = infer_schema_from_path(path, &opts).map_err(ingestion_err_to_py)?;
    schema_to_py_list(py, &s)
}

#[pyfunction(name = "ingest_from_path_infer")]
#[pyo3(signature = (path, options=None))]
fn ingest_from_path_infer_py(
    py: Python<'_>,
    path: &str,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyDataSet> {
    let opts = merge_ingestion_options(py, options)?;
    ingest_from_path_infer(path, &opts)
        .map(PyDataSet::from_inner)
        .map_err(ingestion_err_to_py)
}

#[pyfunction(name = "ingest_from_db")]
#[pyo3(signature = (conn, query, schema, options=None))]
fn ingest_from_db_py(
    py: Python<'_>,
    conn: &str,
    query: &str,
    schema: &Bound<'_, PyAny>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyDataSet> {
    let schema = schema_from_py(schema)?;
    let opts = merge_ingestion_options(py, options)?;
    ingest_from_db(conn, query, &schema, &opts)
        .map(PyDataSet::from_inner)
        .map_err(ingestion_err_to_py)
}

#[pyfunction(name = "ingest_from_db_infer")]
#[pyo3(signature = (conn, query, options=None))]
fn ingest_from_db_infer_py(
    py: Python<'_>,
    conn: &str,
    query: &str,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyDataSet> {
    let opts = merge_ingestion_options(py, options)?;
    ingest_from_db_infer(conn, query, &opts)
        .map(PyDataSet::from_inner)
        .map_err(ingestion_err_to_py)
}

#[pyfunction]
fn sql_query_dataset(ds: &PyDataSet, sql: &str) -> PyResult<PyDataSet> {
    let df = DataFrame::from_dataset(&ds.inner).map_err(ingestion_err_to_py)?;
    sql::query(&df, sql)
        .map_err(ingestion_err_to_py)?
        .collect()
        .map(PyDataSet::from_inner)
        .map_err(ingestion_err_to_py)
}

#[pyfunction]
fn transform_apply_json(ds: &PyDataSet, spec_json: &str) -> PyResult<PyDataSet> {
    let spec: TransformSpec =
        serde_json::from_str(spec_json).map_err(|e| PyValueError::new_err(e.to_string()))?;
    spec.apply(&ds.inner)
        .map(PyDataSet::from_inner)
        .map_err(ingestion_err_to_py)
}

#[pyfunction]
#[pyo3(signature = (ds, options=None))]
fn profile_dataset_json(ds: &PyDataSet, options: Option<&Bound<'_, PyAny>>) -> PyResult<String> {
    let opts = profile_options_from_py(options)?;
    let rep = profile_dataset(&ds.inner, &opts).map_err(ingestion_err_to_py)?;
    render_profile_report_json(&rep).map_err(ingestion_err_to_py)
}

#[pyfunction]
#[pyo3(signature = (ds, options=None))]
fn profile_dataset_markdown(
    ds: &PyDataSet,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let opts = profile_options_from_py(options)?;
    let rep = profile_dataset(&ds.inner, &opts).map_err(ingestion_err_to_py)?;
    Ok(render_profile_report_markdown(&rep))
}

#[pyfunction]
fn validate_dataset_json(ds: &PyDataSet, spec: &Bound<'_, PyAny>) -> PyResult<String> {
    let sp = validation_spec_from_py(spec)?;
    let rep = validate_dataset(&ds.inner, &sp).map_err(ingestion_err_to_py)?;
    render_validation_report_json(&rep).map_err(ingestion_err_to_py)
}

#[pyfunction]
fn validate_dataset_markdown(ds: &PyDataSet, spec: &Bound<'_, PyAny>) -> PyResult<String> {
    let sp = validation_spec_from_py(spec)?;
    let rep = validate_dataset(&ds.inner, &sp).map_err(ingestion_err_to_py)?;
    Ok(render_validation_report_markdown(&rep))
}

#[pyfunction]
#[pyo3(signature = (ds, column, method, options=None))]
fn detect_outliers_json(
    ds: &PyDataSet,
    column: &str,
    method: &Bound<'_, PyAny>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let m = outlier_method_from_py(method)?;
    let opts = outlier_options_from_py(options)?;
    let rep = detect_outliers_dataset(&ds.inner, column, m, &opts).map_err(ingestion_err_to_py)?;
    render_outlier_report_json(&rep).map_err(ingestion_err_to_py)
}

#[pyfunction]
#[pyo3(signature = (ds, column, method, options=None))]
fn detect_outliers_markdown(
    ds: &PyDataSet,
    column: &str,
    method: &Bound<'_, PyAny>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let m = outlier_method_from_py(method)?;
    let opts = outlier_options_from_py(options)?;
    let rep = detect_outliers_dataset(&ds.inner, column, m, &opts).map_err(ingestion_err_to_py)?;
    Ok(render_outlier_report_markdown(&rep))
}

#[pyfunction]
fn processing_reduce(
    py: Python<'_>,
    ds: &PyDataSet,
    column: &str,
    op: &str,
) -> PyResult<Option<PyObject>> {
    let rop = parse_reduce_op(op)?;
    Ok(reduce(&ds.inner, column, rop).map(|v| value_to_py(py, &v)))
}

#[pyfunction]
fn processing_filter(
    py: Python<'_>,
    ds: &PyDataSet,
    predicate: &Bound<'_, PyAny>,
) -> PyResult<PyDataSet> {
    let mut rows = Vec::new();
    for row in &ds.inner.rows {
        let list = PyList::empty(py);
        for v in row {
            list.append(value_to_py(py, v))?;
        }
        let keep: bool = predicate.call1((list,))?.extract()?;
        if keep {
            rows.push(row.clone());
        }
    }
    Ok(PyDataSet::from_inner(DataSet::new(
        ds.inner.schema.clone(),
        rows,
    )))
}

#[pyfunction]
fn processing_map(
    py: Python<'_>,
    ds: &PyDataSet,
    mapper: &Bound<'_, PyAny>,
) -> PyResult<PyDataSet> {
    let mut rows = Vec::with_capacity(ds.inner.row_count());
    for row in &ds.inner.rows {
        let list = PyList::empty(py);
        for v in row {
            list.append(value_to_py(py, v))?;
        }
        let new_list = mapper.call1((list,))?;
        let py_row = new_list
            .downcast::<PyList>()
            .map_err(|_| PyValueError::new_err("mapper must return a list of cell values"))?;
        let n = py_row.len();
        if n != ds.inner.schema.fields.len() {
            return Err(PyValueError::new_err(format!(
                "mapper returned length {n}, expected {} columns",
                ds.inner.schema.fields.len()
            )));
        }
        let mut out_row = Vec::with_capacity(n);
        for i in 0..n {
            let cell = py_row.get_item(i)?;
            out_row.push(value_from_py(&cell)?);
        }
        rows.push(out_row);
    }
    Ok(PyDataSet::from_inner(DataSet::new(
        ds.inner.schema.clone(),
        rows,
    )))
}

#[pyfunction]
#[pyo3(signature = (ds, columns, std_kind=None))]
fn processing_feature_wise_mean_std(
    py: Python<'_>,
    ds: &PyDataSet,
    columns: Vec<String>,
    std_kind: Option<&str>,
) -> PyResult<PyObject> {
    let kind = match std_kind {
        None => VarianceKind::Sample,
        Some(s) => parse_variance_kind(s)?,
    };
    let refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
    let Some(rows) = feature_wise_mean_std(&ds.inner, &refs, kind) else {
        return Err(PyValueError::new_err(
            "feature_wise_mean_std: unknown column or non-numeric type",
        ));
    };
    let out = PyList::empty(py);
    for (name, m) in rows {
        let d = PyDict::new(py);
        d.set_item("column", name)?;
        d.set_item("mean", value_to_py(py, &m.mean))?;
        d.set_item("std_dev", value_to_py(py, &m.std_dev))?;
        out.append(d)?;
    }
    Ok(out.into())
}

#[pyfunction]
fn processing_arg_max_row(
    py: Python<'_>,
    ds: &PyDataSet,
    column: &str,
) -> PyResult<Option<(usize, PyObject)>> {
    match arg_max_row(&ds.inner, column) {
        None => Err(PyValueError::new_err(format!("unknown column '{column}'"))),
        Some(None) => Ok(None),
        Some(Some((i, v))) => Ok(Some((i, value_to_py(py, &v)))),
    }
}

#[pyfunction]
fn processing_arg_min_row(
    py: Python<'_>,
    ds: &PyDataSet,
    column: &str,
) -> PyResult<Option<(usize, PyObject)>> {
    match arg_min_row(&ds.inner, column) {
        None => Err(PyValueError::new_err(format!("unknown column '{column}'"))),
        Some(None) => Ok(None),
        Some(Some((i, v))) => Ok(Some((i, value_to_py(py, &v)))),
    }
}

#[pyfunction]
fn processing_top_k_by_frequency(
    py: Python<'_>,
    ds: &PyDataSet,
    column: &str,
    k: usize,
) -> PyResult<PyObject> {
    let Some(rows) = top_k_by_frequency(&ds.inner, column, k) else {
        return Err(PyValueError::new_err(format!("unknown column '{column}'")));
    };
    let out = PyList::empty(py);
    for (v, c) in rows {
        let pair = PyList::empty(py);
        pair.append(value_to_py(py, &v))?;
        pair.append(c)?;
        out.append(pair)?;
    }
    Ok(out.into())
}

#[pyfunction]
fn extension_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Discover files under a Hive-style `key=value` directory tree (see Rust `ingestion::partition` rustdoc).
///
/// Returns a list of dicts: `{"path": str, "segments": [{"key": str, "value": str}, ...]}`.
#[pyfunction]
#[pyo3(signature = (root, file_pattern=None))]
fn discover_hive_partitioned_files(
    py: Python<'_>,
    root: &str,
    file_pattern: Option<&str>,
) -> PyResult<PyObject> {
    let files = discover_hive_partitioned_files_rs(root, file_pattern).map_err(ingestion_err_to_py)?;
    let list = PyList::empty(py);
    for pf in files {
        let d = PyDict::new(py);
        d.set_item("path", pf.path.to_string_lossy().to_string())?;
        let segs = PyList::empty(py);
        for s in &pf.segments {
            let seg = PyDict::new(py);
            seg.set_item("key", &s.key)?;
            seg.set_item("value", &s.value)?;
            segs.append(seg)?;
        }
        d.set_item("segments", segs)?;
        list.append(d)?;
    }
    Ok(list.into())
}

/// Expand a filesystem glob to existing file paths (sorted).
#[pyfunction]
fn paths_from_glob(py: Python<'_>, pattern: &str) -> PyResult<PyObject> {
    let paths = paths_from_glob_rs(pattern).map_err(ingestion_err_to_py)?;
    let list = PyList::empty(py);
    for p in paths {
        list.append(p.to_string_lossy().to_string())?;
    }
    Ok(list.into())
}

/// Validate paths exist as files; return them in order with duplicates removed (first wins).
#[pyfunction]
fn paths_from_explicit_list(py: Python<'_>, paths: Vec<String>) -> PyResult<PyObject> {
    let pbs: Vec<PathBuf> = paths.into_iter().map(PathBuf::from).collect();
    let out = paths_from_explicit_list_rs(&pbs).map_err(ingestion_err_to_py)?;
    let list = PyList::empty(py);
    for p in out {
        list.append(p.to_string_lossy().to_string())?;
    }
    Ok(list.into())
}

/// Parse a single path component as `key=value`, or return `None` if invalid.
#[pyfunction]
fn parse_partition_segment(py: Python<'_>, component: &str) -> PyResult<PyObject> {
    match parse_partition_segment_rs(component) {
        Some(s) => {
            let d = PyDict::new(py);
            d.set_item("key", s.key)?;
            d.set_item("value", s.value)?;
            Ok(d.into())
        }
        None => Ok(py.None()),
    }
}

#[pymodule]
fn _rust_data_processing(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataSet>()?;
    m.add_class::<PyDataFrame>()?;
    m.add_class::<PySqlContext>()?;
    m.add_class::<PyExecutionEngine>()?;

    m.add_function(wrap_pyfunction!(ingest_from_path_py, m)?)?;
    m.add_function(wrap_pyfunction!(infer_schema_from_path_py, m)?)?;
    m.add_function(wrap_pyfunction!(ingest_from_path_infer_py, m)?)?;
    m.add_function(wrap_pyfunction!(ingest_from_db_py, m)?)?;
    m.add_function(wrap_pyfunction!(ingest_from_db_infer_py, m)?)?;
    m.add_function(wrap_pyfunction!(sql_query_dataset, m)?)?;
    m.add_function(wrap_pyfunction!(transform_apply_json, m)?)?;
    m.add_function(wrap_pyfunction!(profile_dataset_json, m)?)?;
    m.add_function(wrap_pyfunction!(profile_dataset_markdown, m)?)?;
    m.add_function(wrap_pyfunction!(validate_dataset_json, m)?)?;
    m.add_function(wrap_pyfunction!(validate_dataset_markdown, m)?)?;
    m.add_function(wrap_pyfunction!(detect_outliers_json, m)?)?;
    m.add_function(wrap_pyfunction!(detect_outliers_markdown, m)?)?;
    m.add_function(wrap_pyfunction!(processing_reduce, m)?)?;
    m.add_function(wrap_pyfunction!(processing_filter, m)?)?;
    m.add_function(wrap_pyfunction!(processing_map, m)?)?;
    m.add_function(wrap_pyfunction!(processing_feature_wise_mean_std, m)?)?;
    m.add_function(wrap_pyfunction!(processing_arg_max_row, m)?)?;
    m.add_function(wrap_pyfunction!(processing_arg_min_row, m)?)?;
    m.add_function(wrap_pyfunction!(processing_top_k_by_frequency, m)?)?;
    m.add_function(wrap_pyfunction!(extension_version, m)?)?;
    m.add_function(wrap_pyfunction!(discover_hive_partitioned_files, m)?)?;
    m.add_function(wrap_pyfunction!(paths_from_glob, m)?)?;
    m.add_function(wrap_pyfunction!(paths_from_explicit_list, m)?)?;
    m.add_function(wrap_pyfunction!(parse_partition_segment, m)?)?;

    Ok(())
}
