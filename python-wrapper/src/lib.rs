//! PyO3 extension: thin bindings over the `rust-data-processing` crate.
//!
//! Python package layout: native module `rust_data_processing._rust_data_processing`, imported from
//! `rust_data_processing/__init__.py`.

use pyo3::exceptions::{PyIOError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use rust_data_processing::ingestion::{
    ingest_from_path, ingest_from_path_infer, infer_schema_from_path, ExcelSheetSelection,
    IngestionFormat, IngestionOptions,
};
use rust_data_processing::types::{DataSet, DataType, Field, Schema, Value};
use rust_data_processing::IngestionError;

fn ingestion_to_py_err(e: IngestionError) -> PyErr {
    match e {
        IngestionError::Io(err) => PyIOError::new_err(err.to_string()),
        _ => PyValueError::new_err(e.to_string()),
    }
}

fn parse_data_type(s: &str) -> PyResult<DataType> {
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

fn schema_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Schema> {
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

fn parse_format(s: &str) -> PyResult<IngestionFormat> {
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

fn ingestion_options_from_py(obj: Option<&Bound<'_, PyAny>>) -> PyResult<IngestionOptions> {
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

fn value_to_py(py: Python<'_>, v: &Value) -> PyObject {
    match v {
        Value::Null => py.None().into(),
        Value::Int64(i) => (*i).into_py(py),
        Value::Float64(f) => (*f).into_py(py),
        Value::Bool(b) => (*b).into_py(py),
        Value::Utf8(s) => s.clone().into_py(py),
    }
}

fn value_from_py(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Int64(i));
    }
    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Value::Float64(f));
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::Utf8(s));
    }
    Err(PyValueError::new_err(
        "row value must be None, int, float, bool, or str",
    ))
}

fn dataset_from_rows_py(schema: &Bound<'_, PyAny>, rows: &Bound<'_, PyAny>) -> PyResult<DataSet> {
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

/// In-memory tabular dataset (mirrors `rust_data_processing::types::DataSet`).
#[pyclass(name = "DataSet")]
#[derive(Clone)]
pub struct PyDataSet {
    inner: DataSet,
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
        self.inner.schema.fields.iter().map(|f| f.name.clone()).collect()
    }

    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        let list = PyList::empty_bound(py);
        for f in &self.inner.schema.fields {
            let d = PyDict::new_bound(py);
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

    /// Row-major values: `list[list[Optional[scalar]]]` aligned to schema order.
    fn to_rows(&self, py: Python<'_>) -> PyResult<PyObject> {
        let outer = PyList::empty_bound(py);
        for row in &self.inner.rows {
            let inner = PyList::empty_bound(py);
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
    fn from_inner(inner: DataSet) -> Self {
        Self { inner }
    }
}

/// Ingest `path` using an explicit schema. `schema` is `list[dict]` with `name` and `data_type`.
/// `options` is optional `dict` with `format` (`"csv"` / `"json"` / `"parquet"` / `"excel"`) and
/// optional `excel_sheet_selection` (`dict`, see README).
#[pyfunction]
#[pyo3(signature = (path, schema, options=None))]
fn ingest_from_path_py(
    path: &str,
    schema: &Bound<'_, PyAny>,
    options: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyDataSet> {
    let schema = schema_from_py(schema)?;
    let opts = ingestion_options_from_py(options)?;
    ingest_from_path(path, &schema, &opts)
        .map(PyDataSet::from_inner)
        .map_err(ingestion_to_py_err)
}

/// Infer schema from file (extension- or `options.format`-driven).
#[pyfunction]
#[pyo3(signature = (path, options=None))]
fn infer_schema_from_path_py(path: &str, options: Option<&Bound<'_, PyAny>>) -> PyResult<PyObject> {
    let opts = ingestion_options_from_py(options)?;
    let s = infer_schema_from_path(path, &opts).map_err(ingestion_to_py_err)?;
    Python::with_gil(|py| {
        let list = PyList::empty_bound(py);
        for f in s.fields {
            let d = PyDict::new_bound(py);
            d.set_item("name", f.name)?;
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
    })
}

/// Infer schema, then ingest.
#[pyfunction]
#[pyo3(signature = (path, options=None))]
fn ingest_from_path_infer_py(path: &str, options: Option<&Bound<'_, PyAny>>) -> PyResult<PyDataSet> {
    let opts = ingestion_options_from_py(options)?;
    ingest_from_path_infer(path, &opts)
        .map(PyDataSet::from_inner)
        .map_err(ingestion_to_py_err)
}

/// Version of this Python extension crate (kept in sync with the repo release).
#[pyfunction]
fn extension_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn _rust_data_processing(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataSet>()?;
    m.add_function(wrap_pyfunction!(ingest_from_path_py, m)?)?;
    m.add_function(wrap_pyfunction!(infer_schema_from_path_py, m)?)?;
    m.add_function(wrap_pyfunction!(ingest_from_path_infer_py, m)?)?;
    m.add_function(wrap_pyfunction!(extension_version, m)?)?;
    Ok(())
}
