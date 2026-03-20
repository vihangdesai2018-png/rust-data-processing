//! Python callbacks for Rust observer traits (ingestion + execution).

use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::sync::Arc;

use rust_data_processing::IngestionError;
use rust_data_processing::execution::{ExecutionEvent, ExecutionObserver};
use rust_data_processing::ingestion::{
    IngestionContext, IngestionObserver, IngestionOptions, IngestionSeverity, IngestionStats,
};
use rust_data_processing::processing::ReduceOp;

use crate::convert::{metrics_snapshot_to_py, parse_ingestion_severity, value_to_py};

fn ingestion_format_label(f: rust_data_processing::ingestion::IngestionFormat) -> &'static str {
    use rust_data_processing::ingestion::IngestionFormat as F;
    match f {
        F::Csv => "csv",
        F::Json => "json",
        F::Parquet => "parquet",
        F::Excel => "excel",
    }
}

fn severity_str(s: IngestionSeverity) -> &'static str {
    match s {
        IngestionSeverity::Info => "info",
        IngestionSeverity::Warning => "warning",
        IngestionSeverity::Error => "error",
        IngestionSeverity::Critical => "critical",
    }
}

fn ctx_to_pydict(py: Python<'_>, ctx: &IngestionContext) -> PyObject {
    let d = PyDict::new(py);
    let _ = d.set_item("path", ctx.path.to_string_lossy().to_string());
    let _ = d.set_item("format", ingestion_format_label(ctx.format));
    d.into()
}

/// Bridges optional Python callables to [`IngestionObserver`].
pub struct PyIngestionObserver {
    pub on_success: Option<Py<PyAny>>,
    pub on_failure: Option<Py<PyAny>>,
    pub on_alert: Option<Py<PyAny>>,
}

impl PyIngestionObserver {
    pub fn from_pydict(_py: Python<'_>, d: &Bound<'_, PyDict>) -> PyResult<Self> {
        let take_cb = |key: &str| -> PyResult<Option<Py<PyAny>>> {
            match d.get_item(key)? {
                None => Ok(None),
                Some(x) if x.is_none() => Ok(None),
                Some(x) => Ok(Some(x.unbind())),
            }
        };
        Ok(Self {
            on_success: take_cb("on_success")?,
            on_failure: take_cb("on_failure")?,
            on_alert: take_cb("on_alert")?,
        })
    }
}

impl IngestionObserver for PyIngestionObserver {
    fn on_success(&self, ctx: &IngestionContext, stats: IngestionStats) {
        let Some(ref cb) = self.on_success else {
            return;
        };
        Python::with_gil(|py| {
            let ctx_d = ctx_to_pydict(py, ctx);
            let st = PyDict::new(py);
            let _ = st.set_item("rows", stats.rows);
            let _ = cb.bind(py).call1((ctx_d, st));
        });
    }

    fn on_failure(
        &self,
        ctx: &IngestionContext,
        severity: IngestionSeverity,
        error: &IngestionError,
    ) {
        let Some(ref cb) = self.on_failure else {
            return;
        };
        Python::with_gil(|py| {
            let ctx_d = ctx_to_pydict(py, ctx);
            let _ = cb
                .bind(py)
                .call1((ctx_d, severity_str(severity), error.to_string()));
        });
    }

    fn on_alert(
        &self,
        ctx: &IngestionContext,
        severity: IngestionSeverity,
        error: &IngestionError,
    ) {
        Python::with_gil(|py| {
            let ctx_d = ctx_to_pydict(py, ctx);
            let args = (ctx_d, severity_str(severity), error.to_string());
            if let Some(ref cb) = self.on_alert {
                let _ = cb.bind(py).call1(args);
                return;
            }
            if let Some(ref cb) = self.on_failure {
                let _ = cb.bind(py).call1(args);
            }
        });
    }
}

fn reduce_op_label(op: ReduceOp) -> String {
    format!("{op:?}")
}

/// Merge `alert_at_or_above` and `observer` from the options dict into `o`.
pub fn apply_ingestion_observer_options(
    py: Python<'_>,
    d: &Bound<'_, PyDict>,
    o: &mut IngestionOptions,
) -> PyResult<()> {
    if let Some(v) = d.get_item("alert_at_or_above")? {
        let s: String = v.extract()?;
        o.alert_at_or_above = parse_ingestion_severity(&s)?;
    }
    if let Some(v) = d.get_item("observer")? {
        if !v.is_none() {
            let od = v.downcast::<PyDict>()?;
            o.observer = Some(Arc::new(PyIngestionObserver::from_pydict(py, od)?));
        }
    }
    Ok(())
}

/// Serialize [`ExecutionEvent`] into a plain dict for Python (`on_execution_event`).
pub fn execution_event_to_pydict(py: Python<'_>, event: &ExecutionEvent) -> PyResult<PyObject> {
    let d = PyDict::new(py);
    match event {
        ExecutionEvent::RunStarted => {
            d.set_item("kind", "run_started")?;
        }
        ExecutionEvent::ThrottleWaited { duration } => {
            d.set_item("kind", "throttle_waited")?;
            d.set_item("duration_seconds", duration.as_secs_f64())?;
        }
        ExecutionEvent::ChunkStarted {
            start_row,
            row_count,
        } => {
            d.set_item("kind", "chunk_started")?;
            d.set_item("start_row", start_row)?;
            d.set_item("row_count", row_count)?;
        }
        ExecutionEvent::ChunkFinished { output_rows } => {
            d.set_item("kind", "chunk_finished")?;
            d.set_item("output_rows", output_rows)?;
        }
        ExecutionEvent::ReduceStarted { column, op } => {
            d.set_item("kind", "reduce_started")?;
            d.set_item("column", column)?;
            d.set_item("op", reduce_op_label(*op))?;
        }
        ExecutionEvent::ReduceFinished { result } => {
            d.set_item("kind", "reduce_finished")?;
            match result {
                None => d.set_item("result", py.None())?,
                Some(v) => d.set_item("result", value_to_py(py, v))?,
            }
        }
        ExecutionEvent::RunFinished { elapsed, metrics } => {
            d.set_item("kind", "run_finished")?;
            d.set_item("elapsed_seconds", elapsed.as_secs_f64())?;
            d.set_item("metrics", metrics_snapshot_to_py(py, metrics)?)?;
        }
    }
    Ok(d.into())
}

/// Forwards engine events to a Python callable `(event_dict) -> None`.
pub struct PyExecutionObserver {
    pub callback: Py<PyAny>,
}

impl ExecutionObserver for PyExecutionObserver {
    fn on_event(&self, event: &ExecutionEvent) {
        Python::with_gil(|py| {
            if let Ok(d) = execution_event_to_pydict(py, event) {
                let _ = self.callback.bind(py).call1((d,));
            }
        });
    }
}
