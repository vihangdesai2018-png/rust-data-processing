use std::fmt;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::IngestionError;

use super::unified::IngestionFormat;

/// Severity classification used for observer callbacks and alerting thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IngestionSeverity {
    /// Informational event.
    Info,
    /// Warning-level event (non-fatal).
    Warning,
    /// Error-level event (operation failed).
    Error,
    /// Critical error (typically I/O or other infrastructure failures).
    Critical,
}

/// Context about an ingestion attempt.
#[derive(Debug, Clone)]
pub struct IngestionContext {
    /// The input path used for ingestion.
    pub path: PathBuf,
    /// Format used for ingestion.
    pub format: IngestionFormat,
}

/// Minimal stats reported on successful ingestion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestionStats {
    /// Number of ingested rows.
    pub rows: usize,
}

/// Observer interface for ingestion outcomes.
///
/// Implementors can record metrics, logs, or trigger alerts.
pub trait IngestionObserver: Send + Sync {
    /// Called when ingestion succeeds.
    fn on_success(&self, _ctx: &IngestionContext, _stats: IngestionStats) {}

    /// Called when ingestion fails.
    fn on_failure(&self, _ctx: &IngestionContext, _severity: IngestionSeverity, _error: &IngestionError) {}

    /// Called when an ingestion failure meets an alert threshold.
    ///
    /// Default behavior forwards to [`Self::on_failure`].
    fn on_alert(&self, ctx: &IngestionContext, severity: IngestionSeverity, error: &IngestionError) {
        self.on_failure(ctx, severity, error)
    }
}

/// An observer that fans out callbacks to a list of observers.
#[derive(Default)]
pub struct CompositeObserver {
    observers: Vec<Arc<dyn IngestionObserver>>,
}

impl CompositeObserver {
    /// Create a new composite observer from a list of observers.
    pub fn new(observers: Vec<Arc<dyn IngestionObserver>>) -> Self {
        Self { observers }
    }
}

impl fmt::Debug for CompositeObserver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompositeObserver")
            .field("observers_len", &self.observers.len())
            .finish()
    }
}

impl IngestionObserver for CompositeObserver {
    fn on_success(&self, ctx: &IngestionContext, stats: IngestionStats) {
        for o in &self.observers {
            o.on_success(ctx, stats);
        }
    }

    fn on_failure(&self, ctx: &IngestionContext, severity: IngestionSeverity, error: &IngestionError) {
        for o in &self.observers {
            o.on_failure(ctx, severity, error);
        }
    }

    fn on_alert(&self, ctx: &IngestionContext, severity: IngestionSeverity, error: &IngestionError) {
        for o in &self.observers {
            o.on_alert(ctx, severity, error);
        }
    }
}

/// Logs ingestion events to stderr.
#[derive(Debug, Default)]
pub struct StdErrObserver;

impl IngestionObserver for StdErrObserver {
    fn on_success(&self, ctx: &IngestionContext, stats: IngestionStats) {
        eprintln!(
            "[ingest][ok] format={:?} path={} rows={}",
            ctx.format,
            ctx.path.display(),
            stats.rows
        );
    }

    fn on_failure(&self, ctx: &IngestionContext, severity: IngestionSeverity, error: &IngestionError) {
        eprintln!(
            "[ingest][{:?}] format={:?} path={} err={}",
            severity,
            ctx.format,
            ctx.path.display(),
            error
        );
    }

    fn on_alert(&self, ctx: &IngestionContext, severity: IngestionSeverity, error: &IngestionError) {
        eprintln!(
            "[ALERT][ingest][{:?}] format={:?} path={} err={}",
            severity,
            ctx.format,
            ctx.path.display(),
            error
        );
    }
}

/// Appends ingestion events to a local log file.
#[derive(Debug)]
pub struct FileObserver {
    path: PathBuf,
    lock: Mutex<()>,
}

impl FileObserver {
    /// Create a file observer that appends events to `path`.
    ///
    /// Writes are best-effort; failures to open/write the log file are ignored.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            lock: Mutex::new(()),
        }
    }

    fn append_line(&self, line: &str) {
        let _guard = self.lock.lock().ok();
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&self.path) {
            let _ = writeln!(f, "{line}");
        }
    }
}

impl IngestionObserver for FileObserver {
    fn on_success(&self, ctx: &IngestionContext, stats: IngestionStats) {
        self.append_line(&format!(
            "{} ok format={:?} path={} rows={}",
            unix_ts(),
            ctx.format,
            ctx.path.display(),
            stats.rows
        ));
    }

    fn on_failure(&self, ctx: &IngestionContext, severity: IngestionSeverity, error: &IngestionError) {
        self.append_line(&format!(
            "{} fail severity={:?} format={:?} path={} err={}",
            unix_ts(),
            severity,
            ctx.format,
            ctx.path.display(),
            error
        ));
    }

    fn on_alert(&self, ctx: &IngestionContext, severity: IngestionSeverity, error: &IngestionError) {
        self.append_line(&format!(
            "{} ALERT severity={:?} format={:?} path={} err={}",
            unix_ts(),
            severity,
            ctx.format,
            ctx.path.display(),
            error
        ));
    }
}

fn unix_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

