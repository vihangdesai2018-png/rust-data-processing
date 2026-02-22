use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::processing::ReduceOp;
use crate::types::Value;

/// Execution events emitted by the engine.
#[derive(Debug, Clone)]
pub enum ExecutionEvent {
    RunStarted,
    ThrottleWaited { duration: Duration },
    ChunkStarted { start_row: usize, row_count: usize },
    ChunkFinished { output_rows: usize },
    ReduceStarted { column: String, op: ReduceOp },
    ReduceFinished { result: Option<Value> },
    RunFinished {
        elapsed: Duration,
        metrics: ExecutionMetricsSnapshot,
    },
}

/// Observer hook for execution events.
pub trait ExecutionObserver: Send + Sync {
    fn on_event(&self, event: &ExecutionEvent);
}

/// A simple stderr logger for execution events.
#[derive(Default)]
pub struct StdErrExecutionObserver;

impl ExecutionObserver for StdErrExecutionObserver {
    fn on_event(&self, event: &ExecutionEvent) {
        eprintln!("{event:?}");
    }
}

/// Real-time metrics for an execution run.
///
/// The engine updates these counters during execution; callers can snapshot them at any time.
pub struct ExecutionMetrics {
    run_id: AtomicU64,
    started_at: Mutex<Option<Instant>>,
    elapsed_ns: AtomicU64,

    rows_processed: AtomicU64,
    chunks_started: AtomicU64,
    chunks_finished: AtomicU64,
    throttle_wait_ns: AtomicU64,

    active_chunks: AtomicUsize,
    max_active_chunks: AtomicUsize,
}

impl ExecutionMetrics {
    pub fn new() -> Self {
        Self {
            run_id: AtomicU64::new(0),
            started_at: Mutex::new(None),
            elapsed_ns: AtomicU64::new(0),
            rows_processed: AtomicU64::new(0),
            chunks_started: AtomicU64::new(0),
            chunks_finished: AtomicU64::new(0),
            throttle_wait_ns: AtomicU64::new(0),
            active_chunks: AtomicUsize::new(0),
            max_active_chunks: AtomicUsize::new(0),
        }
    }

    pub fn begin_run(&self) {
        let _ = self.run_id.fetch_add(1, Ordering::SeqCst) + 1;
        *self.started_at.lock().expect("metrics mutex poisoned") = Some(Instant::now());

        self.elapsed_ns.store(0, Ordering::SeqCst);
        self.rows_processed.store(0, Ordering::SeqCst);
        self.chunks_started.store(0, Ordering::SeqCst);
        self.chunks_finished.store(0, Ordering::SeqCst);
        self.throttle_wait_ns.store(0, Ordering::SeqCst);
        self.active_chunks.store(0, Ordering::SeqCst);
        self.max_active_chunks.store(0, Ordering::SeqCst);
    }

    pub fn end_run(&self, elapsed: Duration) {
        self.elapsed_ns
            .store(elapsed.as_nanos().min(u64::MAX as u128) as u64, Ordering::SeqCst);
    }

    pub fn on_row_processed(&self) {
        let _ = self.rows_processed.fetch_add(1, Ordering::SeqCst);
    }

    pub fn on_chunk_start(&self) {
        let _ = self.chunks_started.fetch_add(1, Ordering::SeqCst);
        let now = self.active_chunks.fetch_add(1, Ordering::SeqCst) + 1;
        update_max_usize(&self.max_active_chunks, now);
    }

    pub fn on_chunk_end(&self) {
        let _ = self.chunks_finished.fetch_add(1, Ordering::SeqCst);
        let _ = self.active_chunks.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn on_throttle_wait(&self, d: Duration) {
        let add = d.as_nanos().min(u64::MAX as u128) as u64;
        let _ = self.throttle_wait_ns.fetch_add(add, Ordering::SeqCst);
    }

    pub fn snapshot(&self) -> ExecutionMetricsSnapshot {
        let run_id = self.run_id.load(Ordering::SeqCst);
        let elapsed_ns = self.elapsed_ns.load(Ordering::SeqCst);
        let elapsed = if elapsed_ns > 0 {
            Some(Duration::from_nanos(elapsed_ns))
        } else {
            None
        };

        ExecutionMetricsSnapshot {
            run_id,
            elapsed,
            rows_processed: self.rows_processed.load(Ordering::SeqCst),
            chunks_started: self.chunks_started.load(Ordering::SeqCst),
            chunks_finished: self.chunks_finished.load(Ordering::SeqCst),
            throttle_wait: Duration::from_nanos(self.throttle_wait_ns.load(Ordering::SeqCst)),
            max_active_chunks: self.max_active_chunks.load(Ordering::SeqCst),
        }
    }
}

impl Default for ExecutionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

fn update_max_usize(dst: &AtomicUsize, now: usize) {
    loop {
        let cur = dst.load(Ordering::SeqCst);
        if now <= cur {
            break;
        }
        if dst.compare_exchange(cur, now, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            break;
        }
    }
}

/// Immutable snapshot of [`ExecutionMetrics`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionMetricsSnapshot {
    pub run_id: u64,
    pub elapsed: Option<Duration>,
    pub rows_processed: u64,
    pub chunks_started: u64,
    pub chunks_finished: u64,
    pub throttle_wait: Duration,
    pub max_active_chunks: usize,
}

impl fmt::Display for ExecutionMetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "run_id={}, rows_processed={}, chunks={}/{}, max_active_chunks={}, throttle_wait={:?}, elapsed={:?}",
            self.run_id,
            self.rows_processed,
            self.chunks_finished,
            self.chunks_started,
            self.max_active_chunks,
            self.throttle_wait,
            self.elapsed
        )
    }
}

