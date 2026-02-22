//! Execution engine for running processing pipelines with configurable parallelism.
//!
//! This module sits "above" [`crate::processing`] and provides:
//!
//! - Parallel (chunked) execution for filter/map
//! - Resource limits / throttling (e.g., in-flight chunks)
//! - Real-time metrics + observer hooks for monitoring

mod observer;
mod semaphore;

use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use rayon::ThreadPool;
use rayon::ThreadPoolBuilder;

use crate::processing::{reduce, ReduceOp};
use crate::types::{DataSet, Value};

pub use observer::{
    ExecutionEvent, ExecutionMetrics, ExecutionMetricsSnapshot, ExecutionObserver, StdErrExecutionObserver,
};

use semaphore::Semaphore;

/// Configuration for the [`ExecutionEngine`].
#[derive(Debug, Clone)]
pub struct ExecutionOptions {
    /// Number of worker threads used by the engine.
    ///
    /// If `None`, uses the platform's available parallelism.
    pub num_threads: Option<usize>,
    /// Number of rows per chunk.
    ///
    /// Chunking lets the engine bound working-set size and implement throttling.
    pub chunk_size: usize,
    /// Upper bound on concurrently executing chunks.
    ///
    /// This is an additional throttle on top of `num_threads`.
    pub max_in_flight_chunks: usize,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        Self {
            num_threads: Some(n),
            chunk_size: 4_096,
            max_in_flight_chunks: n.max(1),
        }
    }
}

/// A configurable execution engine for in-memory [`DataSet`] pipelines.
pub struct ExecutionEngine {
    pool: ThreadPool,
    opts: ExecutionOptions,
    observer: Option<Arc<dyn ExecutionObserver>>,
    metrics: Arc<ExecutionMetrics>,
}

impl ExecutionEngine {
    /// Create a new engine with the given options.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size == 0`, `max_in_flight_chunks == 0`, or `num_threads == Some(0)`.
    pub fn new(opts: ExecutionOptions) -> Self {
        assert!(opts.chunk_size > 0, "chunk_size must be > 0");
        assert!(
            opts.max_in_flight_chunks > 0,
            "max_in_flight_chunks must be > 0"
        );
        if let Some(n) = opts.num_threads {
            assert!(n > 0, "num_threads must be > 0 when set");
        }

        let n_threads = opts
            .num_threads
            .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1))
            .max(1);

        let pool = ThreadPoolBuilder::new()
            .num_threads(n_threads)
            .build()
            .expect("failed to build rayon thread pool");

        Self {
            pool,
            opts: opts.clone(),
            observer: None,
            metrics: Arc::new(ExecutionMetrics::new()),
        }
    }

    /// Attach an observer for execution events (metrics/logging).
    pub fn with_observer(mut self, observer: Arc<dyn ExecutionObserver>) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Get a handle to real-time execution metrics.
    pub fn metrics(&self) -> Arc<ExecutionMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Execute a parallel filter over the dataset.
    pub fn filter_parallel<F>(&self, dataset: &DataSet, predicate: F) -> DataSet
    where
        F: Fn(&[Value]) -> bool + Send + Sync,
    {
        self.pool.install(|| self.filter_parallel_impl(dataset, &predicate))
    }

    fn filter_parallel_impl(&self, dataset: &DataSet, predicate: &(dyn Fn(&[Value]) -> bool + Send + Sync)) -> DataSet {
        let start = Instant::now();
        self.metrics.begin_run();
        self.emit(ExecutionEvent::RunStarted);

        let sem = Semaphore::new(self.opts.max_in_flight_chunks);
        let chunk_ranges = chunk_ranges(dataset.row_count(), self.opts.chunk_size);

        let per_chunk: Vec<Vec<Vec<Value>>> = chunk_ranges
            .into_par_iter()
            .map(|range| {
                let waited = sem.acquire();
                if waited > Duration::ZERO {
                    self.metrics.on_throttle_wait(waited);
                    self.emit(ExecutionEvent::ThrottleWaited { duration: waited });
                }

                self.metrics.on_chunk_start();
                self.emit(ExecutionEvent::ChunkStarted {
                    start_row: range.start,
                    row_count: range.end - range.start,
                });

                let mut out = Vec::new();
                for row in &dataset.rows[range] {
                    self.metrics.on_row_processed();
                    if predicate(row.as_slice()) {
                        out.push(row.clone());
                    }
                }

                self.emit(ExecutionEvent::ChunkFinished {
                    output_rows: out.len(),
                });
                self.metrics.on_chunk_end();
                sem.release();
                out
            })
            .collect();

        let rows = per_chunk.into_iter().flatten().collect::<Vec<_>>();
        let out = DataSet::new(dataset.schema.clone(), rows);

        self.metrics.end_run(start.elapsed());
        self.emit(ExecutionEvent::RunFinished {
            elapsed: start.elapsed(),
            metrics: self.metrics.snapshot(),
        });

        out
    }

    /// Execute a parallel map over the dataset.
    ///
    /// # Panics
    ///
    /// Panics if `mapper` returns rows with a different length than the schema field count.
    pub fn map_parallel<F>(&self, dataset: &DataSet, mapper: F) -> DataSet
    where
        F: Fn(&[Value]) -> Vec<Value> + Send + Sync,
    {
        self.pool.install(|| self.map_parallel_impl(dataset, &mapper))
    }

    fn map_parallel_impl(&self, dataset: &DataSet, mapper: &(dyn Fn(&[Value]) -> Vec<Value> + Send + Sync)) -> DataSet {
        let start = Instant::now();
        self.metrics.begin_run();
        self.emit(ExecutionEvent::RunStarted);

        let expected_len = dataset.schema.fields.len();
        let sem = Semaphore::new(self.opts.max_in_flight_chunks);
        let chunk_ranges = chunk_ranges(dataset.row_count(), self.opts.chunk_size);

        let per_chunk: Vec<Vec<Vec<Value>>> = chunk_ranges
            .into_par_iter()
            .map(|range| {
                let waited = sem.acquire();
                if waited > Duration::ZERO {
                    self.metrics.on_throttle_wait(waited);
                    self.emit(ExecutionEvent::ThrottleWaited { duration: waited });
                }

                self.metrics.on_chunk_start();
                self.emit(ExecutionEvent::ChunkStarted {
                    start_row: range.start,
                    row_count: range.end - range.start,
                });

                let mut out = Vec::with_capacity(range.end - range.start);
                for row in &dataset.rows[range] {
                    self.metrics.on_row_processed();
                    let mapped = mapper(row.as_slice());
                    assert!(
                        mapped.len() == expected_len,
                        "mapped row length {} does not match schema length {}",
                        mapped.len(),
                        expected_len
                    );
                    out.push(mapped);
                }

                self.emit(ExecutionEvent::ChunkFinished {
                    output_rows: out.len(),
                });
                self.metrics.on_chunk_end();
                sem.release();
                out
            })
            .collect();

        let rows = per_chunk.into_iter().flatten().collect::<Vec<_>>();
        let out = DataSet::new(dataset.schema.clone(), rows);

        self.metrics.end_run(start.elapsed());
        self.emit(ExecutionEvent::RunFinished {
            elapsed: start.elapsed(),
            metrics: self.metrics.snapshot(),
        });

        out
    }

    /// Reduce a column using the existing built-in reduce operation.
    ///
    /// This is currently sequential, but is tracked via the observer/metrics hooks.
    pub fn reduce(&self, dataset: &DataSet, column: &str, op: ReduceOp) -> Option<Value> {
        let start = Instant::now();
        self.metrics.begin_run();
        self.emit(ExecutionEvent::RunStarted);
        self.emit(ExecutionEvent::ReduceStarted {
            column: column.to_string(),
            op,
        });

        let out = reduce(dataset, column, op);

        self.emit(ExecutionEvent::ReduceFinished { result: out.clone() });
        self.metrics.end_run(start.elapsed());
        self.emit(ExecutionEvent::RunFinished {
            elapsed: start.elapsed(),
            metrics: self.metrics.snapshot(),
        });
        out
    }

    fn emit(&self, event: ExecutionEvent) {
        if let Some(obs) = &self.observer {
            obs.on_event(&event);
        }
    }
}

fn chunk_ranges(row_count: usize, chunk_size: usize) -> Vec<std::ops::Range<usize>> {
    if row_count == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity((row_count + chunk_size - 1) / chunk_size);
    let mut start = 0usize;
    while start < row_count {
        let end = (start + chunk_size).min(row_count);
        out.push(start..end);
        start = end;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{ExecutionEngine, ExecutionOptions};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::execution::{ExecutionEvent, ExecutionObserver};
    use crate::types::{DataSet, DataType, Field, Schema, Value};

    fn dataset_of_n(n: usize) -> DataSet {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64)]);
        let mut rows = Vec::with_capacity(n);
        for i in 0..n as i64 {
            rows.push(vec![Value::Int64(i)]);
        }
        DataSet::new(schema, rows)
    }

    #[test]
    fn map_parallel_runs_with_concurrency() {
        let ds = dataset_of_n(400);
        let engine = ExecutionEngine::new(ExecutionOptions {
            num_threads: Some(4),
            chunk_size: 1,
            max_in_flight_chunks: 4,
        });

        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let active2 = Arc::clone(&active);
        let max_active2 = Arc::clone(&max_active);

        let out = engine.map_parallel(&ds, move |row| {
            let now = active2.fetch_add(1, Ordering::SeqCst) + 1;
            // max = max(max, now)
            loop {
                let cur = max_active2.load(Ordering::SeqCst);
                if now <= cur {
                    break;
                }
                if max_active2
                    .compare_exchange(cur, now, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }

            std::thread::sleep(Duration::from_millis(2));
            let _ = active2.fetch_sub(1, Ordering::SeqCst);

            let v = match row[0] {
                Value::Int64(x) => x + 1,
                _ => 0,
            };
            vec![Value::Int64(v)]
        });

        assert_eq!(out.row_count(), ds.row_count());
        assert!(max_active.load(Ordering::SeqCst) > 1);
    }

    struct ConcurrencyObserver {
        active_chunks: AtomicUsize,
        max_active_chunks: AtomicUsize,
    }

    impl ConcurrencyObserver {
        fn new() -> Self {
            Self {
                active_chunks: AtomicUsize::new(0),
                max_active_chunks: AtomicUsize::new(0),
            }
        }
        fn max(&self) -> usize {
            self.max_active_chunks.load(Ordering::SeqCst)
        }
        fn bump_max(&self, now: usize) {
            loop {
                let cur = self.max_active_chunks.load(Ordering::SeqCst);
                if now <= cur {
                    break;
                }
                if self.max_active_chunks
                    .compare_exchange(cur, now, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    impl ExecutionObserver for ConcurrencyObserver {
        fn on_event(&self, event: &ExecutionEvent) {
            match event {
                ExecutionEvent::ChunkStarted { .. } => {
                    let now = self.active_chunks.fetch_add(1, Ordering::SeqCst) + 1;
                    self.bump_max(now);
                }
                ExecutionEvent::ChunkFinished { .. } => {
                    let _ = self.active_chunks.fetch_sub(1, Ordering::SeqCst);
                }
                _ => {}
            }
        }
    }

    #[test]
    fn max_in_flight_chunks_throttles_chunk_concurrency() {
        let ds = dataset_of_n(100);
        let observer = Arc::new(ConcurrencyObserver::new());
        let obs_trait: Arc<dyn ExecutionObserver> = observer.clone();
        let engine = ExecutionEngine::new(ExecutionOptions {
            num_threads: Some(4),
            chunk_size: 1,
            max_in_flight_chunks: 1,
        })
        .with_observer(obs_trait);

        let out = engine.map_parallel(&ds, |_row| {
            // Make each chunk/row take long enough to overlap if not throttled.
            std::thread::sleep(Duration::from_millis(1));
            vec![Value::Int64(1)]
        });

        assert_eq!(out.row_count(), ds.row_count());
        assert_eq!(observer.max(), 1);
    }

    #[test]
    fn metrics_are_available_after_run() {
        let ds = dataset_of_n(60);
        let engine = ExecutionEngine::new(ExecutionOptions {
            num_threads: Some(4),
            chunk_size: 1,
            max_in_flight_chunks: 1,
        });
        let metrics = engine.metrics();

        let out = engine.map_parallel(&ds, |_row| {
            std::thread::sleep(Duration::from_millis(2));
            vec![Value::Int64(1)]
        });

        assert_eq!(out.row_count(), ds.row_count());

        let snap = metrics.snapshot();
        assert_eq!(snap.rows_processed, ds.row_count() as u64);
        assert_eq!(snap.chunks_started, ds.row_count() as u64);
        assert_eq!(snap.chunks_finished, ds.row_count() as u64);
        assert_eq!(snap.max_active_chunks, 1);
        assert!(snap.throttle_wait > Duration::ZERO);
        assert!(snap.elapsed.is_some());
    }
}

