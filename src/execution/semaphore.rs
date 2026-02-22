use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// A small, blocking counting semaphore.
///
/// Used to implement throttling/backpressure for chunked execution.
pub struct Semaphore {
    permits: Mutex<usize>,
    cv: Condvar,
}

impl Semaphore {
    pub fn new(permits: usize) -> Self {
        assert!(permits > 0, "permits must be > 0");
        Self {
            permits: Mutex::new(permits),
            cv: Condvar::new(),
        }
    }

    /// Acquire one permit, blocking until available.
    ///
    /// Returns the time spent waiting (zero if no wait was required).
    pub fn acquire(&self) -> Duration {
        let start = Instant::now();
        let mut waited = false;
        let mut g = self.permits.lock().expect("semaphore mutex poisoned");
        while *g == 0 {
            waited = true;
            g = self.cv.wait(g).expect("semaphore mutex poisoned");
        }
        *g -= 1;
        if waited { start.elapsed() } else { Duration::ZERO }
    }

    /// Release one permit.
    pub fn release(&self) {
        let mut g = self.permits.lock().expect("semaphore mutex poisoned");
        *g += 1;
        self.cv.notify_one();
    }
}

