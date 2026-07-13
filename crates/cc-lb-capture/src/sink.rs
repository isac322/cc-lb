//! Sink for ingesting captured data.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    schema::{CapturedRequestInput, CapturedResponse, Disposition},
    store::CaptureStore,
};

mod writer;

const DEFAULT_MAX_PARTIALS: usize = 4_096;
const DEFAULT_PARTIAL_TTL: Duration = Duration::from_secs(300);
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_BATCH_SIZE: usize = 50;
const DEFAULT_RETENTION_MAX_ROWS: u64 = 100_000;
const OVERFLOW_WARN_INTERVAL_SECS: u64 = 10;

enum CaptureMessage {
    Seed {
        event_id: String,
        request_id: String,
        ts_unix_ms: u64,
    },
    Input(Box<CapturedRequestInput>),
    Response {
        event_id: String,
        response: CapturedResponse,
        disposition: Disposition,
        terminal: bool,
    },
}

/// A non-blocking capture enqueue failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CaptureEnqueueError {
    #[error("capture queue is full")]
    Full,
    #[error("capture queue is closed")]
    Closed,
}

#[derive(Default)]
struct SharedStats {
    dropped: AtomicU64,
    response_without_input: AtomicU64,
    write_failed: AtomicU64,
    partial_ttl_evicted: AtomicU64,
}

/// Cheap-clone producer for lifecycle and routing capture messages.
#[derive(Clone)]
pub struct CaptureSink {
    sender: mpsc::Sender<CaptureMessage>,
    stats: Arc<SharedStats>,
    last_overflow_warn_at: Arc<AtomicU64>,
}

/// Owns graceful shutdown of the single capture writer task.
pub struct CaptureWriterHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl CaptureWriterHandle {
    /// Stops ingestion, drains queued records, and checkpoints the capture WAL.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "capture writer task failed");
        }
    }
}

impl CaptureSink {
    /// Starts a bounded capture queue and its single background writer.
    pub fn new(store: CaptureStore, capacity: usize) -> (Self, CaptureWriterHandle) {
        Self::new_with_config(store, capacity, SinkConfig::default())
    }

    /// Starts a bounded capture queue with a periodic database row cap.
    pub fn new_with_retention(
        store: CaptureStore,
        capacity: usize,
        retention_max_rows: u64,
    ) -> (Self, CaptureWriterHandle) {
        Self::new_with_config(
            store,
            capacity,
            SinkConfig {
                retention_max_rows,
                ..SinkConfig::default()
            },
        )
    }

    fn new_with_config(
        store: CaptureStore,
        capacity: usize,
        config: SinkConfig,
    ) -> (Self, CaptureWriterHandle) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let stats = Arc::new(SharedStats::default());
        let sink = Self {
            sender,
            stats: Arc::clone(&stats),
            last_overflow_warn_at: Arc::new(AtomicU64::new(0)),
        };
        let writer = writer::CaptureWriter::new(store, receiver, stats, config.normalized());
        let join = tokio::spawn(writer.run(shutdown_rx));
        (sink, CaptureWriterHandle { shutdown_tx, join })
    }

    /// Enqueues request identity metadata without waiting for queue capacity.
    pub fn try_seed(
        &self,
        event_id: String,
        request_id: String,
        ts_unix_ms: u64,
    ) -> Result<(), CaptureEnqueueError> {
        self.try_message(CaptureMessage::Seed {
            event_id,
            request_id,
            ts_unix_ms,
        })
    }

    /// Enqueues the required routing input without waiting for queue capacity.
    pub fn try_input(&self, input: CapturedRequestInput) -> Result<(), CaptureEnqueueError> {
        self.try_message(CaptureMessage::Input(Box::new(input)))
    }

    /// Enqueues response state without waiting for queue capacity.
    pub fn try_response(
        &self,
        event_id: String,
        response: CapturedResponse,
        disposition: Disposition,
        terminal: bool,
    ) -> Result<(), CaptureEnqueueError> {
        self.try_message(CaptureMessage::Response {
            event_id,
            response,
            disposition,
            terminal,
        })
    }

    /// Returns queue-overflow drops observed by this sink and its clones.
    pub fn dropped_total(&self) -> u64 {
        self.stats.dropped.load(Ordering::Relaxed)
    }

    /// Returns terminal responses discarded because their required input was absent.
    pub fn response_without_input_total(&self) -> u64 {
        self.stats.response_without_input.load(Ordering::Relaxed)
    }

    /// Returns records lost to isolated capture database write failures.
    pub fn write_failed_total(&self) -> u64 {
        self.stats.write_failed.load(Ordering::Relaxed)
    }

    /// Returns incomplete partials removed by the writer's TTL sweep.
    pub fn partial_ttl_evicted_total(&self) -> u64 {
        self.stats.partial_ttl_evicted.load(Ordering::Relaxed)
    }

    fn try_message(&self, message: CaptureMessage) -> Result<(), CaptureEnqueueError> {
        match self.sender.try_send(message) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(CaptureEnqueueError::Closed),
            Err(mpsc::error::TrySendError::Full(_)) => {
                let dropped_total = self.stats.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                metrics::counter!("cc_lb_capture_dropped_total").increment(1);
                self.warn_overflow(dropped_total);
                Err(CaptureEnqueueError::Full)
            }
        }
    }

    fn warn_overflow(&self, dropped_total: u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let should_warn = self
            .last_overflow_warn_at
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
                (now.saturating_sub(last) >= OVERFLOW_WARN_INTERVAL_SECS).then_some(now)
            })
            .is_ok();
        if should_warn {
            tracing::warn!(dropped_total, "capture queue full; dropping message");
        }
    }
}

#[derive(Clone, Copy)]
struct SinkConfig {
    max_partials: usize,
    partial_ttl: Duration,
    sweep_interval: Duration,
    flush_interval: Duration,
    batch_size: usize,
    retention_max_rows: u64,
}

impl SinkConfig {
    fn normalized(self) -> Self {
        let minimum_interval = Duration::from_millis(1);
        Self {
            max_partials: self.max_partials.max(1),
            partial_ttl: self.partial_ttl,
            sweep_interval: self.sweep_interval.max(minimum_interval),
            flush_interval: self.flush_interval.max(minimum_interval),
            batch_size: self.batch_size.max(1),
            retention_max_rows: self.retention_max_rows,
        }
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            max_partials: DEFAULT_MAX_PARTIALS,
            partial_ttl: DEFAULT_PARTIAL_TTL,
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            batch_size: DEFAULT_BATCH_SIZE,
            retention_max_rows: DEFAULT_RETENTION_MAX_ROWS,
        }
    }
}

#[cfg(test)]
mod tests;
