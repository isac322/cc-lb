use std::sync::Arc;

use cc_lb_contract::{EngineMetricsHook, NoopMetricsHook};
use cc_lb_storage_api::{Storage, UpstreamRateLimitObservationRecord};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::task::JoinHandle;

pub const DEFAULT_UPSTREAM_RATE_LIMIT_CHANNEL_CAPACITY: usize = 4096;

#[derive(Clone)]
pub struct UpstreamRateLimitSink {
    tx: Arc<Sender<UpstreamRateLimitObservationRecord>>,
    metrics: Arc<dyn EngineMetricsHook>,
}

#[derive(Debug, Error)]
pub enum UpstreamRateLimitEnqueueError {
    #[error("upstream rate limit observation queue is full")]
    Full,
    #[error("upstream rate limit observation queue is closed")]
    Closed,
}

impl UpstreamRateLimitSink {
    pub fn new() -> (Self, Receiver<UpstreamRateLimitObservationRecord>) {
        Self::with_metrics(Arc::new(NoopMetricsHook))
    }

    pub fn with_metrics(
        metrics: Arc<dyn EngineMetricsHook>,
    ) -> (Self, Receiver<UpstreamRateLimitObservationRecord>) {
        Self::with_capacity_and_metrics(DEFAULT_UPSTREAM_RATE_LIMIT_CHANNEL_CAPACITY, metrics)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Receiver<UpstreamRateLimitObservationRecord>) {
        Self::with_capacity_and_metrics(capacity, Arc::new(NoopMetricsHook))
    }

    pub fn with_capacity_and_metrics(
        capacity: usize,
        metrics: Arc<dyn EngineMetricsHook>,
    ) -> (Self, Receiver<UpstreamRateLimitObservationRecord>) {
        let bounded_capacity = capacity.max(1);
        let (tx, rx) = mpsc::channel(bounded_capacity);
        (
            Self {
                tx: Arc::new(tx),
                metrics,
            },
            rx,
        )
    }

    pub fn enqueue(
        &self,
        record: UpstreamRateLimitObservationRecord,
    ) -> Result<(), UpstreamRateLimitEnqueueError> {
        match self.tx.try_send(record) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.metrics.record_dropped_events_by("full", 1);
                Err(UpstreamRateLimitEnqueueError::Full)
            }
            Err(TrySendError::Closed(_)) => {
                self.metrics.record_dropped_events_by("closed", 1);
                Err(UpstreamRateLimitEnqueueError::Closed)
            }
        }
    }
}

pub fn start_upstream_rate_limit_writer(
    storage: Arc<dyn Storage>,
    mut receiver: Receiver<UpstreamRateLimitObservationRecord>,
    metrics: Arc<dyn EngineMetricsHook>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(record) = receiver.recv().await {
            match storage.put_observation(&record).await {
                Ok(()) => {}
                Err(source) => {
                    metrics.record_dropped_events_by("worker_drop", 1);
                    tracing::warn!(error = %source, "upstream rate limit observation persistence failed");
                }
            }
        }
    })
}
