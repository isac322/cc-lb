use std::sync::Arc;

use cc_lb_observability::{EngineMetricsHook, NoopMetricsHook};
use cc_lb_storage_api::{UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore};
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
    storage: Arc<dyn UpstreamRateLimitStateStore>,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use cc_lb_observability::NoopMetricsHook;
    use cc_lb_storage_api::{
        StorageResult, UpstreamRateLimitObservationRecord, UpstreamRateLimitStateStore,
    };
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::{UpstreamRateLimitSink, start_upstream_rate_limit_writer};

    #[derive(Default)]
    struct RecordingRateLimitStore {
        records: Mutex<Vec<UpstreamRateLimitObservationRecord>>,
    }

    #[async_trait]
    impl UpstreamRateLimitStateStore for RecordingRateLimitStore {
        async fn put_observation(
            &self,
            record: &UpstreamRateLimitObservationRecord,
        ) -> StorageResult<()> {
            self.records.lock().await.push(record.clone());
            Ok(())
        }

        async fn list_for_upstream_ids(
            &self,
            _upstream_ids: &[Uuid],
        ) -> StorageResult<Vec<UpstreamRateLimitObservationRecord>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn writer_accepts_rate_limit_store_port() {
        // Given: a store that implements only the rate-limit aggregate port.
        let store = Arc::new(RecordingRateLimitStore::default());
        let (sink, receiver) = UpstreamRateLimitSink::new();
        let storage: Arc<dyn UpstreamRateLimitStateStore> = store.clone();
        let writer = start_upstream_rate_limit_writer(storage, receiver, Arc::new(NoopMetricsHook));
        let observation = UpstreamRateLimitObservationRecord {
            upstream_id: Uuid::from_u128(1),
            window: "5h".to_owned(),
            kind: cc_lb_storage_api::RateLimitKind::Requests,
            limit: Some(100),
            remaining: Some(99),
            reset: None,
            observed_at_unix_secs: 1,
        };

        // When: the writer receives a rate-limit observation.
        assert!(sink.enqueue(observation.clone()).is_ok());
        drop(sink);
        assert!(writer.await.is_ok());

        // Then: it persists through that aggregate port.
        assert_eq!(*store.records.lock().await, vec![observation]);
    }
}
