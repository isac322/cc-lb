use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use cc_lb_engine::lifecycle::{PromptCacheObservationEnqueueError, PromptCacheObservationSinkLike};
use cc_lb_observability::{
    cache_observation_dropped_reason, inc_cache_observation_dropped,
    inc_cache_observation_write_failed,
};
use cc_lb_storage_api::{PromptCacheObservationRecord, PromptCacheObservationStore};
use thiserror::Error;
use tokio::sync::mpsc::{self, Sender, error::TrySendError};
use tokio::task::JoinHandle;

pub const DEFAULT_PROMPT_CACHE_OBSERVATION_CHANNEL_CAPACITY: usize = 4096;

#[derive(Clone, Debug)]
pub struct PromptCacheObservationSink {
    tx: Sender<PromptCacheObservationRecord>,
    dropped_counter: Arc<AtomicU64>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum EnqueueError {
    #[error("prompt cache observation queue is full")]
    ChannelFull,
    #[error("prompt cache observation queue is closed")]
    ChannelClosed,
}

impl PromptCacheObservationSink {
    /// Spawns the writer task and returns the sink.
    pub fn new(
        store: Arc<dyn PromptCacheObservationStore + Send + Sync>,
        capacity: usize,
        store_kind: &'static str,
    ) -> (Self, JoinHandle<()>) {
        let bounded_capacity = capacity.max(1);
        let (tx, mut rx) = mpsc::channel(bounded_capacity);
        let sink = Self {
            tx,
            dropped_counter: Arc::new(AtomicU64::new(0)),
        };

        let writer = tokio::spawn(async move {
            while let Some(record) = rx.recv().await {
                if let Err(e) = store.upsert_observation(&record).await {
                    inc_cache_observation_write_failed(store_kind);
                    tracing::warn!(error = ?e, "prompt cache observation write failed");
                }
            }
        });

        (sink, writer)
    }

    /// Non-blocking enqueue. On full channel, increments dropped_counter and returns Err.
    pub fn enqueue(&self, record: PromptCacheObservationRecord) -> Result<(), EnqueueError> {
        match self.tx.try_send(record) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                self.dropped_counter.fetch_add(1, Ordering::Relaxed);
                inc_cache_observation_dropped(cache_observation_dropped_reason::QUEUE_FULL);
                Err(EnqueueError::ChannelFull)
            }
            Err(TrySendError::Closed(_)) => Err(EnqueueError::ChannelClosed),
        }
    }

    /// Reads the dropped counter.
    pub fn dropped_total(&self) -> u64 {
        self.dropped_counter.load(Ordering::Relaxed)
    }
}

impl PromptCacheObservationSinkLike for PromptCacheObservationSink {
    fn enqueue(
        &self,
        record: PromptCacheObservationRecord,
    ) -> Result<(), PromptCacheObservationEnqueueError> {
        PromptCacheObservationSink::enqueue(self, record).map_err(|error| match error {
            EnqueueError::ChannelFull => PromptCacheObservationEnqueueError::ChannelFull,
            EnqueueError::ChannelClosed => PromptCacheObservationEnqueueError::ChannelClosed,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex, atomic::AtomicU64};
    use std::time::Duration;

    use crate::prompt_cache_observation_cache::HASH_SCHEMA_VERSION;
    use async_trait::async_trait;
    use cc_lb_domain::TtlClass;
    use cc_lb_storage_api::{PromptCacheObservationRecord, StorageResult};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;

    #[derive(Clone, Default)]
    struct MockStore {
        records: Arc<Mutex<Vec<PromptCacheObservationRecord>>>,
    }

    #[async_trait]
    impl PromptCacheObservationStore for MockStore {
        async fn upsert_observation(
            &self,
            record: &PromptCacheObservationRecord,
        ) -> StorageResult<()> {
            self.records
                .lock()
                .expect("mock store lock")
                .push(record.clone());
            Ok(())
        }
    }

    fn record(index: u64) -> PromptCacheObservationRecord {
        PromptCacheObservationRecord {
            upstream_id: Uuid::from_u128(0x1234_5678_90ab_cdef_1234_5678_90ab_cdef),
            canonical_model_id: "claude-sonnet-4-5-20250929".to_owned(),
            v3_prefix_key: format!("sha256:{index}"),
            ttl_class: TtlClass::Ephemeral5m,
            expires_at_unix_secs: 1_800 + index,
            last_observed_at_unix_secs: 1_500 + index,
            hash_schema_version: HASH_SCHEMA_VERSION,
            prefix_content_block_index: 0,
            estimated_prefix_tokens: 0,
            token_estimate_source: "local_tiktoken_v1".to_owned(),
            last_provider_cache_read_tokens: Some(0),
            last_provider_cache_creation_tokens: Some(0),
        }
    }

    #[tokio::test]
    async fn overflow_drops_and_counts() {
        let (tx, rx) = mpsc::channel(2);
        let _rx = rx;
        let sink = PromptCacheObservationSink {
            tx,
            dropped_counter: Arc::new(AtomicU64::new(0)),
        };

        for index in 0..2 {
            sink.enqueue(record(index)).expect("record enqueued");
        }

        for index in 2..5 {
            assert_eq!(sink.enqueue(record(index)), Err(EnqueueError::ChannelFull));
        }

        assert_eq!(sink.dropped_total(), 3);
    }

    #[tokio::test]
    async fn writer_processes_records_into_store() {
        let store = MockStore::default();
        let records = Arc::clone(&store.records);
        let (sink, writer) = PromptCacheObservationSink::new(Arc::new(store), 8, "sqlite");

        for index in 0..3 {
            sink.enqueue(record(index)).expect("record enqueued");
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(records.lock().expect("mock store lock").len(), 3);

        drop(sink);
        writer.await.expect("writer exits cleanly");
    }

    #[tokio::test]
    async fn closed_channel_returns_closed_error() {
        let (sink, writer) =
            PromptCacheObservationSink::new(Arc::new(MockStore::default()), 2, "sqlite");

        writer.abort();
        let _ = writer.await;

        assert_eq!(sink.enqueue(record(0)), Err(EnqueueError::ChannelClosed));
        assert_eq!(sink.dropped_total(), 0);
    }
}
