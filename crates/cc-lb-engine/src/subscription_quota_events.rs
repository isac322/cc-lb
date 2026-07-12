use std::sync::Arc;
use std::time::Duration;

use cc_lb_storage_api::{Storage, SubscriptionQuotaSample};
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[doc(hidden)]
pub use cc_lb_quota::unified_observation_to_sample;

pub const DEFAULT_SUBSCRIPTION_QUOTA_CHANNEL_CAPACITY: usize = 4096;

#[derive(Clone, Debug)]
pub struct SubscriptionQuotaSink {
    tx: Arc<Sender<SubscriptionQuotaSample>>,
}

#[derive(Debug, Error)]
pub enum SubscriptionQuotaEnqueueError {
    #[error("subscription quota observation queue is full")]
    Full,
    #[error("subscription quota observation queue is closed")]
    Closed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubscriptionQuotaWriterConfig {
    pub batch_max_records: usize,
    pub flush_max_ms: u64,
}

impl Default for SubscriptionQuotaWriterConfig {
    fn default() -> Self {
        Self {
            batch_max_records: 256,
            flush_max_ms: 100,
        }
    }
}

impl SubscriptionQuotaSink {
    pub fn new() -> (Self, Receiver<SubscriptionQuotaSample>) {
        Self::with_capacity(DEFAULT_SUBSCRIPTION_QUOTA_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Receiver<SubscriptionQuotaSample>) {
        let bounded_capacity = capacity.max(1);
        let (tx, rx) = mpsc::channel(bounded_capacity);
        (Self { tx: Arc::new(tx) }, rx)
    }

    pub fn enqueue(
        &self,
        record: SubscriptionQuotaSample,
    ) -> Result<(), SubscriptionQuotaEnqueueError> {
        match self.tx.try_send(record) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                metrics::counter!("subscription_quota_full").increment(1);
                Err(SubscriptionQuotaEnqueueError::Full)
            }
            Err(TrySendError::Closed(_)) => {
                metrics::counter!("subscription_quota_closed").increment(1);
                Err(SubscriptionQuotaEnqueueError::Closed)
            }
        }
    }
}

pub fn start_subscription_quota_writer(
    storage: Arc<dyn Storage>,
    receiver: Receiver<SubscriptionQuotaSample>,
    config: SubscriptionQuotaWriterConfig,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        run_subscription_quota_writer(storage, receiver, config, cancel).await;
    })
}

async fn run_subscription_quota_writer(
    storage: Arc<dyn Storage>,
    mut receiver: Receiver<SubscriptionQuotaSample>,
    config: SubscriptionQuotaWriterConfig,
    cancel: CancellationToken,
) {
    let config = normalize_config(config);
    loop {
        let first = tokio::select! {
            _ = cancel.cancelled() => {
                let mut batch = Vec::new();
                drain_remaining(&mut receiver, &mut batch);
                flush_batch(&storage, batch).await;
                return;
            }
            record = receiver.recv() => record,
        };
        let Some(first) = first else {
            return;
        };

        let mut batch = Vec::with_capacity(config.batch_max_records);
        batch.push(first);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(config.flush_max_ms);
        let flush_sleep = tokio::time::sleep_until(deadline);
        tokio::pin!(flush_sleep);

        while batch.len() < config.batch_max_records {
            let remaining = config.batch_max_records - batch.len();
            tokio::select! {
                _ = cancel.cancelled() => {
                    drain_remaining(&mut receiver, &mut batch);
                    flush_batch(&storage, batch).await;
                    return;
                }
                _ = &mut flush_sleep => break,
                count = receiver.recv_many(&mut batch, remaining) => {
                    if count == 0 {
                        flush_batch(&storage, batch).await;
                        return;
                    }
                }
            }
        }

        flush_batch(&storage, batch).await;
    }
}

fn normalize_config(config: SubscriptionQuotaWriterConfig) -> SubscriptionQuotaWriterConfig {
    SubscriptionQuotaWriterConfig {
        batch_max_records: config.batch_max_records.max(1),
        flush_max_ms: config.flush_max_ms.max(1),
    }
}

fn drain_remaining(
    receiver: &mut Receiver<SubscriptionQuotaSample>,
    batch: &mut Vec<SubscriptionQuotaSample>,
) {
    while let Ok(record) = receiver.try_recv() {
        batch.push(record);
    }
}

async fn flush_batch(storage: &Arc<dyn Storage>, batch: Vec<SubscriptionQuotaSample>) {
    if batch.is_empty() {
        return;
    }

    metrics::histogram!("subscription_quota_writer_batch_size").record(batch.len() as f64);
    if let Err(error) = storage.record_subscription_quota_samples(&batch).await {
        metrics::counter!("subscription_quota_writer_error").increment(1);
        tracing::warn!(error = %error, batch_size = batch.len(), "subscription quota batch persistence failed");
    }
}
