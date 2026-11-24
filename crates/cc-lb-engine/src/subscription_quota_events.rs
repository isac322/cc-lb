use std::sync::Arc;

use cc_lb_storage_api::SubscriptionQuotaSample;
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};

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
