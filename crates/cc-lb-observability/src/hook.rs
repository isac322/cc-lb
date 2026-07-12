use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};

use crate::{ObservabilityError, ObservabilityHook, ObserveEvent};

pub const DEFAULT_HOOK_CHANNEL_CAPACITY: usize = 4096;

static DROPPED_EVENTS_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct BoundedChannelHook {
    tx: Sender<ObserveEvent>,
}

impl BoundedChannelHook {
    pub fn new() -> (Self, Receiver<ObserveEvent>) {
        Self::with_capacity(DEFAULT_HOOK_CHANNEL_CAPACITY)
    }

    pub fn with_capacity(capacity: usize) -> (Self, Receiver<ObserveEvent>) {
        let bounded_capacity = capacity.max(1);
        let (tx, rx) = mpsc::channel(bounded_capacity);
        (Self { tx }, rx)
    }

    pub fn sender(&self) -> &Sender<ObserveEvent> {
        &self.tx
    }
}

impl ObservabilityHook for BoundedChannelHook {
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
        match self.tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => {
                increment_dropped_events("full");
                Err(ObservabilityError::QueueFull)
            }
            Err(TrySendError::Closed(_)) => {
                increment_dropped_events("closed");
                Err(ObservabilityError::Dropped {
                    reason: "receiver closed".to_owned(),
                })
            }
        }
    }
}

pub fn dropped_events_total() -> u64 {
    DROPPED_EVENTS_TOTAL.load(Ordering::Relaxed)
}

pub fn increment_dropped_events(reason: &str) {
    increment_dropped_events_by(reason, 1);
}

pub fn increment_dropped_events_by(reason: &str, amount: u64) {
    DROPPED_EVENTS_TOTAL.fetch_add(amount, Ordering::Relaxed);
    metrics::counter!("cc_lb_dropped_events_total", "reason" => reason.to_owned())
        .increment(amount);
}
