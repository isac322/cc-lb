//! Lifecycle hooks for capture operations.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio::sync::mpsc::{Sender, error::TrySendError};

use crate::schema::CapturedRequestInput;

#[derive(Clone, Debug)]
pub struct CaptureHandle {
    sender: Sender<CapturedRequestInput>,
    dropped_counter: Arc<AtomicU64>,
}

impl CaptureHandle {
    pub fn new(sender: Sender<CapturedRequestInput>) -> Self {
        Self {
            sender,
            dropped_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn try_capture_input(&self, input: CapturedRequestInput) {
        match self.sender.try_send(input) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(_)) => {
                self.dropped_counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn dropped_total(&self) -> u64 {
        self.dropped_counter.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use cc_lb_domain::{CachePricingSummary, RoutingTrace};
    use tokio::sync::mpsc;

    use super::CaptureHandle;
    use crate::schema::CapturedRequestInput;

    fn input(event_id: &str) -> CapturedRequestInput {
        CapturedRequestInput {
            event_id: event_id.to_owned(),
            request_id: "request-id".to_owned(),
            thread_id: None,
            canonical_model_id: "claude-sonnet-4-5".to_owned(),
            cache_pricing: CachePricingSummary {
                status: "unavailable".to_owned(),
                input_micros_per_million: None,
                cache_creation_5m_micros_per_million: None,
                cache_creation_1h_micros_per_million: None,
                cache_read_micros_per_million: None,
            },
            breakpoints: Vec::new(),
            candidates: Vec::new(),
            subscription_preference_input_upstream_ids: Vec::new(),
            routing_trace: RoutingTrace {
                stages: Vec::new(),
                terminal_decision: None,
            },
            captured_at_unix_ms: 0,
            salt_version: "v11".to_owned(),
            cache_cost_basis_version: "v1".to_owned(),
            capture_schema_version: 1,
            build_version: "test".to_owned(),
        }
    }

    #[test]
    fn capture_input_drops_and_counts_when_channel_is_full() {
        // Given
        let (sender, _receiver) = mpsc::channel(1);
        let handle = CaptureHandle::new(sender);
        let cloned_handle = handle.clone();

        // When
        handle.try_capture_input(input("event-1"));
        handle.try_capture_input(input("event-2"));
        cloned_handle.try_capture_input(input("event-3"));

        // Then
        assert_eq!(handle.dropped_total(), 2);
        assert_eq!(cloned_handle.dropped_total(), 2);
    }
}
