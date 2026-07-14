//! Lifecycle hooks for capture operations.

use std::fmt;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use crate::schema::CapturedRequestInput;
use crate::sink::{CaptureEnqueueError, CaptureSink};

#[derive(Clone)]
pub struct CaptureHandle {
    sink: CaptureSink,
    dropped_counter: Arc<AtomicU64>,
}

impl fmt::Debug for CaptureHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureHandle")
            .field("dropped_counter", &self.dropped_counter)
            .finish()
    }
}

impl CaptureHandle {
    pub fn from_sink(sink: CaptureSink) -> Self {
        Self {
            sink,
            dropped_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn try_capture_input(&self, input: CapturedRequestInput) {
        match self.sink.try_input(input) {
            Ok(()) => {}
            Err(CaptureEnqueueError::Closed) => {}
            Err(CaptureEnqueueError::Full) => {
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

    #[tokio::test(flavor = "current_thread")]
    async fn capture_input_drops_and_counts_when_channel_is_full()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let directory = tempfile::tempdir()?;
        let store =
            crate::store::open_capture_store(&directory.path().join("capture.sqlite")).await?;
        let (sink, writer) = crate::sink::CaptureSink::new(store.clone(), 1);
        let handle = CaptureHandle::from_sink(sink.clone());
        let cloned_handle = handle.clone();

        // When
        handle.try_capture_input(input("event-1"));
        handle.try_capture_input(input("event-2"));
        cloned_handle.try_capture_input(input("event-3"));

        // Then
        assert_eq!(handle.dropped_total(), 2);
        assert_eq!(cloned_handle.dropped_total(), 2);

        writer.shutdown().await;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn capture_handle_from_sink_merges_and_preserves_dropped_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        // Given
        let directory = tempfile::tempdir()?;
        let store =
            crate::store::open_capture_store(&directory.path().join("capture.sqlite")).await?;
        let (sink, writer) = crate::sink::CaptureSink::new(store.clone(), 16);

        // When
        let handle = CaptureHandle::from_sink(sink.clone());

        sink.try_seed(
            "event-1".to_owned(),
            "request-event-1".to_owned(),
            1_700_000_000_000,
        )?;

        handle.try_capture_input(input("event-1"));

        sink.try_response(
            "event-1".to_owned(),
            crate::schema::CapturedResponse {
                input_tokens: Some(2_000),
                output_tokens: Some(500),
                cache_read_input_tokens: Some(1_000),
                cache_creation_input_tokens_5m: Some(200),
                cache_creation_input_tokens_1h: Some(300),
                chosen_upstream_id: None,
                upstream_status: Some(200),
                client_status: Some(200),
                duration_ms: Some(345),
                attempt_num: Some(1),
            },
            crate::schema::Disposition::RoutedDispatchedSuccess,
            true,
        )?;

        writer.shutdown().await;

        // Then
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM capture_v1")
            .fetch_one(store.pool())
            .await?;
        assert_eq!(count, 1);

        // Given a full sink
        let (full_sink, full_writer) = crate::sink::CaptureSink::new(store.clone(), 1);
        let full_handle = CaptureHandle::from_sink(full_sink.clone());

        // When the channel is filled
        full_sink.try_seed("event-2".to_owned(), "request-2".to_owned(), 1)?;

        // And we try to capture another input
        full_handle.try_capture_input(input("event-3"));

        // Then dropped semantics are preserved
        assert_eq!(full_handle.dropped_total(), 1);
        assert_eq!(full_sink.dropped_total(), 1);

        full_writer.shutdown().await;
        Ok(())
    }
}
