use cc_lb_domain::{PrincipalKind, Upstream};
use http::StatusCode;
use thiserror::Error;

/// Non-blocking observability hook boundary.
pub trait ObservabilityHook: Send + Sync {
    /// Observes a lifecycle event.
    fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError>;
}

/// Observability hook failures returned by [`crate::ObservabilityHook`].
#[derive(Debug, Error)]
pub enum ObservabilityError {
    /// The bounded observability queue is full.
    #[error("observability queue full")]
    QueueFull,
    /// The observability hook dropped the event.
    #[error("observability event dropped: {reason}")]
    Dropped {
        /// Redacted drop reason.
        reason: String,
    },
}

/// Observability events emitted by the lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObserveEvent {
    /// Downstream request has entered the proxy.
    RequestStarted {
        /// Request identifier.
        request_id: String,
        /// Downstream user-agent header, when present.
        downstream_user_agent: Option<String>,
    },
    /// Authentication completed successfully.
    AuthnComplete {
        /// Authenticated principal identifier.
        principal_id: String,
        /// Authenticated principal kind.
        kind: PrincipalKind,
    },
    /// Router selected an upstream.
    UpstreamChosen {
        /// Selected upstream.
        upstream: Upstream,
    },
    /// A batch of streamed events passed through the relay.
    Chunk {
        /// Monotonic batch index within the response stream.
        batch_index: u64,
        /// Number of SSE events in the batch.
        event_count: usize,
        /// Total bytes in the batch.
        total_bytes: usize,
    },
    /// Request finished successfully or with an upstream HTTP error.
    RequestFinished {
        /// Final HTTP status code.
        status: StatusCode,
        /// Input token count reported by the upstream, when known.
        input_tokens: Option<u64>,
        /// Output token count reported by the upstream, when known.
        output_tokens: Option<u64>,
        /// Cache write token count reported by the upstream, when known.
        cache_creation_input_tokens: Option<u64>,
        /// Cache read token count reported by the upstream, when known.
        cache_read_input_tokens: Option<u64>,
        /// End-to-end request duration in milliseconds.
        duration_ms: u64,
    },
    /// Lifecycle or plugin error was observed.
    Error {
        /// Stable error code.
        code: String,
        /// Redacted human-readable message.
        message: String,
        /// Error source component.
        source: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observe_event_variants_are_equatable() {
        let events = vec![
            ObserveEvent::RequestStarted {
                request_id: "req".to_owned(),
                downstream_user_agent: Some("ua".to_owned()),
            },
            ObserveEvent::AuthnComplete {
                principal_id: "principal".to_owned(),
                kind: PrincipalKind::InternalKey,
            },
            ObserveEvent::UpstreamChosen {
                upstream: Upstream::AnthropicDirect { base_url: None },
            },
            ObserveEvent::Chunk {
                batch_index: 1,
                event_count: 2,
                total_bytes: 3,
            },
            ObserveEvent::RequestFinished {
                status: StatusCode::OK,
                input_tokens: Some(4),
                output_tokens: Some(5),
                cache_creation_input_tokens: Some(6),
                cache_read_input_tokens: Some(7),
                duration_ms: 8,
            },
            ObserveEvent::Error {
                code: "E".to_owned(),
                message: "redacted".to_owned(),
                source: "plugin".to_owned(),
            },
        ];

        assert_eq!(events, events.clone());
    }
}
