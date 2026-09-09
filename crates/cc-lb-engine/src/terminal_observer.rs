//! Per-request guard that guarantees exactly one
//! `LifecycleEvent::RequestTerminated` is emitted per Anthropic-bound request,
//! except for hard process kills (SIGKILL, abort, OOM, power-loss).
//!
//! # Lifecycle
//!
//! 1. `LifecycleContext::new(...)` is constructed at the top of
//!    `lifecycle::handle` (before authn). It generates a fresh `event_id`
//!    (UUID v7) used as the DB row uniqueness key.
//! 2. As each request phase completes, the lifecycle code calls
//!    `emit_lifecycle` with a `LifecycleEvent::*` variant. The event-driven
//!    subscribers (assembler, pricing, cache observation, etc.) rebuild the
//!    request-event row from those events.
//! 3. On normal completion the lifecycle calls [`LifecycleContext::finish`]
//!    which atomically CAS-es `finalized: false → true` and emits
//!    `LifecycleEvent::RequestTerminated`.
//! 4. On abnormal termination (Drop) the guard runs the same emit path with
//!    `reason = TerminationReason::Dropped`.
//!
//! # Race safety
//!
//! `finalized` is an `AtomicBool` set via `compare_exchange`. Exactly one of
//! `{finish, Drop}` ever emits. The UNIQUE INDEX on `event_id` in
//! `request_events_v1` is the DB-side safety net for any residual race or
//! restart-after-fallback corner case.

use std::error::Error as StdError;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_lb_control::RequestEventBus;
use cc_lb_domain::InternalError;
use cc_lb_lifecycle::{LifecycleEvent, RequestSetupTimings, TerminationReason};
use cc_lb_observability::{RedactionPolicy, truncate_reason};
use cc_lb_storage_api::types::PrincipalKindLite;
use http::StatusCode;
use uuid::Uuid;

use crate::clock::{ClockHandle, unix_millis};

const STREAM_ERROR_CHAIN_MAX_DEPTH: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamTerminationOutcome {
    Completed,
    UpstreamError,
    ProxyError,
    ClientCancelled,
}

impl StreamTerminationOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::UpstreamError => "upstream_error",
            Self::ProxyError => "proxy_error",
            Self::ClientCancelled => "client_cancelled",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamTerminationCause {
    None,
    ProviderError,
    TransportError,
    H2Cancel,
    H2Reset,
    IoReset,
    IoTimeout,
    UnexpectedEof,
    DecodeError,
    FramingError,
    TransformError,
    AffinityError,
    Unknown,
}

impl StreamTerminationCause {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ProviderError => "provider_error",
            Self::TransportError => "transport_error",
            Self::H2Cancel => "h2_cancel",
            Self::H2Reset => "h2_reset",
            Self::IoReset => "io_reset",
            Self::IoTimeout => "io_timeout",
            Self::UnexpectedEof => "unexpected_eof",
            Self::DecodeError => "decode_error",
            Self::FramingError => "framing_error",
            Self::TransformError => "transform_error",
            Self::AffinityError => "affinity_error",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StreamErrorClassification {
    pub(crate) cause: StreamTerminationCause,
    pub(crate) redacted_message: String,
    pub(crate) redacted_chain: String,
    pub(crate) io_kind: Option<String>,
    pub(crate) io_os_error: Option<i32>,
    pub(crate) h2_reason: Option<String>,
}

pub(crate) fn classify_stream_error(error: &(dyn StdError + 'static)) -> StreamErrorClassification {
    let policy = RedactionPolicy::default();
    let redacted_message = truncate_reason(&policy.redact_text(&error.to_string()));
    let redacted_chain = redacted_error_chain(error, &policy, &redacted_message);
    let mut current = Some(error);
    let mut hyper_error = None;
    let mut h2_error = None;
    let mut io_error = None;

    for _ in 0..STREAM_ERROR_CHAIN_MAX_DEPTH {
        let Some(source) = current else {
            break;
        };
        hyper_error = hyper_error.or_else(|| source.downcast_ref::<hyper::Error>());
        h2_error = h2_error.or_else(|| source.downcast_ref::<h2::Error>());
        io_error = io_error.or_else(|| source.downcast_ref::<io::Error>());
        current = source.source();
    }

    let h2_reason = h2_error
        .and_then(h2::Error::reason)
        .map(|reason| reason.to_string());
    let io_error = h2_error.and_then(h2::Error::get_io).or(io_error);
    let io_kind = io_error.map(|error| format!("{:?}", error.kind()));
    let io_os_error = io_error.and_then(io::Error::raw_os_error);

    let cause = if let Some(error) = h2_error {
        if error.reason() == Some(h2::Reason::CANCEL) {
            StreamTerminationCause::H2Cancel
        } else if error.is_reset() || error.reason().is_some() {
            StreamTerminationCause::H2Reset
        } else if let Some(error) = error.get_io() {
            classify_io_error(error)
        } else {
            StreamTerminationCause::TransportError
        }
    } else if let Some(error) = io_error {
        classify_io_error(error)
    } else if let Some(error) = hyper_error {
        if error.is_incomplete_message() {
            StreamTerminationCause::UnexpectedEof
        } else if error.is_timeout() {
            StreamTerminationCause::IoTimeout
        } else {
            StreamTerminationCause::TransportError
        }
    } else {
        StreamTerminationCause::Unknown
    };

    StreamErrorClassification {
        cause,
        redacted_message,
        redacted_chain,
        io_kind,
        io_os_error,
        h2_reason,
    }
}

fn classify_io_error(error: &io::Error) -> StreamTerminationCause {
    match error.kind() {
        io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::NotConnected => StreamTerminationCause::IoReset,
        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock => StreamTerminationCause::IoTimeout,
        io::ErrorKind::UnexpectedEof => StreamTerminationCause::UnexpectedEof,
        io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput => {
            StreamTerminationCause::DecodeError
        }
        _ => StreamTerminationCause::TransportError,
    }
}

fn redacted_error_chain(
    error: &(dyn StdError + 'static),
    policy: &RedactionPolicy,
    redacted_message: &str,
) -> String {
    let mut messages = Vec::with_capacity(STREAM_ERROR_CHAIN_MAX_DEPTH);
    messages.push(redacted_message.to_owned());
    let mut current = error.source();

    for _ in 1..STREAM_ERROR_CHAIN_MAX_DEPTH {
        let Some(source) = current else {
            break;
        };
        let message = truncate_reason(&policy.redact_text(&source.to_string()));
        if messages.last() != Some(&message) {
            messages.push(message);
        }
        current = source.source();
    }
    if current.is_some() {
        messages.push("[error chain truncated]".to_owned());
    }

    truncate_reason(&messages.join(": "))
}

pub(crate) mod error_codes {
    pub(crate) const BODY_TOO_LARGE: &str = "body_too_large";
    pub(crate) const INVALID_JSON: &str = "invalid_json";
    pub(crate) const AUTHENTICATION_FAILED: &str = "authentication_failed";
    pub(crate) const PRINCIPAL_MISSING: &str = "principal_missing";
    pub(crate) const ROUTER_PIPELINE_UNAVAILABLE: &str = "router_pipeline_unavailable";
    pub(crate) const ROUTE_NO_UPSTREAM_AFTER_FILTER: &str = "route_no_upstream_after_filter";
    pub(crate) const ROUTE_NOT_CONFIGURED: &str = "route_not_configured";
    pub(crate) const UPSTREAM_AFFINITY_UNAVAILABLE: &str = "upstream_affinity_unavailable";
    pub(crate) const LIMIT_REJECTED: &str = "limit_rejected";
    pub(crate) const SIGNER_FAILED: &str = "signer_failed";
    pub(crate) const UPSTREAM_DISPATCH_FAILED: &str = "upstream_dispatch_failed";
    pub(crate) const UPSTREAM_4XX: &str = "upstream_4xx";
    pub(crate) const UPSTREAM_5XX: &str = "upstream_5xx";
    pub(crate) const UPSTREAM_STREAM_ERROR: &str = "upstream_stream_error";
    pub(crate) const TOWER_TIMEOUT: &str = "tower_timeout";
    pub(crate) const TERMINAL_DROPPED: &str = "terminal_dropped";
    pub(crate) const CLIENT_CLOSED_REQUEST: &str = "client_closed_request";
}

#[derive(Clone)]
pub struct LifecycleContext {
    inner: Arc<Inner>,
}

struct Inner {
    event_id: String,
    bus: Arc<dyn RequestEventBus>,
    started_unix_ms: u64,
    started: Instant,
    state: Mutex<TerminalState>,
    finalized: AtomicBool,
}

#[derive(Default)]
struct TerminalState {
    request_id: String,
    status: u16,
    error_code: Option<&'static str>,
    internal_errors: Vec<InternalError>,
    limit_reconcile_ms: Option<u64>,
    observability_post_ms: Option<u64>,
    proxy_setup_ms: Option<u64>,
    setup_timings: RequestSetupTimings,
    upstream_body_ms: Option<u64>,
    first_body_chunk_ms: Option<u64>,
}

impl LifecycleContext {
    pub fn new(request_id: String, bus: Arc<dyn RequestEventBus>, clock: &ClockHandle) -> Self {
        let event_id = Uuid::now_v7().to_string();
        let started_unix_ms = unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64;
        Self {
            inner: Arc::new(Inner {
                event_id,
                bus,
                started_unix_ms,
                started: Instant::now(),
                state: Mutex::new(TerminalState {
                    request_id,
                    ..TerminalState::default()
                }),
                finalized: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn event_id(&self) -> &str {
        &self.inner.event_id
    }

    pub(crate) fn set_terminal(&self, status: StatusCode, error_code: &'static str) {
        let mut state = self.lock_state();
        state.status = status.as_u16();
        state.error_code = Some(error_code);
    }

    pub(crate) fn set_success_status(&self, status: StatusCode) {
        let mut state = self.lock_state();
        state.status = status.as_u16();
    }

    pub(crate) fn set_termination_timings(
        &self,
        limit_reconcile_ms: Option<u64>,
        observability_post_ms: Option<u64>,
        proxy_setup_ms: Option<u64>,
        upstream_body_ms: Option<u64>,
        first_body_chunk_ms: Option<u64>,
    ) {
        let mut state = self.lock_state();
        state.limit_reconcile_ms = limit_reconcile_ms;
        state.observability_post_ms = observability_post_ms;
        state.proxy_setup_ms = proxy_setup_ms;
        state.upstream_body_ms = upstream_body_ms;
        state.first_body_chunk_ms = first_body_chunk_ms;
    }

    pub(crate) fn set_setup_timings(&self, timings: RequestSetupTimings) {
        self.lock_state().setup_timings = timings;
    }

    pub(crate) fn set_internal_errors(&self, errors: Vec<InternalError>) {
        self.lock_state().internal_errors = errors;
    }

    pub(crate) fn finish(&self) {
        if self
            .inner
            .finalized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.inner.emit_terminated(None);
    }

    /// Terminate with Tower timeout status (504 GATEWAY_TIMEOUT).
    /// Sets the error code to `TOWER_TIMEOUT` and publishes the final event.
    pub fn terminate_tower_timeout(&self) {
        self.set_terminal(StatusCode::GATEWAY_TIMEOUT, error_codes::TOWER_TIMEOUT);
        self.finish();
    }

    /// Record a body-cap rejection before the server has read the request body.
    pub fn record_body_too_large_rejection(&self, limit_bytes: u64) {
        self.emit_request_started(false);
        self.emit_lifecycle(LifecycleEvent::ParseCompleted {
            event_id: self.event_id().to_owned(),
            result: Err(cc_lb_lifecycle::ParseFailure::BodyTooLarge { limit_bytes }),
        });
        self.set_terminal(StatusCode::PAYLOAD_TOO_LARGE, error_codes::BODY_TOO_LARGE);
        self.finish();
    }

    /// Publish the `RequestStarted` lifecycle event.
    ///
    /// Should be called exactly once at handler entry, after the request has
    /// been parsed enough to know whether the client asked for a streaming
    /// response.
    pub(crate) fn emit_request_started(&self, stream: bool) {
        let request_id = self.lock_state().request_id.clone();
        self.inner
            .bus
            .publish_lifecycle(LifecycleEvent::RequestStarted {
                event_id: self.inner.event_id.clone(),
                request_id,
                ts_ms: self.inner.started_unix_ms,
                stream,
                source_kind: Some("proxy".to_owned()),
                source_ref_id: None,
            });
    }

    /// Publish a lifecycle event on the bus.
    ///
    /// Call sites construct the event with `event_id: self.event_id().to_owned()`.
    /// This is fire-and-forget: overflow drops the event and increments the
    /// bus-side drop counter.
    pub(crate) fn emit_lifecycle(&self, event: LifecycleEvent) {
        self.inner.bus.publish_lifecycle(event);
    }

    pub(crate) fn emit_authentication_completed(
        &self,
        principal_id: String,
        principal_kind: PrincipalKindLite,
    ) {
        self.inner
            .bus
            .publish_lifecycle(LifecycleEvent::AuthenticationCompleted {
                event_id: self.inner.event_id.clone(),
                principal_id,
                principal_kind,
            });
    }

    pub(crate) fn emit_provider_error(&self, code: &str, message: &str, source: &str) {
        self.inner
            .bus
            .publish_lifecycle(LifecycleEvent::ProviderErrorObserved {
                event_id: self.inner.event_id.clone(),
                code: code.to_owned(),
                message: message.to_owned(),
                source: source.to_owned(),
            });
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TerminalState> {
        self.inner
            .state
            .lock()
            .expect("terminal observer state mutex poisoned")
    }
}

impl Inner {
    fn emit_terminated(&self, fallback_error_code: Option<&'static str>) {
        let state = self
            .state
            .lock()
            .expect("terminal observer state mutex poisoned");
        let effective_error_code = state.error_code.or(fallback_error_code);
        let reason = match effective_error_code {
            None => TerminationReason::Success,
            Some(code) if code == error_codes::TERMINAL_DROPPED => TerminationReason::Dropped,
            Some(code) => TerminationReason::ErrorCode(code.to_owned()),
        };
        let duration_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.bus
            .publish_lifecycle(LifecycleEvent::RequestTerminated {
                event_id: self.event_id.clone(),
                reason,
                client_status: state.status,
                duration_ms,
                limit_reconcile_ms: state.limit_reconcile_ms,
                observability_post_ms: state.observability_post_ms,
                proxy_setup_ms: state.proxy_setup_ms,
                setup_timings: state.setup_timings,
                upstream_body_ms: state.upstream_body_ms,
                first_body_chunk_ms: state.first_body_chunk_ms,
                internal_errors: state.internal_errors.clone(),
            });
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if self.finalized.load(Ordering::Acquire) {
            return;
        }
        if self
            .finalized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.emit_terminated(Some(error_codes::TERMINAL_DROPPED));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::event_bus::InMemoryBus;

    fn subscribe(bus: &Arc<InMemoryBus>) -> tokio::sync::broadcast::Receiver<LifecycleEvent> {
        use cc_lb_control::LifecycleBusReceiver;
        let LifecycleBusReceiver::InMemory(rx) = bus.subscribe_lifecycle() else {
            panic!("expected InMemory lifecycle receiver");
        };
        rx
    }

    fn expect_terminated(event: LifecycleEvent) -> (String, TerminationReason, u16) {
        match event {
            LifecycleEvent::RequestTerminated {
                event_id,
                reason,
                client_status,
                ..
            } => (event_id, reason, client_status),
            other => panic!("expected RequestTerminated, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn request_started_marks_proxy_source() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_proxy_source".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );

        observer.emit_request_started(false);

        let event = rx.recv().await.expect("request started delivered");
        assert!(matches!(
            event,
            LifecycleEvent::RequestStarted {
                source_kind: Some(ref source_kind),
                source_ref_id: None,
                ..
            } if source_kind == "proxy"
        ));
    }

    #[tokio::test]
    async fn finish_emits_request_terminated_with_error_code() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_finish".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        let expected_event_id = observer.event_id().to_owned();
        observer.set_terminal(StatusCode::OK, error_codes::UPSTREAM_4XX);
        observer.finish();

        let (event_id, reason, status) =
            expect_terminated(rx.recv().await.expect("event delivered"));
        assert_eq!(event_id, expected_event_id);
        assert_eq!(status, StatusCode::OK.as_u16());
        assert!(
            matches!(reason, TerminationReason::ErrorCode(ref code) if code == error_codes::UPSTREAM_4XX)
        );
    }

    #[tokio::test]
    async fn drop_without_finish_emits_terminal_dropped() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        {
            let observer = LifecycleContext::new(
                "req_drop".to_owned(),
                bus.clone() as Arc<dyn RequestEventBus>,
                &clock,
            );
            let _ = observer.event_id();
        }
        let (_id, reason, _status) =
            expect_terminated(rx.recv().await.expect("drop fallback delivered"));
        assert!(matches!(reason, TerminationReason::Dropped));
    }

    #[tokio::test]
    async fn finish_and_drop_preserve_completed_setup_timings() {
        for finish_explicitly in [true, false] {
            let bus = Arc::new(InMemoryBus::new());
            let mut rx = subscribe(&bus);
            let clock: ClockHandle = Arc::new(SystemClock);
            {
                let observer = LifecycleContext::new(
                    "req-setup-timings".to_owned(),
                    bus.clone() as Arc<dyn RequestEventBus>,
                    &clock,
                );
                observer.set_setup_timings(RequestSetupTimings {
                    json_parse_ms: Some(0.125),
                    cache_structure_ms: Some(0.25),
                    cache_token_key_ms: Some(0.375),
                    cache_count_lookup_ms: Some(0.5),
                    cache_tokenizer_queue_ms: Some(0.625),
                    cache_serialize_ms: Some(0.75),
                    cache_tokenize_ms: Some(0.0),
                    prepare_signer_ms: Some(1.25),
                });
                if finish_explicitly {
                    observer.finish();
                }
            }

            let event = rx.recv().await.expect("terminal event delivered");
            let LifecycleEvent::RequestTerminated { setup_timings, .. } = event else {
                panic!("expected request termination");
            };
            assert_eq!(setup_timings.json_parse_ms, Some(0.125));
            assert_eq!(setup_timings.cache_structure_ms, Some(0.25));
            assert_eq!(setup_timings.cache_token_key_ms, Some(0.375));
            assert_eq!(setup_timings.cache_count_lookup_ms, Some(0.5));
            assert_eq!(setup_timings.cache_tokenizer_queue_ms, Some(0.625));
            assert_eq!(setup_timings.cache_serialize_ms, Some(0.75));
            assert_eq!(setup_timings.cache_tokenize_ms, Some(0.0));
            assert_eq!(setup_timings.prepare_signer_ms, Some(1.25));
        }
    }

    #[tokio::test]
    async fn drop_after_finish_does_not_emit_twice() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        {
            let observer = LifecycleContext::new(
                "req_norace".to_owned(),
                bus.clone() as Arc<dyn RequestEventBus>,
                &clock,
            );
            observer.set_terminal(StatusCode::OK, error_codes::UPSTREAM_4XX);
            observer.finish();
        }
        let (_id, reason, _status) = expect_terminated(rx.recv().await.expect("first delivered"));
        assert!(
            matches!(reason, TerminationReason::ErrorCode(ref code) if code == error_codes::UPSTREAM_4XX)
        );
        let try_again = rx.try_recv();
        assert!(
            try_again.is_err(),
            "Drop must not re-emit after finish; got {try_again:?}"
        );
    }

    #[tokio::test]
    async fn shared_clone_drop_emits_once_on_last_arc() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_shared".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        let clone1 = observer.clone();
        let clone2 = observer.clone();
        drop(observer);
        drop(clone1);
        assert!(
            rx.try_recv().is_err(),
            "emit must not fire while at least one observer Arc lives",
        );
        drop(clone2);
        let (_id, reason, _status) = expect_terminated(rx.recv().await.expect("emit on last drop"));
        assert!(matches!(reason, TerminationReason::Dropped));
    }
    #[test]
    fn stream_error_classifier_extracts_io_reset_and_redacts_bounded_chain() {
        let secret = "Bearer abcdef.ghijkl";
        let error = io::Error::new(
            io::ErrorKind::ConnectionReset,
            format!("{secret} {}", "x".repeat(4_096)),
        );

        let classification = classify_stream_error(&error);

        assert_eq!(classification.cause, StreamTerminationCause::IoReset);
        assert_eq!(classification.io_kind.as_deref(), Some("ConnectionReset"));
        assert!(!classification.redacted_message.contains(secret));
        assert!(
            classification
                .redacted_message
                .contains(cc_lb_observability::REDACTED)
        );
        assert!(classification.redacted_message.len() <= cc_lb_domain::MAX_ERROR_MESSAGE_LEN);
        assert!(!classification.redacted_chain.contains(secret));
        assert!(
            classification
                .redacted_chain
                .contains(cc_lb_observability::REDACTED)
        );
        assert_eq!(classification.io_os_error, None);
        assert!(classification.redacted_chain.len() <= cc_lb_domain::MAX_ERROR_MESSAGE_LEN);
    }

    #[test]
    fn stream_error_classifier_extracts_h2_cancel_reason() {
        let error = h2::Error::from(h2::Reason::CANCEL);

        let classification = classify_stream_error(&error);

        assert_eq!(
            classification.h2_reason,
            Some(h2::Reason::CANCEL.to_string())
        );
    }

    #[test]
    fn stream_error_classifier_distinguishes_unexpected_eof() {
        let error = io::Error::new(io::ErrorKind::UnexpectedEof, "truncated response");

        let classification = classify_stream_error(&error);

        assert_eq!(classification.cause, StreamTerminationCause::UnexpectedEof);
        assert_eq!(classification.io_kind.as_deref(), Some("UnexpectedEof"));
        assert_eq!(classification.redacted_message, "truncated response");
    }

    #[test]
    fn stream_error_chain_deduplicates_wrappers_and_retains_inner_cause() {
        #[derive(Debug)]
        struct Wrapper {
            message: &'static str,
            source: Box<dyn StdError + Send + Sync>,
        }

        impl std::fmt::Display for Wrapper {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(self.message)
            }
        }

        impl StdError for Wrapper {
            fn source(&self) -> Option<&(dyn StdError + 'static)> {
                Some(self.source.as_ref())
            }
        }

        let error = Wrapper {
            message: "delegating wrapper",
            source: Box::new(Wrapper {
                message: "delegating wrapper",
                source: Box::new(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "distinct inner cause",
                )),
            }),
        };

        let classification = classify_stream_error(&error);

        assert_eq!(classification.redacted_message, "delegating wrapper");
        assert_eq!(
            classification.redacted_chain,
            "delegating wrapper: distinct inner cause"
        );
        assert_eq!(classification.cause, StreamTerminationCause::IoReset);
    }
    #[tokio::test]
    async fn classifies_real_hyper_http2_cancelled_body() {
        use http::Request;
        use http_body_util::{BodyExt as _, Full};
        use hyper::client::conn::http2;
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use tokio::net::{TcpListener, TcpStream};
        use tokio::sync::oneshot;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP/2 test listener binds");
        let address = listener.local_addr().expect("HTTP/2 listener address");
        let (headers_received_tx, headers_received_rx) = oneshot::channel();
        let (reset_observed_tx, reset_observed_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("HTTP/2 client connects");
            let mut connection = h2::server::handshake(socket)
                .await
                .expect("HTTP/2 server handshake");
            let (_request, mut respond) = connection
                .accept()
                .await
                .expect("HTTP/2 request present")
                .expect("HTTP/2 request accepted");
            let response = http::Response::builder()
                .status(StatusCode::OK)
                .body(())
                .expect("HTTP/2 response builds");
            let mut body = respond
                .send_response(response, false)
                .expect("HTTP/2 response headers sent");
            tokio::select! {
                received = headers_received_rx => {
                    received.expect("client confirms response headers");
                }
                _ = connection.accept() => {
                    panic!("HTTP/2 connection ended before response-header acknowledgement");
                }
            }
            body.send_reset(h2::Reason::CANCEL);
            tokio::pin!(reset_observed_rx);
            loop {
                tokio::select! {
                    observed = &mut reset_observed_rx => {
                        observed.expect("client observes RST_STREAM");
                        break;
                    }
                    accepted = connection.accept() => {
                        assert!(
                            accepted.is_some(),
                            "HTTP/2 connection closed before reset was observed"
                        );
                    }
                }
            }
        });

        let socket = TcpStream::connect(address)
            .await
            .expect("HTTP/2 client connects");
        let (mut sender, connection) = http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
            .await
            .expect("Hyper HTTP/2 client handshake");
        let client_connection = tokio::spawn(connection);
        let request = Request::builder()
            .uri(format!("http://{address}/cancel"))
            .body(Full::new(bytes::Bytes::new()))
            .expect("HTTP/2 request builds");
        let mut response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            sender.send_request(request),
        )
        .await
        .expect("HTTP/2 response headers are flushed")
        .expect("response headers received");
        assert_eq!(response.status(), StatusCode::OK);
        headers_received_tx
            .send(())
            .expect("server waits for response-header acknowledgement");
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            response.body_mut().frame(),
        )
        .await
        .expect("HTTP/2 reset is flushed")
        .expect("reset produces a body frame result")
        .expect_err("RST_STREAM CANCEL produces a Hyper body error");

        let classification = classify_stream_error(&error);

        assert_eq!(classification.cause, StreamTerminationCause::H2Cancel);
        assert_eq!(
            classification.h2_reason,
            Some(h2::Reason::CANCEL.to_string())
        );
        reset_observed_tx
            .send(())
            .expect("server waits until Hyper exposes the reset");
        server.await.expect("HTTP/2 server task completes");
        client_connection.abort();
    }
}
