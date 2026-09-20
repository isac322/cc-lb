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
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_lb_control::RequestEventBus;
use cc_lb_domain::InternalError;
use cc_lb_lifecycle::{LifecycleEvent, RequestIoTimings, RequestSetupTimings, TerminationReason};
use cc_lb_observability::{ObservabilityHook, ObserveEvent, RedactionPolicy, truncate_reason};
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
    pub(crate) const BODY_READ_FAILED: &str = "body_read_failed";
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
    bus: Option<Arc<dyn RequestEventBus>>,
    started_unix_ms: u64,
    started: Instant,
    state: Mutex<TerminalState>,
    observability_hooks: Mutex<Arc<[Arc<dyn ObservabilityHook>]>>,
    finalized: AtomicBool,
}

#[derive(Default)]
struct TerminalState {
    request_id: String,
    event_kind: Option<cc_lb_request_log::RequestEventKind>,
    status: u16,
    error_code: Option<&'static str>,
    internal_errors: Vec<InternalError>,
    request_span: Option<tracing::Span>,
    request_body_read_ms: Option<u64>,
    request_body_bytes: Option<u64>,
    limit_reconcile_ms: Option<u64>,
    observability_post_ms: Option<u64>,
    proxy_setup_ms: Option<u64>,
    setup_timings: RequestSetupTimings,
    io_timings: RequestIoTimings,
    shape_ms: Option<u64>,
    sign_ms: Option<u64>,
    upstream_ttfb_ms: Option<u64>,
    upstream_body_ms: Option<u64>,
    first_body_chunk_ms: Option<u64>,
    finalize_ms: Option<u64>,
    observe_finished_emitted: bool,
}

impl LifecycleContext {
    pub fn new(request_id: String, bus: Arc<dyn RequestEventBus>, clock: &ClockHandle) -> Self {
        Self::new_inner(request_id, Some(bus), clock)
    }

    pub(crate) fn without_bus(request_id: String, clock: &ClockHandle) -> Self {
        Self::new_inner(request_id, None, clock)
    }

    fn new_inner(
        request_id: String,
        bus: Option<Arc<dyn RequestEventBus>>,
        clock: &ClockHandle,
    ) -> Self {
        let event_id = Uuid::now_v7().to_string();
        let started_unix_ms = unix_millis(clock.now()).min(u128::from(u64::MAX)) as u64;
        Self {
            inner: Arc::new(Inner {
                event_id,
                bus,
                started_unix_ms,
                started: Instant::now(),
                observability_hooks: Mutex::new(Arc::from([])),
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
    pub(crate) fn set_request_span(&self, span: tracing::Span) {
        self.lock_state().request_span.get_or_insert(span);
    }

    /// Record the endpoint classification resolved from the ingress path at
    /// the earliest request boundary. Emitted on `RequestStarted` so early
    /// rejections (400/413/auth) are categorized before parsing runs.
    pub fn set_event_kind(&self, event_kind: cc_lb_request_log::RequestEventKind) {
        self.lock_state().event_kind = Some(event_kind);
    }

    pub(crate) fn set_attempt_timings(
        &self,
        shape_ms: Option<u64>,
        sign_ms: Option<u64>,
        upstream_ttfb_ms: Option<u64>,
    ) {
        let mut state = self.lock_state();
        if let Some(value) = shape_ms {
            state.shape_ms = Some(value);
        }
        if let Some(value) = sign_ms {
            state.sign_ms = Some(value);
        }
        if let Some(value) = upstream_ttfb_ms {
            state.upstream_ttfb_ms = Some(value);
        }
    }

    pub(crate) fn reset_attempt_timings(&self) {
        let mut state = self.lock_state();
        state.shape_ms = None;
        state.sign_ms = None;
        state.upstream_ttfb_ms = None;
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
        if let Some(value) = limit_reconcile_ms {
            state.limit_reconcile_ms = Some(value);
        }
        if let Some(value) = observability_post_ms {
            state.observability_post_ms = Some(value);
        }
        if let Some(value) = proxy_setup_ms {
            state.proxy_setup_ms = Some(value);
        }
        if let Some(value) = upstream_body_ms {
            state.upstream_body_ms = Some(value);
        }
        if let Some(value) = first_body_chunk_ms {
            state.first_body_chunk_ms = Some(value);
        }
    }

    /// Store the measured ingress body read duration and, on success, its
    /// exact collected byte length.
    pub fn set_request_body_timing(
        &self,
        request_body_read_ms: u64,
        request_body_bytes: Option<u64>,
    ) {
        let mut state = self.lock_state();
        state.request_body_read_ms = Some(request_body_read_ms);
        if let Some(value) = request_body_bytes {
            state.request_body_bytes = Some(value);
        }
    }
    /// Merge a completed request/response I/O timing snapshot.
    ///
    /// Missing fields never erase observations recorded by another lifecycle
    /// owner, so ingress, response, and cancellation paths may publish
    /// independent snapshots before finalization.
    pub fn set_io_timings(&self, timings: RequestIoTimings) {
        let mut state = self.lock_state();
        merge_io_timings(&mut state.io_timings, timings);
    }

    pub(crate) fn set_finalize_ms(&self, finalize_ms: u64) {
        self.lock_state().finalize_ms = Some(finalize_ms);
    }

    pub(crate) fn set_upstream_body_ms_if_absent(&self, upstream_body_ms: u64) {
        let mut state = self.lock_state();
        state.upstream_body_ms.get_or_insert(upstream_body_ms);
    }

    pub(crate) fn set_setup_timings(&self, timings: RequestSetupTimings) {
        self.lock_state().setup_timings = timings;
    }

    pub(crate) fn set_internal_errors(&self, errors: Vec<InternalError>) {
        self.lock_state().internal_errors = errors;
    }

    pub(crate) fn mark_observe_finished_emitted(&self) {
        self.lock_state().observe_finished_emitted = true;
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

    /// Record a request body transport failure and publish the terminal event
    /// explicitly so the drop fallback cannot misclassify it.
    pub fn record_body_read_failure(&self) {
        self.emit_request_started(false);
        self.set_terminal(StatusCode::BAD_REQUEST, error_codes::BODY_READ_FAILED);
        self.finish();
    }

    /// Publish the `RequestStarted` lifecycle event.
    ///
    /// Should be called exactly once at handler entry, after the request has
    /// been parsed enough to know whether the client asked for a streaming
    /// response.
    pub(crate) fn emit_request_started(&self, stream: bool) {
        let Some(bus) = self.inner.bus.as_ref() else {
            return;
        };
        let (request_id, event_kind) = {
            let state = self.lock_state();
            (state.request_id.clone(), state.event_kind)
        };
        bus.publish_lifecycle(LifecycleEvent::RequestStarted {
            event_id: self.inner.event_id.clone(),
            request_id,
            ts_ms: self.inner.started_unix_ms,
            stream,
            source_kind: Some("proxy".to_owned()),
            source_ref_id: None,
            event_kind,
        });
    }

    /// Publish a lifecycle event on the bus.
    ///
    /// Call sites construct the event with `event_id: self.event_id().to_owned()`.
    /// This is fire-and-forget: overflow drops the event and increments the
    /// bus-side drop counter.
    pub(crate) fn emit_lifecycle(&self, event: LifecycleEvent) {
        if let Some(bus) = self.inner.bus.as_ref() {
            bus.publish_lifecycle(event);
        }
    }

    pub(crate) fn emit_authentication_completed(
        &self,
        principal_id: String,
        principal_kind: PrincipalKindLite,
    ) {
        if let Some(bus) = self.inner.bus.as_ref() {
            bus.publish_lifecycle(LifecycleEvent::AuthenticationCompleted {
                event_id: self.inner.event_id.clone(),
                principal_id,
                principal_kind,
            });
        }
    }

    pub fn set_observability_hooks(&self, hooks: &[Arc<dyn ObservabilityHook>]) {
        *self
            .inner
            .observability_hooks
            .lock()
            .expect("terminal observer hooks mutex poisoned") = hooks.iter().cloned().collect();
    }

    pub(crate) fn emit_provider_error(&self, code: &str, message: &str, source: &str) {
        if let Some(bus) = self.inner.bus.as_ref() {
            bus.publish_lifecycle(LifecycleEvent::ProviderErrorObserved {
                event_id: self.inner.event_id.clone(),
                code: code.to_owned(),
                message: message.to_owned(),
                source: source.to_owned(),
            });
        }
        let hooks = self
            .inner
            .observability_hooks
            .lock()
            .expect("terminal observer hooks mutex poisoned")
            .clone();
        let event = ObserveEvent::Error {
            code: code.to_owned(),
            message: message.to_owned(),
            source: source.to_owned(),
        };
        for hook in hooks.iter() {
            let _ = hook.observe(event.clone());
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TerminalState> {
        self.inner
            .state
            .lock()
            .expect("terminal observer state mutex poisoned")
    }
}
fn merge_io_timings(current: &mut RequestIoTimings, update: RequestIoTimings) {
    if let Some(value) = update.request_body_first_chunk_ms {
        current.request_body_first_chunk_ms = Some(value);
    }
    if let Some(value) = update.request_body_receive_ms {
        current.request_body_receive_ms = Some(value);
    }
    if let Some(value) = update.request_body_wait_ms {
        current.request_body_wait_ms = Some(value);
    }
    if let Some(value) = update.request_body_process_ms {
        current.request_body_process_ms = Some(value);
    }
    if let Some(value) = update.request_body_chunk_count {
        current.request_body_chunk_count = Some(value);
    }
    if let Some(value) = update.response_body_wait_ms {
        current.response_body_wait_ms = Some(value);
    }
    if let Some(value) = update.response_body_process_ms {
        current.response_body_process_ms = Some(value);
    }
    if let Some(value) = update.response_body_downstream_poll_gap_ms {
        current.response_body_downstream_poll_gap_ms = Some(value);
    }
    if let Some(value) = update.retry_overhead_ms {
        current.retry_overhead_ms = Some(value);
    }
}

fn retry_overhead_ms_for_accounting(retry_overhead_ms: Option<f64>) -> Option<u64> {
    retry_overhead_ms
        .filter(|value| value.is_finite() && *value >= 0.0)
        .map(|value| value as u64)
}

fn terminal_accounted_ms(state: &TerminalState) -> u64 {
    [
        state.request_body_read_ms,
        state.proxy_setup_ms,
        state.shape_ms,
        state.sign_ms,
        state.upstream_ttfb_ms,
        state.upstream_body_ms,
        state.finalize_ms,
        retry_overhead_ms_for_accounting(state.io_timings.retry_overhead_ms),
    ]
    .into_iter()
    .flatten()
    .fold(0_u64, u64::saturating_add)
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
        // I/O child intervals and diagnostic markers overlap their parent stages.
        // Retry overhead is a disjoint parent interval and is accounted once.
        let accounted_ms = terminal_accounted_ms(&state);
        let unaccounted_ms = duration_ms.saturating_sub(accounted_ms);
        if let Some(span) = state.request_span.as_ref() {
            if let Some(finalize_ms) = state.finalize_ms {
                span.record("cc_lb.request.finalize_ms", finalize_ms);
            }
            span.record("cc_lb.request.unaccounted_ms", unaccounted_ms);
        }
        if let Some(bus) = self.bus.as_ref() {
            bus.publish_lifecycle(LifecycleEvent::RequestTerminated {
                event_id: self.event_id.clone(),
                reason,
                client_status: state.status,
                duration_ms,
                request_body_read_ms: state.request_body_read_ms,
                request_body_bytes: state.request_body_bytes,
                limit_reconcile_ms: state.limit_reconcile_ms,
                observability_post_ms: state.observability_post_ms,
                proxy_setup_ms: state.proxy_setup_ms,
                setup_timings: state.setup_timings,
                io_timings: state.io_timings,
                upstream_body_ms: state.upstream_body_ms,
                first_body_chunk_ms: state.first_body_chunk_ms,
                finalize_ms: state.finalize_ms,
                internal_errors: state.internal_errors.clone(),
                event_kind: state.event_kind,
            });
        }
        let terminal_hook_event =
            (!state.observe_finished_emitted).then(|| ObserveEvent::RequestFinished {
                status: StatusCode::from_u16(state.status).unwrap_or(StatusCode::OK),
                input_tokens: None,
                output_tokens: None,
                cache_creation_input_tokens: None,
                cache_read_input_tokens: None,
                duration_ms,
            });
        drop(state);

        if let Some(event) = terminal_hook_event {
            let hooks = self
                .observability_hooks
                .lock()
                .expect("terminal observer hooks mutex poisoned")
                .clone();
            for hook in hooks.iter() {
                let event = event.clone();
                if catch_unwind(AssertUnwindSafe(|| hook.observe(event))).is_err() {
                    tracing::warn!(
                        "observability hook panicked while recording request termination"
                    );
                }
            }
        }
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
    use std::collections::HashMap;

    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt as _};
    use tracing_subscriber::{Layer, Registry};

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
    #[derive(Clone, Default)]
    struct RequestTimingLayer {
        span_ids: Arc<Mutex<Vec<Id>>>,
        values: Arc<Mutex<HashMap<String, u64>>>,
    }

    impl<S> Layer<S> for RequestTimingLayer
    where
        S: Subscriber,
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: LayerContext<'_, S>) {
            if attrs.metadata().name() == "proxy.handle" {
                self.span_ids
                    .lock()
                    .expect("request timing span ids lock")
                    .push(id.clone());
            }
        }

        fn on_record(&self, id: &Id, values: &Record<'_>, _ctx: LayerContext<'_, S>) {
            if self
                .span_ids
                .lock()
                .expect("request timing span ids lock")
                .contains(id)
            {
                values.record(&mut RequestTimingVisitor {
                    values: &self.values,
                });
            }
        }
    }

    struct RequestTimingVisitor<'a> {
        values: &'a Arc<Mutex<HashMap<String, u64>>>,
    }

    impl Visit for RequestTimingVisitor<'_> {
        fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}

        fn record_u64(&mut self, field: &Field, value: u64) {
            if matches!(
                field.name(),
                "cc_lb.request.finalize_ms" | "cc_lb.request.unaccounted_ms"
            ) {
                self.values
                    .lock()
                    .expect("request timing values lock")
                    .insert(field.name().to_owned(), value);
            }
        }
    }

    fn request_span() -> (tracing::Span, Arc<Mutex<HashMap<String, u64>>>) {
        let layer = RequestTimingLayer::default();
        let values = Arc::clone(&layer.values);
        let subscriber = Registry::default().with(layer);
        let span = tracing::subscriber::with_default(subscriber, || {
            tracing::info_span!(
                "proxy.handle",
                cc_lb.request.finalize_ms = tracing::field::Empty,
                cc_lb.request.unaccounted_ms = tracing::field::Empty,
            )
        });
        (span, values)
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
    async fn terminal_event_carries_recorded_event_kind_on_finish_and_drop() {
        for finish_explicitly in [true, false] {
            let bus = Arc::new(InMemoryBus::new());
            let mut rx = subscribe(&bus);
            let clock: ClockHandle = Arc::new(SystemClock);
            {
                let observer = LifecycleContext::new(
                    "req-event-kind".to_owned(),
                    bus.clone() as Arc<dyn RequestEventBus>,
                    &clock,
                );
                observer.set_event_kind(cc_lb_request_log::RequestEventKind::Messages);
                if finish_explicitly {
                    observer.finish();
                }
            }

            let event = rx.recv().await.expect("terminal event delivered");
            let LifecycleEvent::RequestTerminated { event_kind, .. } = event else {
                panic!("expected request termination");
            };
            assert_eq!(
                event_kind,
                Some(cc_lb_request_log::RequestEventKind::Messages)
            );
        }
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
    #[tokio::test]
    async fn terminal_event_preserves_completed_latency_stages() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_latency_stages".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.set_io_timings(RequestIoTimings {
            request_body_first_chunk_ms: Some(0.125),
            request_body_receive_ms: Some(3.5),
            request_body_wait_ms: Some(2.25),
            request_body_process_ms: Some(0.0),
            request_body_chunk_count: Some(0),
            ..RequestIoTimings::default()
        });
        observer.set_io_timings(RequestIoTimings {
            response_body_wait_ms: Some(8.75),
            response_body_process_ms: Some(1.5),
            response_body_downstream_poll_gap_ms: Some(0.0),
            retry_overhead_ms: Some(12.625),
            ..RequestIoTimings::default()
        });
        observer.set_io_timings(RequestIoTimings::default());
        observer.set_request_body_timing(7, Some(4_096));
        observer.set_request_body_timing(8, None);
        observer.set_termination_timings(Some(1), Some(2), Some(3), Some(4), Some(5));
        observer.set_termination_timings(None, None, None, None, None);
        observer.set_upstream_body_ms_if_absent(99);
        observer.set_finalize_ms(6);
        observer.finish();

        let event = rx.recv().await.expect("terminal event delivered");
        let LifecycleEvent::RequestTerminated {
            request_body_read_ms,
            request_body_bytes,
            limit_reconcile_ms,
            observability_post_ms,
            proxy_setup_ms,
            upstream_body_ms,
            first_body_chunk_ms,
            finalize_ms,
            io_timings,
            ..
        } = event
        else {
            panic!("expected request termination");
        };
        assert_eq!(request_body_read_ms, Some(8));
        assert_eq!(request_body_bytes, Some(4_096));
        assert_eq!(limit_reconcile_ms, Some(1));
        assert_eq!(observability_post_ms, Some(2));
        assert_eq!(proxy_setup_ms, Some(3));
        assert_eq!(upstream_body_ms, Some(4));
        assert_eq!(first_body_chunk_ms, Some(5));
        assert_eq!(finalize_ms, Some(6));
        assert_eq!(
            io_timings,
            RequestIoTimings {
                request_body_first_chunk_ms: Some(0.125),
                request_body_receive_ms: Some(3.5),
                request_body_wait_ms: Some(2.25),
                request_body_process_ms: Some(0.0),
                request_body_chunk_count: Some(0),
                response_body_wait_ms: Some(8.75),
                response_body_process_ms: Some(1.5),
                response_body_downstream_poll_gap_ms: Some(0.0),
                retry_overhead_ms: Some(12.625),
            }
        );
    }
    #[test]
    fn attempt_timings_preserve_present_zero_across_missing_updates() {
        let bus = Arc::new(InMemoryBus::new());
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_attempt_timings".to_owned(),
            bus as Arc<dyn RequestEventBus>,
            &clock,
        );

        observer.set_attempt_timings(Some(0), Some(2), Some(3));
        observer.set_attempt_timings(None, None, None);

        let state = observer.lock_state();
        assert_eq!(state.shape_ms, Some(0));
        assert_eq!(state.sign_ms, Some(2));
        assert_eq!(state.upstream_ttfb_ms, Some(3));
    }

    #[test]
    fn retry_parent_is_accounted_once_without_counting_io_children() {
        let mut state = TerminalState {
            request_body_read_ms: Some(3),
            proxy_setup_ms: Some(4),
            shape_ms: Some(5),
            sign_ms: Some(6),
            upstream_ttfb_ms: Some(7),
            upstream_body_ms: Some(8),
            finalize_ms: Some(9),
            ..TerminalState::default()
        };
        assert_eq!(terminal_accounted_ms(&state), 42);

        state.io_timings = RequestIoTimings {
            request_body_first_chunk_ms: Some(100.0),
            request_body_receive_ms: Some(100.0),
            request_body_wait_ms: Some(100.0),
            request_body_process_ms: Some(100.0),
            request_body_chunk_count: Some(1),
            response_body_wait_ms: Some(100.0),
            response_body_process_ms: Some(100.0),
            response_body_downstream_poll_gap_ms: Some(100.0),
            retry_overhead_ms: None,
        };
        assert_eq!(terminal_accounted_ms(&state), 42);

        state.io_timings.retry_overhead_ms = Some(8.625);
        assert_eq!(terminal_accounted_ms(&state), 50);
        state.io_timings.retry_overhead_ms = Some(-1.0);
        assert_eq!(terminal_accounted_ms(&state), 42);
        state.io_timings.retry_overhead_ms = Some(f64::INFINITY);
        assert_eq!(terminal_accounted_ms(&state), 42);
        state.io_timings.retry_overhead_ms = Some(f64::MAX);
        assert_eq!(terminal_accounted_ms(&state), u64::MAX);
    }

    #[tokio::test]
    async fn request_span_records_event_finalize_and_saturating_unaccounted() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_request_span_timings".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        let (span, recorded) = request_span();
        observer.set_request_span(span);
        observer.set_request_body_timing(u64::MAX, None);
        observer.set_termination_timings(None, None, Some(7), Some(11), None);
        observer.set_attempt_timings(Some(13), Some(17), Some(19));
        observer.set_finalize_ms(23);
        observer.finish();

        let event = rx.recv().await.expect("terminal event delivered");
        let LifecycleEvent::RequestTerminated {
            duration_ms,
            finalize_ms,
            ..
        } = event
        else {
            panic!("expected request termination");
        };
        assert_eq!(finalize_ms, Some(23));
        assert_eq!(duration_ms.saturating_sub(u64::MAX), 0);

        let recorded = recorded.lock().expect("request timing values lock");
        assert_eq!(recorded.get("cc_lb.request.finalize_ms"), Some(&23));
        assert_eq!(recorded.get("cc_lb.request.unaccounted_ms"), Some(&0));
    }

    #[tokio::test]
    async fn body_read_failure_finishes_explicitly_with_ingress_timing() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_body_read_failure".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.set_request_body_timing(12, None);
        observer.record_body_read_failure();

        assert!(matches!(
            rx.recv().await.expect("request started delivered"),
            LifecycleEvent::RequestStarted { .. }
        ));
        let event = rx.recv().await.expect("terminal event delivered");
        let LifecycleEvent::RequestTerminated {
            reason,
            client_status,
            request_body_read_ms,
            request_body_bytes,
            ..
        } = event
        else {
            panic!("expected request termination");
        };
        assert!(matches!(
            &reason,
            TerminationReason::ErrorCode(code) if code == error_codes::BODY_READ_FAILED
        ));
        assert_eq!(client_status, StatusCode::BAD_REQUEST.as_u16());
        assert_eq!(request_body_read_ms, Some(12));
        assert_eq!(request_body_bytes, None);
        drop(observer);
        assert!(
            rx.try_recv().is_err(),
            "explicit body read failure must not re-emit through Drop"
        );
    }

    #[tokio::test]
    async fn body_too_large_rejection_preserves_ingress_timing_without_bytes() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_body_too_large".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.set_request_body_timing(3, None);
        observer.record_body_too_large_rejection(1_024);

        assert!(matches!(
            rx.recv().await.expect("request started delivered"),
            LifecycleEvent::RequestStarted { .. }
        ));
        assert!(matches!(
            rx.recv().await.expect("parse failure delivered"),
            LifecycleEvent::ParseCompleted {
                result: Err(cc_lb_lifecycle::ParseFailure::BodyTooLarge { limit_bytes: 1_024 }),
                ..
            }
        ));
        let event = rx.recv().await.expect("terminal event delivered");
        let LifecycleEvent::RequestTerminated {
            client_status,
            request_body_read_ms,
            request_body_bytes,
            ..
        } = event
        else {
            panic!("expected request termination");
        };
        assert_eq!(client_status, StatusCode::PAYLOAD_TOO_LARGE.as_u16());
        assert_eq!(request_body_read_ms, Some(3));
        assert_eq!(request_body_bytes, None);
    }

    #[tokio::test]
    async fn early_rejections_carry_classified_event_kind_on_request_started() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = subscribe(&bus);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_event_kind".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.set_event_kind(cc_lb_request_log::RequestEventKind::CountTokens);
        observer.record_body_too_large_rejection(1_024);

        let started = rx.recv().await.expect("request started delivered");
        let LifecycleEvent::RequestStarted { event_kind, .. } = started else {
            panic!("expected request started");
        };
        assert_eq!(
            event_kind,
            Some(cc_lb_request_log::RequestEventKind::CountTokens),
            "413 rejection must carry the ingress classification"
        );

        let observer = LifecycleContext::new(
            "req_event_kind_read".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.set_event_kind(cc_lb_request_log::RequestEventKind::Files);
        observer.record_body_read_failure();

        // Drain the first request's terminal event, then the second start.
        let _ = rx.recv().await.expect("first terminal delivered");
        let _ = rx.recv().await.expect("first parse failure delivered");
        let started = rx.recv().await.expect("second request started delivered");
        let LifecycleEvent::RequestStarted { event_kind, .. } = started else {
            panic!("expected request started");
        };
        assert_eq!(
            event_kind,
            Some(cc_lb_request_log::RequestEventKind::Files),
            "400 read failure must carry the ingress classification"
        );
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
    struct PanickingHook;

    impl cc_lb_observability::ObservabilityHook for PanickingHook {
        fn observe(
            &self,
            _event: cc_lb_observability::ObserveEvent,
        ) -> Result<(), cc_lb_observability::ObservabilityError> {
            panic!("intentional terminal hook panic");
        }
    }

    #[test]
    fn terminal_drop_isolates_panicking_hook() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let clock: ClockHandle = Arc::new(SystemClock);
            let observer = LifecycleContext::without_bus("panic-hook".to_owned(), &clock);
            let hooks: Vec<Arc<dyn cc_lb_observability::ObservabilityHook>> =
                vec![Arc::new(PanickingHook)];
            observer.set_observability_hooks(&hooks);
            observer.set_terminal(StatusCode::BAD_GATEWAY, error_codes::UPSTREAM_5XX);
            drop(observer);
        }));

        assert!(
            result.is_ok(),
            "hook panic must not escape LifecycleContext drop"
        );
    }
}
