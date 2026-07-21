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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_lb_control::RequestEventBus;
use cc_lb_domain::InternalError;
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_storage_api::types::PrincipalKindLite;
use http::StatusCode;
use uuid::Uuid;

use crate::clock::{ClockHandle, unix_millis};

pub(crate) mod error_codes {
    pub(crate) const BODY_TOO_LARGE: &str = "body_too_large";
    pub(crate) const INVALID_JSON: &str = "invalid_json";
    pub(crate) const AUTHENTICATION_FAILED: &str = "authentication_failed";
    pub(crate) const PRINCIPAL_MISSING: &str = "principal_missing";
    pub(crate) const ROUTER_PIPELINE_UNAVAILABLE: &str = "router_pipeline_unavailable";
    pub(crate) const ROUTE_NO_UPSTREAM_AFTER_FILTER: &str = "route_no_upstream_after_filter";
    pub(crate) const ROUTE_NOT_CONFIGURED: &str = "route_not_configured";
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
}
