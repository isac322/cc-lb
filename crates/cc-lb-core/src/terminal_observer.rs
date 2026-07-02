//! Per-request guard that guarantees exactly one final `RequestEvent` is
//! published to the event bus per Anthropic-bound request, except for hard
//! process kills (SIGKILL, abort, OOM, power-loss).
//!
//! # Lifecycle
//!
//! 1. `LifecycleContext::new(...)` is constructed at the top of
//!    `lifecycle::handle` (before authn). It generates a fresh `event_id`
//!    (UUID v7) used as the DB row uniqueness key. The client-supplied
//!    `request_id` is intentionally distinct because it is forwarded to/from
//!    Anthropic and exposed to PDK plugins and therefore non-unique.
//! 2. As each request phase completes, the lifecycle code calls
//!    `attach_principal`, `attach_route`, `attach_model`, etc. to populate
//!    IDENTITY state. Post-Phase-9 the observer no longer tracks usage/cache;
//!    those fields live on `LifecycleEvent`s owned by the assembler subscriber.
//! 3. On normal completion the lifecycle calls [`LifecycleContext::finish`]
//!    which:
//!      1. atomically CAS-es `finalized: false → true`,
//!      2. snapshots state,
//!      3. calls `bus.publish(RequestEventUpdate::final_(snapshot))`
//!         (synchronous, non-blocking).
//!
//!    There is no `.await` between the CAS and the publish, so cancellation
//!    cannot leave the request in the "finalized but never published" state
//!    that the original async-publish design suffered from.
//! 5. On abnormal termination (Drop) the guard runs the same publish path
//!    with `error_code = "terminal_dropped"` as the fallback outcome
//!    category.
//!
//! # Race safety
//!
//! `finalized` is an `AtomicBool` set via `compare_exchange`. Exactly one of
//! `{finish, Drop}` ever publishes. The UNIQUE INDEX on `event_id` in
//! `request_events_v1` is the DB-side safety net for any residual race or
//! restart-after-fallback corner case.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_plugin_api::{InternalError, RoutingTrace};
use cc_lb_storage_api::{RequestEvent, RequestEventUpstream};
use http::StatusCode;
use uuid::Uuid;

use crate::clock::{ClockHandle, unix_millis};
use crate::event_bus::{RequestEventBus, RequestEventUpdate};

/// Outcome category catalog. Every terminal call site must use one of these
/// constants. `terminal_dropped` is reserved for the `Drop` fallback path.
///
/// `CLIENT_DISCONNECTED` and `TOWER_TIMEOUT` are not yet wired to call sites —
/// Drop fallback currently subsumes both as `TERMINAL_DROPPED`. They remain
/// in the catalog so the next iteration can route to them without churn.
#[allow(dead_code)]
pub(crate) mod error_codes {
    pub(crate) const BODY_TOO_LARGE: &str = "body_too_large";
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
    pub(crate) const CLIENT_DISCONNECTED: &str = "client_disconnected";
    pub(crate) const TOWER_TIMEOUT: &str = "tower_timeout";
    pub(crate) const TERMINAL_DROPPED: &str = "terminal_dropped";
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
    principal_id: Option<String>,
    key_id: Option<String>,
    principal_kind: Option<String>,
    upstream_id: Option<Uuid>,
    upstream_name: Option<String>,
    upstream: Option<RequestEventUpstream>,
    model: Option<String>,
    status: u16,
    error_code: Option<&'static str>,
    upstream_error_type: Option<String>,
    upstream_error_message: Option<String>,
    routing_trace: Option<RoutingTrace>,
    internal_errors: Vec<InternalError>,
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

    #[allow(dead_code)]
    pub(crate) fn event_id(&self) -> &str {
        &self.inner.event_id
    }

    pub(crate) fn attach_principal(
        &self,
        principal_id: String,
        key_id: Option<String>,
        principal_kind: Option<String>,
    ) {
        let mut state = self.lock_state();
        state.principal_id = Some(principal_id);
        state.key_id = key_id;
        state.principal_kind = principal_kind;
    }

    pub(crate) fn attach_route(
        &self,
        upstream_id: Uuid,
        upstream_name: String,
        upstream: Option<RequestEventUpstream>,
    ) {
        let mut state = self.lock_state();
        state.upstream_id = Some(upstream_id);
        state.upstream_name = Some(upstream_name);
        if let Some(u) = upstream {
            state.upstream = Some(u);
        }
    }

    #[allow(dead_code)]
    pub(crate) fn attach_model(&self, model: String) {
        self.lock_state().model = Some(model);
    }

    #[allow(dead_code)]
    pub(crate) fn record_internal_error(&self, error: InternalError) {
        self.lock_state().internal_errors.push(error);
    }

    pub(crate) fn set_routing_trace(&self, trace: RoutingTrace) {
        self.lock_state().routing_trace = Some(trace);
    }

    pub(crate) fn set_terminal(&self, status: StatusCode, error_code: &'static str) {
        let mut state = self.lock_state();
        state.status = status.as_u16();
        state.error_code = Some(error_code);
    }

    pub(crate) fn set_upstream_error(
        &self,
        error_type: Option<String>,
        error_message: Option<String>,
    ) {
        let mut state = self.lock_state();
        state.upstream_error_type = error_type;
        state.upstream_error_message = error_message;
    }

    /// Synchronous publish of the terminal event. Returns silently if another
    /// path (Drop or a previous `finish`) already published.
    pub(crate) fn finish(&self) {
        if self
            .inner
            .finalized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let event = self.inner.make_request_event(None);
        self.inner.emit_terminated(&event);
        self.inner.bus.publish(RequestEventUpdate::final_(event));
    }

    /// Terminate with Tower timeout status (504 GATEWAY_TIMEOUT).
    /// Sets the error code to `TOWER_TIMEOUT` and publishes the final event.
    pub fn terminate_tower_timeout(&self) {
        self.set_terminal(StatusCode::GATEWAY_TIMEOUT, error_codes::TOWER_TIMEOUT);
        self.finish();
    }

    /// Publish the Phase-2 shadow `RequestStarted` lifecycle event.
    ///
    /// Should be called exactly once at handler entry, after the request has
    /// been parsed enough to know whether the client asked for a streaming
    /// response. Legacy telemetry path is unaffected.
    pub(crate) fn emit_request_started(&self, stream: bool) {
        let request_id = self.lock_state().request_id.clone();
        self.inner
            .bus
            .publish_lifecycle(LifecycleEvent::RequestStarted {
                event_id: self.inner.event_id.clone(),
                request_id,
                ts_ms: self.inner.started_unix_ms,
                stream,
            });
    }

    /// Publish an arbitrary Phase-2 shadow lifecycle event on the advisory bus.
    ///
    /// Call sites construct the event with `event_id: self.event_id().to_owned()`.
    /// This is fire-and-forget: overflow drops the event and increments the
    /// bus-side drop counter. Legacy telemetry path is unaffected regardless
    /// of whether the event is delivered.
    pub(crate) fn emit_lifecycle(&self, event: LifecycleEvent) {
        self.inner.bus.publish_lifecycle(event);
    }

    /// Snapshot current state and broadcast as a `Partial` update for live
    /// dashboard consumers. Does NOT flip the `finalized` flag, does NOT
    /// persist (the writer pipeline only persists `Final` events), and may be
    /// called any number of times during a request's lifetime.
    pub(crate) fn publish_partial_snapshot(&self) {
        if self.inner.finalized.load(Ordering::Acquire) {
            return;
        }
        let event = self.inner.make_request_event(None);
        self.inner.bus.publish(RequestEventUpdate::partial(event));
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, TerminalState> {
        self.inner
            .state
            .lock()
            .expect("terminal observer state mutex poisoned")
    }
}

impl Inner {
    /// RFC-0002 Phase 9: the observer only synthesises a MINIMAL RequestEvent
    /// carrying identity + termination fields. Full-fidelity fields (usage,
    /// cost, cache, streaming metrics) are owned by the assembler subscriber
    /// which builds them from LifecycleEvents.
    fn make_request_event(&self, fallback_error_code: Option<&'static str>) -> RequestEvent {
        let state = self
            .state
            .lock()
            .expect("terminal observer state mutex poisoned");
        let duration_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        RequestEvent {
            ts: self.started_unix_ms / 1_000,
            ts_ms: Some(self.started_unix_ms),
            event_id: Some(self.event_id.clone()),
            request_id: state.request_id.clone(),
            principal_id: state.principal_id.clone(),
            key_id: state.key_id.clone(),
            principal_kind: state.principal_kind.clone(),
            upstream: state.upstream,
            upstream_id: state.upstream_id,
            upstream_name: state.upstream_name.clone(),
            model: state.model.clone(),
            status: state.status,
            duration_ms,
            error_code: state
                .error_code
                .or(fallback_error_code)
                .map(|s| s.to_owned()),
            upstream_error_type: state.upstream_error_type.clone(),
            upstream_error_message: state.upstream_error_message.clone(),
            routing_trace: state.routing_trace.clone(),
            internal_errors: state.internal_errors.clone(),
            ..Default::default()
        }
    }
}

impl Inner {
    fn emit_terminated(&self, event: &RequestEvent) {
        let reason = match event.error_code.as_deref() {
            None => TerminationReason::Success,
            Some(code) if code == error_codes::TERMINAL_DROPPED => TerminationReason::Dropped,
            Some(code) => TerminationReason::ErrorCode(code.to_owned()),
        };
        let duration_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.bus
            .publish_lifecycle(LifecycleEvent::RequestTerminated {
                event_id: self.event_id.clone(),
                reason,
                client_status: event.status,
                duration_ms,
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
        let event = self.make_request_event(Some(error_codes::TERMINAL_DROPPED));
        self.emit_terminated(&event);
        self.bus.publish(RequestEventUpdate::final_(event));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::SystemClock;
    use crate::event_bus::{InMemoryBus, RequestEventBus, RequestEventPhase};

    #[tokio::test]
    async fn finish_publishes_final_with_event_id() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_finish".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        let expected_event_id = observer.event_id().to_owned();
        observer.set_terminal(StatusCode::OK, error_codes::UPSTREAM_4XX);
        observer.finish();

        let update = rx.recv().await.expect("update delivered");
        assert_eq!(update.phase, RequestEventPhase::Final);
        assert_eq!(update.event.request_id, "req_finish");
        assert_eq!(
            update.event.event_id.as_deref(),
            Some(expected_event_id.as_str())
        );
        assert_eq!(
            update.event.error_code.as_deref(),
            Some(error_codes::UPSTREAM_4XX)
        );
    }

    #[tokio::test]
    async fn drop_without_finish_publishes_terminal_dropped() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
        let clock: ClockHandle = Arc::new(SystemClock);
        {
            let observer = LifecycleContext::new(
                "req_drop".to_owned(),
                bus.clone() as Arc<dyn RequestEventBus>,
                &clock,
            );
            let _ = observer.event_id();
            // observer drops here without finish.
        }
        let update = rx.recv().await.expect("drop fallback delivered");
        assert_eq!(update.phase, RequestEventPhase::Final);
        assert_eq!(
            update.event.error_code.as_deref(),
            Some(error_codes::TERMINAL_DROPPED)
        );
    }

    #[tokio::test]
    async fn drop_after_finish_does_not_double_publish() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
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
        let first = rx.recv().await.expect("first delivered");
        assert_eq!(first.event.request_id, "req_norace");
        assert_eq!(
            first.event.error_code.as_deref(),
            Some(error_codes::UPSTREAM_4XX)
        );
        // No second event should be queued.
        let try_again = rx.try_recv();
        assert!(
            try_again.is_err(),
            "Drop must not republish after finish; got {try_again:?}"
        );
    }

    #[tokio::test]
    async fn publish_partial_snapshot_emits_partial_without_finalizing() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_partial".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.publish_partial_snapshot();
        observer.publish_partial_snapshot();
        observer.set_terminal(StatusCode::OK, error_codes::UPSTREAM_4XX);
        observer.finish();

        let first = rx.recv().await.expect("first partial");
        assert_eq!(first.phase, RequestEventPhase::Partial);
        let second = rx.recv().await.expect("second partial");
        assert_eq!(second.phase, RequestEventPhase::Partial);
        let final_ev = rx.recv().await.expect("final");
        assert_eq!(final_ev.phase, RequestEventPhase::Final);
        assert_eq!(
            final_ev.event.error_code.as_deref(),
            Some(error_codes::UPSTREAM_4XX)
        );
    }

    #[tokio::test]
    async fn publish_partial_snapshot_is_noop_after_finalize() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = LifecycleContext::new(
            "req_partial_after_final".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.set_terminal(StatusCode::OK, error_codes::UPSTREAM_4XX);
        observer.finish();
        observer.publish_partial_snapshot();
        let first = rx.recv().await.expect("final delivered");
        assert_eq!(first.phase, RequestEventPhase::Final);
        let try_again = rx.try_recv();
        assert!(
            try_again.is_err(),
            "publish_partial_snapshot must be a no-op after finish; got {try_again:?}"
        );
    }

    #[tokio::test]
    async fn shared_clone_drop_publishes_once() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
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
        // Last Arc still alive; no publish yet.
        assert!(
            rx.try_recv().is_err(),
            "publish must not fire while at least one observer Arc lives",
        );
        drop(clone2);
        let update = rx.recv().await.expect("publish on last drop");
        assert_eq!(
            update.event.error_code.as_deref(),
            Some(error_codes::TERMINAL_DROPPED)
        );
    }
}
