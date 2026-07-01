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
//!    state.
//! 3. The SSE parser calls `update_usage` to refresh token counters mid-flight.
//! 4. On normal completion the lifecycle calls [`TerminalObserver::finish`]
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

use cc_lb_plugin_api::{InternalError, RoutingTrace};
use cc_lb_storage_api::{
    RequestCacheBreakpoint, RequestCacheState, RequestEvent, RequestEventUpstream,
};
use http::StatusCode;
use uuid::Uuid;

use crate::clock::{ClockHandle, unix_millis};
use crate::event_bus::{RequestEventBus, RequestEventUpdate};
use crate::usage_parser::UsageCounts;

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

pub(crate) type TerminalObserver = LifecycleContext;

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
    usage: UsageCounts,
    cache_state: Option<RequestCacheState>,
    cache_control_block_count: Option<u64>,
    cache_breakpoints: Vec<RequestCacheBreakpoint>,
    cache_prefix_hash: Option<String>,
    /// Set by success paths (non-stream complete, stream complete) that build
    /// the full event with all timing/cost/streaming metric fields inline.
    /// When present, `make_request_event` returns this as-is with `event_id`
    /// forced to the observer's, rather than synthesizing from state.
    prebuilt_event: Option<RequestEvent>,
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

    pub(crate) fn update_usage(&self, usage: &UsageCounts) {
        self.lock_state().usage = usage.clone();
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

    /// Replace the synthesized event with a fully-built one (success paths
    /// that already construct the entire event inline with timing/cost/stream
    /// metric fields). `make_request_event` will return this verbatim with
    /// `event_id` forced to the observer's value.
    pub(crate) fn set_prebuilt_event(&self, event: RequestEvent) {
        self.lock_state().prebuilt_event = Some(event);
    }

    #[allow(dead_code)]
    pub(crate) fn attach_cache_metadata(
        &self,
        cache_state: Option<RequestCacheState>,
        cache_control_block_count: Option<u64>,
        cache_breakpoints: Vec<RequestCacheBreakpoint>,
        cache_prefix_hash: Option<String>,
    ) {
        let mut state = self.lock_state();
        state.cache_state = cache_state;
        state.cache_control_block_count = cache_control_block_count;
        state.cache_breakpoints = cache_breakpoints;
        state.cache_prefix_hash = cache_prefix_hash;
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
        self.inner.bus.publish(RequestEventUpdate::final_(event));
    }

    /// Terminate with Tower timeout status (504 GATEWAY_TIMEOUT).
    /// Sets the error code to `TOWER_TIMEOUT` and publishes the final event.
    pub fn terminate_tower_timeout(&self) {
        self.set_terminal(StatusCode::GATEWAY_TIMEOUT, error_codes::TOWER_TIMEOUT);
        self.finish();
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
    fn make_request_event(&self, fallback_error_code: Option<&'static str>) -> RequestEvent {
        let state = self
            .state
            .lock()
            .expect("terminal observer state mutex poisoned");
        if let Some(mut event) = state.prebuilt_event.clone() {
            event.event_id = Some(self.event_id.clone());
            if event.error_code.is_none() {
                event.error_code = state
                    .error_code
                    .or(fallback_error_code)
                    .map(|s| s.to_owned());
            }
            return event;
        }
        let duration_ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let mut event = RequestEvent {
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
            cache_state: state.cache_state,
            cache_control_block_count: state.cache_control_block_count,
            cache_breakpoints: state.cache_breakpoints.clone(),
            cache_prefix_hash: state.cache_prefix_hash.clone(),
            input_tokens: state.usage.present.then_some(state.usage.input_tokens),
            output_tokens: state.usage.present.then_some(state.usage.output_tokens),
            cache_creation_input_tokens: state
                .usage
                .present
                .then_some(state.usage.cache_creation_input_tokens),
            cache_creation_input_tokens_5m: (state.usage.cache_creation_input_tokens_5m > 0)
                .then_some(state.usage.cache_creation_input_tokens_5m),
            cache_creation_input_tokens_1h: (state.usage.cache_creation_input_tokens_1h > 0)
                .then_some(state.usage.cache_creation_input_tokens_1h),
            cache_read_input_tokens: state
                .usage
                .present
                .then_some(state.usage.cache_read_input_tokens),
            ..Default::default()
        };
        state.usage.apply_extras_to(&mut event);
        event
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
        let observer = TerminalObserver::new(
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
            let observer = TerminalObserver::new(
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
            let observer = TerminalObserver::new(
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
    async fn update_usage_is_reflected_in_final() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = TerminalObserver::new(
            "req_usage".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );

        let usage = UsageCounts {
            present: true,
            input_tokens: 42,
            output_tokens: 100,
            thinking_tokens: 16,
            ..UsageCounts::default()
        };
        observer.update_usage(&usage);
        observer.set_terminal(StatusCode::OK, error_codes::UPSTREAM_4XX);
        observer.finish();

        let update = rx.recv().await.expect("update delivered");
        assert_eq!(update.event.input_tokens, Some(42));
        assert_eq!(update.event.output_tokens, Some(100));
        assert_eq!(update.event.thinking_tokens, Some(16));
    }

    #[tokio::test]
    async fn publish_partial_snapshot_emits_partial_without_finalizing() {
        let bus = Arc::new(InMemoryBus::new());
        let mut rx = bus.attach_writer(8);
        let clock: ClockHandle = Arc::new(SystemClock);
        let observer = TerminalObserver::new(
            "req_partial".to_owned(),
            bus.clone() as Arc<dyn RequestEventBus>,
            &clock,
        );
        observer.update_usage(&UsageCounts {
            present: true,
            input_tokens: 5,
            output_tokens: 12,
            ..UsageCounts::default()
        });
        observer.publish_partial_snapshot();
        observer.update_usage(&UsageCounts {
            present: true,
            input_tokens: 5,
            output_tokens: 42,
            ..UsageCounts::default()
        });
        observer.publish_partial_snapshot();
        observer.set_terminal(StatusCode::OK, error_codes::UPSTREAM_4XX);
        observer.finish();

        let first = rx.recv().await.expect("first partial");
        assert_eq!(first.phase, RequestEventPhase::Partial);
        assert_eq!(first.event.output_tokens, Some(12));
        let second = rx.recv().await.expect("second partial");
        assert_eq!(second.phase, RequestEventPhase::Partial);
        assert_eq!(second.event.output_tokens, Some(42));
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
        let observer = TerminalObserver::new(
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
        let observer = TerminalObserver::new(
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
