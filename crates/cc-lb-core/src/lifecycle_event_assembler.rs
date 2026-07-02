use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_lifecycle::{
    AuthInfo, CacheBreakpointLite, CacheBreakpointSourceLite, CostBreakdown, EventId,
    LifecycleEvent, ParseInfo, RequestCacheStateLite, RouteInfo, TerminationReason, UsageSnapshot,
};
use cc_lb_plugin_api::{InternalError, RoutingTrace};
use cc_lb_storage_api::types::{
    RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheState,
};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::event_bus::{RequestEventBus, RequestEventUpdate};

pub const DEFAULT_ASSEMBLER_MAP_CAP: usize = 4096;
pub const DEFAULT_ASSEMBLER_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// After receiving `RequestTerminated`, the assembler holds the partial for
/// this grace period so that late `Priced` and `CacheObserved` events (emitted
/// by their subscribers on separate tasks) can still merge into the row.
/// See RFC-0002 synthesis analysis for the ordering rationale.
const FINALIZATION_GRACE: Duration = Duration::from_millis(200);

/// How often the finalization tick runs. Must be small compared to
/// FINALIZATION_GRACE so terminated partials do not sit past their deadline
/// waiting for the next tick.
const FINALIZATION_TICK: Duration = Duration::from_millis(20);

pub struct RequestEventAssemblerHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl RequestEventAssemblerHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle event assembler task panicked");
        }
    }
}

/// Which rows the assembler produces per `RequestTerminated`.
///
/// - `LegacyOnly`: one row with `shadow_event_id=NULL`, `event_id=<lifecycle event_id>`.
///   Backwards-compat with the pre-Phase-9 writer.
/// - `ShadowOnly`: one row with `shadow_event_id=<lifecycle event_id>`,
///   `event_id=<new UUID>`. The RFC-0002 authoritative post-cutover shape.
/// - `Both`: writes BOTH rows above so diffing legacy vs shadow is trivial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssemblerMode {
    LegacyOnly,
    ShadowOnly,
    Both,
}

impl AssemblerMode {
    fn writes_legacy(self) -> bool {
        matches!(self, Self::LegacyOnly | Self::Both)
    }

    fn writes_shadow(self) -> bool {
        matches!(self, Self::ShadowOnly | Self::Both)
    }
}

pub fn spawn_request_event_assembler(
    rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    mode: AssemblerMode,
    bus: Option<Arc<dyn RequestEventBus>>,
) -> RequestEventAssemblerHandle {
    spawn_with_config(
        rx,
        storage,
        mode,
        bus,
        DEFAULT_ASSEMBLER_MAP_CAP,
        DEFAULT_ASSEMBLER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    mode: AssemblerMode,
    bus: Option<Arc<dyn RequestEventBus>>,
    map_cap: usize,
    ttl: Duration,
) -> RequestEventAssemblerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(assembler_loop(
        rx,
        storage,
        mode,
        bus,
        map_cap,
        ttl,
        shutdown_rx,
    ));
    RequestEventAssemblerHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    request_id: Option<String>,
    ts_ms: u64,
    stream: bool,
    parse: Option<ParseInfo>,
    auth: Option<AuthInfo>,
    route: Option<RouteInfo>,
    limit_reservation_id: Option<String>,
    limit_amount: Option<u64>,
    upstream_id: Option<Uuid>,
    upstream_attempt_num: Option<u32>,
    upstream_response_status: Option<u16>,
    usage: UsageSnapshot,
    usage_seen: bool,
    stream_success: Option<u64>,
    stream_error_type: Option<String>,
    stream_error_message: Option<String>,
    cost: Option<CostBreakdown>,
    cache_state: Option<RequestCacheState>,
    cache_control_block_count: Option<u64>,
    cache_breakpoints: Vec<RequestCacheBreakpoint>,
    cache_prefix_hash: Option<String>,
    termination: Option<TerminationInfo>,

    /// `RouteCompleted.routing_trace` top-level field. Populated on both Ok
    /// (redundant with `route.routing_trace`) and Err (only source) paths.
    routing_trace: Option<RoutingTrace>,

    limit_reserve_ms: Option<u64>,

    bulkhead_wait_ms: Option<u64>,
    dns_ms: Option<u64>,
    connect_ms: Option<u64>,
    connection_reused: Option<bool>,
    shape_ms: Option<u64>,
    sign_ms: Option<u64>,
    upstream_ttfb_ms: Option<u64>,

    upstream_body_ms: Option<u64>,
    first_body_chunk_ms: Option<u64>,
    limit_reconcile_ms: Option<u64>,
    observability_post_ms: Option<u64>,
    proxy_setup_ms: Option<u64>,
    internal_errors: Vec<InternalError>,

    stream_body_bytes: Option<u64>,
    stream_body_chunk_count: Option<u64>,
    stream_message_start_ms: Option<u64>,
    stream_content_block_start_ms: Option<u64>,
    stream_first_content_delta_ms: Option<u64>,
    stream_last_content_delta_ms: Option<u64>,
    stream_message_stop_ms: Option<u64>,
    stream_last_chunk_ms: Option<u64>,
    stream_total_ms: Option<u64>,
    stream_content_delta_count: Option<u64>,
    stream_ping_count: Option<u64>,
    stream_inter_token_avg_ms: Option<u64>,
}

struct TerminationInfo {
    reason: TerminationReason,
    client_status: u16,
    duration_ms: u64,
    deadline: Instant,
    expects_priced: bool,
    expects_cache: bool,
}

impl TerminationInfo {
    fn is_ready(&self, partial: &Partial) -> bool {
        (!self.expects_priced || partial.cost.is_some())
            && (!self.expects_cache || partial.cache_state.is_some())
    }
}

impl Partial {
    fn new(now: Instant) -> Self {
        Self {
            inserted_at: Some(now),
            ..Self::default()
        }
    }

    fn touch(&mut self, now: Instant) {
        self.inserted_at = Some(now);
    }

    fn orphan() -> Self {
        Self::default()
    }
}

async fn assembler_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    mode: AssemblerMode,
    bus: Option<Arc<dyn RequestEventBus>>,
    map_cap: usize,
    ttl: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    let mut sweeper = tokio::time::interval(SWEEP_INTERVAL);
    sweeper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    sweeper.tick().await;
    let mut finalization_tick = tokio::time::interval(FINALIZATION_TICK);
    finalization_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    finalization_tick.tick().await;

    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(event) => handle_event(&*storage, bus.as_deref(), &mut partials, mode, map_cap, event).await,
                    None => break,
                }
            }
            _ = finalization_tick.tick() => {
                flush_expired_terminations(&*storage, bus.as_deref(), &mut partials, mode).await;
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(
            &*storage,
            bus.as_deref(),
            &mut partials,
            mode,
            map_cap,
            event,
        )
        .await;
    }
    force_flush_terminations(&*storage, bus.as_deref(), &mut partials, mode).await;
}

#[allow(clippy::too_many_arguments)]
async fn write_finalized_rows(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    mode: AssemblerMode,
    event_id: &EventId,
    partial: &Partial,
    reason: &TerminationReason,
    client_status: u16,
    duration_ms: u64,
    is_orphan: bool,
) {
    let base_row = finalize_base(partial, reason, client_status, duration_ms, is_orphan);
    let mut rows: Vec<RequestEvent> = Vec::new();
    if mode.writes_legacy() {
        let mut legacy = base_row.clone();
        legacy.event_id = Some(event_id.clone());
        legacy.shadow_event_id = None;
        rows.push(legacy);
    }
    if mode.writes_shadow() {
        let mut shadow = base_row;
        shadow.event_id = Some(Uuid::now_v7().to_string());
        shadow.shadow_event_id = Some(event_id.clone());
        rows.push(shadow);
    }
    let mut wrote = 0u64;
    for row in &rows {
        match storage.append_request_event(row).await {
            Ok(()) => {
                wrote += 1;
                if let Some(bus) = bus
                    && row.shadow_event_id.is_some()
                {
                    bus.publish(RequestEventUpdate::final_(row.clone()));
                }
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    shadow_event_id = %event_id,
                    row_shape = if row.shadow_event_id.is_some() { "shadow" } else { "legacy" },
                    "lifecycle event assembler: failed to persist row",
                );
                cc_lb_observability::increment_dropped_events_by(
                    "lifecycle_assembler_storage_error",
                    1,
                );
            }
        }
    }
    if wrote > 0 {
        let outcome = if is_orphan {
            "written_orphan"
        } else {
            "written"
        };
        metrics::counter!("cc_lb_lifecycle_assembler_rows_total", "outcome" => outcome)
            .increment(wrote);
    }
}

async fn flush_expired_terminations(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    partials: &mut HashMap<EventId, Partial>,
    mode: AssemblerMode,
) {
    let now = Instant::now();
    let expired: Vec<EventId> = partials
        .iter()
        .filter_map(|(id, p)| {
            p.termination
                .as_ref()
                .filter(|t| now >= t.deadline)
                .map(|_| id.clone())
        })
        .collect();
    for event_id in expired {
        if let Some(partial) = partials.remove(&event_id) {
            let term = partial
                .termination
                .as_ref()
                .expect("expired implies termination present");
            metrics::counter!(
                "cc_lb_lifecycle_assembler_rows_total",
                "outcome" => "written_after_grace"
            )
            .increment(1);
            write_finalized_rows(
                storage,
                bus,
                mode,
                &event_id,
                &partial,
                &term.reason,
                term.client_status,
                term.duration_ms,
                false,
            )
            .await;
        }
    }
}

async fn force_flush_terminations(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    partials: &mut HashMap<EventId, Partial>,
    mode: AssemblerMode,
) {
    let pending: Vec<EventId> = partials
        .iter()
        .filter_map(|(id, p)| p.termination.as_ref().map(|_| id.clone()))
        .collect();
    for event_id in pending {
        if let Some(partial) = partials.remove(&event_id) {
            let term = partial
                .termination
                .as_ref()
                .expect("pending implies termination present");
            write_finalized_rows(
                storage,
                bus,
                mode,
                &event_id,
                &partial,
                &term.reason,
                term.client_status,
                term.duration_ms,
                false,
            )
            .await;
        }
    }
}

async fn handle_event(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    partials: &mut HashMap<EventId, Partial>,
    mode: AssemblerMode,
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if let LifecycleEvent::RequestTerminated {
        reason,
        client_status,
        duration_ms,
        limit_reconcile_ms,
        observability_post_ms,
        proxy_setup_ms,
        upstream_body_ms,
        first_body_chunk_ms,
        internal_errors,
        ..
    } = &event
    {
        let existing = partials.remove(&event_id);
        let is_orphan = existing.is_none();
        if is_orphan {
            metrics::counter!(
                "cc_lb_lifecycle_assembler_rows_total",
                "outcome" => "terminated_without_partial"
            )
            .increment(1);
            let mut partial = Partial::orphan();
            partial.limit_reconcile_ms = *limit_reconcile_ms;
            partial.observability_post_ms = *observability_post_ms;
            partial.proxy_setup_ms = *proxy_setup_ms;
            partial.upstream_body_ms = *upstream_body_ms;
            partial.first_body_chunk_ms = *first_body_chunk_ms;
            partial.internal_errors = internal_errors.clone();
            write_finalized_rows(
                storage,
                bus,
                mode,
                &event_id,
                &partial,
                reason,
                *client_status,
                *duration_ms,
                true,
            )
            .await;
            return;
        }
        let mut partial = existing.expect("checked !is_orphan");
        partial.limit_reconcile_ms = *limit_reconcile_ms;
        partial.observability_post_ms = *observability_post_ms;
        partial.proxy_setup_ms = *proxy_setup_ms;
        partial.upstream_body_ms = *upstream_body_ms;
        if partial.first_body_chunk_ms.is_none() {
            partial.first_body_chunk_ms = *first_body_chunk_ms;
        }
        partial.internal_errors = internal_errors.clone();
        let expects_priced = partial.usage_seen && partial.cost.is_none();
        let expects_cache =
            partial.upstream_response_status.is_some() && partial.cache_state.is_none();
        let termination = TerminationInfo {
            reason: reason.clone(),
            client_status: *client_status,
            duration_ms: *duration_ms,
            deadline: now + FINALIZATION_GRACE,
            expects_priced,
            expects_cache,
        };
        if termination.is_ready(&partial) {
            write_finalized_rows(
                storage,
                bus,
                mode,
                &event_id,
                &partial,
                &termination.reason,
                termination.client_status,
                termination.duration_ms,
                false,
            )
            .await;
            return;
        }
        partial.termination = Some(termination);
        partial.touch(now);
        partials.insert(event_id, partial);
        return;
    }

    let partial = partials
        .entry(event_id.clone())
        .or_insert_with(|| Partial::new(now));
    partial.touch(now);
    merge(partial, event);

    if partial
        .termination
        .as_ref()
        .is_some_and(|t| t.is_ready(partial))
        && let Some(partial) = partials.remove(&event_id)
    {
        let term = partial
            .termination
            .as_ref()
            .expect("readiness implies termination present");
        write_finalized_rows(
            storage,
            bus,
            mode,
            &event_id,
            &partial,
            &term.reason,
            term.client_status,
            term.duration_ms,
            false,
        )
        .await;
    }

    if partials.len() > map_cap {
        drop_oldest(partials);
    }
}

fn merge(partial: &mut Partial, event: LifecycleEvent) {
    match event {
        LifecycleEvent::RequestStarted {
            request_id,
            ts_ms,
            stream,
            ..
        } => {
            partial.request_id = Some(request_id);
            partial.ts_ms = ts_ms;
            partial.stream = stream;
        }
        LifecycleEvent::ParseCompleted {
            result: Ok(info), ..
        } => {
            partial.cache_control_block_count = info.cache_control_block_count;
            partial.cache_breakpoints = info
                .cache_breakpoints
                .iter()
                .map(cache_breakpoint_from_lite)
                .collect();
            partial.cache_prefix_hash = info.cache_prefix_hash.clone();
            partial.parse = Some(info);
        }
        LifecycleEvent::ParseCompleted { .. } => {}
        LifecycleEvent::AuthCompleted {
            result: Ok(info), ..
        } => {
            partial.auth = Some(info);
        }
        LifecycleEvent::AuthCompleted { .. } => {}
        LifecycleEvent::RouteCompleted {
            result,
            routing_trace,
            ..
        } => {
            if let Some(trace) = routing_trace {
                partial.routing_trace = Some(trace);
            }
            if let Ok(info) = result {
                partial.routing_trace = partial
                    .routing_trace
                    .clone()
                    .or_else(|| info.routing_trace.clone());
                partial.route = Some(info);
            }
        }
        LifecycleEvent::LimitDecision {
            decision:
                cc_lb_lifecycle::LimitDecisionKind::Reserved {
                    reservation_id,
                    amount,
                    limit_reserve_ms,
                },
            ..
        } => {
            partial.limit_reservation_id = Some(reservation_id);
            partial.limit_amount = Some(amount);
            partial.limit_reserve_ms = limit_reserve_ms;
        }
        LifecycleEvent::LimitDecision { .. } => {}
        LifecycleEvent::UpstreamAttempt {
            attempt_num,
            upstream_id,
            ..
        } => {
            partial.upstream_id = Some(upstream_id);
            partial.upstream_attempt_num = Some(attempt_num);
        }
        LifecycleEvent::UpstreamResponseStarted {
            status,
            headers: _,
            bulkhead_wait_ms,
            dns_ms,
            connect_ms,
            connection_reused,
            shape_ms,
            sign_ms,
            upstream_ttfb_ms,
            ..
        } => {
            partial.upstream_response_status = Some(status);
            partial.bulkhead_wait_ms = bulkhead_wait_ms;
            partial.dns_ms = dns_ms;
            partial.connect_ms = connect_ms;
            partial.connection_reused = connection_reused;
            partial.shape_ms = shape_ms;
            partial.sign_ms = sign_ms;
            partial.upstream_ttfb_ms = upstream_ttfb_ms;
        }
        LifecycleEvent::UsageObserved {
            usage,
            source: _source,
            ..
        } => {
            partial.usage = usage;
            partial.usage_seen = true;
        }
        LifecycleEvent::StreamCompleted { result, .. } => match result {
            Ok(success) => {
                partial.usage = success.usage;
                partial.usage_seen = true;
                partial.stream_success = Some(success.sse_event_count);
                partial.stream_body_bytes = success.body_bytes;
                partial.stream_body_chunk_count = success.body_chunk_count;
                if success.first_body_chunk_ms.is_some() {
                    partial.first_body_chunk_ms = success.first_body_chunk_ms;
                }
                partial.stream_message_start_ms = success.stream_message_start_ms;
                partial.stream_content_block_start_ms = success.stream_content_block_start_ms;
                partial.stream_first_content_delta_ms = success.stream_first_content_delta_ms;
                partial.stream_last_content_delta_ms = success.stream_last_content_delta_ms;
                partial.stream_message_stop_ms = success.stream_message_stop_ms;
                partial.stream_last_chunk_ms = success.stream_last_chunk_ms;
                partial.stream_total_ms = success.stream_total_ms;
                partial.stream_content_delta_count = success.content_delta_count;
                partial.stream_ping_count = success.ping_count;
                partial.stream_inter_token_avg_ms = success.inter_token_avg_ms;
            }
            Err(error) => {
                partial.stream_error_type = Some(error.error_type);
                partial.stream_error_message = Some(error.error_message);
            }
        },
        LifecycleEvent::RequestTerminated { .. } => {}
        LifecycleEvent::Priced { cost, .. } => {
            partial.cost = Some(cost);
        }
        LifecycleEvent::CacheObserved { cache_state, .. } => {
            partial.cache_state = Some(cache_state_from_lite(cache_state));
        }
        _ => {}
    }
}

fn finalize_base(
    partial: &Partial,
    reason: &TerminationReason,
    client_status: u16,
    duration_ms: u64,
    is_orphan: bool,
) -> RequestEvent {
    let ts_ms = partial.ts_ms;
    let request_id = partial
        .request_id
        .clone()
        .unwrap_or_else(|| "req_unknown_shadow".to_owned());
    let error_code = if is_orphan {
        Some("terminal_without_partial".to_owned())
    } else {
        match reason {
            TerminationReason::Success => None,
            TerminationReason::Dropped => Some("terminal_dropped".to_owned()),
            TerminationReason::ErrorCode(code) => Some(code.clone()),
            _ => None,
        }
    };
    let (principal_id, key_id, principal_kind) = partial
        .auth
        .as_ref()
        .map(|a| {
            (
                Some(a.principal_id.clone()),
                a.key_id.clone(),
                a.principal_kind.clone(),
            )
        })
        .unwrap_or_default();
    let (upstream_id, upstream_name, route_model, route_ms, route_routing_trace) = partial
        .route
        .as_ref()
        .map(|r| {
            (
                Some(r.upstream_id),
                Some(r.upstream_name.clone()),
                r.model.clone(),
                r.route_ms,
                r.routing_trace.clone(),
            )
        })
        .unwrap_or_default();
    let model = route_model.or_else(|| partial.parse.as_ref().and_then(|p| p.model.clone()));
    let auth_ms = partial.auth.as_ref().and_then(|a| a.auth_ms);
    let routing_trace = partial.routing_trace.clone().or(route_routing_trace);
    let (thread_id, message_id, message_index, message_count, cache_control_message_indices) =
        partial
            .parse
            .as_ref()
            .map(|p| {
                (
                    p.thread_id.clone(),
                    p.message_id.clone(),
                    p.message_index,
                    p.message_count,
                    p.cache_control_message_indices.clone(),
                )
            })
            .unwrap_or_default();

    let cost = partial.cost.clone().unwrap_or_default();
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id,
        principal_id,
        key_id,
        principal_kind,
        upstream_id,
        upstream_name,
        model,
        status: client_status,
        duration_ms,
        error_code,
        upstream_error_type: partial.stream_error_type.clone(),
        upstream_error_message: partial.stream_error_message.clone(),
        input_tokens: partial.usage_seen.then_some(partial.usage.input_tokens),
        output_tokens: partial.usage_seen.then_some(partial.usage.output_tokens),
        cache_creation_input_tokens: partial
            .usage_seen
            .then_some(partial.usage.cache_creation_input_tokens),
        cache_creation_input_tokens_5m: (partial.usage.cache_creation_input_tokens_5m > 0)
            .then_some(partial.usage.cache_creation_input_tokens_5m),
        cache_creation_input_tokens_1h: (partial.usage.cache_creation_input_tokens_1h > 0)
            .then_some(partial.usage.cache_creation_input_tokens_1h),
        cache_read_input_tokens: partial
            .usage_seen
            .then_some(partial.usage.cache_read_input_tokens),
        cache_state: partial.cache_state,
        cache_control_block_count: partial.cache_control_block_count,
        cache_breakpoints: partial.cache_breakpoints.clone(),
        cache_prefix_hash: partial.cache_prefix_hash.clone(),
        cost_usd_micros: cost.total_micros,
        cost_input_micros: cost.input_micros,
        cost_output_micros: cost.output_micros,
        cost_cache_creation_5m_micros: cost.cache_creation_5m_micros,
        cost_cache_creation_1h_micros: cost.cache_creation_1h_micros,
        cost_cache_read_micros: cost.cache_read_micros,
        thinking_tokens: (partial.usage.thinking_tokens > 0)
            .then_some(partial.usage.thinking_tokens),
        web_search_requests: (partial.usage.web_search_requests > 0)
            .then_some(partial.usage.web_search_requests),
        web_fetch_requests: (partial.usage.web_fetch_requests > 0)
            .then_some(partial.usage.web_fetch_requests),
        service_tier: partial.usage.service_tier.clone(),
        inference_geo: partial.usage.inference_geo.clone(),
        sse_event_count: partial.stream_success,
        thread_id,
        message_id,
        message_index,
        message_count,
        cache_control_message_indices,
        auth_ms,
        route_ms,
        limit_reserve_ms: partial.limit_reserve_ms,
        bulkhead_wait_ms: partial.bulkhead_wait_ms,
        dns_ms: partial.dns_ms,
        connect_ms: partial.connect_ms,
        connection_reused: partial.connection_reused,
        limit_reconcile_ms: partial.limit_reconcile_ms,
        observability_post_ms: partial.observability_post_ms,
        proxy_setup_ms: partial.proxy_setup_ms,
        shape_ms: partial.shape_ms,
        sign_ms: partial.sign_ms,
        upstream_ttfb_ms: partial.upstream_ttfb_ms,
        upstream_body_ms: partial.upstream_body_ms,
        first_body_chunk_ms: partial.first_body_chunk_ms,
        body_chunk_count: partial.stream_body_chunk_count,
        body_bytes: partial.stream_body_bytes,
        stream_message_start_ms: partial.stream_message_start_ms,
        stream_content_block_start_ms: partial.stream_content_block_start_ms,
        stream_first_content_delta_ms: partial.stream_first_content_delta_ms,
        stream_last_content_delta_ms: partial.stream_last_content_delta_ms,
        stream_message_stop_ms: partial.stream_message_stop_ms,
        stream_last_chunk_ms: partial.stream_last_chunk_ms,
        stream_total_ms: partial.stream_total_ms,
        content_delta_count: partial.stream_content_delta_count,
        ping_count: partial.stream_ping_count,
        inter_token_avg_ms: partial.stream_inter_token_avg_ms,
        routing_trace,
        internal_errors: partial.internal_errors.clone(),
        iterations: partial.usage.iterations.clone(),
        ..Default::default()
    }
}

fn cache_state_from_lite(state: RequestCacheStateLite) -> RequestCacheState {
    match state {
        RequestCacheStateLite::Hit => RequestCacheState::Hit,
        RequestCacheStateLite::Write => RequestCacheState::Write,
        RequestCacheStateLite::Refresh => RequestCacheState::Refresh,
        RequestCacheStateLite::Miss => RequestCacheState::Miss,
        RequestCacheStateLite::None => RequestCacheState::None,
        RequestCacheStateLite::Unknown => RequestCacheState::Unknown,
    }
}

fn cache_breakpoint_source_from_lite(
    source: CacheBreakpointSourceLite,
) -> RequestCacheBreakpointSource {
    match source {
        CacheBreakpointSourceLite::System => RequestCacheBreakpointSource::System,
        CacheBreakpointSourceLite::Tools => RequestCacheBreakpointSource::Tools,
        CacheBreakpointSourceLite::Message => RequestCacheBreakpointSource::Message,
    }
}

fn cache_breakpoint_from_lite(lite: &CacheBreakpointLite) -> RequestCacheBreakpoint {
    RequestCacheBreakpoint {
        block_index: lite.block_index,
        source: cache_breakpoint_source_from_lite(lite.source),
        path: lite.path.clone(),
        message_index: lite.message_index,
        ttl: lite.ttl.clone(),
        prefix_hash: lite.prefix_hash.clone(),
        prefix_token_count: lite.prefix_token_count,
    }
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_lifecycle_assembler_rows_total",
            "outcome" => "orphan_ttl_evicted"
        )
        .increment(removed as u64);
    }
}

fn drop_oldest(partials: &mut HashMap<EventId, Partial>) {
    let Some((oldest_key, _)) = partials
        .iter()
        .min_by_key(|(_, p)| p.inserted_at.unwrap_or_else(Instant::now))
        .map(|(k, v)| (k.clone(), v.inserted_at))
    else {
        return;
    };
    partials.remove(&oldest_key);
    metrics::counter!(
        "cc_lb_lifecycle_assembler_rows_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cc_lb_lifecycle::{AuthFailure, ParseFailure, StreamError, StreamSuccess};
    use cc_lb_storage_api::{RequestEvent, StorageResult};
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct CapturingStore {
        rows: StdMutex<Vec<RequestEvent>>,
    }

    #[async_trait]
    impl RequestEventStore for CapturingStore {
        async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<()> {
            self.rows.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn query_request_events(
            &self,
            _since: u64,
            _until: u64,
            _limit: usize,
        ) -> StorageResult<Vec<RequestEvent>> {
            Ok(Vec::new())
        }
    }

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn success_terminated_persists_row_with_shadow_event_id() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle =
            spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly, None);

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-1"),
            request_id: "req-1".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::AuthCompleted {
            event_id: eid("legacy-1"),
            result: Ok(AuthInfo {
                principal_id: "p1".into(),
                key_id: Some("k1".into()),
                principal_kind: Some("api_key".into()),
                auth_ms: None,
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-1"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 42,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].shadow_event_id.as_deref(), Some("legacy-1"));
        assert_eq!(rows[0].status, 200);
        assert_eq!(rows[0].duration_ms, 42);
        assert_eq!(rows[0].error_code, None);
        assert_eq!(rows[0].principal_id.as_deref(), Some("p1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stream_error_terminated_carries_error_type_and_message() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle =
            spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly, None);

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-2"),
            request_id: "req-2".into(),
            ts_ms: 1_730_000_000_000,
            stream: true,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::StreamCompleted {
            event_id: eid("legacy-2"),
            result: Err(StreamError {
                error_type: "overloaded_error".into(),
                error_message: "please retry".into(),
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-2"),
            reason: TerminationReason::ErrorCode("upstream_stream_error".into()),
            client_status: 200,
            duration_ms: 1_234,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].upstream_error_type.as_deref(),
            Some("overloaded_error")
        );
        assert_eq!(rows[0].error_code.as_deref(), Some("upstream_stream_error"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parse_failed_terminated_still_persists_error_code() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle =
            spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly, None);

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-3"),
            request_id: "req-3".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::ParseCompleted {
            event_id: eid("legacy-3"),
            result: Err(ParseFailure::BodyTooLarge { limit_bytes: 1000 }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-3"),
            reason: TerminationReason::ErrorCode("body_too_large".into()),
            client_status: 413,
            duration_ms: 5,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, 413);
        assert_eq!(rows[0].error_code.as_deref(), Some("body_too_large"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_partial_writes_orphan_row() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle =
            spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly, None);

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan-terminated"),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 7,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].shadow_event_id.as_deref(),
            Some("orphan-terminated")
        );
        assert_eq!(rows[0].request_id, "req_unknown_shadow");
        assert_eq!(rows[0].status, 499);
        assert_eq!(rows[0].duration_ms, 7);
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("terminal_without_partial")
        );
        assert!(rows[0].input_tokens.is_none());
        assert!(rows[0].cache_state.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ignores_unused_auth_failure_variant() {
        let _ = AuthFailure::AuthenticationFailed { http_status: 401 };
        let _ = StreamSuccess {
            usage: UsageSnapshot::default(),
            sse_event_count: 0,
            ..Default::default()
        };
        let _ = cc_lb_lifecycle::UsageSource::NonStreamBody;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn both_mode_persists_legacy_and_shadow_rows_from_same_terminate() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), AssemblerMode::Both, None);

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-both-1"),
            request_id: "req-both-1".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-both-1"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 7,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(
            rows.len(),
            2,
            "Both mode must persist both legacy and shadow rows"
        );
        let legacy = rows
            .iter()
            .find(|r| r.shadow_event_id.is_none())
            .expect("legacy row");
        let shadow = rows
            .iter()
            .find(|r| r.shadow_event_id.as_deref() == Some("legacy-both-1"))
            .expect("shadow row");
        assert_eq!(legacy.event_id.as_deref(), Some("legacy-both-1"));
        assert_ne!(shadow.event_id.as_deref(), Some("legacy-both-1"));
        assert!(shadow.event_id.is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shadow_row_write_republishes_to_bus_for_admin_sse() {
        use crate::event_bus::{InMemoryBus, RequestEventPhase};

        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let crate::event_bus::BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
            AssemblerMode::ShadowOnly,
            Some(bus.clone() as Arc<dyn RequestEventBus>),
        );

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("sse-1"),
            request_id: "req-sse-1".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("sse-1"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 10,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let update = tokio::time::timeout(Duration::from_millis(200), broadcast_rx.recv())
            .await
            .expect("bus update within timeout")
            .expect("bus receiver did not close");
        assert_eq!(update.phase, RequestEventPhase::Final);
        assert_eq!(update.event.shadow_event_id.as_deref(), Some("sse-1"));
    }
}
