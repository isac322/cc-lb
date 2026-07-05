use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_lb_lifecycle::{
    AuthInfo, CacheBreakpointLite, CacheBreakpointSourceLite,
    CostBreakdown as LifecycleCostBreakdown, EventId, LifecycleEvent, ParseInfo,
    RequestCacheStateLite, RouteInfo, TerminationReason, UsageSnapshot,
};
use cc_lb_plugin_api::{InternalError, RoutingTrace};
use cc_lb_pricing::virtual_cost_micros_full;
use cc_lb_storage_api::types::{
    RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheState, RequestEventPartial,
    RequestEventUpstream,
};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::event_bus::{RequestEventBus, RequestEventUpdate};
use crate::lifecycle::{
    CostBreakdownOptions, cost_breakdown_to_event_options, pricing_upstream_kind_from_label,
};
use crate::metrics_labels::PartialTrigger;

pub const DEFAULT_ASSEMBLER_MAP_CAP: usize = 4096;
pub const DEFAULT_ASSEMBLER_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// After receiving `RequestTerminated`, the assembler holds the partial for
/// this grace period so that late `CacheObserved` events (emitted by a separate
/// subscriber task) can still merge into the row. Pricing is computed inline.
/// See RFC-0002 synthesis analysis for the ordering rationale.
const FINALIZATION_GRACE: Duration = Duration::from_millis(200);

/// How often the finalization tick runs. Must be small compared to
/// FINALIZATION_GRACE so terminated partials do not sit past their deadline
/// waiting for the next tick.
const FINALIZATION_TICK: Duration = Duration::from_millis(20);
const PARTIAL_USAGE_THROTTLE: Duration = Duration::from_millis(250);

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

pub fn spawn_request_event_assembler(
    rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    bus: Option<Arc<dyn RequestEventBus>>,
) -> RequestEventAssemblerHandle {
    spawn_with_config(
        rx,
        storage,
        bus,
        DEFAULT_ASSEMBLER_MAP_CAP,
        DEFAULT_ASSEMBLER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    bus: Option<Arc<dyn RequestEventBus>>,
    map_cap: usize,
    ttl: Duration,
) -> RequestEventAssemblerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(assembler_loop(rx, storage, bus, map_cap, ttl, shutdown_rx));
    RequestEventAssemblerHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    event_id: EventId,
    inserted_at: Option<Instant>,
    request_id: Option<String>,
    ts_ms: u64,
    last_partial_emit_ts: Option<Instant>,
    stream: bool,
    parse: Option<ParseInfo>,
    auth: Option<AuthInfo>,
    route: Option<RouteInfo>,
    upstream_response_status: Option<u16>,
    usage: UsageSnapshot,
    usage_seen: bool,
    stream_success: Option<u64>,
    stream_error_type: Option<String>,
    stream_error_message: Option<String>,
    cost: Option<LifecycleCostBreakdown>,
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
    fn new(now: Instant, event_id: EventId) -> Self {
        Self {
            event_id,
            inserted_at: Some(now),
            ..Self::default()
        }
    }

    fn touch(&mut self, now: Instant) {
        self.inserted_at = Some(now);
    }

    fn orphan(event_id: EventId) -> Self {
        Self {
            event_id,
            ..Self::default()
        }
    }

    fn snapshot_partial(&self, now_ms: u64) -> RequestEventPartial {
        let (principal_id, key_id, principal_kind) = self
            .auth
            .as_ref()
            .map(|auth| {
                (
                    Some(auth.principal_id.clone()),
                    auth.key_id.clone(),
                    auth.principal_kind.clone(),
                )
            })
            .unwrap_or_default();
        let (upstream_id, upstream_name, upstream, route_ms) = self
            .route
            .as_ref()
            .map(|route| {
                (
                    Some(route.upstream_id),
                    Some(route.upstream_name.clone()),
                    Some(RequestEventUpstream::AnthropicDirect),
                    route.route_ms,
                )
            })
            .unwrap_or_default();
        let cost = self.cost_options();
        let ts_ms = self.ts_ms;

        RequestEventPartial {
            event_id: self.event_id.clone(),
            request_id: self
                .request_id
                .clone()
                .unwrap_or_else(|| "req_unknown_shadow".to_owned()),
            ts: ts_ms / 1_000,
            ts_ms,
            last_update_ms: now_ms,
            elapsed_ms: if ts_ms == 0 {
                0
            } else {
                now_ms.saturating_sub(ts_ms)
            },
            stream: self.stream,
            principal_id,
            principal_kind,
            key_id,
            upstream,
            upstream_id,
            upstream_name,
            model: self.model().map(str::to_owned),
            upstream_response_status: self.upstream_response_status,
            input_tokens: self.usage_seen.then_some(self.usage.input_tokens),
            output_tokens: self.usage_seen.then_some(self.usage.output_tokens),
            cache_creation_input_tokens: self
                .usage_seen
                .then_some(self.usage.cache_creation_input_tokens),
            cache_creation_input_tokens_5m: (self.usage_seen
                && self.usage.cache_creation_input_tokens_5m > 0)
                .then_some(self.usage.cache_creation_input_tokens_5m),
            cache_creation_input_tokens_1h: (self.usage_seen
                && self.usage.cache_creation_input_tokens_1h > 0)
                .then_some(self.usage.cache_creation_input_tokens_1h),
            cache_read_input_tokens: self
                .usage_seen
                .then_some(self.usage.cache_read_input_tokens),
            thinking_tokens: (self.usage_seen && self.usage.thinking_tokens > 0)
                .then_some(self.usage.thinking_tokens),
            web_search_requests: (self.usage_seen && self.usage.web_search_requests > 0)
                .then_some(self.usage.web_search_requests),
            web_fetch_requests: (self.usage_seen && self.usage.web_fetch_requests > 0)
                .then_some(self.usage.web_fetch_requests),
            service_tier: self.usage.service_tier.clone(),
            inference_geo: self.usage.inference_geo.clone(),
            cost_usd_micros: cost.total,
            cost_input_micros: cost.input,
            cost_output_micros: cost.output,
            cost_cache_creation_5m_micros: cost.cache_creation_5m,
            cost_cache_creation_1h_micros: cost.cache_creation_1h,
            cost_cache_read_micros: cost.cache_read,
            cache_control_block_count: self.cache_control_block_count,
            cache_prefix_hash: self.cache_prefix_hash.clone(),
            auth_ms: self.auth.as_ref().and_then(|auth| auth.auth_ms),
            route_ms,
            limit_reserve_ms: self.limit_reserve_ms,
            bulkhead_wait_ms: self.bulkhead_wait_ms,
            dns_ms: self.dns_ms,
            connect_ms: self.connect_ms,
            connection_reused: self.connection_reused,
            shape_ms: self.shape_ms,
            sign_ms: self.sign_ms,
            upstream_ttfb_ms: self.upstream_ttfb_ms,
            first_body_chunk_ms: self.first_body_chunk_ms,
        }
    }

    fn model(&self) -> Option<&str> {
        self.route
            .as_ref()
            .and_then(|route| route.model.as_deref())
            .or_else(|| self.parse.as_ref().and_then(|parse| parse.model.as_deref()))
    }

    fn cost_options(&self) -> CostBreakdownOptions {
        self.cost
            .as_ref()
            .map(|cost| CostBreakdownOptions {
                total: cost.total_micros,
                input: cost.input_micros,
                output: cost.output_micros,
                cache_creation_5m: cost.cache_creation_5m_micros,
                cache_creation_1h: cost.cache_creation_1h_micros,
                cache_read: cost.cache_read_micros,
            })
            .unwrap_or_else(|| self.inline_cost_options())
    }

    fn inline_cost_options(&self) -> CostBreakdownOptions {
        if !self.usage_seen {
            return CostBreakdownOptions::default();
        }
        let Some(model) = self.model() else {
            return CostBreakdownOptions::default();
        };
        let upstream_kind = self
            .route
            .as_ref()
            .and_then(|route| route.upstream_kind.as_deref())
            .and_then(pricing_upstream_kind_from_label);
        let breakdown = virtual_cost_micros_full(
            model,
            self.usage.input_tokens,
            self.usage.output_tokens,
            self.usage.cache_creation_input_tokens_5m,
            self.usage.cache_creation_input_tokens_1h,
            self.usage.cache_read_input_tokens,
            upstream_kind,
        );
        cost_breakdown_to_event_options(&breakdown)
    }

    fn usage_partial_due_at(&self, now: Instant) -> bool {
        self.last_partial_emit_ts
            .map(|last| now.duration_since(last) > PARTIAL_USAGE_THROTTLE)
            .unwrap_or(true)
    }

    fn record_usage_partial_emit_at(&mut self, now: Instant) {
        self.last_partial_emit_ts = Some(now);
    }
}

fn partial_emit_trigger(event: &LifecycleEvent) -> Option<PartialTrigger> {
    match event {
        LifecycleEvent::RequestStarted { .. } => Some(PartialTrigger::RequestStarted),
        LifecycleEvent::RouteCompleted { .. } => Some(PartialTrigger::RouteCompleted),
        LifecycleEvent::UpstreamResponseStarted { .. } => {
            Some(PartialTrigger::UpstreamResponseStarted)
        }
        LifecycleEvent::UsageObserved { .. } => Some(PartialTrigger::UsageObserved),
        LifecycleEvent::StreamCompleted { .. } => Some(PartialTrigger::StreamCompleted),
        LifecycleEvent::RequestTerminated { .. } => Some(PartialTrigger::RequestTerminated),
        LifecycleEvent::ParseCompleted { .. }
        | LifecycleEvent::AuthCompleted { .. }
        | LifecycleEvent::AuthenticationCompleted { .. }
        | LifecycleEvent::LimitDecision { .. }
        | LifecycleEvent::UpstreamAttempt { .. }
        | LifecycleEvent::ProviderErrorObserved { .. }
        | LifecycleEvent::Priced { .. }
        | LifecycleEvent::CacheObserved { .. }
        | LifecycleEvent::PromptCacheObservationsProduced { .. } => None,
        _ => {
            tracing::warn!("lifecycle event assembler saw unknown lifecycle event variant");
            None
        }
    }
}

fn publish_partial(
    bus: Option<&dyn RequestEventBus>,
    partial: &Partial,
    trigger: PartialTrigger,
    now_ms: u64,
) {
    if let Some(bus) = bus {
        bus.publish(RequestEventUpdate::partial(
            partial.snapshot_partial(now_ms),
        ));
        metrics::counter!("sse_partials_published_total", "trigger" => trigger.as_str())
            .increment(1);
    }
}

fn publish_usage_partial(
    bus: Option<&dyn RequestEventBus>,
    partial: &mut Partial,
    now: Instant,
    now_ms: u64,
) {
    if partial.usage_partial_due_at(now) {
        if bus.is_some() {
            partial.record_usage_partial_emit_at(now);
        }
        publish_partial(bus, partial, PartialTrigger::UsageObserved, now_ms);
    } else {
        metrics::counter!("sse_partials_throttled_total").increment(1);
    }
}

fn unix_now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(u128::from(u64::MAX)) as u64,
        Err(_) => 0,
    }
}

async fn assembler_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
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
                    Some(event) => handle_event(&*storage, bus.as_deref(), &mut partials, map_cap, event).await,
                    None => break,
                }
            }
            _ = finalization_tick.tick() => {
                flush_expired_terminations(&*storage, bus.as_deref(), &mut partials).await;
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&*storage, bus.as_deref(), &mut partials, map_cap, event).await;
    }
    force_flush_terminations(&*storage, bus.as_deref(), &mut partials).await;
}

#[allow(clippy::too_many_arguments)]
async fn write_finalized_rows(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    event_id: &EventId,
    partial: &Partial,
    reason: &TerminationReason,
    client_status: u16,
    duration_ms: u64,
    is_orphan: bool,
) {
    let row = finalize_base(partial, reason, client_status, duration_ms, is_orphan);
    match storage.append_request_event(&row).await {
        Ok(cursor) => {
            if let Some(bus) = bus {
                bus.publish(RequestEventUpdate::final_(row, cursor));
            }
            let outcome = if is_orphan {
                "written_orphan"
            } else {
                "written"
            };
            metrics::counter!("cc_lb_lifecycle_assembler_rows_total", "outcome" => outcome)
                .increment(1);
        }
        Err(error) => {
            tracing::warn!(
                %error,
                lifecycle_event_id = %event_id,
                "lifecycle event assembler: failed to persist row",
            );
            cc_lb_observability::increment_dropped_events_by(
                "lifecycle_assembler_storage_error",
                1,
            );
        }
    }
}

async fn flush_expired_terminations(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    partials: &mut HashMap<EventId, Partial>,
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
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let now_ms = unix_now_ms();
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
            let mut partial = Partial::orphan(event_id.clone());
            partial.limit_reconcile_ms = *limit_reconcile_ms;
            partial.observability_post_ms = *observability_post_ms;
            partial.proxy_setup_ms = *proxy_setup_ms;
            partial.upstream_body_ms = *upstream_body_ms;
            partial.first_body_chunk_ms = *first_body_chunk_ms;
            partial.internal_errors = internal_errors.clone();
            publish_partial(bus, &partial, PartialTrigger::RequestTerminated, now_ms);
            write_finalized_rows(
                storage,
                bus,
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
        let expects_cache =
            partial.upstream_response_status.is_some() && partial.cache_state.is_none();
        let termination = TerminationInfo {
            reason: reason.clone(),
            client_status: *client_status,
            duration_ms: *duration_ms,
            deadline: now + FINALIZATION_GRACE,
            expects_priced: false,
            expects_cache,
        };
        publish_partial(bus, &partial, PartialTrigger::RequestTerminated, now_ms);
        if termination.is_ready(&partial) {
            write_finalized_rows(
                storage,
                bus,
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

    let trigger = partial_emit_trigger(&event);
    {
        let partial = partials
            .entry(event_id.clone())
            .or_insert_with(|| Partial::new(now, event_id.clone()));
        partial.touch(now);
        merge(partial, event);

        if let Some(trigger) = trigger {
            if trigger == PartialTrigger::UsageObserved {
                publish_usage_partial(bus, partial, now, now_ms);
            } else {
                publish_partial(bus, partial, trigger, now_ms);
            }
        }
    }

    let ready_to_finalize = partials
        .get(&event_id)
        .and_then(|partial| {
            partial
                .termination
                .as_ref()
                .map(|term| term.is_ready(partial))
        })
        .unwrap_or(false);

    if ready_to_finalize && let Some(partial) = partials.remove(&event_id) {
        let term = partial
            .termination
            .as_ref()
            .expect("readiness implies termination present");
        write_finalized_rows(
            storage,
            bus,
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
                    limit_reserve_ms, ..
                },
            ..
        } => {
            partial.limit_reserve_ms = limit_reserve_ms;
        }
        LifecycleEvent::LimitDecision { .. } => {}
        LifecycleEvent::UpstreamAttempt { .. } => {}
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
    let (upstream_id, upstream_name, upstream, route_model, route_ms, route_routing_trace) =
        partial
            .route
            .as_ref()
            .map(|r| {
                (
                    Some(r.upstream_id),
                    Some(r.upstream_name.clone()),
                    Some(RequestEventUpstream::AnthropicDirect),
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

    let cost = partial.cost_options();
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id,
        principal_id,
        key_id,
        principal_kind,
        upstream,
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
        cost_usd_micros: cost.total,
        cost_input_micros: cost.input,
        cost_output_micros: cost.output,
        cost_cache_creation_5m_micros: cost.cache_creation_5m,
        cost_cache_creation_1h_micros: cost.cache_creation_1h,
        cost_cache_read_micros: cost.cache_read,
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
        event_id: Some(partial.event_id.clone()),
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
    use cc_lb_lifecycle::{
        AuthFailure, CostBreakdown, HeaderSnapshot, ParseFailure, RouteInfo, StreamError,
        StreamSuccess, UsageSource,
    };
    use cc_lb_storage_api::{RequestEvent, StorageResult};
    use metrics::{Counter, CounterFn, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use proptest::prelude::*;
    use std::collections::HashMap as StdHashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex as StdMutex, OnceLock};
    use uuid::Uuid;

    #[derive(Default)]
    struct CapturingStore {
        rows: StdMutex<Vec<RequestEvent>>,
        cursor: AtomicU64,
    }

    #[async_trait]
    impl RequestEventStore for CapturingStore {
        async fn append_request_event(&self, event: &RequestEvent) -> StorageResult<u64> {
            self.rows.lock().unwrap().push(event.clone());
            Ok(self.cursor.fetch_add(1, Ordering::Relaxed) + 1)
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

    #[derive(Clone, Default)]
    struct CountingRecorder {
        counts: Arc<StdMutex<StdHashMap<String, u64>>>,
    }

    #[derive(Clone)]
    struct CountingCounter {
        counts: Arc<StdMutex<StdHashMap<String, u64>>>,
        key: String,
    }

    impl CounterFn for CountingCounter {
        fn increment(&self, value: u64) {
            let mut counts = self.counts.lock().expect("recorder lock");
            *counts.entry(self.key.clone()).or_insert(0) += value;
        }

        fn absolute(&self, value: u64) {
            let mut counts = self.counts.lock().expect("recorder lock");
            counts.insert(self.key.clone(), value);
        }
    }

    impl Recorder for CountingRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::from_arc(Arc::new(CountingCounter {
                counts: Arc::clone(&self.counts),
                key: key.to_string(),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::noop()
        }
    }

    impl CountingRecorder {
        fn count_matching(&self, metric: &str, label_fragment: &str) -> u64 {
            self.counts
                .lock()
                .expect("recorder counts")
                .iter()
                .filter(|(key, _)| key.contains(metric) && key.contains(label_fragment))
                .map(|(_, count)| *count)
                .sum()
        }
    }

    fn install_counting_recorder() -> CountingRecorder {
        static RECORDER: OnceLock<CountingRecorder> = OnceLock::new();
        let recorder = RECORDER.get_or_init(CountingRecorder::default).clone();
        let _ = metrics::set_global_recorder(recorder.clone());
        recorder
    }

    fn install_known_pricing_for_tests() {
        let model = "claude-3-5-sonnet-20241022";
        let mut models = StdHashMap::new();
        models.insert(
            model.to_owned(),
            cc_lb_pricing::Pricing {
                model: model.to_owned(),
                input_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(3),
                output_per_million_usd: cc_lb_pricing::UsdPerMillion::from_whole_usd(15),
            },
        );
        cc_lb_pricing::global_catalog().install_snapshot(cc_lb_pricing::CatalogSnapshot {
            fetched_at_ms: 1,
            models,
            raw_json: Vec::new(),
            cache_creation_per_million_usd: StdHashMap::new(),
            cache_read_per_million_usd: StdHashMap::new(),
            status: cc_lb_pricing::CatalogStatus::Ok,
        });
    }

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn success_terminated_persists_row() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None);

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
        assert!(rows[0].event_id.as_deref().is_some_and(|id| !id.is_empty()));
        assert_eq!(rows[0].status, 200);
        assert_eq!(rows[0].duration_ms, 42);
        assert_eq!(rows[0].error_code, None);
        assert_eq!(rows[0].principal_id.as_deref(), Some("p1"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stream_error_terminated_carries_error_type_and_message() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None);

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
        let handle = spawn_request_event_assembler(rx, store.clone(), None);

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
        let handle = spawn_request_event_assembler(rx, store.clone(), None);

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
        assert!(rows[0].event_id.as_deref().is_some_and(|id| !id.is_empty()));
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
    async fn request_terminated_before_started_writes_orphan_and_increments_metric() {
        let recorder = install_counting_recorder();
        let before = recorder.count_matching(
            "cc_lb_lifecycle_assembler_rows_total",
            "terminated_without_partial",
        );
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None);
        let event_id = eid("orphan-before-started");

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: event_id.clone(),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 7,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: Some(1),
            observability_post_ms: Some(2),
            proxy_setup_ms: Some(3),
            upstream_body_ms: Some(4),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "late-start-should-not-reclassify".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id.as_deref(), Some(event_id.as_str()));
        assert_eq!(rows[0].request_id, "req_unknown_shadow");
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("terminal_without_partial")
        );
        assert!(
            recorder.count_matching(
                "cc_lb_lifecycle_assembler_rows_total",
                "terminated_without_partial",
            ) > before
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ignores_unused_auth_failure_variant() {
        let _ = AuthFailure::AuthenticationFailed {
            http_status: 401,
            reason: None,
        };
        let _ = StreamSuccess {
            usage: UsageSnapshot::default(),
            sse_event_count: 0,
            ..Default::default()
        };
        let _ = cc_lb_lifecycle::UsageSource::NonStreamBody;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finalized_row_write_republishes_to_bus_for_admin_sse() {
        use crate::event_bus::InMemoryBus;

        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let crate::event_bus::BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
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

        let mut final_update = None;
        while let Ok(update) = broadcast_rx.try_recv() {
            if let RequestEventUpdate::Final(update) = update {
                final_update = Some(update);
            }
        }
        let final_update = final_update.expect("expected final update");
        assert_eq!(final_update.cursor, 1);
        assert!(
            final_update
                .event
                .event_id
                .as_deref()
                .is_some_and(|id| !id.is_empty())
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_lifecycle_publishes_throttled_partials_and_one_final() {
        use crate::event_bus::{InMemoryBus, RequestEventPhase};

        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let crate::event_bus::BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let event_id = eid("01978c00-0000-7000-8000-000000000002");
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
            Some(bus.clone() as Arc<dyn RequestEventBus>),
        );

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-live-1".into(),
            ts_ms: 1_730_000_000_000,
            stream: true,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RouteCompleted {
            event_id: event_id.clone(),
            result: Ok(RouteInfo {
                upstream_id: Uuid::nil(),
                upstream_name: "primary".to_owned(),
                model: Some("claude-3-5-sonnet-20241022".to_owned()),
                upstream_kind: Some("anthropic_key".to_owned()),
                route_ms: Some(7),
                routing_trace: None,
                predicted_cache_read_tokens: None,
            }),
            routing_trace: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UpstreamResponseStarted {
            event_id: event_id.clone(),
            status: 200,
            headers: HeaderSnapshot::default(),
            bulkhead_wait_ms: Some(1),
            dns_ms: Some(2),
            connect_ms: Some(3),
            connection_reused: Some(false),
            shape_ms: Some(4),
            sign_ms: Some(5),
            upstream_ttfb_ms: Some(6),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: event_id.clone(),
            usage: UsageSnapshot {
                input_tokens: 10,
                output_tokens: 20,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
                ..UsageSnapshot::default()
            },
            source: UsageSource::MessageStart,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: event_id.clone(),
            usage: UsageSnapshot {
                input_tokens: 10,
                output_tokens: 25,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
                ..UsageSnapshot::default()
            },
            source: UsageSource::MessageDelta,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::StreamCompleted {
            event_id: event_id.clone(),
            result: Ok(StreamSuccess {
                usage: UsageSnapshot {
                    input_tokens: 10,
                    output_tokens: 30,
                    cache_creation_input_tokens: 3,
                    cache_read_input_tokens: 4,
                    ..UsageSnapshot::default()
                },
                sse_event_count: 3,
                first_body_chunk_ms: Some(11),
                ..StreamSuccess::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: event_id.clone(),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 123,
            first_body_chunk_ms: Some(11),
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: Some(12),
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let mut updates = Vec::new();
        while let Ok(update) = broadcast_rx.try_recv() {
            updates.push(update);
        }

        let partial_count = updates
            .iter()
            .filter(|update| update.phase() == RequestEventPhase::Partial)
            .count();
        assert!(
            (5..=6).contains(&partial_count),
            "expected 5-6 partials, got {partial_count}: {updates:?}"
        );
        let finals: Vec<_> = updates
            .iter()
            .filter_map(|update| match update {
                RequestEventUpdate::Final(final_update) => Some(final_update),
                RequestEventUpdate::Partial(_) => None,
            })
            .collect();
        assert_eq!(finals.len(), 1);
        assert_eq!(finals[0].cursor, 1);
        assert_eq!(finals[0].event.event_id.as_deref(), Some(event_id.as_str()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delayed_pricing_subscriber_does_not_affect_inline_final_cost() {
        use crate::event_bus::InMemoryBus;

        install_known_pricing_for_tests();
        let bus = Arc::new(InMemoryBus::new());
        let rx = bus.attach_lifecycle_assembler(16);
        let mut pricing_rx = bus.attach_lifecycle_pricing(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
            Some(bus.clone() as Arc<dyn RequestEventBus>),
        );
        let delayed_bus = bus.clone();
        let delayed_pricing = tokio::spawn(async move {
            while let Some(event) = pricing_rx.recv().await {
                if let LifecycleEvent::RequestTerminated { event_id, .. } = event {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    delayed_bus.publish_lifecycle(LifecycleEvent::Priced {
                        event_id,
                        cost: CostBreakdown {
                            total_micros: Some(-999),
                            input_micros: Some(-999),
                            output_micros: Some(-999),
                            cache_creation_5m_micros: None,
                            cache_creation_1h_micros: None,
                            cache_read_micros: None,
                        },
                    });
                    break;
                }
            }
        });
        let event_id = eid("inline-pricing-delay");

        bus.publish_lifecycle(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-inline-pricing-delay".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
        });
        bus.publish_lifecycle(LifecycleEvent::RouteCompleted {
            event_id: event_id.clone(),
            result: Ok(RouteInfo {
                upstream_id: Uuid::nil(),
                upstream_name: "primary".to_owned(),
                model: Some("claude-3-5-sonnet-20241022".to_owned()),
                upstream_kind: Some("anthropic_key".to_owned()),
                route_ms: Some(7),
                routing_trace: None,
                predicted_cache_read_tokens: None,
            }),
            routing_trace: None,
        });
        bus.publish_lifecycle(LifecycleEvent::UsageObserved {
            event_id: event_id.clone(),
            usage: UsageSnapshot {
                input_tokens: 1_000,
                output_tokens: 500,
                cache_creation_input_tokens_5m: 25,
                cache_read_input_tokens: 50,
                ..UsageSnapshot::default()
            },
            source: UsageSource::NonStreamBody,
        });
        bus.publish_lifecycle(LifecycleEvent::RequestTerminated {
            event_id: event_id.clone(),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 42,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            upstream_body_ms: None,
        });

        handle.shutdown().await;
        delayed_pricing.await.expect("delayed pricing task joins");

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id.as_deref(), Some(event_id.as_str()));
        assert!(rows[0].cost_usd_micros.is_some());
        assert_ne!(rows[0].cost_usd_micros, Some(-999));
        assert_ne!(rows[0].cost_input_micros, Some(-999));
        assert_ne!(rows[0].cost_output_micros, Some(-999));
    }

    proptest! {
        #[test]
        fn usage_partial_emit_timestamp_is_monotonic(deltas in prop::collection::vec(0u64..=500, 0..64)) {
            let mut partial = Partial::new(Instant::now(), eid("prop-usage"));
            let mut now = Instant::now();
            let mut previous_emit = partial.last_partial_emit_ts;

            for delta in deltas {
                now += Duration::from_millis(delta);
                if partial.usage_partial_due_at(now) {
                    partial.record_usage_partial_emit_at(now);
                }
                if let (Some(previous), Some(current)) = (previous_emit, partial.last_partial_emit_ts) {
                    prop_assert!(current >= previous);
                }
                previous_emit = partial.last_partial_emit_ts;
            }
        }
    }
}
