use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cc_lb_control::RequestEventBus;
use cc_lb_domain::{InternalError, RoutingTrace};
use cc_lb_lifecycle::{
    AuthInfo, EventId, LifecycleEvent, ParseInfo, RequestIoTimings, RouteInfo, TerminationReason,
    UsageSnapshot,
};
use cc_lb_observability::EngineMetricsHook;
use cc_lb_request_log::{
    CostBreakdown as LifecycleCostBreakdown, RequestCacheBreakpoint, RequestCacheState,
    RequestEventKind, RequestEventPartial, RequestEventUpdate, RequestEventUpstream,
};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::metrics_labels::PartialTrigger;
pub const DEFAULT_ASSEMBLER_MAP_CAP: usize = 4096;
pub const DEFAULT_ASSEMBLER_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// After receiving `RequestTerminated`, the assembler holds the partial for
/// this grace period so that late `CacheObserved` events (emitted by a separate
/// subscriber task) can still merge into the row.
/// See RFC-0002 synthesis analysis for the ordering rationale.
const FINALIZATION_GRACE: Duration = Duration::from_millis(200);

/// How often the finalization tick runs. Must be small compared to
/// FINALIZATION_GRACE so terminated partials do not sit past their deadline
/// waiting for the next tick.
const FINALIZATION_TICK: Duration = Duration::from_millis(20);
const PARTIAL_USAGE_THROTTLE: Duration = Duration::from_millis(250);
#[derive(Default)]
struct ParseIdentity {
    thread_id: Option<String>,
    observed_session_id: Option<String>,
    request_kind: Option<String>,
    claude_agent_id: Option<String>,
    claude_parent_agent_id: Option<String>,
    parent_session_id: Option<String>,
    client_app: Option<String>,
    session_id_source: Option<String>,
    message_id: Option<String>,
    message_index: Option<u64>,
    message_count: Option<u64>,
    cache_control_message_indices: Vec<u64>,
}

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
    metrics: Arc<dyn EngineMetricsHook>,
) -> RequestEventAssemblerHandle {
    spawn_with_config(
        rx,
        storage,
        bus,
        metrics,
        DEFAULT_ASSEMBLER_MAP_CAP,
        DEFAULT_ASSEMBLER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    bus: Option<Arc<dyn RequestEventBus>>,
    metrics: Arc<dyn EngineMetricsHook>,
    map_cap: usize,
    ttl: Duration,
) -> RequestEventAssemblerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(assembler_loop(
        rx,
        storage,
        bus,
        metrics,
        map_cap,
        ttl,
        shutdown_rx,
    ));
    RequestEventAssemblerHandle { shutdown_tx, join }
}

#[derive(Default)]
struct CostBreakdownOptions {
    total: Option<i64>,
    input: Option<i64>,
    output: Option<i64>,
    cache_creation_5m: Option<i64>,
    cache_creation_1h: Option<i64>,
    cache_read: Option<i64>,
}

#[derive(Default)]
struct Partial {
    event_id: EventId,
    inserted_at: Option<Instant>,
    request_id: Option<String>,
    source_kind: Option<String>,
    event_kind: Option<RequestEventKind>,
    source_ref_id: Option<String>,
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
    thinking_budget_tokens: Option<u64>,
    reasoning_effort: Option<String>,
    cache_breakpoints: Vec<RequestCacheBreakpoint>,
    cache_prefix_hash: Option<String>,
    termination: Option<TerminationInfo>,

    /// `RouteCompleted.routing_trace` top-level field. Populated on both Ok
    /// (redundant with `route.routing_trace`) and Err (only source) paths.
    routing_trace: Option<RoutingTrace>,

    request_body_read_ms: Option<u64>,
    request_body_bytes: Option<u64>,
    io_timings: RequestIoTimings,
    limit_reserve_ms: Option<u64>,
    json_parse_ms: Option<f64>,
    cache_structure_ms: Option<f64>,
    cache_token_key_ms: Option<f64>,
    cache_count_lookup_ms: Option<f64>,
    cache_tokenizer_queue_ms: Option<f64>,
    cache_serialize_ms: Option<f64>,
    cache_tokenize_ms: Option<f64>,
    prepare_signer_ms: Option<f64>,

    bulkhead_wait_ms: Option<u64>,
    dns_ms: Option<u64>,
    connect_ms: Option<u64>,
    connection_reused: Option<bool>,
    shape_ms: Option<u64>,
    sign_ms: Option<u64>,
    upstream_ttfb_ms: Option<u64>,

    upstream_body_ms: Option<u64>,
    finalize_ms: Option<u64>,
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
            .unwrap_or((None, None, None));
        let (upstream_id, upstream_name, upstream, route_ms, route_matched_v3_cache_key) = self
            .route
            .as_ref()
            .map(|route| {
                (
                    Some(route.upstream_id),
                    Some(route.upstream_name.clone()),
                    Some(RequestEventUpstream::AnthropicDirect),
                    route.route_ms,
                    route.matched_v3_cache_key.clone(),
                )
            })
            .unwrap_or((None, None, None, None, None));
        let cost = self.cost_options();
        let ts_ms = self.ts_ms;

        RequestEventPartial {
            event_id: self.event_id.clone(),
            request_id: self
                .request_id
                .clone()
                .unwrap_or_else(|| "req_unknown_shadow".to_owned()),
            source_kind: self.source_kind.clone(),
            source_ref_id: self.source_ref_id.clone(),
            event_kind: self.event_kind,
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
            thread_id: self
                .parse
                .as_ref()
                .and_then(|parse| parse.thread_id.clone()),
            observed_session_id: self
                .parse
                .as_ref()
                .and_then(|parse| parse.observed_session_id.clone()),
            request_kind: self
                .parse
                .as_ref()
                .and_then(|parse| parse.request_kind.clone()),
            claude_agent_id: self
                .parse
                .as_ref()
                .and_then(|parse| parse.claude_agent_id.clone()),
            claude_parent_agent_id: self
                .parse
                .as_ref()
                .and_then(|parse| parse.claude_parent_agent_id.clone()),
            parent_session_id: self
                .parse
                .as_ref()
                .and_then(|parse| parse.parent_session_id.clone()),
            client_app: self
                .parse
                .as_ref()
                .and_then(|parse| parse.client_app.clone()),
            session_id_source: self
                .parse
                .as_ref()
                .and_then(|parse| parse.session_id_source.clone()),
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
            thinking_budget_tokens: self.thinking_budget_tokens,
            reasoning_effort: self.reasoning_effort.clone(),
            cache_prefix_hash: self.cache_prefix_hash.clone(),
            matched_v3_cache_key: route_matched_v3_cache_key.or_else(|| {
                self.parse
                    .as_ref()
                    .and_then(|parse| parse.matched_v3_cache_key.clone())
            }),
            quota_urgency_5h: self.route.as_ref().and_then(|route| route.quota_urgency_5h),
            quota_urgency_7d: self.route.as_ref().and_then(|route| route.quota_urgency_7d),
            quota_urgency_combined: self
                .route
                .as_ref()
                .and_then(|route| route.quota_urgency_combined),
            quota_warning_multiplier: self
                .route
                .as_ref()
                .and_then(|route| route.quota_warning_multiplier),
            request_body_read_ms: self.request_body_read_ms,
            request_body_bytes: self.request_body_bytes,
            request_body_first_chunk_ms: self.io_timings.request_body_first_chunk_ms,
            request_body_receive_ms: self.io_timings.request_body_receive_ms,
            request_body_wait_ms: self.io_timings.request_body_wait_ms,
            request_body_process_ms: self.io_timings.request_body_process_ms,
            request_body_chunk_count: self.io_timings.request_body_chunk_count,
            response_body_wait_ms: self.io_timings.response_body_wait_ms,
            response_body_process_ms: self.io_timings.response_body_process_ms,
            response_body_downstream_poll_gap_ms: self
                .io_timings
                .response_body_downstream_poll_gap_ms,
            retry_overhead_ms: self.io_timings.retry_overhead_ms,
            auth_ms: self.auth.as_ref().and_then(|auth| auth.auth_ms),
            route_ms,
            limit_reserve_ms: self.limit_reserve_ms,
            json_parse_ms: self.json_parse_ms,
            cache_structure_ms: self.cache_structure_ms,
            cache_token_key_ms: self.cache_token_key_ms,
            cache_count_lookup_ms: self.cache_count_lookup_ms,
            cache_tokenizer_queue_ms: self.cache_tokenizer_queue_ms,
            cache_serialize_ms: self.cache_serialize_ms,
            cache_tokenize_ms: self.cache_tokenize_ms,
            prepare_signer_ms: self.prepare_signer_ms,
            bulkhead_wait_ms: self.bulkhead_wait_ms,
            dns_ms: self.dns_ms,
            connect_ms: self.connect_ms,
            connection_reused: self.connection_reused,
            shape_ms: self.shape_ms,
            sign_ms: self.sign_ms,
            upstream_ttfb_ms: self.upstream_ttfb_ms,
            upstream_body_ms: self.upstream_body_ms,
            finalize_ms: self.finalize_ms,
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
        if let Some(cost) = self.cost.as_ref() {
            return CostBreakdownOptions {
                total: cost.total_micros,
                input: cost.input_micros,
                output: cost.output_micros,
                cache_creation_5m: cost.cache_creation_5m_micros,
                cache_creation_1h: cost.cache_creation_1h_micros,
                cache_read: cost.cache_read_micros,
            };
        }
        self.estimated_cost_options().unwrap_or_default()
    }

    // Display-only estimate for in-progress rows (authoritative `Priced` arrives
    // at termination). Must never set `self.cost`: that feeds billing/limit
    // reconciliation, which must stay driven solely by the real `Priced` event.
    fn estimated_cost_options(&self) -> Option<CostBreakdownOptions> {
        self.estimated_cost_options_with(|model, usage, upstream_kind| {
            cc_lb_pricing::virtual_cost_micros_full(
                model,
                usage.input_tokens,
                usage.output_tokens,
                usage.cache_creation_input_tokens_5m,
                usage.cache_creation_input_tokens_1h,
                usage.cache_read_input_tokens,
                upstream_kind,
                usage.service_tier.as_deref(),
            )
            .into()
        })
    }

    fn estimated_cost_options_with(
        &self,
        estimate: impl FnOnce(
            &str,
            &UsageSnapshot,
            Option<cc_lb_pricing::UpstreamKind>,
        ) -> LifecycleCostBreakdown,
    ) -> Option<CostBreakdownOptions> {
        if !self.usage_seen {
            return None;
        }
        let model = self.model()?;
        let upstream_kind = self
            .route
            .as_ref()
            .and_then(|route| route.upstream_kind.as_deref())
            .and_then(|label| match label {
                "anthropic_key" => Some(cc_lb_pricing::UpstreamKind::AnthropicKey),
                "anthropic_oauth" => Some(cc_lb_pricing::UpstreamKind::AnthropicOAuth),
                _ => None,
            });
        Some(CostBreakdownOptions::from(estimate(
            model,
            &self.usage,
            upstream_kind,
        )))
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
        LifecycleEvent::ParseCompleted { result: Ok(_), .. } => {
            Some(PartialTrigger::ParseCompleted)
        }
        LifecycleEvent::AuthCompleted { result: Ok(_), .. } => Some(PartialTrigger::AuthCompleted),
        LifecycleEvent::RouteCompleted { .. } => Some(PartialTrigger::RouteCompleted),
        LifecycleEvent::UpstreamResponseStarted { .. } => {
            Some(PartialTrigger::UpstreamResponseStarted)
        }
        LifecycleEvent::UsageObserved { .. } => Some(PartialTrigger::UsageObserved),
        LifecycleEvent::StreamCompleted { .. } => Some(PartialTrigger::StreamCompleted),
        LifecycleEvent::RequestTerminated { .. } => Some(PartialTrigger::RequestTerminated),
        LifecycleEvent::ParseCompleted { result: Err(_), .. }
        | LifecycleEvent::AuthCompleted { result: Err(_), .. }
        | LifecycleEvent::AuthenticationCompleted { .. }
        | LifecycleEvent::LimitDecision { .. }
        | LifecycleEvent::UpstreamAttempt { .. }
        | LifecycleEvent::ProviderErrorObserved { .. }
        | LifecycleEvent::RequestLogUpstreamErrorObserved { .. }
        | LifecycleEvent::Priced { .. }
        | LifecycleEvent::CacheObserved { .. }
        | LifecycleEvent::PromptCacheObservationsProduced { .. } => None,
        _ => {
            tracing::warn!("lifecycle event assembler saw unknown lifecycle event variant");
            None
        }
    }
}

impl From<LifecycleCostBreakdown> for CostBreakdownOptions {
    fn from(cost: LifecycleCostBreakdown) -> Self {
        Self {
            total: cost.total_micros,
            input: cost.input_micros,
            output: cost.output_micros,
            cache_creation_5m: cost.cache_creation_5m_micros,
            cache_creation_1h: cost.cache_creation_1h_micros,
            cache_read: cost.cache_read_micros,
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
    metrics: Arc<dyn EngineMetricsHook>,
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
                        Some(event) => handle_event(&*storage, bus.as_deref(), metrics.as_ref(), &mut partials, map_cap, event).await,
                        None => break,
                    }
                }
                _ = finalization_tick.tick() => {
                    flush_expired_terminations(&*storage, bus.as_deref(), metrics.as_ref(), &mut partials).await;
                }
            _ = sweeper.tick() => {
                flush_expired_orphans(
                    &*storage,
                    bus.as_deref(),
                    metrics.as_ref(),
                    &mut partials,
                    ttl,
                )
                .await;
            }
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(
            &*storage,
            bus.as_deref(),
            metrics.as_ref(),
            &mut partials,
            map_cap,
            event,
        )
        .await;
    }
    force_flush_terminations(&*storage, bus.as_deref(), metrics.as_ref(), &mut partials).await;
}

#[allow(clippy::too_many_arguments)]
async fn write_finalized_rows(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    metrics_hook: &dyn EngineMetricsHook,
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
            metrics_hook.record_dropped_events_by("lifecycle_assembler_storage_error", 1);
        }
    }
}

async fn flush_expired_terminations(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    metrics: &dyn EngineMetricsHook,
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
                metrics,
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
    metrics: &dyn EngineMetricsHook,
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
                metrics,
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

fn apply_setup_timings(partial: &mut Partial, timings: &cc_lb_lifecycle::RequestSetupTimings) {
    partial.json_parse_ms = partial.json_parse_ms.or(timings.json_parse_ms);
    partial.cache_structure_ms = partial.cache_structure_ms.or(timings.cache_structure_ms);
    partial.cache_token_key_ms = partial.cache_token_key_ms.or(timings.cache_token_key_ms);
    partial.cache_count_lookup_ms = partial
        .cache_count_lookup_ms
        .or(timings.cache_count_lookup_ms);
    partial.cache_tokenizer_queue_ms = partial
        .cache_tokenizer_queue_ms
        .or(timings.cache_tokenizer_queue_ms);
    partial.cache_serialize_ms = partial.cache_serialize_ms.or(timings.cache_serialize_ms);
    partial.cache_tokenize_ms = partial.cache_tokenize_ms.or(timings.cache_tokenize_ms);
    partial.prepare_signer_ms = partial.prepare_signer_ms.or(timings.prepare_signer_ms);
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

async fn handle_event(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    metrics: &dyn EngineMetricsHook,
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
        request_body_read_ms,
        request_body_bytes,
        finalize_ms,
        limit_reconcile_ms,
        observability_post_ms,
        proxy_setup_ms,
        setup_timings,
        upstream_body_ms,
        first_body_chunk_ms,
        io_timings,
        internal_errors,
        event_kind,
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
            partial.request_body_read_ms = *request_body_read_ms;
            partial.request_body_bytes = *request_body_bytes;
            partial.finalize_ms = *finalize_ms;
            partial.limit_reconcile_ms = *limit_reconcile_ms;
            partial.observability_post_ms = *observability_post_ms;
            partial.proxy_setup_ms = *proxy_setup_ms;
            apply_setup_timings(&mut partial, setup_timings);
            partial.upstream_body_ms = *upstream_body_ms;
            partial.first_body_chunk_ms = *first_body_chunk_ms;
            partial.internal_errors = internal_errors.clone();
            partial.io_timings = *io_timings;
            // Terminal-carried classification is the only metadata left when
            // `RequestStarted` was dropped on the writer channel.
            partial.event_kind = *event_kind;
            publish_partial(bus, &partial, PartialTrigger::RequestTerminated, now_ms);
            write_finalized_rows(
                storage,
                bus,
                metrics,
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
        partial.request_body_read_ms = partial.request_body_read_ms.or(*request_body_read_ms);
        partial.request_body_bytes = partial.request_body_bytes.or(*request_body_bytes);
        partial.finalize_ms = partial.finalize_ms.or(*finalize_ms);
        partial.limit_reconcile_ms = partial.limit_reconcile_ms.or(*limit_reconcile_ms);
        partial.observability_post_ms = partial.observability_post_ms.or(*observability_post_ms);
        partial.proxy_setup_ms = partial.proxy_setup_ms.or(*proxy_setup_ms);
        apply_setup_timings(&mut partial, setup_timings);
        partial.upstream_body_ms = partial.upstream_body_ms.or(*upstream_body_ms);
        merge_io_timings(&mut partial.io_timings, *io_timings);
        if partial.first_body_chunk_ms.is_none() {
            partial.first_body_chunk_ms = *first_body_chunk_ms;
        }
        partial.internal_errors = internal_errors.clone();
        // Terminal kind is a fallback only: a classification already merged
        // from `RequestStarted` (including renewal precedence) stays
        // authoritative.
        partial.event_kind = partial.event_kind.or(*event_kind);
        let expects_priced =
            partial.usage_seen && partial.model().is_some() && partial.cost.is_none();
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
        publish_partial(bus, &partial, PartialTrigger::RequestTerminated, now_ms);
        if termination.is_ready(&partial) {
            write_finalized_rows(
                storage,
                bus,
                metrics,
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
            metrics,
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
            source_kind,
            source_ref_id,
            event_kind,
            ..
        } => {
            partial.request_id = Some(request_id);
            partial.ts_ms = ts_ms;
            partial.stream = stream;
            partial.source_kind = source_kind;
            partial.source_ref_id = source_ref_id;
            // `source_kind = "renewal"` is authoritative even when the
            // producer also classified the replayed /v1/messages path.
            partial.event_kind = Some(RequestEventKind::effective(
                partial.source_kind.as_deref(),
                event_kind,
            ));
        }
        LifecycleEvent::ParseCompleted {
            result: Ok(info), ..
        } => {
            if partial.request_body_bytes.is_none() {
                partial.request_body_bytes = Some(info.body_bytes);
            }
            partial.cache_control_block_count = info.cache_control_block_count;
            partial.thinking_budget_tokens = info.thinking_budget_tokens;
            partial.reasoning_effort = info.reasoning_effort.clone();
            partial.cache_breakpoints = info.cache_breakpoints.clone();
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
        LifecycleEvent::UpstreamAttempt { attempt_num, .. } => {
            if attempt_num > 1 {
                partial.upstream_response_status = None;
                partial.bulkhead_wait_ms = None;
                partial.dns_ms = None;
                partial.connect_ms = None;
                partial.connection_reused = None;
                partial.shape_ms = None;
                partial.sign_ms = None;
                partial.upstream_ttfb_ms = None;
            }
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
        LifecycleEvent::RequestLogUpstreamErrorObserved {
            error_type,
            error_message,
            ..
        } => {
            if !error_type.is_empty() {
                partial.stream_error_type = Some(error_type);
                partial.stream_error_message = Some(error_message);
            } else if partial.stream_error_message.is_none() {
                partial.stream_error_message = Some(error_message);
            }
        }
        LifecycleEvent::ProviderErrorObserved { .. } => {}
        LifecycleEvent::RequestTerminated { .. } => {}
        LifecycleEvent::Priced { cost, .. } => {
            partial.cost = Some(cost);
        }
        LifecycleEvent::CacheObserved { cache_state, .. } => {
            partial.cache_state = Some(cache_state);
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
    let (
        upstream_id,
        upstream_name,
        upstream,
        route_model,
        route_ms,
        route_routing_trace,
        matched_v3_cache_key,
        breakpoint_content_block_index,
        matched_content_block_index,
        lookback_distance,
        predicted_cache_read_tokens,
        predicted_cache_creation_tokens_5m,
        predicted_cache_creation_tokens_1h,
        token_estimate_source,
        cache_value_micros,
        formula_winner_upstream_id,
        kept_upstream_id,
        lineage_would_have_predicted_read_tokens,
        lineage_would_have_picked_upstream_id,
    ) = partial
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
                r.matched_v3_cache_key.clone(),
                r.breakpoint_content_block_index.map(u64::from),
                r.matched_content_block_index.map(u64::from),
                r.lookback_distance.map(u64::from),
                r.predicted_cache_read_tokens.map(u64::from),
                r.predicted_cache_creation_tokens_5m.map(u64::from),
                r.predicted_cache_creation_tokens_1h.map(u64::from),
                r.token_estimate_source.clone(),
                r.cache_value_micros,
                r.formula_winner_upstream_id,
                r.kept_upstream_id,
                r.lineage_would_have_predicted_read_tokens.map(u64::from),
                r.lineage_would_have_picked_upstream_id,
            )
        })
        .unwrap_or((
            None, None, None, None, None, None, None, None, None, None, None, None, None, None,
            None, None, None, None, None,
        ));
    let model = route_model.or_else(|| partial.parse.as_ref().and_then(|p| p.model.clone()));
    let auth_ms = partial.auth.as_ref().and_then(|a| a.auth_ms);
    let routing_trace = partial.routing_trace.clone().or(route_routing_trace);
    let identity = partial
        .parse
        .as_ref()
        .map(|p| ParseIdentity {
            thread_id: p.thread_id.clone(),
            observed_session_id: p.observed_session_id.clone(),
            request_kind: p.request_kind.clone(),
            claude_agent_id: p.claude_agent_id.clone(),
            claude_parent_agent_id: p.claude_parent_agent_id.clone(),
            parent_session_id: p.parent_session_id.clone(),
            client_app: p.client_app.clone(),
            session_id_source: p.session_id_source.clone(),
            message_id: p.message_id.clone(),
            message_index: p.message_index,
            message_count: p.message_count,
            cache_control_message_indices: p.cache_control_message_indices.clone(),
        })
        .unwrap_or_default();

    let cost = partial.cost_options();
    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        request_id,
        source_kind: partial.source_kind.clone(),
        event_kind: partial.event_kind,
        source_ref_id: partial.source_ref_id.clone(),
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
        thinking_budget_tokens: partial.thinking_budget_tokens,
        reasoning_effort: partial.reasoning_effort.clone(),
        cache_breakpoints: partial.cache_breakpoints.clone(),
        cache_prefix_hash: partial.cache_prefix_hash.clone(),
        matched_v3_cache_key: matched_v3_cache_key.or_else(|| {
            partial
                .parse
                .as_ref()
                .and_then(|parse| parse.matched_v3_cache_key.clone())
        }),
        breakpoint_content_block_index,
        matched_content_block_index,
        lookback_distance,
        predicted_cache_read_tokens,
        predicted_cache_creation_tokens_5m,
        predicted_cache_creation_tokens_1h,
        token_estimate_source,
        cache_value_micros,
        formula_winner_upstream_id,
        kept_upstream_id,
        quota_urgency_5h: partial
            .route
            .as_ref()
            .and_then(|route| route.quota_urgency_5h),
        quota_urgency_7d: partial
            .route
            .as_ref()
            .and_then(|route| route.quota_urgency_7d),
        quota_urgency_combined: partial
            .route
            .as_ref()
            .and_then(|route| route.quota_urgency_combined),
        quota_warning_multiplier: partial
            .route
            .as_ref()
            .and_then(|route| route.quota_warning_multiplier),
        lineage_would_have_predicted_read_tokens,
        lineage_would_have_picked_upstream_id,
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
        thread_id: identity.thread_id,
        observed_session_id: identity.observed_session_id,
        request_kind: identity.request_kind,
        claude_agent_id: identity.claude_agent_id,
        claude_parent_agent_id: identity.claude_parent_agent_id,
        parent_session_id: identity.parent_session_id,
        client_app: identity.client_app,
        session_id_source: identity.session_id_source,
        message_id: identity.message_id,
        message_index: identity.message_index,
        message_count: identity.message_count,
        cache_control_message_indices: identity.cache_control_message_indices,
        auth_ms,
        route_ms,
        request_body_read_ms: partial.request_body_read_ms,
        request_body_bytes: partial.request_body_bytes,
        request_body_first_chunk_ms: partial.io_timings.request_body_first_chunk_ms,
        request_body_receive_ms: partial.io_timings.request_body_receive_ms,
        request_body_wait_ms: partial.io_timings.request_body_wait_ms,
        request_body_process_ms: partial.io_timings.request_body_process_ms,
        request_body_chunk_count: partial.io_timings.request_body_chunk_count,
        response_body_wait_ms: partial.io_timings.response_body_wait_ms,
        response_body_process_ms: partial.io_timings.response_body_process_ms,
        response_body_downstream_poll_gap_ms: partial
            .io_timings
            .response_body_downstream_poll_gap_ms,
        retry_overhead_ms: partial.io_timings.retry_overhead_ms,
        limit_reserve_ms: partial.limit_reserve_ms,
        json_parse_ms: partial.json_parse_ms,
        cache_structure_ms: partial.cache_structure_ms,
        cache_token_key_ms: partial.cache_token_key_ms,
        cache_count_lookup_ms: partial.cache_count_lookup_ms,
        cache_tokenizer_queue_ms: partial.cache_tokenizer_queue_ms,
        cache_serialize_ms: partial.cache_serialize_ms,
        cache_tokenize_ms: partial.cache_tokenize_ms,
        prepare_signer_ms: partial.prepare_signer_ms,
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
        finalize_ms: partial.finalize_ms,
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

async fn flush_expired_orphans(
    storage: &dyn RequestEventStore,
    bus: Option<&dyn RequestEventBus>,
    metrics: &dyn EngineMetricsHook,
    partials: &mut HashMap<EventId, Partial>,
    ttl: Duration,
) {
    let now = Instant::now();
    let expired: Vec<EventId> = partials
        .iter()
        .filter_map(|(event_id, partial)| {
            partial
                .inserted_at
                .filter(|inserted_at| now.duration_since(*inserted_at) >= ttl)
                .map(|_| event_id.clone())
        })
        .collect();
    for event_id in expired {
        if let Some(partial) = partials.remove(&event_id) {
            write_finalized_rows(
                storage,
                bus,
                metrics,
                &event_id,
                &partial,
                &TerminationReason::Dropped,
                499,
                0,
                true,
            )
            .await;
        }
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
    use cc_lb_control::BusReceiver;
    use cc_lb_domain::{
        CandidateUrgency, StageDecision, SubscriptionPreferenceTrace, SubscriptionTier,
        TerminalDecision, TerminalStrategy,
    };
    use cc_lb_lifecycle::{
        AuthFailure, ParseFailure, RouteInfo, StreamError, StreamSuccess, UsageSource,
    };
    use cc_lb_observability::{EngineMetricsHook, NoopMetricsHook};
    use cc_lb_request_log::{CostBreakdown, HeaderSnapshot};
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

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    fn noop_metrics() -> Arc<dyn EngineMetricsHook> {
        Arc::new(NoopMetricsHook)
    }

    include!("lifecycle_event_assembler_quota_support_test.rs");
    include!("lifecycle_event_assembler_quota_tests.rs");

    #[test]
    fn cost_options_estimates_from_usage_until_authoritative_price_arrives() {
        let mut partial = Partial {
            parse: Some(ParseInfo {
                model: Some("claude-sonnet-4-5-20250929".to_owned()),
                ..Default::default()
            }),
            usage: UsageSnapshot {
                input_tokens: 1000,
                output_tokens: 500,
                ..Default::default()
            },
            usage_seen: false,
            ..Default::default()
        };
        assert!(partial.estimated_cost_options().is_none());

        partial.usage_seen = true;
        let estimated = partial
            .estimated_cost_options_with(|model, usage, upstream_kind| {
                assert_eq!(model, "claude-sonnet-4-5-20250929");

                assert_eq!(usage.input_tokens, 1000);
                assert_eq!(usage.output_tokens, 500);
                assert_eq!(upstream_kind, None);
                CostBreakdown {
                    total_micros: Some(31337),
                    input_micros: Some(1000),
                    output_micros: Some(30337),
                    ..Default::default()
                }
            })
            .expect("usage and model should produce an estimate");
        assert_eq!(estimated.total, Some(31337));
        assert_eq!(estimated.input, Some(1000));
        assert_eq!(estimated.output, Some(30337));

        partial.cost = Some(CostBreakdown {
            total_micros: Some(4242),
            ..Default::default()
        });
        assert_eq!(partial.cost_options().total, Some(4242));
    }

    #[test]
    fn client_cancelled_partial_body_timing_survives_late_stream_error() {
        let event_id = eid("cancelled-partial-body");
        let mut partial = Partial::new(Instant::now(), event_id.clone());
        partial.request_id = Some("req-cancelled-partial-body".to_owned());
        partial.request_body_read_ms = Some(9);
        partial.request_body_bytes = Some(917_567);
        partial.upstream_body_ms = Some(41);
        partial.finalize_ms = Some(4);
        partial.io_timings = RequestIoTimings {
            request_body_first_chunk_ms: Some(0.0),
            request_body_receive_ms: Some(2.25),
            request_body_wait_ms: Some(7.5),
            request_body_process_ms: Some(1.125),
            request_body_chunk_count: Some(3),
            response_body_wait_ms: Some(40.5),
            response_body_process_ms: Some(0.625),
            response_body_downstream_poll_gap_ms: Some(12.75),
            retry_overhead_ms: None,
        };

        merge(
            &mut partial,
            LifecycleEvent::StreamCompleted {
                event_id,
                result: Err(StreamError {
                    error_type: "client_closed_request".to_owned(),
                    error_message: "downstream disconnected".to_owned(),
                }),
            },
        );

        let snapshot = partial.snapshot_partial(0);
        assert_eq!(snapshot.request_body_read_ms, Some(9));
        assert_eq!(snapshot.request_body_bytes, Some(917_567));
        assert_eq!(snapshot.upstream_body_ms, Some(41));
        assert_eq!(snapshot.finalize_ms, Some(4));
        assert_eq!(snapshot.request_body_first_chunk_ms, Some(0.0));
        assert_eq!(snapshot.request_body_receive_ms, Some(2.25));
        assert_eq!(snapshot.request_body_wait_ms, Some(7.5));
        assert_eq!(snapshot.request_body_process_ms, Some(1.125));
        assert_eq!(snapshot.request_body_chunk_count, Some(3));
        assert_eq!(snapshot.response_body_wait_ms, Some(40.5));
        assert_eq!(snapshot.response_body_process_ms, Some(0.625));
        assert_eq!(snapshot.response_body_downstream_poll_gap_ms, Some(12.75));
        assert_eq!(snapshot.retry_overhead_ms, None);

        let event = finalize_base(&partial, &TerminationReason::Dropped, 499, 54, false);
        assert_eq!(event.request_body_read_ms, Some(9));
        assert_eq!(event.request_body_bytes, Some(917_567));
        assert_eq!(event.upstream_body_ms, Some(41));
        assert_eq!(event.finalize_ms, Some(4));
        assert_eq!(event.stream_total_ms, None);
        assert_eq!(event.request_body_first_chunk_ms, Some(0.0));
        assert_eq!(event.request_body_receive_ms, Some(2.25));
        assert_eq!(event.request_body_wait_ms, Some(7.5));
        assert_eq!(event.request_body_process_ms, Some(1.125));
        assert_eq!(event.request_body_chunk_count, Some(3));
        assert_eq!(event.response_body_wait_ms, Some(40.5));
        assert_eq!(event.response_body_process_ms, Some(0.625));
        assert_eq!(event.response_body_downstream_poll_gap_ms, Some(12.75));
        assert_eq!(event.retry_overhead_ms, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn missing_terminal_timings_do_not_erase_observed_values() {
        let store = CapturingStore::default();
        let metrics = noop_metrics();
        let event_id = eid("preserve-observed-terminal-timings");
        let mut partial = Partial::new(Instant::now(), event_id.clone());
        partial.request_id = Some("req-preserve-observed-terminal-timings".to_owned());
        partial.request_body_read_ms = Some(9);
        partial.request_body_bytes = Some(917_567);
        partial.upstream_body_ms = Some(41);
        partial.finalize_ms = Some(4);
        let mut partials = HashMap::from([(event_id.clone(), partial)]);

        handle_event(
            &store,
            None,
            metrics.as_ref(),
            &mut partials,
            DEFAULT_ASSEMBLER_MAP_CAP,
            LifecycleEvent::RequestTerminated {
                event_id,
                reason: TerminationReason::Dropped,
                client_status: 499,
                duration_ms: 54,
                request_body_read_ms: None,
                request_body_bytes: None,
                finalize_ms: None,
                first_body_chunk_ms: None,
                internal_errors: Vec::new(),
                limit_reconcile_ms: None,
                observability_post_ms: None,
                proxy_setup_ms: None,
                setup_timings: Default::default(),
                io_timings: Default::default(),
                upstream_body_ms: None,
                event_kind: None,
            },
        )
        .await;

        let rows = store.rows.lock().expect("capturing store lock");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].request_body_read_ms, Some(9));
        assert_eq!(rows[0].request_body_bytes, Some(917_567));
        assert_eq!(rows[0].upstream_body_ms, Some(41));
        assert_eq!(rows[0].finalize_ms, Some(4));
        assert_eq!(rows[0].stream_total_ms, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_terminal_missing_timings_preserves_first_terminal_timings() {
        let store = CapturingStore::default();
        let metrics = noop_metrics();
        let event_id = eid("preserve-first-terminal-timings");
        let mut partial = Partial::new(Instant::now(), event_id.clone());
        partial.request_id = Some("req-preserve-first-terminal-timings".to_owned());
        partial.upstream_response_status = Some(200);
        let mut partials = HashMap::from([(event_id.clone(), partial)]);

        handle_event(
            &store,
            None,
            metrics.as_ref(),
            &mut partials,
            DEFAULT_ASSEMBLER_MAP_CAP,
            LifecycleEvent::RequestTerminated {
                event_id: event_id.clone(),
                reason: TerminationReason::Dropped,
                client_status: 499,
                duration_ms: 54,
                request_body_read_ms: Some(0),
                request_body_bytes: Some(917_567),
                finalize_ms: Some(4),
                limit_reconcile_ms: Some(5),
                observability_post_ms: Some(6),
                proxy_setup_ms: Some(0),
                setup_timings: cc_lb_lifecycle::RequestSetupTimings {
                    json_parse_ms: Some(0.0),
                    cache_structure_ms: Some(1.0),
                    cache_token_key_ms: Some(2.0),
                    cache_count_lookup_ms: Some(3.0),
                    cache_tokenizer_queue_ms: Some(4.0),
                    cache_serialize_ms: Some(5.0),
                    cache_tokenize_ms: Some(6.0),
                    prepare_signer_ms: Some(7.0),
                },
                io_timings: RequestIoTimings {
                    request_body_first_chunk_ms: Some(0.125),
                    request_body_receive_ms: Some(2.5),
                    request_body_wait_ms: Some(0.0),
                    request_body_process_ms: Some(1.25),
                    request_body_chunk_count: Some(0),
                    response_body_wait_ms: Some(9.5),
                    response_body_process_ms: Some(0.0),
                    response_body_downstream_poll_gap_ms: Some(3.75),
                    retry_overhead_ms: Some(12.5),
                },
                upstream_body_ms: Some(41),
                first_body_chunk_ms: Some(0),
                internal_errors: Vec::new(),
                event_kind: None,
            },
        )
        .await;

        handle_event(
            &store,
            None,
            metrics.as_ref(),
            &mut partials,
            DEFAULT_ASSEMBLER_MAP_CAP,
            LifecycleEvent::RequestTerminated {
                event_id,
                reason: TerminationReason::Dropped,
                client_status: 499,
                duration_ms: 54,
                request_body_read_ms: None,
                request_body_bytes: None,
                finalize_ms: None,
                limit_reconcile_ms: None,
                observability_post_ms: None,
                proxy_setup_ms: None,
                setup_timings: Default::default(),
                io_timings: Default::default(),
                upstream_body_ms: None,
                first_body_chunk_ms: None,
                internal_errors: Vec::new(),
                event_kind: None,
            },
        )
        .await;

        force_flush_terminations(&store, None, metrics.as_ref(), &mut partials).await;

        let rows = store.rows.lock().expect("capturing store lock");
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.request_body_read_ms, Some(0));
        assert_eq!(row.request_body_bytes, Some(917_567));
        assert_eq!(row.finalize_ms, Some(4));
        assert_eq!(row.limit_reconcile_ms, Some(5));
        assert_eq!(row.observability_post_ms, Some(6));
        assert_eq!(row.proxy_setup_ms, Some(0));
        assert_eq!(row.json_parse_ms, Some(0.0));
        assert_eq!(row.cache_structure_ms, Some(1.0));
        assert_eq!(row.cache_token_key_ms, Some(2.0));
        assert_eq!(row.cache_count_lookup_ms, Some(3.0));
        assert_eq!(row.cache_tokenizer_queue_ms, Some(4.0));
        assert_eq!(row.cache_serialize_ms, Some(5.0));
        assert_eq!(row.request_body_first_chunk_ms, Some(0.125));
        assert_eq!(row.request_body_receive_ms, Some(2.5));
        assert_eq!(row.request_body_wait_ms, Some(0.0));
        assert_eq!(row.request_body_process_ms, Some(1.25));
        assert_eq!(row.request_body_chunk_count, Some(0));
        assert_eq!(row.response_body_wait_ms, Some(9.5));
        assert_eq!(row.response_body_process_ms, Some(0.0));
        assert_eq!(row.response_body_downstream_poll_gap_ms, Some(3.75));
        assert_eq!(row.retry_overhead_ms, Some(12.5));
        assert_eq!(row.cache_tokenize_ms, Some(6.0));
        assert_eq!(row.prepare_signer_ms, Some(7.0));
        assert_eq!(row.upstream_body_ms, Some(41));
        assert_eq!(row.first_body_chunk_ms, Some(0));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn success_terminated_persists_row() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-1"),
            request_id: "req-1".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::AuthCompleted {
            event_id: eid("legacy-1"),
            result: Ok(AuthInfo {
                principal_id: "p1".into(),
                key_id: Some("k1".into()),
                principal_kind: Some("machine".into()),
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
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
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
    async fn setup_timings_survive_success_and_early_failure_termination() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());

        for (event_id, status, reason) in [
            ("setup-success", 200, TerminationReason::Success),
            (
                "setup-signer-failure",
                502,
                TerminationReason::ErrorCode("signer_failed".to_owned()),
            ),
        ] {
            tx.send(LifecycleEvent::RequestStarted {
                event_id: eid(event_id),
                request_id: format!("req-{event_id}"),
                ts_ms: 1_730_000_000_000,
                stream: false,
                source_kind: Some("proxy".to_owned()),
                source_ref_id: None,
                event_kind: None,
            })
            .await
            .expect("send request started");
            tx.send(LifecycleEvent::RequestTerminated {
                event_id: eid(event_id),
                reason,
                client_status: status,
                duration_ms: 10,
                request_body_read_ms: None,
                request_body_bytes: None,
                finalize_ms: None,
                limit_reconcile_ms: None,
                observability_post_ms: None,
                proxy_setup_ms: Some(4),
                setup_timings: cc_lb_lifecycle::RequestSetupTimings {
                    json_parse_ms: Some(0.125),
                    cache_structure_ms: Some(0.25),
                    cache_token_key_ms: Some(0.375),
                    cache_count_lookup_ms: Some(0.5),
                    cache_tokenizer_queue_ms: Some(0.625),
                    cache_serialize_ms: Some(0.75),
                    cache_tokenize_ms: Some(0.0),
                    prepare_signer_ms: Some(1.25),
                },
                io_timings: Default::default(),
                upstream_body_ms: None,
                first_body_chunk_ms: None,
                internal_errors: Vec::new(),
                event_kind: None,
            })
            .await
            .expect("send termination");
        }
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().expect("capturing store lock");
        assert_eq!(rows.len(), 2);
        for row in rows.iter() {
            assert_eq!(row.json_parse_ms, Some(0.125));
            assert_eq!(row.cache_structure_ms, Some(0.25));
            assert_eq!(row.cache_token_key_ms, Some(0.375));
            assert_eq!(row.cache_count_lookup_ms, Some(0.5));
            assert_eq!(row.cache_tokenizer_queue_ms, Some(0.625));
            assert_eq!(row.cache_serialize_ms, Some(0.75));
            assert_eq!(row.cache_tokenize_ms, Some(0.0));
            assert_eq!(row.prepare_signer_ms, Some(1.25));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn source_metadata_from_request_started_propagates_to_final_event() {
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());
        let event_id = eid("source-metadata");

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-source-metadata".to_owned(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: Some("proxy".to_owned()),
            source_ref_id: Some("ingress-123".to_owned()),
            event_kind: None,
        })
        .await
        .expect("send request started");
        tx.send(LifecycleEvent::RequestTerminated {
            event_id,
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 42,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: None,
        })
        .await
        .expect("send request terminated");
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().expect("capturing store lock");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].source_kind.as_deref(), Some("proxy"));
        assert_eq!(rows[0].source_ref_id.as_deref(), Some("ingress-123"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_kind_from_request_started_propagates_to_final_event() {
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());
        let event_id = eid("event-kind");

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-event-kind".to_owned(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: Some("proxy".to_owned()),
            source_ref_id: None,
            event_kind: Some(RequestEventKind::CountTokens),
        })
        .await
        .expect("send request started");
        tx.send(LifecycleEvent::RequestTerminated {
            event_id,
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 12,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: None,
        })
        .await
        .expect("send request terminated");
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().expect("capturing store lock");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_kind, Some(RequestEventKind::CountTokens));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn event_kind_renewal_precedence_overrides_classified_path() {
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());
        let event_id = eid("event-kind-renewal");

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-event-kind-renewal".to_owned(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: Some("renewal".to_owned()),
            source_ref_id: Some("session:9".to_owned()),
            // A renewal replaying /v1/messages must not land as messages.
            event_kind: Some(RequestEventKind::Messages),
        })
        .await
        .expect("send request started");
        tx.send(LifecycleEvent::RequestTerminated {
            event_id,
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 3,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: None,
        })
        .await
        .expect("send request terminated");
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().expect("capturing store lock");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_kind, Some(RequestEventKind::Renewal));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_event_kind_classifies_orphan_shadow_row() {
        // When `RequestStarted` is dropped on the writer channel, the
        // terminal event's classification must still categorize the shadow
        // row instead of leaving it unclassified.
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan-kind"),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 7,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: Some(RequestEventKind::Messages),
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_kind, Some(RequestEventKind::Messages));
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("terminal_without_partial")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_event_kind_leaves_orphan_unclassified() {
        // A terminal event from a producer that never classified the request
        // must not invent a classification for the shadow row.
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan-no-kind"),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 7,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_kind, None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_event_kind_fills_partial_missing_started_kind() {
        // A partial created by mid-lifecycle events (start event lost) picks
        // up the terminal classification at finalization.
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());
        let event_id = eid("partial-kind");

        tx.send(LifecycleEvent::UsageObserved {
            event_id: event_id.clone(),
            usage: UsageSnapshot::default(),
            source: UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id,
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 3,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: Some(RequestEventKind::Messages),
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_kind, Some(RequestEventKind::Messages));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_event_kind_does_not_override_started_classification() {
        // Renewal precedence: the classification merged from `RequestStarted`
        // stays authoritative over whatever the terminal event carries.
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());
        let event_id = eid("started-wins");

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-started-wins".to_owned(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: Some("renewal".to_owned()),
            source_ref_id: Some("session:7".to_owned()),
            event_kind: Some(RequestEventKind::Renewal),
        })
        .await
        .expect("send request started");
        tx.send(LifecycleEvent::RequestTerminated {
            event_id,
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 3,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: Some(RequestEventKind::Messages),
        })
        .await
        .expect("send request terminated");
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_kind, Some(RequestEventKind::Renewal));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stream_error_terminated_carries_error_type_and_message() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-2"),
            request_id: "req-2".into(),
            ts_ms: 1_730_000_000_000,
            stream: true,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
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
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
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
    async fn request_log_upstream_error_populates_final_row_with_broad_code() {
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());
        let event_id = eid("request-log-upstream-error");

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-private-error".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestLogUpstreamErrorObserved {
            event_id: event_id.clone(),
            error_type: "rate_limit_error".into(),
            error_message: "bounded".into(),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id,
            reason: TerminationReason::ErrorCode("upstream_4xx".into()),
            client_status: 429,
            duration_ms: 12,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].error_code.as_deref(), Some("upstream_4xx"));
        assert_eq!(
            rows[0].upstream_error_type.as_deref(),
            Some("rate_limit_error")
        );
        assert_eq!(rows[0].upstream_error_message.as_deref(), Some("bounded"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parse_failed_terminated_still_persists_error_code() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-3"),
            request_id: "req-3".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
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
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
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
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan-terminated"),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 7,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
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
        let handle = spawn_request_event_assembler(rx, store.clone(), None, noop_metrics());
        let event_id = eid("orphan-before-started");

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: event_id.clone(),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 7,
            request_body_read_ms: Some(2),
            request_body_bytes: Some(0),
            finalize_ms: Some(1),
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: Some(1),
            observability_post_ms: Some(2),
            proxy_setup_ms: Some(3),
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: Some(4),
            event_kind: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "late-start-should-not-reclassify".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
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
        assert_eq!(rows[0].request_body_read_ms, Some(2));
        assert_eq!(rows[0].request_body_bytes, Some(0));
        assert_eq!(rows[0].finalize_ms, Some(1));
        assert_eq!(rows[0].upstream_body_ms, Some(4));
        assert_eq!(rows[0].stream_total_ms, None);
        assert!(
            recorder.count_matching(
                "cc_lb_lifecycle_assembler_rows_total",
                "terminated_without_partial",
            ) > before
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn renewal_orphan_flush_waits_for_full_renewal_window_then_finalizes() {
        // Given
        let store = CapturingStore::default();
        let metrics = noop_metrics();
        let event_id = eid("renewal:orphan-session:1");
        let mut partials = HashMap::new();
        handle_event(
            &store,
            None,
            metrics.as_ref(),
            &mut partials,
            DEFAULT_ASSEMBLER_MAP_CAP,
            LifecycleEvent::RequestStarted {
                event_id: event_id.clone(),
                request_id: "renewal-orphan-request".to_owned(),
                ts_ms: 1_730_000_000_000,
                stream: false,
                source_kind: Some("renewal".to_owned()),
                source_ref_id: Some("orphan-session:1".to_owned()),
                event_kind: None,
            },
        )
        .await;
        assert_eq!(DEFAULT_ASSEMBLER_TTL, Duration::from_secs(300));

        // When
        partials
            .get_mut(&event_id)
            .expect("renewal partial is present")
            .inserted_at = Some(
            Instant::now()
                .checked_sub(Duration::from_secs(30))
                .expect("monotonic clock has advanced past 30 seconds"),
        );
        flush_expired_orphans(
            &store,
            None,
            metrics.as_ref(),
            &mut partials,
            DEFAULT_ASSEMBLER_TTL,
        )
        .await;

        // Then
        assert!(store.rows.lock().expect("capturing store lock").is_empty());

        partials
            .get_mut(&event_id)
            .expect("renewal partial remains before the renewal threshold")
            .inserted_at = Some(
            Instant::now()
                .checked_sub(DEFAULT_ASSEMBLER_TTL)
                .expect("monotonic clock has advanced past the renewal threshold"),
        );
        flush_expired_orphans(
            &store,
            None,
            metrics.as_ref(),
            &mut partials,
            DEFAULT_ASSEMBLER_TTL,
        )
        .await;
        let rows = store.rows.lock().expect("capturing store lock");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id.as_deref(), Some(event_id.as_str()));
        assert_eq!(rows[0].source_kind.as_deref(), Some("renewal"));
        assert_eq!(rows[0].source_ref_id.as_deref(), Some("orphan-session:1"));
        // Renewal precedence: source_kind = "renewal" wins even though the
        // producer did not classify the replayed /v1/messages path.
        assert_eq!(rows[0].event_kind, Some(RequestEventKind::Renewal));
        assert_eq!(rows[0].status, 499);
        assert_eq!(
            rows[0].error_code.as_deref(),
            Some("terminal_without_partial")
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
        use cc_lb_control::BusReceiver;
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            noop_metrics(),
        );

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("sse-1"),
            request_id: "req-sse-1".into(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("sse-1"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 10,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
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
    async fn request_started_then_route_publishes_ordered_enrichment_baseline() {
        use crate::event_bus::InMemoryBus;
        use cc_lb_control::BusReceiver;

        // Given an assembler with an in-memory subscriber.
        let (tx, rx) = mpsc::channel(4);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let event_id = eid("request-started-route-baseline");
        let handle = spawn_request_event_assembler(
            rx,
            store,
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            noop_metrics(),
        );

        // When RequestStarted and successful RouteCompleted arrive in order.
        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-baseline".into(),
            ts_ms: 1_730_000_000_000,
            stream: true,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
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
                matched_v3_cache_key: None,
                breakpoint_content_block_index: None,
                matched_content_block_index: None,
                lookback_distance: None,
                predicted_cache_creation_tokens_5m: None,
                predicted_cache_creation_tokens_1h: None,
                token_estimate_source: None,
                cache_value_micros: None,
                formula_winner_upstream_id: None,
                kept_upstream_id: None,
                quota_urgency_5h: None,
                quota_urgency_7d: None,
                quota_urgency_combined: None,
                quota_warning_multiplier: None,
                lineage_would_have_predicted_read_tokens: None,
                lineage_would_have_picked_upstream_id: None,
            }),
            routing_trace: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        // Then the subscriber sees the initial request snapshot before route enrichment.
        let first = broadcast_rx.try_recv().expect("RequestStarted partial");
        let second = broadcast_rx.try_recv().expect("RouteCompleted partial");
        assert!(broadcast_rx.try_recv().is_err());
        let RequestEventUpdate::Partial(started) = first else {
            panic!("expected RequestStarted partial");
        };
        let RequestEventUpdate::Partial(routed) = second else {
            panic!("expected RouteCompleted partial");
        };
        assert_eq!(started.event_id, event_id);
        assert_eq!(started.request_id, "req-baseline");
        assert!(started.stream);
        assert_eq!(started.model, None);
        assert_eq!(started.upstream_name, None);
        assert_eq!(routed.model.as_deref(), Some("claude-3-5-sonnet-20241022"));
        assert_eq!(routed.upstream_name.as_deref(), Some("primary"));
        assert_eq!(routed.route_ms, Some(7));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn thinking_budget_tokens_preserve_partial_and_final_parity() {
        use crate::event_bus::InMemoryBus;

        // Given request fixtures with and without enabled thinking budgets.
        let cases = [
            (
                "thinking-budget-enabled",
                r#"{"model":"claude-sonnet-4-5-20250929","messages":[],"thinking":{"type":"enabled","budget_tokens":18000},"output_config":{"effort":"max"}}"#,
                Some(18_000),
                Some("max"),
            ),
            (
                "thinking-budget-absent",
                r#"{"model":"claude-sonnet-4-5-20250929","messages":[]}"#,
                None,
                None,
            ),
        ];
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_request_event_assembler(
            rx,
            store,
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            noop_metrics(),
        );

        // When each fixture is parsed, assembled, and terminated.
        for (event_id, body, _, _) in cases {
            let value: serde_json::Value = sonic_rs::from_str(body).expect("valid request fixture");
            let thinking_budget_tokens = value
                .get("thinking")
                .filter(|thinking| {
                    thinking.get("type").and_then(serde_json::Value::as_str) == Some("enabled")
                })
                .and_then(|thinking| thinking.get("budget_tokens"))
                .and_then(serde_json::Value::as_u64);
            let reasoning_effort = value
                .get("output_config")
                .and_then(|output_config| output_config.get("effort"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let event_id = eid(event_id);
            tx.send(LifecycleEvent::RequestStarted {
                event_id: event_id.clone(),
                request_id: format!("req-{event_id}"),
                source_kind: None,
                source_ref_id: None,
                event_kind: None,
                ts_ms: 1_730_000_000_000,
                stream: false,
            })
            .await
            .unwrap();
            tx.send(LifecycleEvent::ParseCompleted {
                event_id: event_id.clone(),
                result: Ok(ParseInfo {
                    path: "/v1/messages".to_owned(),
                    method: "POST".to_owned(),
                    model: value
                        .get("model")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    stream: false,
                    body_bytes: u64::try_from(body.len()).expect("fixture body length fits u64"),
                    thinking_budget_tokens,
                    reasoning_effort,
                    ..ParseInfo::default()
                }),
            })
            .await
            .unwrap();
            tx.send(LifecycleEvent::RequestTerminated {
                event_id,
                reason: TerminationReason::Success,
                client_status: 200,
                duration_ms: 10,
                request_body_read_ms: None,
                request_body_bytes: None,
                finalize_ms: None,
                first_body_chunk_ms: None,
                internal_errors: Vec::new(),
                limit_reconcile_ms: None,
                observability_post_ms: None,
                proxy_setup_ms: None,
                setup_timings: Default::default(),
                io_timings: Default::default(),
                upstream_body_ms: None,
                event_kind: None,
            })
            .await
            .unwrap();
        }
        drop(tx);
        handle.shutdown().await;

        // Then the post-parse partial and final event preserve the same value.
        let mut updates = Vec::new();
        while let Ok(update) = broadcast_rx.try_recv() {
            updates.push(update);
        }
        for (event_id, _, expected_budget, expected_effort) in cases {
            let parse_partial = updates
                .iter()
                .filter_map(|update| match update {
                    RequestEventUpdate::Partial(partial) if partial.event_id == event_id => {
                        Some(partial)
                    }
                    RequestEventUpdate::Partial(_) | RequestEventUpdate::Final(_) => None,
                })
                .nth(1)
                .expect("partial snapshot immediately after ParseCompleted");
            let final_event = updates
                .iter()
                .find_map(|update| match update {
                    RequestEventUpdate::Final(final_update)
                        if final_update.event.event_id.as_deref() == Some(event_id) =>
                    {
                        Some(&final_update.event)
                    }
                    RequestEventUpdate::Partial(_) | RequestEventUpdate::Final(_) => None,
                })
                .expect("final request event");

            assert_eq!(parse_partial.thinking_budget_tokens, expected_budget);
            assert_eq!(final_event.thinking_budget_tokens, expected_budget);
            assert_eq!(parse_partial.reasoning_effort.as_deref(), expected_effort);
            assert_eq!(final_event.reasoning_effort.as_deref(), expected_effort);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn classification_fields_preserve_partial_and_final_parity() {
        use crate::event_bus::InMemoryBus;

        let (tx, rx) = mpsc::channel(8);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let handle = spawn_request_event_assembler(
            rx,
            store,
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            noop_metrics(),
        );
        let event_id = eid("classification-parity");

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-classification-parity".to_owned(),
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
            ts_ms: 1_730_000_000_000,
            stream: false,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::ParseCompleted {
            event_id: event_id.clone(),
            result: Ok(ParseInfo {
                path: "/v1/messages".to_owned(),
                method: "POST".to_owned(),
                observed_session_id: Some("session-123".to_owned()),
                request_kind: Some("subagent".to_owned()),
                claude_agent_id: Some("agent-123".to_owned()),
                claude_parent_agent_id: Some("agent-parent".to_owned()),
                parent_session_id: Some("session-parent".to_owned()),
                client_app: Some("cli-bg".to_owned()),
                session_id_source: Some("x-claude-code-session-id".to_owned()),
                ..ParseInfo::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: event_id.clone(),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 10,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let mut updates = Vec::new();
        while let Ok(update) = broadcast_rx.try_recv() {
            updates.push(update);
        }
        let parse_partial = updates
            .iter()
            .filter_map(|update| match update {
                RequestEventUpdate::Partial(partial) if partial.event_id == event_id => {
                    Some(partial)
                }
                RequestEventUpdate::Partial(_) | RequestEventUpdate::Final(_) => None,
            })
            .nth(1)
            .expect("partial snapshot immediately after ParseCompleted");
        let final_event = updates
            .iter()
            .find_map(|update| match update {
                RequestEventUpdate::Final(final_update)
                    if final_update.event.event_id.as_deref() == Some(event_id.as_str()) =>
                {
                    Some(&final_update.event)
                }
                RequestEventUpdate::Partial(_) | RequestEventUpdate::Final(_) => None,
            })
            .expect("final request event");
        assert_eq!(
            parse_partial.observed_session_id.as_deref(),
            Some("session-123")
        );
        assert_eq!(parse_partial.request_kind.as_deref(), Some("subagent"));
        assert_eq!(parse_partial.claude_agent_id.as_deref(), Some("agent-123"));
        assert_eq!(
            parse_partial.claude_parent_agent_id.as_deref(),
            Some("agent-parent")
        );
        assert_eq!(
            parse_partial.parent_session_id.as_deref(),
            Some("session-parent")
        );
        assert_eq!(parse_partial.client_app.as_deref(), Some("cli-bg"));
        assert_eq!(
            parse_partial.session_id_source.as_deref(),
            Some("x-claude-code-session-id")
        );

        assert_eq!(
            parse_partial.observed_session_id,
            final_event.observed_session_id
        );
        assert_eq!(parse_partial.request_kind, final_event.request_kind);
        assert_eq!(parse_partial.claude_agent_id, final_event.claude_agent_id);
        assert_eq!(
            parse_partial.claude_parent_agent_id,
            final_event.claude_parent_agent_id
        );
        assert_eq!(
            parse_partial.parent_session_id,
            final_event.parent_session_id
        );
        assert_eq!(parse_partial.client_app, final_event.client_app);
        assert_eq!(
            parse_partial.session_id_source,
            final_event.session_id_source
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn full_lifecycle_publishes_throttled_partials_and_one_final() {
        use crate::event_bus::InMemoryBus;
        use cc_lb_control::BusReceiver;

        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let event_id = eid("01978c00-0000-7000-8000-000000000002");
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            noop_metrics(),
        );

        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-live-1".into(),
            ts_ms: 1_730_000_000_000,
            stream: true,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::ParseCompleted {
            event_id: event_id.clone(),
            result: Ok(ParseInfo {
                path: "/v1/messages".to_owned(),
                method: "POST".to_owned(),
                model: Some("claude-3-5-sonnet-20241022".to_owned()),
                stream: true,
                body_bytes: 128,
                thread_id: Some("thread-live-1".to_owned()),
                ..ParseInfo::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::AuthCompleted {
            event_id: event_id.clone(),
            result: Ok(AuthInfo {
                principal_id: "principal-live-1".to_owned(),
                key_id: Some("key-live-1".to_owned()),
                principal_kind: Some("machine".to_owned()),
                auth_ms: Some(5),
            }),
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
                matched_v3_cache_key: None,
                breakpoint_content_block_index: None,
                matched_content_block_index: None,
                lookback_distance: None,
                predicted_cache_creation_tokens_5m: None,
                predicted_cache_creation_tokens_1h: None,
                token_estimate_source: None,
                cache_value_micros: None,
                formula_winner_upstream_id: None,
                kept_upstream_id: None,
                quota_urgency_5h: None,
                quota_urgency_7d: None,
                quota_urgency_combined: None,
                quota_warning_multiplier: None,
                lineage_would_have_predicted_read_tokens: None,
                lineage_would_have_picked_upstream_id: None,
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
                body_bytes: Some(456),
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
            request_body_read_ms: Some(17),
            request_body_bytes: None,
            finalize_ms: Some(3),
            first_body_chunk_ms: Some(11),
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: RequestIoTimings {
                request_body_first_chunk_ms: Some(0.0),
                request_body_receive_ms: Some(6.5),
                request_body_wait_ms: Some(5.75),
                request_body_process_ms: Some(0.75),
                request_body_chunk_count: Some(0),
                response_body_wait_ms: Some(20.25),
                response_body_process_ms: Some(1.5),
                response_body_downstream_poll_gap_ms: Some(4.125),
                retry_overhead_ms: None,
            },
            upstream_body_ms: Some(12),
            event_kind: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let mut updates = Vec::new();
        while let Ok(update) = broadcast_rx.try_recv() {
            updates.push(update);
        }

        let partials = updates
            .iter()
            .filter_map(|update| match update {
                RequestEventUpdate::Partial(partial) => Some(partial),
                RequestEventUpdate::Final(_) => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            partials.len(),
            8,
            "unexpected partial sequence: {updates:?}"
        );
        assert!(partials.iter().all(|partial| partial.event_id == event_id));

        let started = partials[0];
        assert_eq!(started.request_id, "req-live-1");
        assert!(started.stream);
        assert_eq!(started.model, None);
        assert_eq!(started.principal_id, None);
        assert_eq!(started.upstream_name, None);

        let parsed = partials[1];
        let parsed_json = serde_json::to_value(parsed).expect("serialize Parse partial");
        assert_eq!(parsed_json["thread_id"], "thread-live-1");
        assert_eq!(parsed.model.as_deref(), Some("claude-3-5-sonnet-20241022"));
        assert_eq!(parsed.principal_id, None);
        assert_eq!(parsed.upstream_name, None);
        assert_eq!(parsed.request_body_bytes, Some(128));

        let authenticated = partials[2];
        assert_eq!(
            authenticated.principal_id.as_deref(),
            Some("principal-live-1")
        );
        assert_eq!(authenticated.key_id.as_deref(), Some("key-live-1"));
        assert_eq!(authenticated.principal_kind.as_deref(), Some("machine"));
        assert_eq!(authenticated.upstream_name, None);

        let routed = partials[3];
        assert_eq!(routed.upstream_name.as_deref(), Some("primary"));
        assert_eq!(routed.upstream_id, Some(Uuid::nil()));
        assert_eq!(routed.route_ms, Some(7));
        let terminated = partials[7];
        assert_eq!(terminated.request_body_read_ms, Some(17));
        assert_eq!(terminated.request_body_bytes, Some(128));
        assert_eq!(terminated.finalize_ms, Some(3));
        assert_eq!(terminated.upstream_body_ms, Some(12));
        assert_eq!(terminated.request_body_first_chunk_ms, Some(0.0));
        assert_eq!(terminated.request_body_receive_ms, Some(6.5));
        assert_eq!(terminated.request_body_wait_ms, Some(5.75));
        assert_eq!(terminated.request_body_process_ms, Some(0.75));
        assert_eq!(terminated.request_body_chunk_count, Some(0));
        assert_eq!(terminated.response_body_wait_ms, Some(20.25));
        assert_eq!(terminated.response_body_process_ms, Some(1.5));
        assert_eq!(terminated.response_body_downstream_poll_gap_ms, Some(4.125));
        assert_eq!(terminated.retry_overhead_ms, None);

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
        assert_eq!(finals[0].event.request_body_read_ms, Some(17));
        assert_eq!(finals[0].event.request_body_bytes, Some(128));
        assert_eq!(finals[0].event.finalize_ms, Some(3));
        assert_eq!(finals[0].event.upstream_body_ms, Some(12));
        assert_eq!(
            finals[0].event.request_body_first_chunk_ms,
            terminated.request_body_first_chunk_ms
        );
        assert_eq!(
            finals[0].event.request_body_receive_ms,
            terminated.request_body_receive_ms
        );
        assert_eq!(
            finals[0].event.request_body_wait_ms,
            terminated.request_body_wait_ms
        );
        assert_eq!(
            finals[0].event.request_body_process_ms,
            terminated.request_body_process_ms
        );
        assert_eq!(
            finals[0].event.request_body_chunk_count,
            terminated.request_body_chunk_count
        );
        assert_eq!(
            finals[0].event.response_body_wait_ms,
            terminated.response_body_wait_ms
        );
        assert_eq!(
            finals[0].event.response_body_process_ms,
            terminated.response_body_process_ms
        );
        assert_eq!(
            finals[0].event.response_body_downstream_poll_gap_ms,
            terminated.response_body_downstream_poll_gap_ms
        );
        assert_eq!(
            finals[0].event.retry_overhead_ms,
            terminated.retry_overhead_ms
        );
        assert_eq!(finals[0].event.body_bytes, Some(456));
        assert!(matches!(updates.last(), Some(RequestEventUpdate::Final(_))));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn parse_and_auth_failures_emit_no_enrichment_partials_and_finalize_once() {
        use crate::event_bus::InMemoryBus;
        use cc_lb_control::BusReceiver;

        // Given an assembler subscribed before a request begins.
        let (tx, rx) = mpsc::channel(8);
        let store = Arc::new(CapturingStore::default());
        let bus = Arc::new(InMemoryBus::new());
        let BusReceiver::InMemory(mut broadcast_rx) = bus.subscribe() else {
            panic!("expected InMemory receiver");
        };
        let event_id = eid("parse-auth-failure-control");
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            noop_metrics(),
        );

        // When parsing and authentication both fail before termination.
        tx.send(LifecycleEvent::RequestStarted {
            event_id: event_id.clone(),
            request_id: "req-failure-control".to_owned(),
            ts_ms: 1_730_000_000_000,
            stream: false,
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::ParseCompleted {
            event_id: event_id.clone(),
            result: Err(ParseFailure::InvalidJson),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::AuthCompleted {
            event_id: event_id.clone(),
            result: Err(AuthFailure::AuthenticationFailed {
                http_status: 401,
                reason: Some("invalid_key".to_owned()),
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: event_id.clone(),
            reason: TerminationReason::ErrorCode("invalid_request".to_owned()),
            client_status: 400,
            duration_ms: 9,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            event_kind: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        // Then neither failed stage emits an empty frame and finalization happens once.
        let mut updates = Vec::new();
        while let Ok(update) = broadcast_rx.try_recv() {
            updates.push(update);
        }
        assert_eq!(updates.len(), 3, "unexpected failure sequence: {updates:?}");
        assert!(matches!(updates[0], RequestEventUpdate::Partial(_)));
        assert!(matches!(updates[1], RequestEventUpdate::Partial(_)));
        assert!(matches!(updates[2], RequestEventUpdate::Final(_)));
        assert_eq!(store.rows.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delayed_pricing_subscriber_does_not_affect_finalized_row() {
        use crate::event_bus::InMemoryBus;

        let bus = Arc::new(InMemoryBus::new());
        let rx = bus.attach_lifecycle_assembler(16);
        let mut pricing_rx = bus.attach_lifecycle_pricing(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(
            rx,
            store.clone(),
            Some(bus.clone() as Arc<dyn RequestEventBus>),
            noop_metrics(),
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
            source_kind: None,
            source_ref_id: None,
            event_kind: None,
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
                matched_v3_cache_key: None,
                breakpoint_content_block_index: None,
                matched_content_block_index: None,
                lookback_distance: None,
                predicted_cache_creation_tokens_5m: None,
                predicted_cache_creation_tokens_1h: None,
                token_estimate_source: None,
                cache_value_micros: None,
                formula_winner_upstream_id: None,
                kept_upstream_id: None,
                quota_urgency_5h: None,
                quota_urgency_7d: None,
                quota_urgency_combined: None,
                quota_warning_multiplier: None,
                lineage_would_have_predicted_read_tokens: None,
                lineage_would_have_picked_upstream_id: None,
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
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            event_kind: None,
        });

        handle.shutdown().await;
        delayed_pricing.await.expect("delayed pricing task joins");

        let rows = store.rows.lock().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id.as_deref(), Some(event_id.as_str()));
        assert_eq!(rows[0].cost_usd_micros, None);
        assert_eq!(rows[0].cost_input_micros, None);
        assert_eq!(rows[0].cost_output_micros, None);
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
