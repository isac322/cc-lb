use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_lifecycle::{
    AuthInfo, CacheBreakpointLite, CacheBreakpointSourceLite, CostBreakdown, EventId,
    LifecycleEvent, ParseInfo, RequestCacheStateLite, RouteInfo, TerminationReason, UsageSnapshot,
};
use cc_lb_storage_api::types::{
    RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestCacheState,
};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

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
) -> RequestEventAssemblerHandle {
    spawn_with_config(
        rx,
        storage,
        mode,
        DEFAULT_ASSEMBLER_MAP_CAP,
        DEFAULT_ASSEMBLER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    mode: AssemblerMode,
    map_cap: usize,
    ttl: Duration,
) -> RequestEventAssemblerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(assembler_loop(rx, storage, mode, map_cap, ttl, shutdown_rx));
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
                    Some(event) => handle_event(&*storage, &mut partials, mode, map_cap, event).await,
                    None => break,
                }
            }
            _ = finalization_tick.tick() => {
                flush_expired_terminations(&*storage, &mut partials, mode).await;
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&*storage, &mut partials, mode, map_cap, event).await;
    }
    force_flush_terminations(&*storage, &mut partials, mode).await;
}

#[allow(clippy::too_many_arguments)]
async fn write_finalized_rows(
    storage: &dyn RequestEventStore,
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
            Ok(()) => wrote += 1,
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
            let partial = Partial::orphan();
            write_finalized_rows(
                storage,
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
            result: Ok(info), ..
        } => {
            partial.route = Some(info);
        }
        LifecycleEvent::RouteCompleted { .. } => {}
        LifecycleEvent::LimitDecision {
            decision:
                cc_lb_lifecycle::LimitDecisionKind::Reserved {
                    reservation_id,
                    amount,
                },
            ..
        } => {
            partial.limit_reservation_id = Some(reservation_id);
            partial.limit_amount = Some(amount);
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
        LifecycleEvent::UpstreamResponseStarted { status, .. } => {
            partial.upstream_response_status = Some(status);
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
    let (upstream_id, upstream_name, route_model) = partial
        .route
        .as_ref()
        .map(|r| {
            (
                Some(r.upstream_id),
                Some(r.upstream_name.clone()),
                r.model.clone(),
            )
        })
        .unwrap_or_default();
    let model = route_model.or_else(|| partial.parse.as_ref().and_then(|p| p.model.clone()));

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
        let handle = spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly);

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
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-1"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 42,
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
        let handle = spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly);

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
        let handle = spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly);

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
        let handle = spawn_request_event_assembler(rx, store.clone(), AssemblerMode::ShadowOnly);

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan-terminated"),
            reason: TerminationReason::Dropped,
            client_status: 499,
            duration_ms: 7,
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
        };
        let _ = cc_lb_lifecycle::UsageSource::NonStreamBody;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn both_mode_persists_legacy_and_shadow_rows_from_same_terminate() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone(), AssemblerMode::Both);

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
}
