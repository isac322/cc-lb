use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_lifecycle::{
    AuthInfo, EventId, LifecycleEvent, ParseInfo, RouteInfo, TerminationReason, UsageSnapshot,
};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

pub const DEFAULT_ASSEMBLER_MAP_CAP: usize = 4096;
pub const DEFAULT_ASSEMBLER_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

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
) -> RequestEventAssemblerHandle {
    spawn_with_config(
        rx,
        storage,
        DEFAULT_ASSEMBLER_MAP_CAP,
        DEFAULT_ASSEMBLER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    map_cap: usize,
    ttl: Duration,
) -> RequestEventAssemblerHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(assembler_loop(rx, storage, map_cap, ttl, shutdown_rx));
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
}

async fn assembler_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    storage: Arc<dyn RequestEventStore>,
    map_cap: usize,
    ttl: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    let mut sweeper = tokio::time::interval(SWEEP_INTERVAL);
    sweeper.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    sweeper.tick().await;

    loop {
        tokio::select! {
            biased;
            event = rx.recv() => {
                match event {
                    Some(event) => handle_event(&*storage, &mut partials, map_cap, event).await,
                    None => break,
                }
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&*storage, &mut partials, map_cap, event).await;
    }
}

async fn handle_event(
    storage: &dyn RequestEventStore,
    partials: &mut HashMap<EventId, Partial>,
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
        if let Some(partial) = partials.remove(&event_id) {
            let row = finalize(&event_id, partial, reason, *client_status, *duration_ms);
            if let Err(error) = storage.append_request_event(&row).await {
                tracing::warn!(
                    %error,
                    shadow_event_id = %event_id,
                    "lifecycle event assembler: failed to persist shadow row",
                );
                cc_lb_observability::increment_dropped_events_by(
                    "lifecycle_assembler_storage_error",
                    1,
                );
            } else {
                metrics::counter!("cc_lb_lifecycle_assembler_rows_total", "outcome" => "written")
                    .increment(1);
            }
        } else {
            metrics::counter!(
                "cc_lb_lifecycle_assembler_rows_total",
                "outcome" => "terminated_without_partial"
            )
            .increment(1);
        }
        return;
    }

    let partial = partials
        .entry(event_id)
        .or_insert_with(|| Partial::new(now));
    partial.touch(now);
    merge(partial, event);

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
        _ => {}
    }
}

fn finalize(
    legacy_event_id: &EventId,
    partial: Partial,
    reason: &TerminationReason,
    client_status: u16,
    duration_ms: u64,
) -> RequestEvent {
    let ts_ms = partial.ts_ms;
    let request_id = partial
        .request_id
        .clone()
        .unwrap_or_else(|| "req_unknown_shadow".to_owned());
    let error_code = match reason {
        TerminationReason::Success => None,
        TerminationReason::Dropped => Some("terminal_dropped".to_owned()),
        TerminationReason::ErrorCode(code) => Some(code.clone()),
        _ => None,
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

    RequestEvent {
        ts: ts_ms / 1_000,
        ts_ms: Some(ts_ms),
        event_id: Some(Uuid::now_v7().to_string()),
        shadow_event_id: Some(legacy_event_id.clone()),
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
        let handle = spawn_request_event_assembler(rx, store.clone());

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
        let handle = spawn_request_event_assembler(rx, store.clone());

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
        let handle = spawn_request_event_assembler(rx, store.clone());

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
    async fn terminated_without_partial_does_not_panic() {
        let (tx, rx) = mpsc::channel(16);
        let store = Arc::new(CapturingStore::default());
        let handle = spawn_request_event_assembler(rx, store.clone());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan-terminated"),
            reason: TerminationReason::Dropped,
            client_status: 0,
            duration_ms: 0,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        assert!(store.rows.lock().unwrap().is_empty());
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
}
