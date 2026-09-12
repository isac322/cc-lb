use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_lb_domain::PrincipalKind;
use cc_lb_lifecycle::{EventId, LifecycleEvent, UsageSnapshot};
use cc_lb_observability::{ObservabilityHook, ObserveEvent};
use cc_lb_storage_api::types::PrincipalKindLite;
use http::StatusCode;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub const DEFAULT_HOOK_ADAPTER_MAP_CAP: usize = 4096;
pub const DEFAULT_HOOK_ADAPTER_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

pub struct ObservabilityHookAdapterHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl ObservabilityHookAdapterHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "observability hook adapter task panicked");
        }
    }
}

pub fn spawn_observability_hook_adapter(
    rx: mpsc::Receiver<LifecycleEvent>,
    hooks: Vec<Arc<dyn ObservabilityHook>>,
) -> ObservabilityHookAdapterHandle {
    spawn_with_config(
        rx,
        hooks,
        DEFAULT_HOOK_ADAPTER_MAP_CAP,
        DEFAULT_HOOK_ADAPTER_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    hooks: Vec<Arc<dyn ObservabilityHook>>,
    map_cap: usize,
    ttl: Duration,
) -> ObservabilityHookAdapterHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(adapter_loop(rx, hooks, map_cap, ttl, shutdown_rx));
    ObservabilityHookAdapterHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    usage: UsageSnapshot,
    usage_seen: bool,
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

async fn adapter_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    hooks: Vec<Arc<dyn ObservabilityHook>>,
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
                    Some(event) => handle_event(&hooks, &mut partials, map_cap, event),
                    None => break,
                }
            }
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&hooks, &mut partials, map_cap, event);
    }
}

fn handle_event(
    hooks: &[Arc<dyn ObservabilityHook>],
    partials: &mut HashMap<EventId, Partial>,
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if let LifecycleEvent::RequestTerminated {
        client_status,
        duration_ms,
        ..
    } = &event
    {
        let partial = partials.remove(&event_id);
        emit_finished(hooks, partial.as_ref(), *client_status, *duration_ms);
        return;
    }

    match &event {
        LifecycleEvent::AuthenticationCompleted {
            principal_id,
            principal_kind,
            ..
        } => {
            emit_observe_event(
                hooks,
                ObserveEvent::AuthnComplete {
                    principal_id: principal_id.clone(),
                    kind: principal_kind_into(*principal_kind),
                },
            );
            return;
        }
        LifecycleEvent::ProviderErrorObserved {
            code,
            message,
            source,
            ..
        } => {
            emit_observe_event(
                hooks,
                ObserveEvent::Error {
                    code: code.clone(),
                    message: message.clone(),
                    source: source.clone(),
                },
            );
            return;
        }
        LifecycleEvent::RequestLogUpstreamErrorObserved { .. } => return,
        _ => {}
    }

    let partial = partials
        .entry(event_id)
        .or_insert_with(|| Partial::new(now));
    partial.touch(now);
    match event {
        LifecycleEvent::RequestStarted { .. } => {}
        LifecycleEvent::UsageObserved { usage, .. } => {
            partial.usage = usage;
            partial.usage_seen = true;
        }
        LifecycleEvent::StreamCompleted {
            result: Ok(success),
            ..
        } => {
            partial.usage = success.usage;
            partial.usage_seen = true;
        }
        _ => {}
    }

    if partials.len() > map_cap {
        drop_oldest(partials);
    }
}

fn emit_observe_event(hooks: &[Arc<dyn ObservabilityHook>], event: ObserveEvent) {
    for hook in hooks {
        let _result = hook.observe(event.clone());
    }
}

fn principal_kind_into(principal_kind: PrincipalKindLite) -> PrincipalKind {
    match principal_kind {
        PrincipalKindLite::Human | PrincipalKindLite::Machine => PrincipalKind::ApiKey,
    }
}

fn emit_finished(
    hooks: &[Arc<dyn ObservabilityHook>],
    partial: Option<&Partial>,
    client_status: u16,
    duration_ms: u64,
) {
    let status = StatusCode::from_u16(client_status).unwrap_or(StatusCode::OK);
    let (input_tokens, output_tokens, cache_creation, cache_read) = partial
        .filter(|p| p.usage_seen)
        .map(|p| {
            (
                (p.usage.input_tokens > 0).then_some(p.usage.input_tokens),
                (p.usage.output_tokens > 0).then_some(p.usage.output_tokens),
                (p.usage.cache_creation_input_tokens > 0)
                    .then_some(p.usage.cache_creation_input_tokens),
                (p.usage.cache_read_input_tokens > 0).then_some(p.usage.cache_read_input_tokens),
            )
        })
        .unwrap_or((None, None, None, None));
    let event = ObserveEvent::RequestFinished {
        status,
        input_tokens,
        output_tokens,
        cache_creation_input_tokens: cache_creation,
        cache_read_input_tokens: cache_read,
        duration_ms,
    };
    let mut delivered = 0u64;
    for hook in hooks {
        if hook.observe(event.clone()).is_ok() {
            delivered = delivered.saturating_add(1);
        }
    }
    metrics::counter!(
        "cc_lb_lifecycle_hook_adapter_fires_total",
        "outcome" => "delivered"
    )
    .increment(delivered);
    if delivered == 0 && !hooks.is_empty() {
        metrics::counter!(
            "cc_lb_lifecycle_hook_adapter_fires_total",
            "outcome" => "all_dropped"
        )
        .increment(1);
    }
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_lifecycle_hook_adapter_fires_total",
            "outcome" => "orphan_ttl_evicted"
        )
        .increment(removed as u64);
    }
}

fn drop_oldest(partials: &mut HashMap<EventId, Partial>) {
    let Some(oldest_key) = partials
        .iter()
        .min_by_key(|(_, p)| p.inserted_at.unwrap_or_else(Instant::now))
        .map(|(k, _)| k.clone())
    else {
        return;
    };
    partials.remove(&oldest_key);
    metrics::counter!(
        "cc_lb_lifecycle_hook_adapter_fires_total",
        "outcome" => "cap_evicted"
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_lifecycle::{StreamSuccess, TerminationReason, UsageSource};
    use cc_lb_observability::{ObservabilityError, ObserveEvent};
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct RecordingHook {
        events: StdMutex<Vec<ObserveEvent>>,
    }

    impl ObservabilityHook for RecordingHook {
        fn observe(&self, event: ObserveEvent) -> Result<(), ObservabilityError> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_fires_request_finished_with_usage() {
        let (tx, rx) = mpsc::channel(16);
        let hook = Arc::new(RecordingHook::default());
        let handle = spawn_observability_hook_adapter(rx, vec![hook.clone()]);

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-a"),
            request_id: "req-a".into(),
            ts_ms: 0,
            stream: false,
            source_kind: None,
            source_ref_id: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("legacy-a"),
            usage: UsageSnapshot {
                input_tokens: 10,
                output_tokens: 20,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 4,
                ..Default::default()
            },
            source: UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-a"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 55,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let events = hook.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            ObserveEvent::RequestFinished {
                status,
                input_tokens,
                output_tokens,
                cache_creation_input_tokens,
                cache_read_input_tokens,
                duration_ms,
            } => {
                assert_eq!(*status, StatusCode::OK);
                assert_eq!(*input_tokens, Some(10));
                assert_eq!(*output_tokens, Some(20));
                assert_eq!(*cache_creation_input_tokens, Some(3));
                assert_eq!(*cache_read_input_tokens, Some(4));
                assert_eq!(*duration_ms, 55);
            }
            other => panic!("expected RequestFinished, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_partial_still_fires_with_none_tokens() {
        let (tx, rx) = mpsc::channel(16);
        let hook = Arc::new(RecordingHook::default());
        let handle = spawn_observability_hook_adapter(rx, vec![hook.clone()]);

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-b"),
            reason: TerminationReason::ErrorCode("body_too_large".into()),
            client_status: 413,
            duration_ms: 5,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let events = hook.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            ObserveEvent::RequestFinished {
                status,
                input_tokens,
                duration_ms,
                ..
            } => {
                assert_eq!(status.as_u16(), 413);
                assert_eq!(*input_tokens, None);
                assert_eq!(*duration_ms, 5);
            }
            other => panic!("expected RequestFinished, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stream_success_populates_usage_for_terminator() {
        let (tx, rx) = mpsc::channel(16);
        let hook = Arc::new(RecordingHook::default());
        let handle = spawn_observability_hook_adapter(rx, vec![hook.clone()]);

        tx.send(LifecycleEvent::RequestStarted {
            event_id: eid("legacy-c"),
            request_id: "req-c".into(),
            ts_ms: 0,
            stream: true,
            source_kind: None,
            source_ref_id: None,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::StreamCompleted {
            event_id: eid("legacy-c"),
            result: Ok(StreamSuccess {
                usage: UsageSnapshot {
                    input_tokens: 7,
                    output_tokens: 9,
                    ..Default::default()
                },
                sse_event_count: 3,
                ..Default::default()
            }),
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("legacy-c"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 88,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            upstream_body_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        let events = hook.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            ObserveEvent::RequestFinished {
                input_tokens,
                output_tokens,
                ..
            } => {
                assert_eq!(*input_tokens, Some(7));
                assert_eq!(*output_tokens, Some(9));
            }
            other => panic!("expected RequestFinished, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_log_upstream_error_direct_injection_emits_no_observe_error() {
        let (tx, rx) = mpsc::channel(1);
        let hook = Arc::new(RecordingHook::default());
        let handle = spawn_observability_hook_adapter(rx, vec![hook.clone()]);

        tx.send(LifecycleEvent::RequestLogUpstreamErrorObserved {
            event_id: eid("request-log-private"),
            error_type: "rate_limit_error".into(),
            error_message: "bounded".into(),
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;

        assert!(hook.events.lock().unwrap().is_empty());
    }
}
