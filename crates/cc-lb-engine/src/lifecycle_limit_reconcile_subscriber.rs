//! Limit-reservation reconcile subscriber.
//!
//! Reconciles limit reservations by consuming `LifecycleEvent`s instead
//! of running inline on the handler. Tracks (per `event_id`):
//! - the reservation id carried on [`LifecycleEvent::LimitDecision`],
//! - the latest [`UsageSnapshot`] observed on
//!   [`LifecycleEvent::UsageObserved`] and [`LifecycleEvent::StreamCompleted`],
//! - the cost breakdown carried on [`LifecycleEvent::Priced`].
//!
//! On [`LifecycleEvent::RequestTerminated`], the subscriber calls
//! [`LimitEngine::reconcile_by_id`].
//!
//! Orphan protection is handled by the `LimitEngine`'s TTL sweeper — it
//! evicts reservations older than `ttl_secs` regardless of whether the
//! subscriber saw a `RequestTerminated` for them.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cc_lb_lifecycle::{EventId, LifecycleEvent, TerminationReason, UsageSnapshot};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::api_keys::limit_engine::LimitEngine;

pub const DEFAULT_LIMIT_RECONCILE_MAP_CAP: usize = 4096;
pub const DEFAULT_LIMIT_RECONCILE_TTL: Duration = Duration::from_secs(300);
const SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Same rationale as the assembler's grace period: `Priced` is emitted by
/// the pricing subscriber on a separate task and may arrive AFTER
/// `RequestTerminated` at this subscriber's channel. Holding the partial for
/// this window lets the cost merge in before we call `reconcile_by_id`.
const FINALIZATION_GRACE: Duration = Duration::from_millis(200);

/// Must be small compared to FINALIZATION_GRACE so terminated partials do
/// not sit past their deadline waiting for the next tick.
const FINALIZATION_TICK: Duration = Duration::from_millis(20);

/// Handle to the spawned subscriber task.
pub struct LimitReconcileSubscriberHandle {
    shutdown_tx: oneshot::Sender<()>,
    join: JoinHandle<()>,
}

impl LimitReconcileSubscriberHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
        if let Err(error) = self.join.await {
            tracing::warn!(%error, "lifecycle limit reconcile subscriber task panicked");
        }
    }
}

pub fn spawn_lifecycle_limit_reconcile_subscriber(
    rx: mpsc::Receiver<LifecycleEvent>,
    engine: Arc<LimitEngine>,
) -> LimitReconcileSubscriberHandle {
    spawn_with_config(
        rx,
        engine,
        DEFAULT_LIMIT_RECONCILE_MAP_CAP,
        DEFAULT_LIMIT_RECONCILE_TTL,
    )
}

pub fn spawn_with_config(
    rx: mpsc::Receiver<LifecycleEvent>,
    engine: Arc<LimitEngine>,
    map_cap: usize,
    ttl: Duration,
) -> LimitReconcileSubscriberHandle {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let join = tokio::spawn(subscriber_loop(rx, engine, map_cap, ttl, shutdown_rx));
    LimitReconcileSubscriberHandle { shutdown_tx, join }
}

#[derive(Default)]
struct Partial {
    inserted_at: Option<Instant>,
    reservation_id: Option<String>,
    usage: UsageSnapshot,
    usage_seen: bool,
    cost_micros: Option<i64>,
    termination: Option<TerminationInfo>,
}

struct TerminationInfo {
    reason: TerminationReason,
    deadline: Instant,
    expects_priced: bool,
}

impl TerminationInfo {
    fn is_ready(&self, partial: &Partial) -> bool {
        !self.expects_priced || partial.cost_micros.is_some()
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
}

async fn subscriber_loop(
    mut rx: mpsc::Receiver<LifecycleEvent>,
    engine: Arc<LimitEngine>,
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
                    Some(event) => handle_event(&engine, &mut partials, map_cap, event),
                    None => break,
                }
            }
            _ = finalization_tick.tick() => flush_expired_terminations(&engine, &mut partials),
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(&engine, &mut partials, map_cap, event);
    }
    force_flush_terminations(&engine, &mut partials);
}

fn flush_expired_terminations(engine: &LimitEngine, partials: &mut HashMap<EventId, Partial>) {
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
            let reason = partial
                .termination
                .as_ref()
                .map(|t| t.reason.clone())
                .expect("expired implies termination present");
            finalize(engine, partial, &reason);
        }
    }
}

fn force_flush_terminations(engine: &LimitEngine, partials: &mut HashMap<EventId, Partial>) {
    let pending: Vec<EventId> = partials
        .iter()
        .filter_map(|(id, p)| p.termination.as_ref().map(|_| id.clone()))
        .collect();
    for event_id in pending {
        if let Some(partial) = partials.remove(&event_id) {
            let reason = partial
                .termination
                .as_ref()
                .map(|t| t.reason.clone())
                .expect("pending implies termination present");
            finalize(engine, partial, &reason);
        }
    }
}

fn handle_event(
    engine: &LimitEngine,
    partials: &mut HashMap<EventId, Partial>,
    map_cap: usize,
    event: LifecycleEvent,
) {
    let now = Instant::now();
    let event_id = event.event_id().clone();

    if let LifecycleEvent::RequestTerminated { reason, .. } = &event {
        let existing = partials.remove(&event_id);
        let Some(mut partial) = existing else {
            metrics::counter!(
                "cc_lb_limit_reconcile_subscriber_rows_total",
                "outcome" => "terminated_without_partial",
            )
            .increment(1);
            return;
        };
        let expects_priced = partial.usage_seen && partial.cost_micros.is_none();
        let termination = TerminationInfo {
            reason: reason.clone(),
            deadline: now + FINALIZATION_GRACE,
            expects_priced,
        };
        if termination.is_ready(&partial) {
            finalize(engine, partial, &termination.reason);
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
        let reason = partial
            .termination
            .as_ref()
            .map(|t| t.reason.clone())
            .expect("readiness implies termination present");
        finalize(engine, partial, &reason);
    }

    if partials.len() > map_cap {
        drop_oldest(partials);
    }
}

fn merge(partial: &mut Partial, event: LifecycleEvent) {
    match event {
        LifecycleEvent::LimitDecision {
            decision: cc_lb_lifecycle::LimitDecisionKind::Reserved { reservation_id, .. },
            ..
        } => {
            partial.reservation_id = Some(reservation_id);
        }
        LifecycleEvent::LimitDecision { .. } => {}
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
        LifecycleEvent::Priced { cost, .. } => {
            partial.cost_micros = cost.total_micros;
        }
        _ => {}
    }
}

fn finalize(engine: &LimitEngine, partial: Partial, reason: &TerminationReason) {
    let Some(reservation_id) = partial.reservation_id else {
        metrics::counter!(
            "cc_lb_limit_reconcile_subscriber_rows_total",
            "outcome" => "no_reservation",
        )
        .increment(1);
        return;
    };
    if reservation_id.is_empty() {
        metrics::counter!(
            "cc_lb_limit_reconcile_subscriber_rows_total",
            "outcome" => "empty_reservation_id",
        )
        .increment(1);
        return;
    }
    // Only reconcile on Success. On errors, handler-side (or TTL sweeper)
    // handles the refund path — matching legacy behavior.
    if !matches!(reason, TerminationReason::Success) {
        metrics::counter!(
            "cc_lb_limit_reconcile_subscriber_rows_total",
            "outcome" => "skipped_non_success",
        )
        .increment(1);
        return;
    }
    if !partial.usage_seen {
        metrics::counter!(
            "cc_lb_limit_reconcile_subscriber_rows_total",
            "outcome" => "skipped_no_usage",
        )
        .increment(1);
        return;
    }
    let input = partial.usage.input_tokens;
    let output = partial.usage.output_tokens;
    let cost = partial.cost_micros.unwrap_or(0);

    let reconciled = engine.reconcile_by_id(&reservation_id, input, output, cost);
    let outcome = if reconciled {
        "reconciled"
    } else {
        "id_unknown"
    };
    metrics::counter!(
        "cc_lb_limit_reconcile_subscriber_rows_total",
        "outcome" => outcome,
    )
    .increment(1);
}

fn sweep_orphans(partials: &mut HashMap<EventId, Partial>, ttl: Duration) {
    let now = Instant::now();
    let before = partials.len();
    partials.retain(|_, p| p.inserted_at.is_none_or(|t| now.duration_since(t) < ttl));
    let removed = before.saturating_sub(partials.len());
    if removed > 0 {
        metrics::counter!(
            "cc_lb_limit_reconcile_subscriber_rows_total",
            "outcome" => "orphan_ttl_evicted",
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
        "cc_lb_limit_reconcile_subscriber_rows_total",
        "outcome" => "cap_evicted",
    )
    .increment(1);
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;
    use crate::api_keys::concurrent_guard::KeyConcurrencyManager;
    use cc_lb_lifecycle::{LimitDecisionKind, UsageSource};

    fn build_engine() -> Arc<LimitEngine> {
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            cc_lb_testkit::fixed_clock(1_700_000_000),
        )
    }

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn authoritative_mode_skips_when_reservation_unknown() {
        let (tx, rx) = mpsc::channel(16);
        let engine = build_engine();
        let handle = spawn_lifecycle_limit_reconcile_subscriber(rx, engine.clone());

        tx.send(LifecycleEvent::LimitDecision {
            event_id: eid("evt-2"),
            decision: LimitDecisionKind::Reserved {
                reservation_id: "unknown-id".to_owned(),
                amount: 100,
                limit_reserve_ms: None,
            },
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::UsageObserved {
            event_id: eid("evt-2"),
            usage: UsageSnapshot {
                input_tokens: 5,
                output_tokens: 10,
                ..Default::default()
            },
            source: UsageSource::NonStreamBody,
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-2"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 20,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn non_success_termination_skips_reconcile() {
        let (tx, rx) = mpsc::channel(16);
        let engine = build_engine();
        let handle = spawn_lifecycle_limit_reconcile_subscriber(rx, engine.clone());

        tx.send(LifecycleEvent::LimitDecision {
            event_id: eid("evt-3"),
            decision: LimitDecisionKind::Reserved {
                reservation_id: "res-3".to_owned(),
                amount: 100,
                limit_reserve_ms: None,
            },
        })
        .await
        .unwrap();
        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("evt-3"),
            reason: TerminationReason::ErrorCode("upstream_5xx".into()),
            client_status: 500,
            duration_ms: 20,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn terminated_without_reservation_id_does_not_panic() {
        let (tx, rx) = mpsc::channel(16);
        let engine = build_engine();
        let handle = spawn_lifecycle_limit_reconcile_subscriber(rx, engine.clone());

        tx.send(LifecycleEvent::RequestTerminated {
            event_id: eid("orphan"),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 1,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;
    }
    #[test]
    fn t2__reconcile_subscriber_reconciles_tokens_and_cost_on_success() {
        use cc_lb_storage_api::types::{KeyStatus, Limit, LimitKind, StoredApiKeyRecord};
        use metrics_util::debugging::DebugValue;

        let (recorder, snapshotter) = cc_lb_testkit::local_recorder();
        let _recorder_guard = cc_lb_testkit::install_local_recorder(&recorder);

        let engine = build_engine();
        let view = crate::api_keys::principal_view::PrincipalView::for_tests(
            "principal-1",
            true,
            Vec::new(),
            Vec::new(),
            HashMap::new(),
        );
        let record = StoredApiKeyRecord {
            key_hash_b64: "key-1".to_owned(),
            status: KeyStatus::Active,
            limit_overrides: vec![
                Limit {
                    kind: LimitKind::TotalTokens,
                    window_secs: 60,
                    cap_micros: 100,
                },
                Limit {
                    kind: LimitKind::CostUsd,
                    window_secs: 60,
                    cap_micros: 1_000,
                },
            ],
            ..StoredApiKeyRecord::default()
        };
        let reservation = engine
            .reserve(
                &view,
                &record,
                "principal-1",
                "claude-test",
                100,
                0,
                Some(120),
            )
            .expect("reservation succeeds");
        let reservation_id = reservation.id().to_owned();
        reservation.forget();
        let event_id = eid("reconcile-success");
        let mut partials = HashMap::new();

        for event in [
            LifecycleEvent::LimitDecision {
                event_id: event_id.clone(),
                decision: LimitDecisionKind::Reserved {
                    reservation_id,
                    amount: 100,
                    limit_reserve_ms: None,
                },
            },
            LifecycleEvent::UsageObserved {
                event_id: event_id.clone(),
                usage: UsageSnapshot {
                    input_tokens: 40,
                    output_tokens: 0,
                    ..UsageSnapshot::default()
                },
                source: UsageSource::NonStreamBody,
            },
            LifecycleEvent::Priced {
                event_id: event_id.clone(),
                cost: cc_lb_request_log::CostBreakdown {
                    total_micros: Some(120),
                    ..Default::default()
                },
            },
            LifecycleEvent::RequestTerminated {
                event_id: event_id.clone(),
                reason: TerminationReason::Success,
                client_status: 200,
                duration_ms: 20,
                request_body_read_ms: None,
                request_body_bytes: None,
                finalize_ms: None,
                first_body_chunk_ms: None,
                internal_errors: Vec::new(),
                limit_reconcile_ms: None,
                observability_post_ms: None,
                proxy_setup_ms: None,
                setup_timings: Default::default(),
                upstream_body_ms: None,
            },
        ] {
            handle_event(
                &engine,
                &mut partials,
                DEFAULT_LIMIT_RECONCILE_MAP_CAP,
                event,
            );
        }

        assert!(partials.is_empty());
        let headers = engine.headers_for("key-1", "principal-1");
        assert_eq!(
            headers
                .iter()
                .find(|(name, _)| name == "anthropic-ratelimit-tokens-remaining")
                .map(|(_, value)| value.as_str()),
            Some("60")
        );
        assert_eq!(
            engine
                .reserve(
                    &view,
                    &record,
                    "principal-1",
                    "claude-test",
                    0,
                    0,
                    Some(881),
                )
                .err(),
            Some(crate::api_keys::limit_engine::RejectReason::CostRateLimit)
        );
        engine
            .reserve(
                &view,
                &record,
                "principal-1",
                "claude-test",
                0,
                0,
                Some(880),
            )
            .expect("reconciled cost leaves exactly 880 micros available");

        let samples = snapshotter.snapshot().into_vec();
        let counter_value = |name: &str, labels: &[(&str, &str)]| {
            samples
                .iter()
                .filter(|(key, _, _, _)| {
                    key.key().name() == name
                        && labels.iter().all(|(expected_key, expected_value)| {
                            key.key().labels().any(|label| {
                                label.key() == *expected_key && label.value() == *expected_value
                            })
                        })
                })
                .map(|(_, _, _, value)| match value {
                    DebugValue::Counter(value) => *value,
                    other => panic!("expected counter {name}, got {other:?}"),
                })
                .sum::<u64>()
        };
        assert_eq!(
            counter_value(
                "cc_lb_limit_reconcile_subscriber_rows_total",
                &[("outcome", "reconciled")]
            ),
            1
        );
        assert_eq!(
            counter_value(
                "cc_lb_limit_reconcile_subscriber_rows_total",
                &[("outcome", "id_unknown")]
            ),
            0
        );
        assert_eq!(
            counter_value("cc_lb_limit_reservation_ttl_evicted_total", &[]),
            0
        );
    }
}
