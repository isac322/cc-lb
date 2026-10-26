//! Limit-reservation reconcile subscriber.
//!
//! Reconciles limit reservations by consuming `LifecycleEvent`s instead
//! of running inline on the handler. Tracks (per `event_id`):
//! - the reservation id carried on [`LifecycleEvent::LimitDecision`],
//! - the latest [`UsageSnapshot`] observed on
//!   [`LifecycleEvent::UsageObserved`] and [`LifecycleEvent::StreamCompleted`],
//! - the cost breakdown carried on [`LifecycleEvent::Priced`].
//!
//! On [`LifecycleEvent::RequestTerminated`], the subscriber settles the
//! reservation exactly once: successful HTTP responses and all WebSocket turns
//! with observed usage are reconciled via [`LimitEngine::reconcile_by_id`].
//! WebSocket interruptions do not erase measured usage. Other outcomes are
//! refunded in full via [`LimitEngine::refund_by_id`]. The handler hands every committed response's
//! reservation to this subscriber (`ResponseAccountingGuard::forget`) whatever
//! its outcome, so no other owner refunds it. Reservations dropped before that
//! handoff (client cancel, failover pre-commit) are refunded by the handle's
//! RAII guard; the engine removes the record on first settlement, so the later
//! terminal event here is a no-op.
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

use cc_lb_control::api_keys::limit_engine::LimitEngine;

// A private, statically dispatched boundary lets tests observe real settlement
// calls without adding public engine hooks or runtime observation state.
trait ReservationSettlement {
    fn refund_by_id(&self, id: &str) -> bool;
    fn reconcile_by_id(&self, id: &str, input: u64, output: u64, cost: i64) -> bool;
}

impl ReservationSettlement for LimitEngine {
    #[inline]
    fn refund_by_id(&self, id: &str) -> bool {
        LimitEngine::refund_by_id(self, id)
    }

    #[inline]
    fn reconcile_by_id(&self, id: &str, input: u64, output: u64, cost: i64) -> bool {
        LimitEngine::reconcile_by_id(self, id, input, output, cost)
    }
}

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
    websocket: bool,
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
                    Some(event) => handle_event(engine.as_ref(), &mut partials, map_cap, event),
                    None => break,
                }
            }
            _ = finalization_tick.tick() => flush_expired_terminations(&engine, &mut partials),
            _ = sweeper.tick() => sweep_orphans(&mut partials, ttl),
            _ = &mut shutdown => break,
        }
    }

    while let Ok(event) = rx.try_recv() {
        handle_event(engine.as_ref(), &mut partials, map_cap, event);
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
    engine: &impl ReservationSettlement,
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
        // Measured WebSocket failures are billable too. Wait for their existing
        // background pricing event, while unmeasured outcomes refund immediately.
        let expects_priced = (partial.websocket || matches!(reason, TerminationReason::Success))
            && partial.usage_seen
            && partial.cost_micros.is_none();
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
        LifecycleEvent::ParseCompleted {
            result: Ok(info), ..
        } => {
            partial.websocket = info.method == "GET" && info.path == "/v1/responses";
        }
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

fn finalize(engine: &impl ReservationSettlement, partial: Partial, reason: &TerminationReason) {
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
    // HTTP keeps its existing full-refund error policy. WebSocket turns settle
    // measured work even when their transport or response ended unsuccessfully.
    if !partial.usage_seen || (!partial.websocket && !matches!(reason, TerminationReason::Success))
    {
        let refunded = engine.refund_by_id(&reservation_id);
        let outcome = match (refunded, matches!(reason, TerminationReason::Success)) {
            (false, _) => "already_released",
            (true, true) => "refunded_no_usage",
            (true, false) => "refunded_non_success",
        };
        metrics::counter!(
            "cc_lb_limit_reconcile_subscriber_rows_total",
            "outcome" => outcome,
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
mod tests {
    use super::*;
    use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
    use cc_lb_lifecycle::{LimitDecisionKind, UsageSource};

    fn build_engine() -> Arc<LimitEngine> {
        LimitEngine::new(
            Arc::new(KeyConcurrencyManager::new()),
            Arc::new(cc_lb_clock::SystemClock),
        )
    }

    fn eid(s: &str) -> EventId {
        s.to_owned()
    }

    fn accounting_fixture() -> (
        Arc<LimitEngine>,
        cc_lb_control::api_keys::principal_view::PrincipalView,
        cc_lb_storage_api::StoredApiKeyRecord,
        Arc<KeyConcurrencyManager>,
    ) {
        use cc_lb_storage_api::principal::{Limit, LimitKind};

        let concurrency = Arc::new(KeyConcurrencyManager::new());
        let engine = LimitEngine::new(
            concurrency.clone(),
            Arc::new(cc_lb_clock::TestClock::new_at_secs(1_800_000_000)),
        );
        let limits = vec![
            Limit {
                kind: LimitKind::TotalTokens,
                window_secs: 60,
                cap_micros: 1_000,
            },
            Limit {
                kind: LimitKind::CostUsd,
                window_secs: 60,
                cap_micros: 1_000,
            },
            Limit {
                kind: LimitKind::Concurrent,
                window_secs: 0,
                cap_micros: 1,
            },
        ];
        let view = cc_lb_control::api_keys::principal_view::PrincipalView::for_tests(
            "ws-accounting-principal",
            true,
            vec!["gpt-5-codex".to_owned()],
            limits,
            HashMap::new(),
        );
        let record = cc_lb_storage_api::StoredApiKeyRecord {
            key_hash_b64: "ws-accounting-key".to_owned(),
            ..Default::default()
        };
        (engine, view, record, concurrency)
    }

    fn reserved_partial(
        engine: &Arc<LimitEngine>,
        view: &cc_lb_control::api_keys::principal_view::PrincipalView,
        record: &cc_lb_storage_api::StoredApiKeyRecord,
        method: &str,
        usage: Option<UsageSnapshot>,
    ) -> Partial {
        let Ok(reservation) = engine.reserve(
            view,
            record,
            "ws-accounting-principal",
            "gpt-5-codex",
            100,
            0,
            Some(200),
        ) else {
            panic!("accounting fixture must admit its initial reservation");
        };
        let mut partial = Partial {
            reservation_id: Some(reservation.id().to_owned()),
            cost_micros: usage.as_ref().map(|_| 25),
            ..Default::default()
        };
        reservation.forget();
        merge(
            &mut partial,
            LifecycleEvent::ParseCompleted {
                event_id: eid("ws-accounting"),
                result: Ok(cc_lb_lifecycle::ParseInfo {
                    path: "/v1/responses".to_owned(),
                    method: method.to_owned(),
                    model: Some("gpt-5-codex".to_owned()),
                    stream: true,
                    ..Default::default()
                }),
            },
        );
        if let Some(usage) = usage {
            merge(
                &mut partial,
                LifecycleEvent::UsageObserved {
                    event_id: eid("ws-accounting"),
                    usage,
                    source: UsageSource::NonStreamBody,
                },
            );
        }
        partial
    }

    fn remaining_tokens(
        engine: &LimitEngine,
        record: &cc_lb_storage_api::StoredApiKeyRecord,
    ) -> String {
        let headers = engine.headers_for(&record.key_hash_b64, "ws-accounting-principal");
        let Some((_, remaining)) = headers
            .into_iter()
            .find(|(name, _)| name == "anthropic-ratelimit-tokens-remaining")
        else {
            panic!("configured token limit must produce a remaining header");
        };
        remaining
    }

    #[test]
    fn websocket_interrupted_turn_reconciles_observed_usage_and_cost_once() {
        use cc_lb_control::api_keys::limit_engine::RejectReason;

        for reason in [
            TerminationReason::Dropped,
            TerminationReason::ErrorCode("client_closed_request".to_owned()),
            TerminationReason::ErrorCode("upstream_stream_error".to_owned()),
        ] {
            let (engine, view, record, concurrency) = accounting_fixture();
            let partial = reserved_partial(
                &engine,
                &view,
                &record,
                "GET",
                Some(UsageSnapshot {
                    input_tokens: 7,
                    output_tokens: 11,
                    ..Default::default()
                }),
            );
            let Some(reservation_id) = partial.reservation_id.clone() else {
                panic!("the turn must retain its reservation identity");
            };
            assert_eq!(remaining_tokens(&engine, &record), "900");
            assert_eq!(concurrency.current(&record.key_hash_b64), 1);

            finalize(engine.as_ref(), partial, &reason);

            assert_eq!(remaining_tokens(&engine, &record), "982");
            assert_eq!(concurrency.current(&record.key_hash_b64), 0);
            assert!(!engine.reconcile_by_id(&reservation_id, 7, 11, 25));
            assert!(!engine.refund_by_id(&reservation_id));
            assert_eq!(remaining_tokens(&engine, &record), "982");
            assert!(matches!(
                engine.reserve(
                    &view,
                    &record,
                    "ws-accounting-principal",
                    "gpt-5-codex",
                    0,
                    0,
                    Some(976),
                ),
                Err(RejectReason::CostRateLimit)
            ));
        }
    }

    #[test]
    fn websocket_interrupted_turn_without_usage_refunds_and_releases_once() {
        let (engine, view, record, concurrency) = accounting_fixture();
        let partial = reserved_partial(&engine, &view, &record, "GET", None);
        let Some(reservation_id) = partial.reservation_id.clone() else {
            panic!("the turn must retain its reservation identity");
        };
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);

        finalize(engine.as_ref(), partial, &TerminationReason::Dropped);

        assert_eq!(remaining_tokens(&engine, &record), "1000");
        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
        assert!(!engine.refund_by_id(&reservation_id));
        assert!(!engine.reconcile_by_id(&reservation_id, 0, 0, 0));
        assert_eq!(remaining_tokens(&engine, &record), "1000");
    }

    #[test]
    fn http_responses_error_keeps_existing_full_refund_policy() {
        let (engine, view, record, concurrency) = accounting_fixture();
        let partial = reserved_partial(
            &engine,
            &view,
            &record,
            "POST",
            Some(UsageSnapshot {
                input_tokens: 7,
                output_tokens: 11,
                ..Default::default()
            }),
        );

        finalize(
            engine.as_ref(),
            partial,
            &TerminationReason::ErrorCode("upstream_stream_error".to_owned()),
        );

        assert_eq!(remaining_tokens(&engine, &record), "1000");
        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
    }

    #[test]
    fn websocket_failed_usage_waits_for_background_price_before_settlement() {
        let (engine, view, record, concurrency) = accounting_fixture();
        let mut partial = reserved_partial(
            &engine,
            &view,
            &record,
            "GET",
            Some(UsageSnapshot {
                input_tokens: 7,
                output_tokens: 11,
                ..Default::default()
            }),
        );
        partial.cost_micros = None;
        let event_id = eid("ws-delayed-price");
        let mut partials = HashMap::from([(event_id.clone(), partial)]);
        handle_event(
            engine.as_ref(),
            &mut partials,
            16,
            LifecycleEvent::RequestTerminated {
                event_id: event_id.clone(),
                reason: TerminationReason::ErrorCode("upstream_stream_error".to_owned()),
                client_status: 502,
                duration_ms: 20,
                first_body_chunk_ms: None,
                first_content_delta_ms: None,
                dns_ms: None,
                connect_ms: None,
                connection_reused: None,
                internal_errors: Vec::new(),
                proxy_setup_ms: None,
                setup_timings: Default::default(),
                io_timings: Default::default(),
                upstream_body_ms: None,
                request_body_read_ms: None,
                request_body_bytes: None,
                finalize_ms: None,
                event_kind: None,
            },
        );
        assert_eq!(concurrency.current(&record.key_hash_b64), 1);
        assert_eq!(remaining_tokens(&engine, &record), "900");
        assert!(partials.contains_key(&event_id));

        handle_event(
            engine.as_ref(),
            &mut partials,
            16,
            LifecycleEvent::Priced {
                event_id: event_id.clone(),
                cost: cc_lb_request_log::CostBreakdown {
                    total_micros: Some(25),
                    ..Default::default()
                },
            },
        );
        assert!(!partials.contains_key(&event_id));
        assert_eq!(concurrency.current(&record.key_hash_b64), 0);
        assert_eq!(remaining_tokens(&engine, &record), "982");
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
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            event_kind: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn non_success_termination_with_released_reservation_is_noop() {
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
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            event_kind: None,
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
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            event_kind: None,
        })
        .await
        .unwrap();
        drop(tx);
        handle.shutdown().await;
    }

    struct CountingSettlement<'a> {
        engine: &'a LimitEngine,
        refunds: std::cell::Cell<usize>,
        reconciles: std::cell::Cell<usize>,
    }

    impl ReservationSettlement for CountingSettlement<'_> {
        fn refund_by_id(&self, id: &str) -> bool {
            self.refunds.set(self.refunds.get() + 1);
            self.engine.refund_by_id(id)
        }

        fn reconcile_by_id(&self, id: &str, input: u64, output: u64, cost: i64) -> bool {
            self.reconciles.set(self.reconciles.get() + 1);
            self.engine.reconcile_by_id(id, input, output, cost)
        }
    }

    fn terminal_event(event_id: &str, reason: TerminationReason) -> LifecycleEvent {
        let client_status = if matches!(reason, TerminationReason::Success) {
            200
        } else {
            502
        };
        LifecycleEvent::RequestTerminated {
            event_id: event_id.to_owned(),
            reason,
            client_status,
            duration_ms: 20,
            first_body_chunk_ms: None,
            first_content_delta_ms: None,
            dns_ms: None,
            connect_ms: None,
            connection_reused: None,
            internal_errors: Vec::new(),
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            io_timings: Default::default(),
            upstream_body_ms: None,
            request_body_read_ms: None,
            request_body_bytes: None,
            finalize_ms: None,
            event_kind: None,
        }
    }

    #[test]
    fn real_settlement_invokes_exactly_one_refund_or_reconcile_per_terminal_turn() {
        for (method, measured, reason) in [
            ("GET", true, TerminationReason::Dropped),
            (
                "GET",
                true,
                TerminationReason::ErrorCode("upstream_stream_error".to_owned()),
            ),
            ("GET", true, TerminationReason::Success),
            ("GET", false, TerminationReason::Dropped),
            ("GET", false, TerminationReason::Success),
            (
                "POST",
                true,
                TerminationReason::ErrorCode("upstream_stream_error".to_owned()),
            ),
        ] {
            let (engine, view, record, concurrency) = accounting_fixture();
            let usage = measured.then_some(UsageSnapshot {
                input_tokens: 7,
                output_tokens: 11,
                ..Default::default()
            });
            let partial = reserved_partial(&engine, &view, &record, method, usage);
            let reservation_id = partial.reservation_id.clone().unwrap();
            let observer = CountingSettlement {
                engine: engine.as_ref(),
                refunds: std::cell::Cell::new(0),
                reconciles: std::cell::Cell::new(0),
            };
            let event_id = "ws-exactly-once";
            let mut partials = HashMap::from([(event_id.to_owned(), partial)]);
            let terminal = terminal_event(event_id, reason.clone());
            handle_event(&observer, &mut partials, 16, terminal.clone());
            let reconciled =
                measured && (method == "GET" || matches!(reason, TerminationReason::Success));
            assert_eq!(observer.reconciles.get(), usize::from(reconciled));
            assert_eq!(observer.refunds.get(), usize::from(!reconciled));
            assert_eq!(concurrency.current(&record.key_hash_b64), 0);
            assert_eq!(
                remaining_tokens(&engine, &record),
                if reconciled { "982" } else { "1000" }
            );
            assert!(!partials.contains_key(event_id));

            handle_event(&observer, &mut partials, 16, terminal);
            assert_eq!(
                observer.reconciles.get(),
                usize::from(reconciled),
                "terminal replay cannot reconcile twice"
            );
            assert_eq!(
                observer.refunds.get(),
                usize::from(!reconciled),
                "terminal replay cannot refund twice"
            );
            assert!(!engine.refund_by_id(&reservation_id));
            assert!(!engine.reconcile_by_id(&reservation_id, 7, 11, 25));
            assert_eq!(
                remaining_tokens(&engine, &record),
                if reconciled { "982" } else { "1000" }
            );
            assert_eq!(concurrency.current(&record.key_hash_b64), 0);
            let probe = engine.reserve(
                &view,
                &record,
                "ws-accounting-principal",
                "gpt-5-codex",
                0,
                0,
                Some(1_000),
            );
            if reconciled {
                assert!(
                    matches!(
                        probe,
                        Err(cc_lb_control::api_keys::limit_engine::RejectReason::CostRateLimit)
                    ),
                    "measured cost remains charged after an interrupted WebSocket response"
                );
            } else {
                let probe = probe.expect("a full refund restores the exact cost budget");
                drop(probe);
            }
        }
    }
}
