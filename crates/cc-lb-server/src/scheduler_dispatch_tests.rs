#![allow(non_snake_case)]

use super::*;

#[path = "scheduler_dispatch_tests/support.rs"]
mod support;

use ::http::StatusCode;
use async_trait::async_trait;
use cc_lb_control::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_control::api_keys::key_store::{CreateParams, KeyStore};
use cc_lb_control::api_keys::limit_engine::LimitEngine;
use cc_lb_control::{LifecycleBusReceiver, RequestEventBus};
use cc_lb_engine::attempt_rail::AttemptIntent;
use cc_lb_engine::cache_keepalive::{
    CacheKeepaliveEnqueuer, DispatchOutcome, KeepaliveDispatchContext, KeepaliveDispatcher,
    RenewalFinalization, RenewalUsage, RequestSnapshot,
};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_pricing::{CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion, global_catalog};
use cc_lb_scheduler::jobs::upstream_affinity_purge::UpstreamAffinityPurgeJob;
use cc_lb_scheduler::retry::JobOutcome;
use cc_lb_scheduler::worker::CronJob;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    CacheKeepaliveEnqueueState, CacheKeepaliveSessionStatus, CacheKeepaliveSessionStore,
    CacheKeepaliveTerminalReason, CacheTtl, Limit, LimitKind, PrincipalKindLite,
};
use cc_lb_testkit::{InMemoryStorage, fixed_clock};
use serde_json::Value;
use std::sync::Mutex;
use std::time::Duration;

use crate::cache_keepalive_enqueuer::CacheKeepaliveTaskPusher;

use self::support::{FailingPusher, Fixture, empty_dynamic_view};

struct StaticRenewalDispatcher {
    finalization: Mutex<Option<RenewalFinalization>>,
}

#[async_trait]
impl KeepaliveDispatcher for StaticRenewalDispatcher {
    async fn dispatch(
        &self,
        _snapshot: &RequestSnapshot,
        _context: KeepaliveDispatchContext,
    ) -> DispatchOutcome {
        DispatchOutcome::CacheHit {
            cache_anchor_age: Duration::ZERO,
            finalization: self
                .finalization
                .lock()
                .expect("finalization lock")
                .take()
                .expect("one renewal finalization"),
        }
    }
}

#[tokio::test]
async fn pool_quota_snapshot_job_records_current_windows() {
    let clock = fixed_clock(1_700_000_000);
    let storage = InMemoryStorage::with_clock(clock.clone());
    let dynamic_view = empty_dynamic_view();

    let outcome = crate::scheduler_dispatch::cron::handle_pool_quota_snapshot(
        &storage,
        dynamic_view.as_ref(),
        &*clock,
        cc_lb_scheduler::jobs::pool_quota_snapshot::PoolQuotaSnapshotCronJob::new(1_700_000_000),
    )
    .await
    .expect("pool quota snapshot job succeeds");

    assert_eq!(outcome, JobOutcome::Done);
    let records = cc_lb_storage_api::PoolQuotaHistoryStore::list_pool_quota_snapshots_in_range(
        &storage,
        cc_lb_admin::subscription_quotas::POOLED_HISTORY_WINDOWS,
        1_700_000_000,
        1_700_000_000,
    )
    .await
    .expect("pool quota snapshots load");
    assert_eq!(records.len(), 3);
    assert_eq!(
        records
            .iter()
            .map(|record| record.window)
            .collect::<Vec<_>>(),
        cc_lb_admin::subscription_quotas::POOLED_HISTORY_WINDOWS,
    );
    for record in records {
        assert_eq!(record.snapshot_at_unix_secs, 1_700_000_000);
        assert_eq!(record.computed_at_unix_millis, 1_700_000_000_000);
        assert_eq!(record.utilization, None);
        assert_eq!(record.eligible_upstreams, 0);
        assert_eq!(record.contributing_upstreams, 0);
        assert_eq!(record.stale_upstreams, 0);
        assert_eq!(record.missing_observation_upstreams, 0);
    }
}

#[tokio::test]
async fn t3__default_upstream_affinity_purge_physically_removes_stale_null_expiry_rows() {
    let fixture = Fixture::new_sqlite().await;
    sqlx::query(
        "INSERT INTO upstream_affinity_v1
         (principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
    )
    .bind("principal")
    .bind("anthropic")
    .bind("anthropic_web_search_encrypted_content")
    .bind(vec![7_u8; 32])
    .bind(fixture.upstream_id.to_string())
    .bind(0_i64)
    .execute(fixture.sqlite_pool())
    .await
    .expect("seed stale upstream affinity");

    let outcome = fixture
        .dispatch(Arc::new(FailingPusher))
        .dispatch_singleton(CronJob::UpstreamAffinityPurge(
            UpstreamAffinityPurgeJob::default(),
        ))
        .await
        .expect("dispatch upstream affinity purge");

    assert_eq!(outcome, JobOutcome::Done);
    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM upstream_affinity_v1")
        .fetch_one(fixture.sqlite_pool())
        .await
        .expect("count upstream affinity rows");
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn t3__upstream_affinity_purge_uses_the_captured_configured_retention() {
    let fixture = Fixture::new_sqlite().await;
    let now_unix_secs = 1_700_000_000;
    for (value_sha256, observed_at_unix_secs) in [
        (vec![8_u8; 32], now_unix_secs - 2 * 86_400),
        (vec![9_u8; 32], now_unix_secs - 12 * 3_600),
    ] {
        sqlx::query(
            "INSERT INTO upstream_affinity_v1
             (principal_id, provider, kind, value_sha256, upstream_id, observed_at_unix_secs, expires_at_unix_secs)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL)",
        )
        .bind("principal")
        .bind("anthropic")
        .bind("anthropic_web_search_encrypted_content")
        .bind(value_sha256)
        .bind(fixture.upstream_id.to_string())
        .bind(i64::from(observed_at_unix_secs))
        .execute(fixture.sqlite_pool())
        .await
        .expect("seed upstream affinity");
    }
    let mut config = Config::default();
    config.upstream_affinity.ttl_days = 1;

    let outcome = fixture
        .dispatch_with_config(Arc::new(FailingPusher), config)
        .dispatch_singleton(CronJob::UpstreamAffinityPurge(
            UpstreamAffinityPurgeJob::default(),
        ))
        .await
        .expect("dispatch upstream affinity purge");

    assert_eq!(outcome, JobOutcome::Done);
    let remaining: Vec<Vec<u8>> =
        sqlx::query_scalar("SELECT value_sha256 FROM upstream_affinity_v1")
            .fetch_all(fixture.sqlite_pool())
            .await
            .expect("read remaining upstream affinity rows");
    assert_eq!(remaining, vec![vec![9_u8; 32]]);
}

#[tokio::test]
async fn t2__renewal_reconciles_real_reservation_once_before_forget() {
    // Given
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(100, 0, 10);
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(key_id.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        fixed_clock(1_700_000_000),
    );
    let dispatch = fixture.dispatch_with_limit_engine(Arc::clone(&pusher), limit_engine.clone());

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job.clone())
        .await
        .expect("dispatch first keepalive delivery");
    let redelivery = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch duplicate keepalive delivery");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    assert!(matches!(redelivery, JobOutcome::Noop));
    assert_eq!(request_event_count(&fixture).await, 1);
    let key_store = KeyStore::new(fixture.managed_store.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load key")
        .expect("key exists");
    let view = fixture.dynamic_view().load();
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            0,
            3_900,
            None,
        )
        .expect("one reconcile leaves 3900 input tokens available");
    assert!(
        limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                0,
                1,
                None,
            )
            .is_err(),
        "a duplicate reconcile or full RAII refund would leave more than 3900 tokens"
    );
    drop(verifier);
}

#[tokio::test]
async fn t2__renewal_reconcile_false_errors_before_forget_and_lifecycle_publish() {
    // Given
    let fixture = Fixture::new().await;
    let bus = fixture.event_bus();
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = bus.subscribe_lifecycle() else {
        panic!("expected in-memory lifecycle receiver");
    };
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let key_store = KeyStore::new(fixture.managed_store.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load key")
        .expect("key exists");
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        fixed_clock(1_700_000_000),
    );
    let view = fixture.dynamic_view().load();
    let reservation = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            0,
            100,
            None,
        )
        .expect("create reservation to invalidate before finalization");
    let reservation_id = reservation.id().to_owned();
    assert!(limit_engine.refund_by_id(&reservation_id));
    let accounting_guard = AttemptIntent::from_reservation(reservation)
        .into_reserved()
        .into_response_accounting_guard();
    let keepalive_dispatcher = Arc::new(StaticRenewalDispatcher {
        finalization: Mutex::new(Some(RenewalFinalization {
            usage: RenewalUsage {
                input_tokens: 100,
                ..RenewalUsage::default()
            },
            status: 200,
            duration: Duration::from_millis(7),
            accounting_guard,
        })),
    });
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue keepalive with invalidated reservation finalization");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch_with_limit_engine_and_keepalive_dispatcher(
        pusher,
        limit_engine.clone(),
        keepalive_dispatcher,
    );

    // When
    let error = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect_err("false reconcile must abort finalization");

    // Then
    assert!(
        error
            .to_string()
            .contains("cache keepalive renewal reconcile failed"),
        "unexpected error: {error}"
    );
    assert_eq!(request_event_count(&fixture).await, 1);
    assert!(
        lifecycle_rx.try_recv().is_err(),
        "false reconcile must not publish synthetic success lifecycle events"
    );
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            0,
            4_000,
            None,
        )
        .expect("invalidated reservation remains refunded after guard drop");
    assert!(
        limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                0,
                1,
                None,
            )
            .is_err(),
        "extra capacity would indicate an unintended second refund"
    );
    drop(verifier);
}

#[tokio::test]
async fn t2__observe_only_renewal_writes_priced_durable_row_and_projection_without_limit_decision()
{
    // Given
    install_test_pricing();
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(10, 40, 10);
    let bus = fixture.event_bus();
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = bus.subscribe_lifecycle() else {
        panic!("expected in-memory lifecycle receiver");
    };
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue observe-only keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch observe-only keepalive");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    assert_eq!(request_event_count(&fixture).await, 1);
    let events = fixture
        .storage
        .query_request_events(0, u64::MAX, 10)
        .await
        .expect("query renewal events");
    assert_eq!(events[0].key_id, None);
    assert_eq!(events[0].cost_usd_micros, Some(140));
    let projections = fixture
        .storage
        .list_cache_keepalive_turns("principal", "session-hash")
        .await
        .expect("load observe-only turn projection");
    assert_eq!(projections.len(), 1);
    assert_eq!(projections[0].accounting_key_id, None);
    assert_eq!(projections[0].cost_micros, 140);
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load rescheduled observe-only session")
        .expect("observe-only session exists");
    assert_eq!(record.accounting_key_id, None);
    let mut saw_limit_decision = false;
    while let Ok(event) = lifecycle_rx.try_recv() {
        saw_limit_decision |= matches!(event, LifecycleEvent::LimitDecision { .. });
    }
    assert!(!saw_limit_decision);
}

#[tokio::test]
async fn t2__repeated_null_key_renewals_remain_observe_only() {
    // Given
    install_test_pricing();
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(10, 40, 10);
    let bus = fixture.event_bus();
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = bus.subscribe_lifecycle() else {
        panic!("expected in-memory lifecycle receiver");
    };
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue initial observe-only keepalive");
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    // When
    let first_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(1).await)
        .await
        .expect("dispatch first observe-only renewal");
    let second_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(2).await)
        .await
        .expect("dispatch repeated observe-only renewal");

    // Then
    assert!(matches!(first_outcome, JobOutcome::Done));
    assert!(matches!(second_outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load twice-rescheduled session")
        .expect("observe-only session exists");
    assert_eq!(record.generation, 3);
    assert_eq!(record.accounting_key_id, None);
    let projections = fixture
        .storage
        .list_cache_keepalive_turns("principal", "session-hash")
        .await
        .expect("load observe-only renewal projections")
        .into_iter()
        .map(|turn| (turn.accounting_key_id, turn.cost_micros))
        .collect::<Vec<_>>();
    assert_eq!(projections, vec![(None, 140), (None, 140)]);
    let events = fixture
        .storage
        .query_request_events(0, u64::MAX, 10)
        .await
        .expect("query repeated observe-only renewal events");
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| event.key_id.is_none()));
    assert!(
        events
            .iter()
            .all(|event| event.cost_usd_micros == Some(140))
    );
    let mut saw_limit_decision = false;
    while let Ok(event) = lifecycle_rx.try_recv() {
        saw_limit_decision |= matches!(event, LifecycleEvent::LimitDecision { .. });
    }
    assert!(!saw_limit_decision);
}

#[tokio::test]
async fn t2__real_request_reseeds_observe_only_session_without_double_reserve() {
    // Given
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(100, 0, 10);
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue initial observe-only keepalive");
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        fixed_clock(1_700_000_000),
    );
    let dispatch = fixture.dispatch_with_limit_engine(Arc::clone(&pusher), limit_engine.clone());
    dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(1).await)
        .await
        .expect("dispatch initial observe-only renewal");
    let stale_observe_only_job = fixture.pending_keepalive_job(2).await;
    let mut real_request = fixture.enqueue_request();
    real_request.accounting_key_id = Some(key_id.clone());

    // When
    enqueuer
        .enqueue_cache_keepalive(real_request)
        .await
        .expect("reseed keepalive from real request");
    let stale_outcome = dispatch
        .dispatch_cache_keepalive(stale_observe_only_job)
        .await
        .expect("dispatch stale observe-only renewal");
    let reseeded_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(3).await)
        .await
        .expect("dispatch reseeded renewal");

    // Then
    assert!(matches!(stale_outcome, JobOutcome::Noop));
    assert!(matches!(reseeded_outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load reseeded session")
        .expect("reseeded session exists");
    assert_eq!(record.accounting_key_id.as_deref(), Some(key_id.as_str()));
    assert_eq!(request_event_count(&fixture).await, 2);
    let key_store = KeyStore::new(fixture.managed_store.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load reseeded accounting key")
        .expect("reseeded accounting key exists");
    let view = fixture.dynamic_view().load();
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            0,
            3_900,
            None,
        )
        .expect("one real renewal reconcile leaves 3900 input tokens available");
    assert!(
        limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                0,
                1,
                None,
            )
            .is_err(),
        "a stale observe-only turn must not reserve and a reseeded turn must reconcile exactly once"
    );
    drop(verifier);
}

#[tokio::test]
async fn t2__renewal_inline_price_matches_pricing_subscriber_priced_event() {
    // Given
    install_test_pricing();
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(10, 40, 0);
    let bus = fixture.event_bus();
    let pricing_rx = bus.attach_lifecycle_pricing(32);
    let LifecycleBusReceiver::InMemory(mut lifecycle_rx) = bus.subscribe_lifecycle() else {
        panic!("expected in-memory lifecycle receiver");
    };
    let pricing_handle = cc_lb_pricing::spawn_lifecycle_pricing_subscriber(
        pricing_rx,
        bus.clone() as Arc<dyn RequestEventBus>,
    );
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue priced keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch priced keepalive");
    pricing_handle.shutdown().await;

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    let events = fixture
        .storage
        .query_request_events(0, u64::MAX, 10)
        .await
        .expect("query renewal events");
    let inline_cost = events[0].cost_usd_micros;
    let mut subscriber_cost = None;
    while let Ok(event) = lifecycle_rx.try_recv() {
        if let LifecycleEvent::Priced { cost, .. } = event {
            subscriber_cost = cost.total_micros;
        }
    }
    assert_eq!(inline_cost, subscriber_cost);
    assert_eq!(inline_cost, Some(140));
}

#[tokio::test]
async fn t2__saturated_lifecycle_bus_still_persists_and_reconciles_once() {
    // Given
    let fixture = Fixture::new().await;
    fixture.http.return_cache_hit_usage(100, 0, 10);
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let _full_receivers = saturate_lifecycle_bus(&fixture);
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(key_id.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue saturated keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        fixed_clock(1_700_000_000),
    );
    let dispatch = fixture.dispatch_with_limit_engine(pusher, limit_engine.clone());

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch saturated keepalive");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    assert_eq!(request_event_count(&fixture).await, 1);
    let turn_count = fixture
        .storage
        .list_cache_keepalive_turns("principal", "session-hash")
        .await
        .expect("count turn projections")
        .len();
    assert_eq!(turn_count, 1);
    let key_store = KeyStore::new(fixture.managed_store.clone());
    let key = key_store
        .get("principal", &key_id)
        .await
        .expect("load key")
        .expect("key exists");
    let view = fixture.dynamic_view().load();
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            0,
            3_900,
            None,
        )
        .expect("direct reconcile must happen despite full bus channels");
    drop(verifier);
}

#[tokio::test]
async fn t2__renewal_response_persists_one_attributed_event_and_projection_set() {
    // Given
    let fixture = Fixture::new().await;
    let key_store = KeyStore::new(fixture.managed_store.clone());
    let (_key, secret) = key_store
        .create(
            "principal",
            CreateParams {
                upstream_kind: cc_lb_storage_api::types::UpstreamKind::AnthropicKey,
                label: "renewal accounting".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 10,
                }],
                principal_kind: PrincipalKindLite::Machine,
            },
        )
        .await
        .expect("create renewal accounting key");
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    let key_id = api_key_id(secret.expose());
    request.accounting_key_id = Some(key_id.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    let row_count = fixture
        .storage
        .query_request_events(0, u64::MAX, 10)
        .await
        .expect("count renewal event rows")
        .len();
    assert_eq!(row_count, 1);
    let events = fixture
        .storage
        .query_request_events(0, u64::MAX, 10)
        .await
        .expect("query renewal events");
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.event_id.as_deref(), Some("renewal:session-hash:1"));
    assert_eq!(event.source_kind.as_deref(), Some("renewal"));
    assert_eq!(event.source_ref_id.as_deref(), Some("session-hash:1"));
    assert_eq!(event.principal_id.as_deref(), Some("principal"));
    assert_eq!(event.key_id.as_deref(), Some(key_id.as_str()));
    assert_eq!(event.upstream_id, Some(fixture.upstream_id));
    assert_eq!(event.model.as_deref(), Some("claude-test"));
    assert!(event.cost_usd_micros.is_some());
    let turn_count = fixture
        .storage
        .list_cache_keepalive_turns("principal", "session-hash")
        .await
        .expect("count renewal turn projections")
        .len();
    let decision_count = usize::from(
        fixture
            .storage
            .get_cache_keepalive_decision_for_principal("principal", "session-hash:1")
            .await
            .expect("count renewal decision projections")
            .is_some(),
    );
    assert_eq!(turn_count, 1);
    assert_eq!(decision_count, 1);
}

#[tokio::test]
async fn t2__cache_keepalive_disable_mid_cycle_drops_open_reservation_without_finalizing() {
    // Given
    let fixture = Fixture::new().await;
    let key_id = create_accounting_key(&fixture, LimitKind::InputTokens, 4_000).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(key_id.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue keepalive with an accounting key");
    let job = fixture.pending_keepalive_job(1).await;
    let limit_engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        fixed_clock(1_700_000_000),
    );
    fixture.disable_cache_keepalive_on_next_http_dispatch();
    let dispatch = fixture.dispatch_with_limit_engine(pusher, limit_engine.clone());

    // When
    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("disable after dispatch starts stops the renewal cleanly");

    // Then
    assert!(matches!(outcome, JobOutcome::Done));
    assert_eq!(
        fixture.http.requests.lock().expect("requests lock").len(),
        1,
        "the disable hook must run after the reservation-backed upstream dispatch"
    );
    assert_eq!(request_event_count(&fixture).await, 0);
    let session = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load disabled keepalive session")
        .expect("keepalive session exists");
    assert_eq!(session.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        session.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    let key = KeyStore::new(fixture.managed_store.clone())
        .get("principal", &key_id)
        .await
        .expect("load accounting key")
        .expect("accounting key exists");
    let view = fixture.dynamic_view().load();
    let verifier = limit_engine
        .reserve(
            view.principal_view.as_ref(),
            &key,
            "principal",
            "claude-test",
            0,
            4_000,
            None,
        )
        .expect("dropped reservation refunds the full renewal amount before the TTL sweep");
    assert!(
        limit_engine
            .reserve(
                view.principal_view.as_ref(),
                &key,
                "principal",
                "claude-test",
                0,
                1,
                None,
            )
            .is_err(),
        "the verifier proves the original reservation refunded exactly once"
    );
    drop(verifier);
}

fn api_key_id(secret: &str) -> String {
    secret
        .strip_prefix("sk-cclb-")
        .and_then(|value| value.split_once('_'))
        .map(|(key_id, _)| key_id.to_owned())
        .expect("test key includes key id")
}

async fn create_accounting_key(fixture: &Fixture, kind: LimitKind, cap_micros: i64) -> String {
    let key_store = KeyStore::new(fixture.managed_store.clone());
    let (_, secret) = key_store
        .create(
            "principal",
            CreateParams {
                upstream_kind: cc_lb_storage_api::types::UpstreamKind::AnthropicKey,
                label: "renewal accounting".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![Limit {
                    kind,
                    window_secs: 60,
                    cap_micros,
                }],
                principal_kind: PrincipalKindLite::Machine,
            },
        )
        .await
        .expect("create renewal accounting key");
    api_key_id(secret.expose())
}

async fn request_event_count(fixture: &Fixture) -> i64 {
    i64::try_from(
        fixture
            .storage
            .query_request_events(0, u64::MAX, usize::MAX)
            .await
            .expect("count request events")
            .len(),
    )
    .expect("request event count fits i64")
}

fn install_test_pricing() {
    let mut models = std::collections::HashMap::new();
    models.insert(
        "claude-test".to_owned(),
        Pricing {
            model: "claude-test".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: UsdPerMillion::from_whole_usd(3),
            by_tier: std::collections::BTreeMap::new(),
        },
    );
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: 0,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: std::collections::HashMap::new(),
        cache_read_per_million_usd: std::collections::HashMap::new(),
        cache_creation_per_million_usd_by_tier: std::collections::HashMap::new(),
        cache_read_per_million_usd_by_tier: std::collections::HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

fn saturate_lifecycle_bus(fixture: &Fixture) -> Vec<tokio::sync::mpsc::Receiver<LifecycleEvent>> {
    let bus = fixture.event_bus();
    let receivers = vec![
        bus.attach_lifecycle_writer(1),
        bus.attach_lifecycle_assembler(1),
        bus.attach_lifecycle_pricing(1),
        bus.attach_lifecycle_limit_reconcile(1),
    ];
    bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: "saturate".to_owned(),
        request_id: "saturate".to_owned(),
        ts_ms: 0,
        stream: false,
        source_kind: None,
        source_ref_id: None,
    });
    receivers
}

#[tokio::test]
async fn t2__durable_cache_keepalive_job_reschedules_without_rewriting_payload() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let initial_payload = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load initial session")
        .expect("initial session exists")
        .encrypted_payload;
    let first_job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    let first_outcome = dispatch
        .dispatch_cache_keepalive(first_job)
        .await
        .expect("dispatch first cache keepalive job");

    assert!(matches!(first_outcome, JobOutcome::Done));
    let first_record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load first rescheduled session")
        .expect("first rescheduled session exists");
    assert_eq!(first_record.generation, 2);
    assert_eq!(first_record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(first_record.encrypted_payload, initial_payload);

    let second_job = fixture.pending_keepalive_job(2).await;
    let second_outcome = dispatch
        .dispatch_cache_keepalive(second_job)
        .await
        .expect("dispatch second cache keepalive job");

    assert!(matches!(second_outcome, JobOutcome::Done));
    let second_record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load second rescheduled session")
        .expect("second rescheduled session exists");
    assert_eq!(second_record.generation, 3);
    assert_eq!(second_record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(second_record.encrypted_payload, initial_payload);
    fixture.pending_keepalive_job(3).await;

    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].url, "http://fake-upstream.local/v1/messages");
    assert_eq!(requests[0].headers["x-api-key"], "sk-ant-rotated");
    assert_eq!(requests[0].headers["anthropic-version"], "2023-06-01");
    let body: Value = serde_json::from_slice(&requests[0].body).expect("body json");
    assert_eq!(body["max_tokens"], 0);
    assert!(body.get("stream").is_none());
    let signer_calls = fixture.signer_calls.lock().expect("signer calls lock");
    assert_eq!(
        signer_calls.as_slice(),
        &["fake-upstream:", "fake-upstream:"]
    );
}

#[tokio::test]
async fn t2__legacy_cache_keepalive_payload_migrates_on_first_cache_hit() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let request = fixture.enqueue_request();
    let legacy_plaintext =
        serde_json::to_vec(&request.snapshot.to_persisted()).expect("serialize legacy payload");
    let legacy_payload = fixture.encrypt_generation_payload(1, &legacy_plaintext);
    fixture.replace_payload_during_next_enqueue(1, legacy_payload.clone());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let initial = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load legacy session")
        .expect("legacy session exists");
    assert_eq!(initial.generation, 1);
    assert_eq!(initial.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(initial.enqueue_state, CacheKeepaliveEnqueueState::Enqueued);
    assert_eq!(initial.encrypted_payload, legacy_payload);
    let first_job = fixture.pending_keepalive_job(1).await;
    assert_eq!(first_job.generation, 1);
    let dispatch = fixture.dispatch(Arc::clone(&pusher));

    let first_outcome = dispatch
        .dispatch_cache_keepalive(first_job)
        .await
        .expect("dispatch legacy cache keepalive job");

    assert!(matches!(first_outcome, JobOutcome::Done));
    let migrated = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load migrated session")
        .expect("migrated session exists");
    assert_eq!(migrated.generation, 2);
    assert_ne!(migrated.encrypted_payload, legacy_payload);

    let second_outcome = dispatch
        .dispatch_cache_keepalive(fixture.pending_keepalive_job(2).await)
        .await
        .expect("dispatch migrated cache keepalive job");

    assert!(matches!(second_outcome, JobOutcome::Done));
    let rescheduled = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load rescheduled migrated session")
        .expect("rescheduled migrated session exists");
    assert_eq!(rescheduled.generation, 3);
    assert_eq!(rescheduled.encrypted_payload, migrated.encrypted_payload);
}

#[tokio::test]
async fn t2__cache_keepalive_missing_accounting_key_terminalizes_without_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some("missing-accounting-key".to_owned());
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn t2__cache_keepalive_accounting_key_uses_real_reserve_before_dispatch() {
    let fixture = Fixture::new().await;
    let key_store = KeyStore::new(fixture.managed_store.clone());
    let (record, _) = key_store
        .create(
            "principal",
            CreateParams {
                upstream_kind: cc_lb_storage_api::types::UpstreamKind::AnthropicKey,
                label: "renewal accounting".to_owned(),
                description: None,
                expires_at_unix_secs: None,
                limit_overrides: vec![Limit {
                    kind: LimitKind::Requests,
                    window_secs: 60,
                    cap_micros: 0,
                }],
                principal_kind: PrincipalKindLite::Machine,
            },
        )
        .await
        .expect("create accounting key");
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.accounting_key_id = Some(record.key_hash_b64);
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn t2__cache_keepalive_redelivered_job_dispatches_once() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let first = dispatch
        .dispatch_cache_keepalive(job.clone())
        .await
        .expect("dispatch first delivery");
    let redelivery = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch redelivery");

    assert!(matches!(first, JobOutcome::Done));
    assert!(matches!(redelivery, JobOutcome::Noop));
    assert_eq!(
        fixture.http.requests.lock().expect("requests lock").len(),
        1
    );
    assert_eq!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .len(),
        1
    );
}

#[tokio::test]
async fn t2__cache_keepalive_lease_mismatch_skips_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    assert!(
        CacheKeepaliveSessionStore::claim_cache_keepalive_turn(
            fixture.storage.as_ref(),
            "session-hash",
            1,
            1_700_000_000,
        )
        .await
        .expect("mark turn running")
    );
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch running turn");

    assert!(matches!(outcome, JobOutcome::Noop));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load leased session")
        .expect("leased session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.enqueue_state, CacheKeepaliveEnqueueState::Running);
    assert_eq!(record.running_since_unix_secs, Some(1_700_000_000));
    assert!(
        fixture
            .http
            .requests
            .lock()
            .expect("requests lock")
            .is_empty()
    );
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn t2__hit_reschedule_enqueue_failure_terminalizes_new_generation() {
    let fixture = Fixture::new().await;
    let enqueuer = fixture.enqueuer(fixture.pusher());
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let first_job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));

    let result = dispatch.dispatch_cache_keepalive(first_job).await;

    assert!(result.is_err());
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
}

#[tokio::test]
async fn t2__cache_keepalive_job_terminalizes_on_decrypt_failure() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let corrupt_payload = vec![0; 32];
    fixture.replace_payload_during_next_enqueue(1, corrupt_payload.clone());
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let corrupt = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load corrupt session")
        .expect("corrupt session exists");
    assert_eq!(corrupt.generation, 1);
    assert_eq!(corrupt.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(corrupt.enqueue_state, CacheKeepaliveEnqueueState::Enqueued);
    assert_eq!(corrupt.encrypted_payload, corrupt_payload);
    assert_eq!(job.generation, 1);
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DecryptFailed)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn t2__cache_keepalive_job_terminalizes_on_deserialization_failure() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let encrypted = fixture.encrypt_generation_payload(1, br#"{}"#);
    fixture.replace_payload_during_next_enqueue(1, encrypted.clone());
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let corrupt = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load invalid serialized session")
        .expect("invalid serialized session exists");
    assert_eq!(corrupt.generation, 1);
    assert_eq!(corrupt.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(corrupt.enqueue_state, CacheKeepaliveEnqueueState::Enqueued);
    assert_eq!(corrupt.encrypted_payload, encrypted);
    assert_eq!(job.generation, 1);
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DecryptFailed)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn t2__cache_keepalive_miss_terminalizes_session() {
    let fixture = Fixture::new().await;
    fixture.http.return_cache_miss();
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::CacheMiss)
    );
}

#[tokio::test]
async fn t2__cache_keepalive_unsupported_provider_terminalizes_session() {
    let fixture = Fixture::new_with_upstream_kind(UpstreamKind::AnthropicApiKey).await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::UnsupportedProvider)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn t2__cache_keepalive_dispatch_error_terminalizes_session() {
    let fixture = Fixture::new().await;
    fixture.http.return_status(
        StatusCode::INTERNAL_SERVER_ERROR,
        serde_json::json!({"error":"boom"}),
    );
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::DispatchError)
    );
}

#[tokio::test]
async fn t2__cache_keepalive_hit_terminalizes_when_max_refreshes_reached() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let mut job = fixture.pending_keepalive_job(1).await;
    job.max_refreshes = 1;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::MaxRefreshes)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn t2__cache_keepalive_hit_terminalizes_when_max_duration_reached() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let mut job = fixture.pending_keepalive_job(1).await;
    job.max_total_duration_secs = 0;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::MaxDuration)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn t2__cache_keepalive_durable_cancel_terminalizes_and_skips_queued_job() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let queued_job = fixture.pending_keepalive_job(1).await;
    enqueuer
        .cancel_cache_keepalive(cc_lb_engine::cache_keepalive::CacheKeepaliveCancelRequest {
            session_key_hash: "session-hash".to_owned(),
            reason: cc_lb_engine::cache_keepalive::CancelReason::UserTurnDetected,
        })
        .await
        .expect("cancel durable keepalive");
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));

    let outcome = dispatch
        .dispatch_cache_keepalive(queued_job)
        .await
        .expect("dispatch terminalized keepalive job");

    assert!(matches!(outcome, JobOutcome::Noop));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load cancelled session")
        .expect("cancelled session exists");
    assert_eq!(record.generation, 1);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    assert_eq!(record.running_since_unix_secs, None);
    assert!(record.encrypted_payload.is_empty());
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn t2__cache_keepalive_generation_cas_pure() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue initial durable keepalive");
    let queued_old_job = fixture.pending_keepalive_job(1).await;
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("replace durable keepalive from new real request");
    fixture.pending_keepalive_job(2).await;
    let dispatch = fixture.dispatch(Arc::new(FailingPusher));

    let outcome = dispatch
        .dispatch_cache_keepalive(queued_old_job)
        .await
        .expect("dispatch stale keepalive job");

    assert!(matches!(outcome, JobOutcome::Noop));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.generation, 2);
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Active);
    assert_eq!(record.refresh_count, 0);
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
    assert!(
        fixture
            .signer_calls
            .lock()
            .expect("signer calls lock")
            .is_empty()
    );
}

#[tokio::test]
async fn t2__cache_keepalive_expired_session_terminalizes_before_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    let mut request = fixture.enqueue_request();
    request.cache_anchor_age = Duration::from_secs(CacheTtl::Ttl5m.as_secs() + 1);
    enqueuer
        .enqueue_cache_keepalive(request)
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Expired)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn t2__cache_keepalive_revoked_principal_terminalizes_before_dispatch() {
    let fixture = Fixture::new().await;
    let pusher: Arc<dyn CacheKeepaliveTaskPusher> = fixture.pusher();
    let enqueuer = fixture.enqueuer(Arc::clone(&pusher));
    enqueuer
        .enqueue_cache_keepalive(fixture.enqueue_request())
        .await
        .expect("enqueue durable keepalive");
    let job = fixture.pending_keepalive_job(1).await;
    fixture.revoke_principal();
    let dispatch = fixture.dispatch(pusher);

    let outcome = dispatch
        .dispatch_cache_keepalive(job)
        .await
        .expect("dispatch cache keepalive job");

    assert!(matches!(outcome, JobOutcome::Done));
    let record = fixture
        .storage
        .get_cache_keepalive_session("session-hash")
        .await
        .expect("load session")
        .expect("session exists");
    assert_eq!(record.status, CacheKeepaliveSessionStatus::Terminal);
    assert_eq!(
        record.terminal_reason,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    let requests = fixture.http.requests.lock().expect("requests lock").clone();
    assert!(requests.is_empty());
}
