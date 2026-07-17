use std::collections::HashMap;
use std::sync::Arc;

use cc_lb_contract::{
    CostBreakdown, LifecycleEvent, NoopMetricsHook, RequestEventBus, TerminationReason,
    UsageSnapshot, UsageSource,
};
use cc_lb_engine::api_keys::concurrent_guard::KeyConcurrencyManager;
use cc_lb_engine::api_keys::limit_engine::LimitEngine;
use cc_lb_engine::api_keys::principal_view::PrincipalView;
use cc_lb_engine::{
    InMemoryBus, SystemClock, spawn_lifecycle_limit_reconcile_subscriber,
    spawn_request_event_assembler,
};
use cc_lb_storage_api::types::{KeyStatus, Limit as StoredLimit, LimitKind, StoredApiKeyRecord};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};

use super::support::RenewalFixture;

const PRINCIPAL_ID: &str = "renewal-principal";
const KEY_ID: &str = "renewal-limit-key";
const RESERVATION_AMOUNT: i64 = 100;

#[tokio::test]
async fn renewal_terminal_bus_delivery_does_not_duplicate_direct_sqlite_row() {
    // Given
    let fixture = RenewalFixture::new(0).await;
    let event_id = "renewal:session-1:generation-1".to_owned();
    let direct_row = RequestEvent {
        ts: 1_730_000_000,
        request_id: "renewal-direct-row".to_owned(),
        event_id: Some(event_id.clone()),
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some("session-1:generation-1".to_owned()),
        status: 200,
        duration_ms: 12,
        ..RequestEvent::default()
    };
    let direct_cursor = fixture
        .storage
        .append_request_event(&direct_row)
        .await
        .expect("persist direct renewal row");
    let bus = Arc::new(InMemoryBus::new());
    let assembler_rx = bus.attach_lifecycle_assembler(4);
    let assembler = spawn_request_event_assembler(
        assembler_rx,
        fixture.storage.clone(),
        None,
        Arc::new(NoopMetricsHook),
    );

    // When
    bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: event_id.clone(),
        request_id: "renewal-bus-row".to_owned(),
        ts_ms: 1_730_000_000_000,
        stream: false,
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some("session-1:generation-1".to_owned()),
    });
    bus.publish_lifecycle(LifecycleEvent::RequestTerminated {
        event_id,
        reason: TerminationReason::Success,
        client_status: 200,
        duration_ms: 12,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        upstream_body_ms: None,
    });
    assembler.shutdown().await;

    // Then
    assert_eq!(direct_cursor, 1);
    assert_eq!(fixture.request_event_count().await, 1);
}

#[tokio::test]
async fn duplicate_renewal_terminal_reconciles_the_reservation_once() {
    // Given
    let view = Arc::new(PrincipalView::for_tests(
        PRINCIPAL_ID,
        true,
        vec!["*".to_owned()],
        Vec::new(),
        HashMap::new(),
    ));
    let record = StoredApiKeyRecord {
        key_hash_b64: KEY_ID.to_owned(),
        status: KeyStatus::Active,
        limit_overrides: vec![StoredLimit {
            kind: LimitKind::OutputTokens,
            window_secs: 60,
            cap_micros: RESERVATION_AMOUNT,
        }],
        ..StoredApiKeyRecord::default()
    };
    let engine = LimitEngine::new(
        Arc::new(KeyConcurrencyManager::new()),
        Arc::new(SystemClock),
    );
    let reservation = engine
        .reserve(
            &view,
            &record,
            PRINCIPAL_ID,
            "claude-renewal",
            RESERVATION_AMOUNT,
            0,
            None,
        )
        .expect("reserve renewal output capacity");
    let reservation_id = reservation.id().to_owned();
    let event_id = "renewal:session-2:generation-1".to_owned();
    let bus = Arc::new(InMemoryBus::new());
    let reconcile_rx = bus.attach_lifecycle_limit_reconcile(8);
    let subscriber = spawn_lifecycle_limit_reconcile_subscriber(reconcile_rx, engine.clone());

    // When
    bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: event_id.clone(),
        request_id: "renewal-limit-row".to_owned(),
        ts_ms: 1_730_000_000_000,
        stream: false,
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some("session-2:generation-1".to_owned()),
    });
    bus.publish_lifecycle(LifecycleEvent::LimitDecision {
        event_id: event_id.clone(),
        decision: cc_lb_contract::LimitDecisionKind::Reserved {
            reservation_id: reservation_id.clone(),
            amount: RESERVATION_AMOUNT as u64,
            limit_reserve_ms: None,
        },
    });
    bus.publish_lifecycle(LifecycleEvent::UsageObserved {
        event_id: event_id.clone(),
        usage: UsageSnapshot {
            output_tokens: 40,
            ..UsageSnapshot::default()
        },
        source: UsageSource::NonStreamBody,
    });
    bus.publish_lifecycle(LifecycleEvent::Priced {
        event_id: event_id.clone(),
        cost: CostBreakdown {
            total_micros: Some(0),
            ..CostBreakdown::default()
        },
    });
    let terminal = LifecycleEvent::RequestTerminated {
        event_id,
        reason: TerminationReason::Success,
        client_status: 200,
        duration_ms: 12,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        upstream_body_ms: None,
    };
    bus.publish_lifecycle(terminal.clone());
    bus.publish_lifecycle(terminal);
    subscriber.shutdown().await;

    // Then
    assert!(
        !engine.reconcile_by_id(&reservation_id, 0, 40, 0),
        "the renewal subscriber must consume the reservation id before a duplicate can reconcile"
    );
    let verifier = engine
        .reserve(&view, &record, PRINCIPAL_ID, "claude-renewal", 60, 0, None)
        .expect("one reconciliation leaves exactly 60 output tokens available");
    assert!(
        engine
            .reserve(&view, &record, PRINCIPAL_ID, "claude-renewal", 1, 0, None)
            .is_err(),
        "a duplicate reconciliation must not refund more than the 60 unused tokens"
    );
    drop(verifier);
    drop(reservation);
}
