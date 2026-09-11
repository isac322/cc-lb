#[path = "renewal_accounting_e2e/support.rs"]
mod support;

#[cfg(feature = "postgres")]
#[path = "renewal_accounting_e2e/postgres.rs"]
mod postgres;

use std::sync::Arc;

use cc_lb_control::{InMemoryBus, RequestEventBus};
use cc_lb_lifecycle::LifecycleEvent;
use cc_lb_storage_api::CacheKeepaliveTerminalReason;

use support::RenewalAccountingScenario;

#[tokio::test]
async fn t2__renewal_cycle_bills_once() {
    // Given
    let scenario = RenewalAccountingScenario::new().await;
    let session = scenario.schedule(Some("renewal-key")).await;
    assert!(scenario.claim(&session).await);

    // When
    let completion = scenario.dispatch_and_finalize(&session, true, None).await;

    // Then
    assert!(completion.reservation_id.is_some());
    assert_eq!(scenario.row_counts().await, (1, 1, 1));
    assert_eq!(scenario.http_calls(), 1);
    scenario.assert_full_capacity_available().await;
}

#[tokio::test]
async fn t2__renewal_duplicate_bills_once() {
    // Given
    let scenario = RenewalAccountingScenario::new().await;
    let session = scenario.schedule(Some("renewal-key")).await;
    assert!(scenario.claim(&session).await);
    scenario.dispatch_and_finalize(&session, true, None).await;

    // When
    let duplicate_claimed = scenario.claim(&session).await;

    // Then
    assert!(!duplicate_claimed);
    assert_eq!(scenario.http_calls(), 1);
    assert_eq!(scenario.row_counts().await, (1, 1, 1));
    scenario.assert_full_capacity_available().await;
}

#[tokio::test]
async fn t2__renewal_observeonly_then_reseed() {
    // Given
    let scenario = RenewalAccountingScenario::new().await;
    let observe_only = scenario.schedule(None).await;
    assert!(scenario.claim(&observe_only).await);

    // When
    let observe_only_completion = scenario
        .dispatch_and_finalize(&observe_only, false, None)
        .await;
    let reseeded = scenario.schedule(Some("renewal-key")).await;
    let stale_claimed = scenario.claim(&observe_only).await;
    assert!(scenario.claim(&reseeded).await);
    let reseeded_completion = scenario.dispatch_and_finalize(&reseeded, true, None).await;

    // Then
    assert_eq!(observe_only_completion.reservation_id, None);
    assert_eq!(observe_only_completion.event_key_id, None);
    assert_eq!(observe_only_completion.projection_key_id, None);
    assert!(!stale_claimed);
    assert_eq!(
        reseeded_completion.event_key_id.as_deref(),
        Some("renewal-key")
    );
    assert_eq!(scenario.row_counts().await, (2, 2, 2));
    scenario.assert_full_capacity_available().await;
}

#[tokio::test]
async fn t2__renewal_survives_full_channels() {
    // Given
    let scenario = RenewalAccountingScenario::new().await;
    let bus = Arc::new(InMemoryBus::new());
    let receivers = [
        bus.attach_lifecycle_writer(1),
        bus.attach_lifecycle_assembler(1),
        bus.attach_lifecycle_pricing(1),
        bus.attach_lifecycle_limit_reconcile(1),
    ];
    bus.publish_lifecycle(LifecycleEvent::RequestStarted {
        event_id: "full-channels".to_owned(),
        request_id: "full-channels".to_owned(),
        ts_ms: 0,
        stream: false,
        source_kind: None,
        source_ref_id: None,
    });
    let session = scenario.schedule(Some("renewal-key")).await;
    assert!(scenario.claim(&session).await);

    // When
    scenario
        .dispatch_and_finalize(&session, true, Some(bus.as_ref()))
        .await;

    // Then
    assert_eq!(scenario.row_counts().await, (1, 1, 1));
    scenario.assert_full_capacity_available().await;
    drop(receivers);
}

#[tokio::test]
async fn t2__renewal_disable_stops_cleanly() {
    // Given
    let scenario = RenewalAccountingScenario::new().await;
    let session = scenario.schedule(Some("renewal-key")).await;
    assert!(scenario.claim(&session).await);
    let finalization = scenario.dispatch(&session, true).await;

    // When
    scenario
        .disable_after_dispatch(&session, finalization)
        .await;

    // Then
    assert_eq!(scenario.http_calls(), 1);
    assert_eq!(scenario.row_counts().await, (0, 0, 0));
    assert_eq!(
        scenario.terminal_reason().await,
        Some(CacheKeepaliveTerminalReason::Cancelled)
    );
    scenario.assert_full_capacity_available().await;
}

#[tokio::test]
async fn t2__renewal_stale_running_reclaimed_no_redispatch() {
    // Given
    let scenario = RenewalAccountingScenario::new().await;
    let session = scenario.schedule(None).await;
    assert!(scenario.claim(&session).await);
    scenario.dispatch_and_finalize(&session, false, None).await;

    // When
    scenario
        .terminalize(&session, CacheKeepaliveTerminalReason::Stale)
        .await;
    let reclaimed_claimed = scenario.claim(&session).await;

    // Then
    assert!(!reclaimed_claimed);
    assert_eq!(scenario.http_calls(), 1);
    assert_eq!(scenario.row_counts().await, (1, 1, 1));
    assert_eq!(
        scenario.terminal_reason().await,
        Some(CacheKeepaliveTerminalReason::Stale)
    );
}

#[tokio::test]
async fn t2__renewal_duplicate_dispatch_blocked_by_claim() {
    // Given
    let scenario = RenewalAccountingScenario::new().await;
    let session = scenario.schedule(None).await;

    // When
    let claim_winners = scenario.concurrent_claims(&session).await;
    assert_eq!(claim_winners, 1);
    scenario.dispatch_and_finalize(&session, false, None).await;

    // Then
    assert_eq!(scenario.http_calls(), 1);
    assert_eq!(scenario.row_counts().await, (1, 1, 1));
}
