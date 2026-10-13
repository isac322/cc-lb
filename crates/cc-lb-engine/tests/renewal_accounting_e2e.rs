#[path = "renewal_accounting_e2e/support.rs"]
mod support;

#[cfg(feature = "postgres")]
#[path = "renewal_accounting_e2e/postgres.rs"]
mod postgres;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cc_lb_control::{InMemoryBus, RequestEventBus};
use cc_lb_engine::{
    RequestDiagnosticsDelivery, RequestDiagnosticsOptions, RequestDiagnosticsSink,
    spawn_request_event_assembler_with_diagnostics,
};
use cc_lb_lifecycle::{LifecycleEvent, TerminationReason};
use cc_lb_observability::NoopMetricsHook;
use cc_lb_request_log::{
    RequestDiagnosticsRecord, RequestEventKind, render_request_diagnostics_line,
};
use cc_lb_storage_api::CacheKeepaliveTerminalReason;

use support::RenewalAccountingScenario;

#[tokio::test]
async fn renewal_cycle_bills_once() {
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
async fn renewal_duplicate_bills_once() {
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
async fn renewal_observeonly_then_reseed() {
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
async fn renewal_survives_full_channels() {
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
        event_kind: None,
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
async fn renewal_disable_stops_cleanly() {
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
async fn renewal_stale_running_reclaimed_no_redispatch() {
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
async fn renewal_duplicate_dispatch_blocked_by_claim() {
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

#[derive(Default)]
struct RecordingDiagnosticsSink {
    lines: Mutex<Vec<serde_json::Value>>,
}

#[async_trait]
impl RequestDiagnosticsSink for RecordingDiagnosticsSink {
    async fn offer(&self, record: RequestDiagnosticsRecord, _delivery: RequestDiagnosticsDelivery) {
        let line = render_request_diagnostics_line(&record).expect("diagnostics record renders");
        let value = serde_json::from_slice(&line).expect("diagnostics line is JSON");
        self.lines.lock().expect("lines lock").push(value);
    }

    async fn shutdown(&self) {}
}

#[tokio::test]
async fn renewal_row_yields_one_diagnostics_line_with_the_finalizer_seq() {
    // Given: the keepalive finalizer inserted the renewal row first.
    let scenario = RenewalAccountingScenario::new().await;
    let session = scenario.schedule(Some("renewal-key")).await;
    assert!(scenario.claim(&session).await);
    let completion = scenario.dispatch_and_finalize(&session, true, None).await;
    let sink = Arc::new(RecordingDiagnosticsSink::default());
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let assembler = spawn_request_event_assembler_with_diagnostics(
        rx,
        scenario.request_event_store(),
        None,
        Arc::new(NoopMetricsHook),
        RequestDiagnosticsOptions {
            sink: Some(sink.clone() as Arc<dyn RequestDiagnosticsSink>),
            strip_payload: false,
        },
    );

    // When: the renewal's lifecycle events reach the assembler afterwards.
    tx.send(LifecycleEvent::RequestStarted {
        event_id: completion.event_id.clone(),
        request_id: "renewal-observability".to_owned(),
        ts_ms: 1_003_000,
        stream: false,
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(completion.source_ref_id.clone()),
        event_kind: Some(RequestEventKind::Renewal),
    })
    .await
    .expect("assembler is running");
    tx.send(LifecycleEvent::RequestTerminated {
        event_id: completion.event_id.clone(),
        reason: TerminationReason::Success,
        client_status: 200,
        duration_ms: 5,
        request_body_read_ms: None,
        request_body_bytes: None,
        finalize_ms: None,
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
        event_kind: Some(RequestEventKind::Renewal),
    })
    .await
    .expect("assembler is running");
    drop(tx);
    assembler.shutdown().await;

    // Then: the assembler's insert hit ON CONFLICT, and its single line carries the
    // finalizer row's cursor.
    assert_eq!(scenario.row_counts().await, (1, 1, 1));
    let lines = sink.lines.lock().expect("lines lock").clone();
    assert_eq!(
        lines.len(),
        1,
        "exactly one line per renewal row: {lines:?}"
    );
    assert_eq!(lines[0]["event_id"], completion.event_id.as_str());
    assert_eq!(lines[0]["seq"], completion.cursor);
    assert_eq!(lines[0]["source_kind"], "renewal");
}
