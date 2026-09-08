use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use cc_lb_control::{BusReceiver, LifecycleBusReceiver, RequestEventBus};
use cc_lb_lifecycle::{
    LifecycleEvent, ParseInfo, RouteInfo, TerminationReason, UsageSnapshot, UsageSource,
};
use cc_lb_pricing::{
    CatalogSnapshot, CatalogStatus, Pricing, UsdPerMillion, global_catalog,
    spawn_lifecycle_pricing_subscriber,
};
use cc_lb_request_log::RequestEventUpdate;
use tokio::sync::{broadcast, mpsc};
use uuid::Uuid;

#[derive(Default)]
struct RecordingBus {
    lifecycle_events: Mutex<Vec<LifecycleEvent>>,
}

impl RequestEventBus for RecordingBus {
    fn publish(&self, _update: RequestEventUpdate) {}

    fn subscribe(&self) -> BusReceiver {
        let (_, rx) = broadcast::channel(1);
        BusReceiver::InMemory(rx)
    }

    fn publish_lifecycle(&self, event: LifecycleEvent) {
        self.lifecycle_events
            .lock()
            .expect("recording bus lock")
            .push(event);
    }

    fn subscribe_lifecycle(&self) -> LifecycleBusReceiver {
        let (_, rx) = broadcast::channel(1);
        LifecycleBusReceiver::InMemory(rx)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn renewal_is_priced_only_after_terminal_with_oauth_route_kind() {
    // Given a renewal lifecycle that has usage and an OAuth upstream route, but no terminal.
    install_renewal_pricing();
    let incomplete_bus = Arc::new(RecordingBus::default());
    let (incomplete_tx, incomplete_rx) = mpsc::channel(4);
    let incomplete = spawn_lifecycle_pricing_subscriber(incomplete_rx, incomplete_bus.clone());
    send_renewal_pricing_inputs(&incomplete_tx, "renewal-incomplete").await;
    drop(incomplete_tx);
    incomplete.shutdown().await;

    // Then no priced event is emitted before RequestTerminated.
    assert!(
        incomplete_bus
            .lifecycle_events
            .lock()
            .expect("incomplete recording bus lock")
            .is_empty()
    );

    // When the equivalent renewal lifecycle terminates.
    let complete_bus = Arc::new(RecordingBus::default());
    let (complete_tx, complete_rx) = mpsc::channel(4);
    let complete = spawn_lifecycle_pricing_subscriber(complete_rx, complete_bus.clone());
    send_renewal_pricing_inputs(&complete_tx, "renewal-complete").await;
    complete_tx
        .send(LifecycleEvent::RequestTerminated {
            event_id: "renewal-complete".to_owned(),
            reason: TerminationReason::Success,
            client_status: 200,
            duration_ms: 12,
            first_body_chunk_ms: None,
            internal_errors: Vec::new(),
            limit_reconcile_ms: None,
            observability_post_ms: None,
            proxy_setup_ms: None,
            setup_timings: Default::default(),
            upstream_body_ms: None,
        })
        .await
        .expect("send renewal terminal");
    drop(complete_tx);
    complete.shutdown().await;

    // Then exactly one terminal price is emitted for the OAuth-routed renewal.
    let lifecycle_events = complete_bus
        .lifecycle_events
        .lock()
        .expect("complete recording bus lock");
    assert!(matches!(
        lifecycle_events.as_slice(),
        [LifecycleEvent::Priced { event_id, cost }] if event_id == "renewal-complete"
            && cost.total_micros == Some(140)
    ));
}

fn install_renewal_pricing() {
    let mut models = HashMap::new();
    models.insert(
        "claude-renewal".to_owned(),
        Pricing {
            model: "claude-renewal".to_owned(),
            input_per_million_usd: UsdPerMillion::from_whole_usd(2),
            output_per_million_usd: UsdPerMillion::from_whole_usd(3),
            by_tier: Default::default(),
        },
    );
    global_catalog().install_snapshot(CatalogSnapshot {
        payload_hash: String::new(),
        fetched_at_ms: 0,
        models,
        raw_json: Vec::new(),
        cache_creation_per_million_usd: HashMap::new(),
        cache_read_per_million_usd: HashMap::new(),
        cache_creation_per_million_usd_by_tier: HashMap::new(),
        cache_read_per_million_usd_by_tier: HashMap::new(),
        status: CatalogStatus::Ok,
    });
}

async fn send_renewal_pricing_inputs(tx: &mpsc::Sender<LifecycleEvent>, event_id: &str) {
    tx.send(LifecycleEvent::RequestStarted {
        event_id: event_id.to_owned(),
        request_id: format!("request-{event_id}"),
        ts_ms: 1_730_000_000_000,
        stream: false,
        source_kind: Some("renewal".to_owned()),
        source_ref_id: Some(format!("session-{event_id}")),
    })
    .await
    .expect("send renewal start");
    tx.send(LifecycleEvent::ParseCompleted {
        event_id: event_id.to_owned(),
        result: Ok(ParseInfo {
            model: Some("claude-renewal".to_owned()),
            ..ParseInfo::default()
        }),
    })
    .await
    .expect("send renewal parse");
    tx.send(LifecycleEvent::RouteCompleted {
        event_id: event_id.to_owned(),
        result: Ok(RouteInfo {
            upstream_id: Uuid::nil(),
            upstream_name: "renewal-oauth".to_owned(),
            model: Some("claude-renewal".to_owned()),
            upstream_kind: Some("anthropic_oauth".to_owned()),
            route_ms: None,
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
    .expect("send renewal route");
    tx.send(LifecycleEvent::UsageObserved {
        event_id: event_id.to_owned(),
        usage: UsageSnapshot {
            input_tokens: 10,
            output_tokens: 40,
            ..UsageSnapshot::default()
        },
        source: UsageSource::NonStreamBody,
    })
    .await
    .expect("send renewal usage");
}
