use std::collections::HashMap;
use std::sync::Arc;

use cc_lb_domain::{
    PrincipalKindLite, RoutingTrace, StageDecision, SubscriptionPreferenceTrace, SubscriptionTier,
    WrhKeySource,
};
use cc_lb_lifecycle::{EventId, LifecycleEvent, RouteFailure, RouteInfo, TerminationReason};
use cc_lb_observability::NoopMetricsHook;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::{Partial, handle_event, partials_len, spawn_lifecycle_routing_tier_subscriber};

fn eid(s: &str) -> EventId {
    s.to_owned()
}

fn authenticated(event_id: &str) -> LifecycleEvent {
    LifecycleEvent::AuthenticationCompleted {
        event_id: eid(event_id),
        principal_id: "principal-a".into(),
        principal_kind: PrincipalKindLite::Machine,
    }
}

fn stage(tier: Option<SubscriptionTier>) -> StageDecision {
    StageDecision {
        stage_name: "subscription-preference".into(),
        upstream_id: Some(Uuid::from_u128(3)),
        reason: Some("subscription_alive".into()),
        duration_us: 10,
        subscription_preference: tier.map(|chosen_tier| SubscriptionPreferenceTrace {
            chosen_tier,
            candidates: Vec::new(),
            wrh_key_source: WrhKeySource::RequestId,
            previous_tier: None,
            rendezvous_salt_version: None,
            formula_version: None,
            cache_cost_basis_version: None,
            formula_winner_upstream_id: None,
            kept_upstream_id: None,
            incumbent_upstream_id: None,
            estimated_switch_cache_loss_micros: None,
            cache_loss_status: None,
            switch_gate_reason: None,
            bucket_v3_cache_affinity_key: None,
            lineage_would_have_predicted_read_tokens: None,
            lineage_would_have_picked_upstream_id: None,
        }),
        cache_affinity: None,
    }
}

fn route_completed(event_id: &str, tier: Option<SubscriptionTier>) -> LifecycleEvent {
    LifecycleEvent::RouteCompleted {
        event_id: eid(event_id),
        result: Ok(RouteInfo {
            upstream_id: Uuid::from_u128(3),
            upstream_name: "upstream-a".into(),
            model: Some("claude-sonnet-4".into()),
            upstream_kind: None,
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
            quota_weight_factor: None,
            quota_cache_multiplier: None,
            quota_warning_multiplier: None,
            quota_effective_weight: None,
            quota_uniform_fallback: None,
            wrh_key_source: None,
            lineage_would_have_predicted_read_tokens: None,
            lineage_would_have_picked_upstream_id: None,
        }),
        routing_trace: Some(RoutingTrace {
            stages: vec![stage(tier)],
            terminal_decision: None,
        }),
    }
}

fn route_failed(event_id: &str) -> LifecycleEvent {
    LifecycleEvent::RouteCompleted {
        event_id: eid(event_id),
        result: Err(RouteFailure::RouteNotConfigured),
        routing_trace: None,
    }
}

fn terminated(event_id: &str) -> LifecycleEvent {
    LifecycleEvent::RequestTerminated {
        event_id: eid(event_id),
        reason: TerminationReason::Success,
        client_status: 200,
        duration_ms: 12,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        upstream_body_ms: None,
    }
}

async fn send_events(events: Vec<LifecycleEvent>) {
    let (tx, rx) = mpsc::channel(16);
    let handle = spawn_lifecycle_routing_tier_subscriber(rx, Arc::new(NoopMetricsHook));
    for event in events {
        tx.send(event).await.expect("subscriber channel open");
    }
    drop(tx);
    handle.shutdown().await;
}

fn route_drains_map(tier: SubscriptionTier) {
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        authenticated("route-a"),
    );
    assert_eq!(partials_len(&partials), 1);
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        route_completed("route-a", Some(tier)),
    );
    assert_eq!(partials_len(&partials), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_then_route_with_known_base_stage_drains_map() {
    send_events(vec![
        authenticated("known-base"),
        route_completed("known-base", Some(SubscriptionTier::KnownBase)),
    ])
    .await;
    route_drains_map(SubscriptionTier::KnownBase);
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_then_route_with_partial_base_stage_drains_map() {
    send_events(vec![
        authenticated("partial-base"),
        route_completed("partial-base", Some(SubscriptionTier::PartialBase)),
    ])
    .await;
    route_drains_map(SubscriptionTier::PartialBase);
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_then_route_with_overage_stage_drains_map() {
    send_events(vec![
        authenticated("overage"),
        route_completed("overage", Some(SubscriptionTier::Overage)),
    ])
    .await;
    route_drains_map(SubscriptionTier::Overage);
}

#[tokio::test(flavor = "current_thread")]
async fn authentication_then_route_with_unknown_probe_stage_drains_map() {
    send_events(vec![
        authenticated("unknown-probe"),
        route_completed("unknown-probe", Some(SubscriptionTier::UnknownProbe)),
    ])
    .await;
    route_drains_map(SubscriptionTier::UnknownProbe);
}

#[tokio::test(flavor = "current_thread")]
async fn route_without_authentication_early_drops() {
    send_events(vec![route_completed(
        "missing-auth",
        Some(SubscriptionTier::KnownBase),
    )])
    .await;
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        route_completed("missing-auth", Some(SubscriptionTier::KnownBase)),
    );
    assert_eq!(partials_len(&partials), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn route_without_subscription_preference_stage_does_nothing() {
    send_events(vec![
        authenticated("no-preference"),
        route_completed("no-preference", None),
    ])
    .await;
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        authenticated("no-preference"),
    );
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        route_completed("no-preference", None),
    );
    assert_eq!(partials_len(&partials), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn terminated_evicts_map_entry() {
    send_events(vec![authenticated("terminated"), terminated("terminated")]).await;
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        authenticated("terminated"),
    );
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        terminated("terminated"),
    );
    assert_eq!(partials_len(&partials), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn route_failure_does_not_panic() {
    send_events(vec![
        authenticated("route-fail"),
        route_failed("route-fail"),
    ])
    .await;
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        authenticated("route-fail"),
    );
    handle_event(
        &NoopMetricsHook,
        &mut partials,
        16,
        route_failed("route-fail"),
    );
    assert_eq!(partials_len(&partials), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn map_cap_evicts_oldest_when_full() {
    let mut partials: HashMap<EventId, Partial> = HashMap::new();
    let map_cap = 2;
    for idx in 0..=map_cap {
        handle_event(
            &NoopMetricsHook,
            &mut partials,
            map_cap,
            authenticated(&format!("auth-{idx}")),
        );
    }
    assert_eq!(partials_len(&partials), map_cap);
}
