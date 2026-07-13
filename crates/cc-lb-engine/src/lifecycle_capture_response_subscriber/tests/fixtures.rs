use std::error::Error;

use cc_lb_capture::{
    schema::{CaptureRecord, CapturedRequestInput},
    sink::CaptureSink,
    store::open_capture_store,
};
use cc_lb_domain::{CachePricingSummary, RoutingTrace};
use cc_lb_lifecycle::{
    LifecycleEvent, RouteFailure, RouteInfo, TerminationReason, UsageSnapshot, UsageSource,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::super::spawn_lifecycle_capture_response_subscriber;

pub(super) type TestResult<T> = Result<T, Box<dyn Error>>;

pub(super) fn input(event_id: &str, request_id: &str) -> CapturedRequestInput {
    CapturedRequestInput {
        event_id: event_id.to_owned(),
        request_id: request_id.to_owned(),
        thread_id: None,
        canonical_model_id: "claude-sonnet-4-5".to_owned(),
        cache_pricing: CachePricingSummary {
            status: "unavailable".to_owned(),
            input_micros_per_million: None,
            cache_creation_5m_micros_per_million: None,
            cache_creation_1h_micros_per_million: None,
            cache_read_micros_per_million: None,
        },
        breakpoints: Vec::new(),
        candidates: Vec::new(),
        subscription_preference_input_upstream_ids: Vec::new(),
        routing_trace: RoutingTrace {
            stages: Vec::new(),
            terminal_decision: None,
        },
        captured_at_unix_ms: 1_700_000_000_123,
        salt_version: "v11".to_owned(),
        cache_cost_basis_version: "v1".to_owned(),
        capture_schema_version: 1,
        build_version: "test".to_owned(),
    }
}

pub(super) fn started(event_id: &str, request_id: &str) -> LifecycleEvent {
    LifecycleEvent::RequestStarted {
        event_id: event_id.to_owned(),
        request_id: request_id.to_owned(),
        ts_ms: 1_700_000_000_000,
        stream: false,
    }
}

pub(super) fn route(event_id: &str, upstream_id: Uuid) -> LifecycleEvent {
    LifecycleEvent::RouteCompleted {
        event_id: event_id.to_owned(),
        result: Ok(RouteInfo {
            upstream_id,
            upstream_name: format!("upstream-{upstream_id}"),
            model: Some("claude-sonnet-4-5".to_owned()),
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
        routing_trace: None,
    }
}

pub(super) fn no_route(event_id: &str) -> LifecycleEvent {
    LifecycleEvent::RouteCompleted {
        event_id: event_id.to_owned(),
        result: Err(RouteFailure::RouteNoUpstreamAfterFilter),
        routing_trace: None,
    }
}

pub(super) fn attempt(event_id: &str, attempt_num: u32, upstream_id: Uuid) -> LifecycleEvent {
    LifecycleEvent::UpstreamAttempt {
        event_id: event_id.to_owned(),
        attempt_num,
        upstream_id,
    }
}

pub(super) fn response_started(event_id: &str, status: u16) -> LifecycleEvent {
    LifecycleEvent::UpstreamResponseStarted {
        event_id: event_id.to_owned(),
        status,
        headers: cc_lb_request_log::HeaderSnapshot::default(),
        bulkhead_wait_ms: None,
        dns_ms: None,
        connect_ms: None,
        connection_reused: None,
        shape_ms: None,
        sign_ms: None,
        upstream_ttfb_ms: None,
    }
}

pub(super) fn usage_observed(event_id: &str) -> LifecycleEvent {
    LifecycleEvent::UsageObserved {
        event_id: event_id.to_owned(),
        usage: UsageSnapshot {
            input_tokens: 2_000,
            output_tokens: 500,
            cache_creation_input_tokens: 500,
            cache_creation_input_tokens_5m: 200,
            cache_creation_input_tokens_1h: 300,
            cache_read_input_tokens: 1_000,
            ..UsageSnapshot::default()
        },
        source: UsageSource::NonStreamBody,
    }
}

pub(super) fn terminated(
    event_id: &str,
    reason: TerminationReason,
    client_status: u16,
) -> LifecycleEvent {
    LifecycleEvent::RequestTerminated {
        event_id: event_id.to_owned(),
        reason,
        client_status,
        duration_ms: 345,
        limit_reconcile_ms: None,
        observability_post_ms: None,
        proxy_setup_ms: None,
        upstream_body_ms: None,
        first_body_chunk_ms: None,
        internal_errors: Vec::new(),
    }
}

pub(super) async fn run(
    events: Vec<LifecycleEvent>,
    inputs: Vec<CapturedRequestInput>,
) -> TestResult<Vec<CaptureRecord>> {
    let directory = tempfile::tempdir()?;
    let store = open_capture_store(&directory.path().join("capture.sqlite")).await?;
    let (sink, writer) = CaptureSink::new(store.clone(), 128);
    let (tx, rx) = mpsc::channel(128);
    let subscriber = spawn_lifecycle_capture_response_subscriber(rx, sink.clone());

    for input in inputs {
        sink.try_input(input)?;
    }
    for event in events {
        tx.send(event).await?;
    }
    drop(tx);
    subscriber.shutdown().await;
    writer.shutdown().await;

    let payloads =
        sqlx::query_scalar::<_, String>("SELECT payload_json FROM capture_v1 ORDER BY event_id")
            .fetch_all(store.pool())
            .await?;
    payloads
        .into_iter()
        .map(|payload| serde_json::from_str(&payload).map_err(Into::into))
        .collect()
}
