#![cfg(feature = "capture")]

mod capture_matrix_support;
mod common;

use bytes::Bytes;
use capture_matrix_support::{MESSAGE_BODY, capture_config, capture_record, open_capture_pool};
use cc_lb_domain::{Principal, PrincipalKind, SubscriptionPreferenceTrace, UpstreamCandidate};
use cc_lb_engine::builtin_filters::subscription_preference::{
    SALT_VERSION, SubscriptionPreferenceFilter,
};
use cc_lb_routing::{FilterPlugin, RoutingContext};
use fake_anthropic::AppConfig;
use http::{HeaderMap, Method};

#[tokio::test]
async fn captured_subscription_preference_input_replays_to_identical_trace() {
    // Given: the real binary captures one request routed across two quota-bearing OAuth candidates.
    let directory = tempfile::tempdir().expect("create replay capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let mut server =
        common::spawn_capture_test_server_with_oauth_upstreams(&config, AppConfig::default()).await;
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        MESSAGE_BODY,
        &[("request-id", "capture-replay")],
    )
    .await
    .expect("post replay source request");
    assert_eq!(response.status, 200);
    server.graceful_shutdown();
    let pool = open_capture_pool(&capture_path).await;
    let record = capture_record(&pool, "capture-replay").await;

    // When: only the exact candidate subset seen by v11 is replayed through its public filter API.
    assert_eq!(SALT_VERSION, record.input.salt_version);
    let captured_trace = captured_subscription_trace(&record.input.routing_trace);
    let candidates = replay_candidates(
        &record.input.candidates,
        &record.input.subscription_preference_input_upstream_ids,
    );
    let filter = SubscriptionPreferenceFilter::new();
    let context = replay_context(&record.input);
    let principal = replay_principal();
    let replayed = filter
        .filter(&context, &principal, &candidates)
        .expect("replay subscription-preference filter")
        .subscription_preference
        .expect("replay subscription trace");

    // Then: deterministic formula fields match exactly, without comparing stage duration_us.
    assert_eq!(replayed, captured_trace);
    let mut mutated_candidates = candidates;
    let quota = mutated_candidates
        .first_mut()
        .and_then(|candidate| candidate.subscription_quotas.first_mut())
        .expect("captured replay candidate has quota utilization");
    // Low utilization creates positive pressure for either seeded candidate.
    quota.utilization = Some(0.10);
    let mutated = filter
        .filter(&context, &principal, &mutated_candidates)
        .expect("replay mutated subscription-preference filter")
        .subscription_preference
        .expect("mutated replay subscription trace");
    assert_ne!(mutated, captured_trace);
}

fn captured_subscription_trace(
    routing_trace: &cc_lb_domain::RoutingTrace,
) -> SubscriptionPreferenceTrace {
    routing_trace
        .stages
        .iter()
        .find_map(|stage| stage.subscription_preference.clone())
        .expect("captured subscription-preference trace")
}

fn replay_candidates(
    candidates: &[UpstreamCandidate],
    input_upstream_ids: &[uuid::Uuid],
) -> Vec<UpstreamCandidate> {
    input_upstream_ids
        .iter()
        .map(|upstream_id| {
            candidates
                .iter()
                .find(|candidate| candidate.upstream_id == *upstream_id)
                .expect("captured subscription input references full candidate")
                .clone()
        })
        .collect()
}

fn replay_context(input: &cc_lb_capture::schema::CapturedRequestInput) -> RoutingContext {
    RoutingContext {
        request_id: input.request_id.clone(),
        thread_id: input.thread_id.clone(),
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        canonical_model_id: input.canonical_model_id.clone(),
        cache_pricing: input.cache_pricing.clone(),
    }
}

fn replay_principal() -> Principal {
    Principal {
        id: "capture-replay".to_owned(),
        kind: PrincipalKind::InternalKey,
        claims: serde_json::Map::new(),
    }
}
