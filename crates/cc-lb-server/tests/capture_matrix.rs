#![cfg(feature = "capture")]

use crate::{capture_matrix_support, common};

use capture_matrix_support::{
    MESSAGE_BODY, capture_config, count_distinct_event_ids, count_request_id, count_rows,
    joined_row, open_capture_pool,
};
use fake_anthropic::{AppConfig, MessageScript, ScriptedMessageResponse};
use http::StatusCode;
use tokio::task::JoinSet;

#[tokio::test]
async fn routed_requests_create_unique_rows_when_client_request_ids_collide() {
    // Given: capture is enabled and two of four requests share a client-supplied request ID.
    let directory = tempfile::tempdir().expect("create capture matrix directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let mut server = common::spawn_capture_test_server_with_extra_config(&config).await;
    let request_ids = [
        "capture-matrix-collision",
        "capture-matrix-collision",
        "capture-matrix-unique-a",
        "capture-matrix-unique-b",
    ];

    // When: all requests complete before one proven graceful shutdown flushes capture.
    for request_id in request_ids {
        let response = common::http_post(
            server.proxy_addr,
            "/v1/messages",
            MESSAGE_BODY,
            &[("request-id", request_id)],
        )
        .await
        .expect("post captured request");
        assert_eq!(response.status, 200);
    }
    server.graceful_shutdown();

    // Then: lifecycle event IDs, not colliding request IDs, define four distinct rows.
    let pool = open_capture_pool(&capture_path).await;
    assert_eq!(count_rows(&pool).await, 4);
    assert_eq!(count_distinct_event_ids(&pool).await, 4);
    assert_eq!(count_request_id(&pool, "capture-matrix-collision").await, 2);
}

#[tokio::test]
async fn saturated_capture_queue_drops_records_without_delaying_proxy_responses() {
    // Given: a one-message capture queue and enough simultaneous requests to saturate it.
    const REQUEST_COUNT: usize = 64;
    let directory = tempfile::tempdir().expect("create overflow capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1);
    let mut server = common::spawn_capture_test_server_with_extra_config(&config).await;
    let mut requests = JoinSet::new();

    // When: all clients use the real proxy concurrently under one bounded deadline.
    for index in 0..REQUEST_COUNT {
        let proxy_addr = server.proxy_addr;
        requests.spawn(async move {
            let request_id = format!("capture-overflow-{index}");
            common::http_post(
                proxy_addr,
                "/v1/messages",
                MESSAGE_BODY,
                &[("request-id", request_id.as_str())],
            )
            .await
        });
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while let Some(result) = requests.join_next().await {
            let response = result
                .expect("join overflow request")
                .expect("send overflow request");
            assert_eq!(response.status, 200);
        }
    })
    .await
    .expect("capture saturation must not delay proxy responses");
    server.graceful_shutdown();

    // Then: capture lost work under pressure while every client succeeded and the process stayed up.
    let pool = open_capture_pool(&capture_path).await;
    assert!(count_rows(&pool).await < REQUEST_COUNT as i64);
    let stderr = server.finish_stderr();
    assert!(stderr.contains("capture queue full; dropping message"));
}

#[tokio::test]
async fn oauth_unauthorized_retry_captures_max_attempt_and_final_status() {
    // Given: a valid OAuth upstream returns 401 once, refreshes, and then returns 200.
    let script = MessageScript::new();
    script.push_response(ScriptedMessageResponse::error(
        StatusCode::UNAUTHORIZED,
        "authentication_error",
        "expired test access token",
    ));
    script.push_response(ScriptedMessageResponse::ok());
    let fake_config = AppConfig {
        message_script: Some(script),
        ..AppConfig::default()
    };
    let directory = tempfile::tempdir().expect("create retry capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let mut server =
        common::spawn_capture_test_server_with_oauth_upstreams(&config, fake_config).await;

    // When: one request traverses both upstream attempts before capture is drained.
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        MESSAGE_BODY,
        &[("request-id", "capture-oauth-retry")],
    )
    .await
    .expect("post OAuth retry request");
    assert_eq!(response.status, 200);
    server.graceful_shutdown();

    // Then: the joined row keeps the maximum attempt and latest non-401 status.
    let pool = open_capture_pool(&capture_path).await;
    let row = joined_row(&pool, "capture-oauth-retry").await;
    assert_eq!(row.disposition, "routed_dispatched_success");
    assert_eq!(row.attempt_num, Some(2));
    assert_eq!(row.upstream_status, Some(200));
}

#[tokio::test]
async fn early_reject_produces_no_capture_row() {
    let directory = tempfile::tempdir().expect("create early reject capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let mut server =
        common::spawn_capture_test_server_with_apikey_mode(&config, vec![], AppConfig::default())
            .await;

    let response = common::http_post_with_api_key(
        server.proxy_addr,
        "/v1/messages",
        "invalid-key",
        MESSAGE_BODY,
        &[("request-id", "capture-early-reject")],
    )
    .await
    .expect("post early reject request");
    assert_eq!(response.status, 401);
    server.graceful_shutdown();

    let pool = open_capture_pool(&capture_path).await;
    let count = count_rows(&pool).await;
    if count > 0 {
        let row = joined_row(&pool, "capture-early-reject").await;
        println!("Row: {:?}", row);
    }
    assert_eq!(count, 0);
}

#[tokio::test]
async fn limit_reject_captures_null_usage() {
    let directory = tempfile::tempdir().expect("create limit reject capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let principal_limits = vec![cc_lb_storage_api::principal::Limit {
        kind: cc_lb_storage_api::principal::LimitKind::Requests,
        window_secs: 60,
        cap_micros: 0,
    }];
    let mut server = common::spawn_capture_test_server_with_apikey_mode(
        &config,
        principal_limits,
        AppConfig::default(),
    )
    .await;

    let api_key = server.managed_key.as_ref().unwrap().plaintext.clone();
    let response = common::http_post_with_api_key(
        server.proxy_addr,
        "/v1/messages",
        &api_key,
        MESSAGE_BODY,
        &[("request-id", "capture-limit-reject")],
    )
    .await
    .expect("post limit reject request");
    assert_eq!(response.status, 429);
    server.graceful_shutdown();

    let pool = open_capture_pool(&capture_path).await;
    let row = joined_row(&pool, "capture-limit-reject").await;
    assert_eq!(row.disposition, "routed_limit_rejected");
    assert_eq!(row.input_tokens, None);
    assert_eq!(row.output_tokens, None);
}

#[tokio::test]
async fn client_disconnect_captures_partial_row() {
    let directory = tempfile::tempdir().expect("create disconnect capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let mut server = common::spawn_capture_test_server_with_extra_config(&config).await;

    capture_matrix_support::disconnect_after_message_start(
        server.proxy_addr,
        "capture-client-disconnect",
    )
    .await
    .expect("disconnect client");

    // Give the server a moment to detect the disconnect and emit the capture record
    // before we send SIGTERM. This prevents a race where SIGTERM arrives before
    // the disconnect is processed, which could lead to the request being cancelled
    // during drain in a way that might bypass capture or race with the sink shutdown.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    server.graceful_shutdown();

    let pool = open_capture_pool(&capture_path).await;
    let row = joined_row(&pool, "capture-client-disconnect").await;
    assert_eq!(row.disposition, "routed_client_disconnected");
}

#[tokio::test]
async fn replay_consistency_matches_deterministic_trace() {
    use bytes::Bytes;
    use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
    use cc_lb_routing::{FilterPlugin, RoutingContext};
    use http::{HeaderMap, Method};

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
    .expect("post replay request");
    assert_eq!(response.status, 200);
    server.graceful_shutdown();

    let pool = open_capture_pool(&capture_path).await;
    let record = capture_matrix_support::capture_record(&pool, "capture-replay").await;
    let input = &record.input;

    let mut candidates = input.candidates.clone();
    candidates.retain(|c| {
        input
            .subscription_preference_input_upstream_ids
            .contains(&c.upstream_id)
    });

    let ctx = RoutingContext {
        request_id: input.request_id.clone(),
        thread_id: input.thread_id.clone(),
        requested_service_tier: None,
        downstream_headers: HeaderMap::new(),
        method: Method::POST,
        path: "/v1/messages".to_owned(),
        query: None,
        body_bytes: Bytes::new(),
        canonical_model_id: input.canonical_model_id.clone(),
        cache_pricing: input.cache_pricing.clone(),
    };

    let principal = cc_lb_domain::Principal {
        id: "test-principal".to_owned(),
        kind: cc_lb_domain::PrincipalKind::ApiKey,
        claims: serde_json::Map::new(),
    };

    let filter = SubscriptionPreferenceFilter::new();
    let output = filter
        .filter(&ctx, &principal, &candidates)
        .expect("filter");

    let original_trace = input
        .routing_trace
        .stages
        .iter()
        .find(|s| s.stage_name == "subscription-preference")
        .unwrap()
        .subscription_preference
        .as_ref()
        .unwrap();
    let replay_trace = output.subscription_preference.as_ref().unwrap();

    assert_eq!(original_trace.chosen_tier, replay_trace.chosen_tier);
    assert_eq!(
        original_trace.formula_winner_upstream_id,
        replay_trace.formula_winner_upstream_id
    );
    assert_eq!(
        original_trace.kept_upstream_id,
        replay_trace.kept_upstream_id
    );
    assert_eq!(original_trace.formula_version, replay_trace.formula_version);

    assert_eq!(
        original_trace.candidates.len(),
        replay_trace.candidates.len()
    );
    for (orig_c, replay_c) in original_trace
        .candidates
        .iter()
        .zip(replay_trace.candidates.iter())
    {
        assert_eq!(orig_c.upstream_id, replay_c.upstream_id);
        assert_eq!(orig_c.tier, replay_c.tier);
        assert_eq!(
            orig_c.estimated_input_cost_micros,
            replay_c.estimated_input_cost_micros
        );
        assert_eq!(orig_c.quota_urgency, replay_c.quota_urgency);
    }

    candidates[0].subscription_quotas[0].utilization = Some(0.10);
    let output_mutated = filter
        .filter(&ctx, &principal, &candidates)
        .expect("filter");
    let mutated_trace = output_mutated.subscription_preference.as_ref().unwrap();
    assert_ne!(
        original_trace.candidates[0].quota_urgency,
        mutated_trace.candidates[0].quota_urgency
    );
}
