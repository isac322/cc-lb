#![cfg(feature = "capture")]

use crate::{capture_matrix_support, common};

use capture_matrix_support::{
    MESSAGE_BODY, capture_config, count_rows, disconnect_after_message_start, joined_row,
    open_capture_pool,
};
use cc_lb_storage_api::{Limit, LimitKind};
use fake_anthropic::AppConfig;

#[tokio::test]
async fn authentication_rejection_before_routing_creates_no_capture_row() {
    // Given: API-key authentication is enabled and capture starts with route-free readiness.
    let directory = tempfile::tempdir().expect("create early-reject capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let mut server = common::spawn_capture_test_server_with_apikey_mode(
        &config,
        Vec::new(),
        AppConfig::default(),
    )
    .await;

    // When: an invalid downstream key is rejected before candidate routing.
    let response = common::http_post_with_api_key(
        server.proxy_addr,
        "/v1/messages",
        "invalid-key",
        MESSAGE_BODY,
        &[("request-id", "capture-early-reject")],
    )
    .await
    .expect("post early-reject request");
    assert_eq!(response.status, 401);
    server.graceful_shutdown();

    // Then: no input existed to join, so capture persists no response-only row.
    let pool = open_capture_pool(&capture_path).await;
    assert_eq!(count_rows(&pool).await, 0);
}

#[tokio::test]
async fn limit_rejection_after_routing_captures_row_without_usage() {
    // Given: a zero-cap request limit deterministically rejects the first authenticated request.
    let directory = tempfile::tempdir().expect("create limit-reject capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let limits = vec![Limit {
        kind: LimitKind::Requests,
        window_secs: 60,
        cap_micros: 0,
    }];
    let mut server =
        common::spawn_capture_test_server_with_apikey_mode(&config, limits, AppConfig::default())
            .await;
    let key = server.managed_key.as_ref().expect("managed test key");

    // When: routing selects an upstream but limit reservation rejects before dispatch.
    let response = common::http_post_with_api_key(
        server.proxy_addr,
        "/v1/messages",
        &key.plaintext,
        MESSAGE_BODY,
        &[("request-id", "capture-limit-reject")],
    )
    .await
    .expect("post limit-reject request");
    assert_eq!(response.status, 429);
    server.graceful_shutdown();

    // Then: the row records the routed limit disposition with every usage column null.
    let pool = open_capture_pool(&capture_path).await;
    let row = joined_row(&pool, "capture-limit-reject").await;
    assert_eq!(row.disposition, "routed_limit_rejected");
    assert_eq!(row.client_status, Some(429));
    assert_eq!(row.attempt_num, Some(0));
    assert_eq!(row.input_tokens, None);
    assert_eq!(row.output_tokens, None);
    assert_eq!(row.cache_read_input_tokens, None);
    assert_eq!(row.cache_creation_5m, None);
    assert_eq!(row.cache_creation_1h, None);
}

#[tokio::test]
async fn client_disconnect_during_slow_stream_captures_499_partial_row() {
    // Given: the fake upstream emits a deliberately slow streaming response.
    let directory = tempfile::tempdir().expect("create disconnect capture directory");
    let capture_path = directory.path().join("capture.sqlite");
    let config = capture_config(&capture_path, 1_024);
    let fake_config = AppConfig {
        slow_mode_bps: 512,
        ..AppConfig::default()
    };
    let mut server = common::spawn_capture_test_server_with_fake_config(&config, fake_config).await;

    // When: the client closes only after receiving the upstream message_start frame.
    disconnect_after_message_start(server.proxy_addr, "capture-client-disconnect")
        .await
        .expect("disconnect slow streaming client");
    server.graceful_shutdown();

    // Then: downstream drop handling finalizes one partial row as status 499.
    let pool = open_capture_pool(&capture_path).await;
    let row = joined_row(&pool, "capture-client-disconnect").await;
    assert_eq!(row.disposition, "routed_client_disconnected");
    assert_eq!(row.client_status, Some(499));
    assert_eq!(row.attempt_num, Some(1));
}
