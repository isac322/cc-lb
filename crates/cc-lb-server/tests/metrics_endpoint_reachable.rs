use crate::common;

#[tokio::test]
async fn metrics_endpoint_reachable() {
    let server = common::spawn_test_server().await;
    let _ = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[],"max_tokens":1}"#,
        &[],
    )
    .await
    .expect("prime metrics");
    let metrics = common::http_get(server.metrics_addr, "/metrics")
        .await
        .expect("metrics endpoint");

    assert_eq!(metrics.status, 200);
    // The trace layer records these before the response reaches the client,
    // so they are deterministically present once the request has returned.
    assert!(
        metrics
            .body
            .lines()
            .any(|line| line.starts_with("cc_lb_requests_started_total{")
                && line.contains("status=\"200\""))
    );
    assert!(
        metrics
            .body
            .lines()
            .any(|line| line.starts_with("cc_lb_request_headers_duration_seconds"))
    );
    // The trace layer cannot see attribution, so its series carry no
    // principal, upstream, or model labels (previously all "unknown").
    let trace_layer_series = metrics.body.lines().filter(|line| {
        line.starts_with("cc_lb_requests_started_total")
            || line.starts_with("cc_lb_request_headers_duration_seconds")
            || line.starts_with("cc_lb_response_body_chunks_total")
    });
    for line in trace_layer_series {
        for key in ["principal=", "upstream=", "model="] {
            assert!(!line.contains(key), "unattributable label {key} on {line}");
        }
    }
}
