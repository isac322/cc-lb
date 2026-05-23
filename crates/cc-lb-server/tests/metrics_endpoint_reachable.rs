mod common;

#[tokio::test]
async fn metrics_endpoint_reachable() {
    let server = common::spawn_test_server().await;
    let _ = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[],"max_tokens":1}"#,
        &[],
    )
    .await
    .expect("prime metrics");
    let metrics = common::http_get(server.metrics_addr, "/metrics")
        .await
        .expect("metrics endpoint");

    assert_eq!(metrics.status, 200);
    assert!(metrics.body.contains("cc_lb_requests_total"));
    assert!(metrics.body.contains("cc_lb_request_duration_seconds"));
    assert!(metrics.body.contains("cc_lb_tokens_total"));
    assert!(metrics.body.contains("cc_lb_virtual_cost_usd_total"));
    assert!(metrics.body.contains("direction=\"input\""));
    assert!(metrics.body.contains("direction=\"output\""));
    assert!(metrics.body.contains("status=\"200\""));
    assert!(
        metrics
            .body
            .lines()
            .any(|line| line.contains("cc_lb_request_duration_seconds")
                && line.contains("status=\"200\""))
    );
}
