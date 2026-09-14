use crate::common;

#[tokio::test]
async fn middleware_order() {
    let server = common::spawn_test_server().await;
    let body = r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"this body is intentionally larger than the configured cap because it repeats text text text text text text text text text text text text text text text text text text text text text text text text"}],"max_tokens":1}"#;
    let response = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        body,
        &[("Connection", "X-Hop-Test"), ("X-Hop-Test", "strip-me")],
    )
    .await
    .expect("oversized post");

    assert_eq!(response.status, 413);
    assert!(!response.headers.to_ascii_lowercase().contains("x-hop-test"));
}
