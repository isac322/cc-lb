use crate::common;

#[tokio::test]
async fn server_starts_and_responds() {
    let server = common::spawn_test_server().await;
    let response = common::http_post(server.proxy_addr, "/v1/messages", &server.managed_key.plaintext, r#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#, &[])
    .await
    .expect("post messages");

    assert_eq!(response.status, 200);
    assert!(response.body.contains(r#""type":"message""#));
}

#[tokio::test]
async fn responses_http_path_remains_an_http_regression() {
    let server = common::spawn_test_server().await;
    let response = common::http_post(
        server.proxy_addr,
        "/v1/responses",
        &server.managed_key.plaintext,
        r#"{"model":"gpt-5","input":"hello"}"#,
        &[],
    )
    .await
    .expect("post responses");

    assert_eq!(response.status, 503);
    assert!(response.body.contains(r#""error":{"#));
    assert!(!response.body.contains(r#""type":"not_found_error""#));
}
