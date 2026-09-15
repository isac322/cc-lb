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
