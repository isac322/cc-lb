use crate::common;

#[tokio::test]
async fn multi_route_dispatch() {
    let server = common::spawn_test_server().await;
    let messages = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        &server.managed_key.plaintext,
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[],"max_tokens":1}"#,
        &[],
    )
    .await
    .expect("messages route");
    let models = common::proxy_get(
        server.proxy_addr,
        "/v1/models",
        &server.managed_key.plaintext,
    )
    .await
    .expect("models route");
    let files = common::proxy_get(
        server.proxy_addr,
        "/v1/files",
        &server.managed_key.plaintext,
    )
    .await
    .expect("files route");
    let delete_file = common::http_delete(
        server.proxy_addr,
        "/v1/files/file_abc123",
        &server.managed_key.plaintext,
    )
    .await
    .expect("delete file route");
    let count_tokens = common::http_post(
        server.proxy_addr,
        "/v1/messages/count_tokens",
        &server.managed_key.plaintext,
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[]}"#,
        &[],
    )
    .await
    .expect("count tokens route");
    let unknown = common::http_post(
        server.proxy_addr,
        "/v1/unknown",
        &server.managed_key.plaintext,
        "{}",
        &[],
    )
    .await
    .expect("unknown route");

    assert_eq!(unknown.status, 404);
    assert!(unknown.body.contains(r#""type":"operation_not_supported""#));

    assert_eq!(messages.status, 200);
    assert!(messages.body.contains(r#""type":"message""#));
    for response in [models, files, delete_file, count_tokens] {
        assert_eq!(response.status, 404);
        assert!(
            response
                .body
                .contains(r#""type":"operation_not_supported""#)
        );
    }
}
