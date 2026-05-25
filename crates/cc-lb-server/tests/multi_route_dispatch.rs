mod common;

#[tokio::test]
async fn multi_route_dispatch() {
    let server = common::spawn_test_server().await;
    let messages = common::http_post(
        server.proxy_addr,
        "/v1/messages",
        r#"{"model":"claude-3-5-sonnet-20241022","messages":[],"max_tokens":1}"#,
        &[],
    )
    .await
    .expect("messages route");
    let models = common::http_get(server.proxy_addr, "/v1/models")
        .await
        .expect("models route");
    let files = common::http_get(server.proxy_addr, "/v1/files")
        .await
        .expect("files route");
    let delete_file = common::http_delete(server.proxy_addr, "/v1/files/file_abc123")
        .await
        .expect("delete file route");

    assert_eq!(messages.status, 200);
    assert!(messages.body.contains(r#""type":"message""#));
    assert_eq!(models.status, 200);
    assert!(models.body.contains(r#""type":"list""#));
    assert_eq!(files.status, 200);
    assert!(files.body.contains(r#""type":"list""#));
    assert_eq!(delete_file.status, 200);
    assert!(delete_file.body.contains(r#""deleted""#));
}
