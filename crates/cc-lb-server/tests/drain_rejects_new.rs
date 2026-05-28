#![cfg(any())]

mod drain_common;

#[tokio::test]
async fn drain_rejects_new() {
    let app = drain_common::start_router(5, drain_common::default_fake_config()).await;

    app.controller.trigger();
    let response = drain_common::http_post_without_api_key(
        app.proxy_addr,
        "/v1/messages",
        drain_common::STREAM_BODY,
        &[("accept", "text/event-stream")],
    )
    .await
    .expect("drain response");

    assert_eq!(response.status, 503);
    assert!(response.retry_after_is_60());
    assert!(response.body.contains("draining"));
    println!(
        "T28 reject sample status={} retry_after=60 body={}",
        response.status, response.body
    );

    app.server.abort();
}
