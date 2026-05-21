mod chaos_common;

#[tokio::test]
async fn truncate_mid_stream() {
    std::env::set_var("CC_LB_CHAOS_LATENCY_MS", "0");
    std::env::set_var("CC_LB_CHAOS_DROP_PCT", "0");
    std::env::set_var("CC_LB_CHAOS_RST_AFTER_BYTES", "0");
    std::env::set_var("CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS", "2");

    let router = chaos_common::start_router(chaos_common::default_fake_config()).await;
    let bytes = chaos_common::raw_stream_post(
        router.proxy_addr,
        "/v1/messages",
        chaos_common::STREAM_BODY,
        &[("accept", "text/event-stream")],
    )
    .await
    .expect("stream response");
    let body = String::from_utf8_lossy(chaos_common::body_bytes(&bytes));

    assert_eq!(chaos_common::count_sse_data_events(&body), 2, "body={body}");
    assert!(!body.contains("message_stop"), "body={body}");
}
