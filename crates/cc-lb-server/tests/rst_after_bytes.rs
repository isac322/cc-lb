#![cfg(any())]

mod chaos_common;

#[tokio::test]
async fn rst_after_bytes() {
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_LATENCY_MS", "0") };
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_DROP_PCT", "0") };
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_RST_AFTER_BYTES", "128") };
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS", "0") };

    let router = chaos_common::start_router(chaos_common::default_fake_config()).await;
    let body = "x".repeat(4096);
    let bytes = chaos_common::raw_stream_post(router.proxy_addr, "/v1/files", &body, &[])
        .await
        .expect("file response");
    let body = chaos_common::body_bytes(&bytes);

    assert!(body.len() < 256, "truncated body len={}", body.len());
}
