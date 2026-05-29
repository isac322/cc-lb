#![cfg(any())]

mod chaos_common;

use std::time::{Duration, Instant};

#[tokio::test]
async fn latency_injection() {
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_LATENCY_MS", "500") };
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_DROP_PCT", "0") };
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_RST_AFTER_BYTES", "0") };
    // SAFETY: test-only; single-threaded test runner, no concurrent env access
    unsafe { std::env::set_var("CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS", "0") };

    let router = chaos_common::start_router(chaos_common::default_fake_config()).await;
    let started = Instant::now();
    let response = chaos_common::http_get(router.proxy_addr, "/v1/models")
        .await
        .expect("models response");
    let elapsed = started.elapsed();

    assert_eq!(response.status, 200);
    assert!(elapsed >= Duration::from_millis(450), "elapsed={elapsed:?}");
    assert!(elapsed < Duration::from_millis(1500), "elapsed={elapsed:?}");
}
