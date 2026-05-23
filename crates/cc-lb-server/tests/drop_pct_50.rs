mod chaos_common;

#[tokio::test]
async fn drop_pct_50() {
    // TODO: Audit that the environment access only happens in single-threaded code.
    unsafe { std::env::set_var("CC_LB_CHAOS_LATENCY_MS", "0") };
    // TODO: Audit that the environment access only happens in single-threaded code.
    unsafe { std::env::set_var("CC_LB_CHAOS_DROP_PCT", "50") };
    // TODO: Audit that the environment access only happens in single-threaded code.
    unsafe { std::env::set_var("CC_LB_CHAOS_RST_AFTER_BYTES", "0") };
    // TODO: Audit that the environment access only happens in single-threaded code.
    unsafe { std::env::set_var("CC_LB_CHAOS_TRUNCATE_AFTER_EVENTS", "0") };

    let router = chaos_common::start_router(chaos_common::default_fake_config()).await;
    let mut dropped = 0_u64;
    let mut passed = 0_u64;
    for _ in 0..200 {
        match chaos_common::http_get(router.proxy_addr, "/v1/models").await {
            Ok(response) if response.status == 502 => dropped += 1,
            Ok(response) if response.status == 200 => passed += 1,
            Ok(response) => panic!("unexpected status {}", response.status),
            Err(_) => dropped += 1,
        }
    }

    assert!(
        (60..=140).contains(&dropped),
        "dropped={dropped} passed={passed}"
    );
    assert_eq!(dropped + passed, 200);
}
