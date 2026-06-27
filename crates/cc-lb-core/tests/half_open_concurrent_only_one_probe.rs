use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{BreakerConfig, BreakerError, CircuitBreaker, TestClock};
use tokio::sync::Barrier;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_open_allows_only_one_concurrent_probe() -> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TestClock::new_at_secs(100));
    let breaker = CircuitBreaker::new(
        "bedrock",
        BreakerConfig {
            failures_to_open: 1,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(5),
            half_open_max_in_flight: 1,
        },
        clock.clone(),
    );
    breaker.permit()?.record_failure();
    clock.advance_secs(6);

    let barrier = Arc::new(Barrier::new(10));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let breaker = breaker.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            breaker.permit()
        });
    }

    let mut permits = Vec::new();
    let mut half_open_full = 0_u32;
    while let Some(result) = tasks.join_next().await {
        match result? {
            Ok(permit) => {
                assert!(permit.is_half_open());
                permits.push(permit);
            }
            Err(BreakerError::HalfOpenFull) => half_open_full += 1,
            Err(source) => return Err(Box::new(source) as Box<dyn std::error::Error>),
        }
    }

    let ok = permits.len();
    println!("half_open_ok={ok} half_open_full={half_open_full}");
    assert_eq!(ok, 1);
    assert_eq!(half_open_full, 9);
    Ok(())
}
