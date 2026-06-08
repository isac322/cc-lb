use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{BreakerConfig, BreakerState, CircuitBreaker, TestClock};

#[test]
fn five_failures_within_window_open_breaker() -> Result<(), Box<dyn std::error::Error>> {
    let breaker = CircuitBreaker::with_clock(
        "bedrock",
        BreakerConfig {
            failures_to_open: 5,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(30),
            half_open_max_in_flight: 1,
        },
        Arc::new(TestClock::new_at_secs(100)),
    );

    for attempt in 1..=5 {
        breaker.permit()?.record_failure();
        println!(
            "attempt={attempt} state={:?} failures={}",
            breaker.current_state(),
            breaker.failure_count()
        );
    }

    assert_eq!(breaker.current_state(), BreakerState::Open);
    Ok(())
}
