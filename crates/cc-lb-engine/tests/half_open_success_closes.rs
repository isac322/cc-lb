use std::sync::Arc;
use std::time::Duration;

use cc_lb_clock::TestClock;
use cc_lb_engine::{BreakerRuntimeConfig, BreakerState, CircuitBreaker};

#[test]
fn half_open_success_closes_breaker() -> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(TestClock::new_at_secs(100));
    let breaker = CircuitBreaker::new(
        "bedrock",
        BreakerRuntimeConfig {
            failures_to_open: 1,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(5),
            half_open_max_in_flight: 1,
        },
        clock.clone(),
    );
    breaker.permit()?.record_failure();
    clock.advance_secs(6);

    let permit = breaker.permit()?;
    assert!(permit.is_half_open());
    permit.record_success();

    assert_eq!(breaker.current_state(), BreakerState::Closed);
    assert_eq!(breaker.failure_count(), 0);
    assert_eq!(breaker.half_open_in_flight(), 0);
    println!("state_after_success={:?}", breaker.current_state());
    Ok(())
}
