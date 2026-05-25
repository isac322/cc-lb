use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{BreakerConfig, BreakerState, CircuitBreaker, MockClock};

#[test]
fn half_open_failure_reopens_breaker() -> Result<(), Box<dyn std::error::Error>> {
    let clock = Arc::new(MockClock::new(100));
    let breaker = CircuitBreaker::with_clock(
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
    clock.advance(Duration::from_secs(6));

    let permit = breaker.permit()?;
    assert!(permit.is_half_open());
    permit.record_failure();

    assert_eq!(breaker.current_state(), BreakerState::Open);
    assert_eq!(breaker.half_open_in_flight(), 0);
    println!(
        "state_after_half_open_failure={:?}",
        breaker.current_state()
    );
    Ok(())
}
