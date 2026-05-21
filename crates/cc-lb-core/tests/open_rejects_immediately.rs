use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use cc_lb_core::{BreakerConfig, BreakerError, CircuitBreaker, MockClock};

#[test]
fn open_breaker_returns_error_without_calling_upstream() -> Result<(), Box<dyn std::error::Error>> {
    let breaker = CircuitBreaker::with_clock(
        "bedrock",
        BreakerConfig {
            failures_to_open: 1,
            failure_window: Duration::from_secs(10),
            half_open_after: Duration::from_secs(30),
            half_open_max_in_flight: 1,
        },
        Arc::new(MockClock::new(100)),
    );
    breaker.permit()?.record_failure();

    let upstream_called = AtomicBool::new(false);
    let upstream = || {
        upstream_called.store(true, Ordering::SeqCst);
        panic!("upstream should not be called while breaker is open");
    };

    match breaker.permit() {
        Ok(permit) => {
            upstream();
            permit.record_success();
        }
        Err(BreakerError::Open { retry_after }) => {
            println!("open_rejected retry_after_secs={}", retry_after.as_secs());
        }
        Err(source) => return Err(Box::new(source)),
    }

    assert!(!upstream_called.load(Ordering::SeqCst));
    Ok(())
}
