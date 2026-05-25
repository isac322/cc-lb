use cc_lb_core::{BreakerConfig, BreakerRegistry};
use std::time::Duration;

#[test]
fn circuit_breaker_isolation_current_registry_smoke() {
    let registry = BreakerRegistry::new();
    let breaker = registry.get_or_create(
        "upstream-a",
        BreakerConfig {
            failures_to_open: 2,
            failure_window: Duration::from_secs(30),
            half_open_after: Duration::from_secs(1),
            half_open_max_in_flight: 1,
        },
    );

    assert_eq!(breaker.failure_count(), 0);
}
