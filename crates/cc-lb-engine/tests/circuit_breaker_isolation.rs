use cc_lb_engine::BreakerRegistry;

#[test]
fn circuit_breaker_isolation_current_registry_smoke() {
    let _registry = BreakerRegistry::new();
}
