use cc_lb_engine::BreakerRegistry;

#[test]
fn lifecycle_request_events_current_registry_smoke() {
    let _registry = BreakerRegistry::new();
}
