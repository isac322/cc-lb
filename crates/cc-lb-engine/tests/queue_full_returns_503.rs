use cc_lb_engine::BulkheadRegistry;

#[test]
fn queue_full_returns_503_current_bulkhead_registry_smoke() {
    let _registry = BulkheadRegistry::new();
}
