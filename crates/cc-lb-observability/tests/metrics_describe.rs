use cc_lb_observability::{MetricKind, metric_definitions, register_metrics};

#[test]
fn describes_all_required_metrics() {
    register_metrics();

    let definitions = metric_definitions();
    let names = definitions
        .iter()
        .map(|definition| definition.name)
        .collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "cc_lb_requests_total",
            "cc_lb_request_duration_seconds",
            "cc_lb_oauth_refresh_total",
            "cc_lb_quota_active_principals_total",
            "cc_lb_quota_rejected_total",
            "cc_lb_dropped_events_total",
            "cc_lb_circuit_breaker_state",
            "cc_lb_panic_total",
            "cc_lb_drain_in_progress",
            "cc_lb_drain_force_closed_total",
            "cc_lb_config_reload_total",
            "cc_lb_config_reload_failed_total",
            "cc_lb_tls_reload_total",
            "cc_lb_sse_events_total",
            "cc_lb_extism_call_duration_seconds",
            "cc_lb_tokens_total",
            "cc_lb_virtual_cost_usd_total",
            "cclb_api_key_requests_total",
            "cclb_api_key_tokens_total",
            "cclb_api_key_cost_usd_micro_total",
            "cclb_api_key_concurrent",
            "cclb_limit_hits_total",
            "cclb_price_catalog_refresh_failures_total",
            "cclb_price_catalog_missing_field_total",
            "cclb_price_catalog_validation_failures_total",
            "cclb_usage_writer_dropped_total",
            "cclb_limit_state_writer_dropped_total",
            "cclb_audit_writer_dropped_total",
            "cclb_key_auth_failures_total",
            "cclb_concurrent_rejects_total",
            "cclb_streaming_usage_missing_total",
            "cclb_rebind_total",
            "cclb_rebind_duration_seconds",
            "cclb_oauth_refresh_total",
            "cclb_oauth_refresh_duration_seconds",
            "cclb_oauth_refresh_lag_seconds",
            "cclb_wasm_cache_materialize_total",
            "cclb_reconcile_total",
            "cclb_notify_received_total",
        ]
    );

    assert_eq!(definitions.len(), 39);
    assert_eq!(definitions[0].kind, MetricKind::Counter);
    assert_eq!(definitions[1].kind, MetricKind::Histogram);
    assert_eq!(definitions[3].kind, MetricKind::Gauge);

    for name in names {
        println!("OK {name}");
    }
}
