use cc_lb_observability::{metric_definitions, register_metrics, MetricKind};

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
        ]
    );

    assert_eq!(definitions.len(), 15);
    assert_eq!(definitions[0].kind, MetricKind::Counter);
    assert_eq!(definitions[1].kind, MetricKind::Histogram);
    assert_eq!(definitions[3].kind, MetricKind::Gauge);

    for name in names {
        println!("OK {name}");
    }
}
