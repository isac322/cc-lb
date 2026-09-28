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
            "cc_lb_dropped_events_total",
            "cc_lb_circuit_breaker_state",
            "cc_lb_panic_total",
            "cc_lb_drain_in_progress",
            "cc_lb_drain_force_closed_total",
            "cc_lb_tls_reload_total",
            "cc_lb_sse_events_total",
            "cc_lb_plugin_call_duration_seconds",
            "cc_lb_plugin_pool_memories_utilization_ratio",
            "cc_lb_plugin_pool_core_instances_utilization_ratio",
            "cc_lb_plugin_pool_memories_used",
            "cc_lb_plugin_pool_core_instances_used",
            "cc_lb_plugin_pool_memories_total",
            "cc_lb_plugin_pool_core_instances_total",
            "cc_lb_plugin_memory_max_pages",
            "cc_lb_plugin_memory_reservation_bytes",
            "cc_lb_plugin_memory_guard_bytes",
            "cc_lb_plugin_pool_virtual_reservation_bytes",
            "cc_lb_plugin_pool_saturation_total",
            "cc_lb_tokens_total",
            "cc_lb_virtual_cost_usd_total",
            "cc_lb_stream_terminations_total",
            "cclb_api_key_requests_total",
            "cclb_api_key_tokens_total",
            "cclb_api_key_cost_usd_micro_total",
            "cclb_api_key_concurrent",
            "cclb_limit_hits_total",
            "cclb_price_catalog_missing_field_total",
            "cclb_audit_writer_dropped_total",
            "cclb_key_auth_failures_total",
            "cclb_concurrent_rejects_total",
            "cc_lb_cache_hit_total",
            "cc_lb_cache_miss_total",
            "cc_lb_cache_observation_dropped_total",
            "cc_lb_cache_observation_write_failed_total",
            "cc_lb_prompt_cache_analysis_duration_seconds",
            "cc_lb_prompt_cache_token_count_cache_total",
            "cc_lb_prompt_cache_tokenizer_inflight",
            "cc_lb_prompt_cache_tokenized_bytes_total",
            "cc_lb_prompt_cache_tokenized_tokens_total",
            "cc_lb_prompt_cache_tokenizer_fallback_prefixes_total",
            "cc_lb_prompt_cache_analysis_worker_failed_total",
        ]
    );

    assert_eq!(definitions.len(), 44);
    assert_eq!(definitions[0].kind, MetricKind::Counter);
    assert_eq!(definitions[1].kind, MetricKind::Histogram);
    assert_eq!(definitions[3].kind, MetricKind::Gauge);

    for name in names {
        println!("OK {name}");
    }
}
