#![allow(non_snake_case)]

#[cfg(feature = "postgres")]
async fn required_ci_postgres_url() -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let fixture = cc_lb_storage_conformance::postgres_fixture().await?;
    let database_url = fixture.database_url().to_owned();
    fixture.teardown().await?;
    Ok(database_url)
}

#[path = "api_wildcard_forwarding.rs"]
mod api_wildcard_forwarding;
#[path = "backward_compat_observe.rs"]
mod backward_compat_observe;
#[path = "build_metadata_present.rs"]
mod build_metadata_present;
#[path = "cache_memory_bound.rs"]
mod cache_memory_bound;
#[path = "common.rs"]
mod common;
#[path = "composite_signer_dispatch.rs"]
mod composite_signer_dispatch;
#[path = "composite_signer_factory.rs"]
mod composite_signer_factory;
#[path = "db_unreachable_503.rs"]
mod db_unreachable_503;
#[path = "dispatch_uses_resolved_upstream_base_url.rs"]
mod dispatch_uses_resolved_upstream_base_url;
#[path = "files_content_explicit_route.rs"]
mod files_content_explicit_route;
#[path = "header_preservation_contract.rs"]
mod header_preservation_contract;
#[path = "healthcheck_common.rs"]
mod healthcheck_common;
#[path = "hot_reload_race.rs"]
mod hot_reload_race;
#[path = "managed_key_multi_instance.rs"]
mod managed_key_multi_instance;
#[path = "metrics_endpoint_reachable.rs"]
mod metrics_endpoint_reachable;
#[path = "multi_instance_storage_tail.rs"]
mod multi_instance_storage_tail;
#[path = "multi_instance_truncated_partial.rs"]
mod multi_instance_truncated_partial;
#[path = "oauth_refresh.rs"]
mod oauth_refresh;
#[path = "observation_failure_isolation.rs"]
mod observation_failure_isolation;
#[path = "preflight.rs"]
mod preflight;
#[path = "preflight_common.rs"]
mod preflight_common;
#[path = "prompt_cache_observation_metrics.rs"]
mod prompt_cache_observation_metrics;
#[path = "reload_common.rs"]
mod reload_common;
#[path = "reload_evicts_stale_slots.rs"]
mod reload_evicts_stale_slots;
#[path = "restart_required_matrix.rs"]
mod restart_required_matrix;
#[path = "scheduler_factory.rs"]
mod scheduler_factory;
#[path = "scheduler_init_hard_fail.rs"]
mod scheduler_init_hard_fail;
#[path = "scheduler_startup_hard_fail.rs"]
mod scheduler_startup_hard_fail;
#[path = "scheduler_startup_no_migration_collision.rs"]
mod scheduler_startup_no_migration_collision;
#[path = "t2/admin_security_headers.rs"]
mod t2__admin_security_headers;
#[path = "t2/app_dispatch.rs"]
mod t2__app_dispatch;
#[path = "t2/builtin_authn_smoke.rs"]
mod t2__builtin_authn_smoke;
#[path = "t2/cache_keepalive_server_wiring.rs"]
mod t2__cache_keepalive_server_wiring;
#[path = "t2/drain.rs"]
mod t2__drain;
#[path = "t2/dynamic_view_rebind.rs"]
mod t2__dynamic_view_rebind;
#[path = "t2/healthz.rs"]
mod t2__healthz;
#[path = "t2/middleware_order.rs"]
mod t2__middleware_order;
#[path = "t2/multi_route_dispatch.rs"]
mod t2__multi_route_dispatch;
#[path = "t2/notify_listener.rs"]
mod t2__notify_listener;
#[path = "t2/oauth_usage_proxy.rs"]
mod t2__oauth_usage_proxy;
#[path = "t2/proxy_body_limits.rs"]
mod t2__proxy_body_limits;
#[path = "t2/proxy_error_fallbacks.rs"]
mod t2__proxy_error_fallbacks;
#[path = "t2/readyz_503_during_drain.rs"]
mod t2__readyz_503_during_drain;
#[path = "t2/readyz_503_when_no_upstream_ready.rs"]
mod t2__readyz_503_when_no_upstream_ready;
#[path = "t2/reconciliation.rs"]
mod t2__reconciliation;
#[path = "t2/rfc_0002_terminal_mapping.rs"]
mod t2__rfc_0002_terminal_mapping;
#[path = "t3/file_watch_debounced.rs"]
mod t3__file_watch_debounced;
#[path = "t3/invalid_new_config_keeps_old.rs"]
mod t3__invalid_new_config_keeps_old;
#[path = "t3/load_bad_cert.rs"]
mod t3__load_bad_cert;
#[path = "t3/load_certs_path.rs"]
mod t3__load_certs_path;
#[path = "t3/per_principal_reload_fault_injection.rs"]
mod t3__per_principal_reload_fault_injection;
#[path = "t3/reload_atomic_swap.rs"]
mod t3__reload_atomic_swap;
#[path = "t3/restart_required_field_warns.rs"]
mod t3__restart_required_field_warns;
#[path = "t3/sighup_reloads_quota_defaults.rs"]
mod t3__sighup_reloads_quota_defaults;
#[path = "t3/wasm_filter_output_bound.rs"]
mod t3__wasm_filter_output_bound;
#[path = "e2e/t5/process/mod.rs"]
pub(crate) mod t5__process;
#[path = "thinking_budget_service_tier_e2e.rs"]
mod thinking_budget_service_tier_e2e;
#[path = "tls_common.rs"]
mod tls_common;
#[path = "tls_parse.rs"]
mod tls_parse;
#[path = "ulimit_low_warns.rs"]
mod ulimit_low_warns;
#[path = "upstream_probe_warn_only.rs"]
mod upstream_probe_warn_only;
#[path = "upstream_rate_limit_persisted_end_to_end.rs"]
mod upstream_rate_limit_persisted_end_to_end;
#[path = "upstream_warmup_attempts_persist.rs"]
mod upstream_warmup_attempts_persist;
#[path = "upstream_warmup_logshape.rs"]
mod upstream_warmup_logshape;
#[path = "version_includes_sha.rs"]
mod version_includes_sha;
#[path = "wasm_host_adversarial_policy.rs"]
mod wasm_host_adversarial_policy;
#[path = "wasm_host_shape.rs"]
mod wasm_host_shape;
