#[path = "admin_security_headers.rs"]
mod admin_security_headers;
#[path = "admin_separate_listener.rs"]
mod admin_separate_listener;
#[path = "api_wildcard_forwarding.rs"]
mod api_wildcard_forwarding;
#[path = "backend_kind_mismatch_fatal.rs"]
mod backend_kind_mismatch_fatal;
#[path = "backward_compat_observe.rs"]
mod backward_compat_observe;
#[path = "bad_postgres_url_fatal.rs"]
mod bad_postgres_url_fatal;
#[path = "build_metadata_present.rs"]
mod build_metadata_present;
#[path = "builtin_authn_smoke.rs"]
mod builtin_authn_smoke;
#[path = "cache_keepalive_server_wiring.rs"]
mod cache_keepalive_server_wiring;
#[path = "cache_memory_bound.rs"]
mod cache_memory_bound;
#[cfg(feature = "capture")]
#[path = "capture_dispositions.rs"]
mod capture_dispositions;
#[cfg(feature = "capture")]
#[path = "capture_e2e.rs"]
mod capture_e2e;
#[cfg(feature = "capture")]
#[path = "capture_isolation.rs"]
mod capture_isolation;
#[cfg(feature = "capture")]
#[path = "capture_matrix.rs"]
mod capture_matrix;
#[cfg(feature = "capture")]
#[path = "capture_matrix_support.rs"]
mod capture_matrix_support;
#[cfg(feature = "capture")]
#[path = "capture_prior_filter.rs"]
mod capture_prior_filter;
#[cfg(feature = "capture")]
#[path = "capture_replay.rs"]
mod capture_replay;
#[path = "capture_zero_cost.rs"]
mod capture_zero_cost;
#[path = "chaos_common.rs"]
mod chaos_common;
#[path = "claude_fable_5_proxy_path.rs"]
mod claude_fable_5_proxy_path;
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
#[path = "doctor_command.rs"]
mod doctor_command;
#[path = "drain_common.rs"]
mod drain_common;
#[path = "drain_finishes_in_flight.rs"]
mod drain_finishes_in_flight;
#[path = "drain_rejects_new.rs"]
mod drain_rejects_new;
#[path = "drain_timeout_force_closes.rs"]
mod drain_timeout_force_closes;
#[path = "drop_pct_50.rs"]
mod drop_pct_50;
#[path = "dynamic_view_rebind.rs"]
mod dynamic_view_rebind;
#[path = "file_watch_debounced.rs"]
mod file_watch_debounced;
#[path = "files_content_explicit_route.rs"]
mod files_content_explicit_route;
#[path = "header_preservation_contract.rs"]
mod header_preservation_contract;
#[path = "healthcheck_common.rs"]
mod healthcheck_common;
#[path = "healthz_always_200.rs"]
mod healthz_always_200;
#[path = "hot_reload_race.rs"]
mod hot_reload_race;
#[path = "invalid_new_config_keeps_old.rs"]
mod invalid_new_config_keeps_old;
#[path = "latency_injection.rs"]
mod latency_injection;
#[path = "load_bad_cert.rs"]
mod load_bad_cert;
#[path = "load_certs_ok.rs"]
mod load_certs_ok;
#[path = "managed_key_multi_instance.rs"]
mod managed_key_multi_instance;
#[path = "metrics_endpoint_reachable.rs"]
mod metrics_endpoint_reachable;
#[path = "middleware_order.rs"]
mod middleware_order;
#[path = "missing_aead_key_fatal.rs"]
mod missing_aead_key_fatal;
#[path = "multi_instance_storage_tail.rs"]
mod multi_instance_storage_tail;
#[path = "multi_instance_truncated_partial.rs"]
mod multi_instance_truncated_partial;
#[path = "multi_route_dispatch.rs"]
mod multi_route_dispatch;
#[path = "notify_listener.rs"]
mod notify_listener;
#[path = "oauth_refresh.rs"]
mod oauth_refresh;
#[path = "oauth_usage_proxy.rs"]
mod oauth_usage_proxy;
#[path = "observation_failure_isolation.rs"]
mod observation_failure_isolation;
#[path = "per_principal_reload_fault_injection.rs"]
mod per_principal_reload_fault_injection;
#[path = "pool_exhaustion.rs"]
mod pool_exhaustion;
#[path = "postgres_full_storage_live.rs"]
mod postgres_full_storage_live;
#[path = "preflight.rs"]
mod preflight;
#[path = "preflight_common.rs"]
mod preflight_common;
#[path = "prompt_cache_live_qa.rs"]
mod prompt_cache_live_qa;
#[path = "prompt_cache_observation_metrics.rs"]
mod prompt_cache_observation_metrics;
#[path = "prompt_cache_shadow_disabled_is_no_op.rs"]
mod prompt_cache_shadow_disabled_is_no_op;
#[path = "proxy_body_limits.rs"]
mod proxy_body_limits;
#[path = "proxy_error_fallbacks.rs"]
mod proxy_error_fallbacks;
#[path = "readyz_503_during_drain.rs"]
mod readyz_503_during_drain;
#[path = "readyz_503_when_no_upstream_ready.rs"]
mod readyz_503_when_no_upstream_ready;
#[path = "reconciliation.rs"]
mod reconciliation;
#[path = "reload_atomic_swap.rs"]
mod reload_atomic_swap;
#[path = "reload_common.rs"]
mod reload_common;
#[path = "reload_evicts_stale_slots.rs"]
mod reload_evicts_stale_slots;
#[path = "restart_required_field_warns.rs"]
mod restart_required_field_warns;
#[path = "restart_required_matrix.rs"]
mod restart_required_matrix;
#[path = "rfc_0002_fix_live_qa.rs"]
mod rfc_0002_fix_live_qa;
#[path = "rst_after_bytes.rs"]
mod rst_after_bytes;
#[path = "scheduler_factory.rs"]
mod scheduler_factory;
#[path = "scheduler_init_hard_fail.rs"]
mod scheduler_init_hard_fail;
#[path = "scheduler_startup_hard_fail.rs"]
mod scheduler_startup_hard_fail;
#[path = "scheduler_startup_no_migration_collision.rs"]
mod scheduler_startup_no_migration_collision;
#[path = "server_starts_and_responds.rs"]
mod server_starts_and_responds;
#[path = "sighup_reloads_quota_defaults.rs"]
mod sighup_reloads_quota_defaults;
#[path = "thinking_budget_service_tier_e2e.rs"]
mod thinking_budget_service_tier_e2e;
#[path = "tls_common.rs"]
mod tls_common;
#[path = "truncate_mid_stream.rs"]
mod truncate_mid_stream;
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
#[path = "validate_fail_missing_tls.rs"]
mod validate_fail_missing_tls;
#[path = "validate_ok_config.rs"]
mod validate_ok_config;
#[path = "version_includes_sha.rs"]
mod version_includes_sha;
#[path = "wasm_host_adversarial_policy.rs"]
mod wasm_host_adversarial_policy;
#[path = "wasm_host_shape.rs"]
mod wasm_host_shape;
