#[path = "admin_security_headers.rs"]
mod admin_security_headers;
#[path = "admin_separate_listener.rs"]
mod admin_separate_listener;
#[path = "bad_postgres_url_fatal.rs"]
mod bad_postgres_url_fatal;
#[path = "build_metadata_present.rs"]
mod build_metadata_present;
#[path = "cache_keepalive_server_wiring.rs"]
mod cache_keepalive_server_wiring;
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
#[path = "dynamic_view_rebind.rs"]
mod dynamic_view_rebind;
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
#[path = "proxy_body_limits.rs"]
mod proxy_body_limits;
#[path = "proxy_error_fallbacks.rs"]
mod proxy_error_fallbacks;
#[path = "reconciliation.rs"]
mod reconciliation;
#[path = "reload_atomic_swap.rs"]
mod reload_atomic_swap;
#[path = "rfc_0002_fix_live_qa.rs"]
mod rfc_0002_fix_live_qa;
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
#[path = "thinking_budget_service_tier_e2e.rs"]
mod thinking_budget_service_tier_e2e;
#[path = "tls_common.rs"]
mod tls_common;
#[path = "ulimit_low_warns.rs"]
mod ulimit_low_warns;
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
