mod common;
mod router_lifecycle_support;

#[path = "assembler_partial_property.rs"]
mod assembler_partial_property;
#[path = "attempt_rail_trybuild.rs"]
mod attempt_rail_trybuild;
#[path = "audit_writer_smoke.rs"]
mod audit_writer_smoke;
#[path = "authn_rail_trybuild.rs"]
mod authn_rail_trybuild;
#[path = "bulkhead_drops_release_permit.rs"]
mod bulkhead_drops_release_permit;
#[path = "cache_hit_metric.rs"]
mod cache_hit_metric;
#[path = "cache_keepalive_proxy_path.rs"]
mod cache_keepalive_proxy_path;
#[path = "cache_ttl_floor_respected.rs"]
mod cache_ttl_floor_respected;
#[path = "case_insensitive.rs"]
mod case_insensitive;
#[path = "circuit_breaker_isolation.rs"]
mod circuit_breaker_isolation;
#[path = "closed_to_open_on_failures.rs"]
mod closed_to_open_on_failures;
#[path = "concurrent_guard_smoke.rs"]
mod concurrent_guard_smoke;
#[path = "half_open_concurrent_only_one_probe.rs"]
mod half_open_concurrent_only_one_probe;
#[path = "half_open_failure_reopens.rs"]
mod half_open_failure_reopens;
#[path = "half_open_success_closes.rs"]
mod half_open_success_closes;
#[path = "key_store_smoke.rs"]
mod key_store_smoke;
#[path = "latency_stages_emit.rs"]
mod latency_stages_emit;
#[path = "lifecycle_401_refresh_fail.rs"]
mod lifecycle_401_refresh_fail;
#[path = "lifecycle_401_refresh_success.rs"]
mod lifecycle_401_refresh_success;
#[path = "lifecycle_cache_keepalive_durable_cancel.rs"]
mod lifecycle_cache_keepalive_durable_cancel;
#[path = "lifecycle_cache_keepalive_noop.rs"]
mod lifecycle_cache_keepalive_noop;
#[path = "lifecycle_candidate_builder.rs"]
mod lifecycle_candidate_builder;
#[path = "lifecycle_client_disconnect.rs"]
mod lifecycle_client_disconnect;
#[path = "lifecycle_event_policy.rs"]
mod lifecycle_event_policy;
#[path = "lifecycle_filter_pipeline.rs"]
mod lifecycle_filter_pipeline;
#[path = "lifecycle_happy.rs"]
mod lifecycle_happy;
#[path = "lifecycle_model_gate.rs"]
mod lifecycle_model_gate;
#[path = "lifecycle_no_candidates.rs"]
mod lifecycle_no_candidates;
#[path = "lifecycle_non_stream_provider_error.rs"]
mod lifecycle_non_stream_provider_error;
#[path = "lifecycle_observation_retry_path.rs"]
mod lifecycle_observation_retry_path;
#[path = "lifecycle_observation_upstream_keyed.rs"]
mod lifecycle_observation_upstream_keyed;
#[path = "lifecycle_oversized_body.rs"]
mod lifecycle_oversized_body;
#[path = "lifecycle_preview_route.rs"]
mod lifecycle_preview_route;
#[path = "lifecycle_request_events.rs"]
mod lifecycle_request_events;
#[path = "lifecycle_response_header_passthrough.rs"]
mod lifecycle_response_header_passthrough;
#[path = "lifecycle_router_drives_credentials.rs"]
mod lifecycle_router_drives_credentials;
#[path = "lifecycle_routing_failure.rs"]
mod lifecycle_routing_failure;
#[path = "lifecycle_terminal.rs"]
mod lifecycle_terminal;
#[path = "limit_engine_smoke.rs"]
mod limit_engine_smoke;
#[path = "load_once_bind_dispatch.rs"]
mod load_once_bind_dispatch;
#[path = "loom_principal_view.rs"]
mod loom_principal_view;
#[path = "no_modification_of_success_body.rs"]
mod no_modification_of_success_body;
#[path = "open_rejects_immediately.rs"]
mod open_rejects_immediately;
#[path = "open_to_half_open_after_timeout.rs"]
mod open_to_half_open_after_timeout;
#[path = "per_upstream_isolation.rs"]
mod per_upstream_isolation;
#[path = "pg_listener_recovery.rs"]
mod pg_listener_recovery;
#[path = "pg_notify_fanout.rs"]
mod pg_notify_fanout;
#[path = "preserve_anthropic_headers.rs"]
mod preserve_anthropic_headers;
#[path = "principal_view_pipeline_cache.rs"]
mod principal_view_pipeline_cache;
#[path = "principal_view_smoke.rs"]
mod principal_view_smoke;
#[path = "prompt_cache_byte_oracle.rs"]
mod prompt_cache_byte_oracle;
#[path = "prompt_cache_routing_component.rs"]
mod prompt_cache_routing_component;
#[path = "prompt_cache_structural_properties.rs"]
mod prompt_cache_structural_properties;
#[path = "queue_full_returns_503.rs"]
mod queue_full_returns_503;
#[path = "quota_header_surface_baseline.rs"]
mod quota_header_surface_baseline;
#[path = "renewal_accounting_e2e.rs"]
mod renewal_accounting_e2e;
#[path = "request_context.rs"]
mod request_context;
#[path = "request_context_proxy_parity.rs"]
mod request_context_proxy_parity;
#[path = "resolves_anthropic_host.rs"]
mod resolves_anthropic_host;
#[path = "response_observation_regressions.rs"]
mod response_observation_regressions;
#[path = "response_transform_paths.rs"]
mod response_transform_paths;
#[path = "rfc_0002_fix_live_qa.rs"]
mod rfc_0002_fix_live_qa;
#[path = "semaphore_bounds_concurrency.rs"]
mod semaphore_bounds_concurrency;
#[path = "snapshot_error_responses.rs"]
mod snapshot_error_responses;
#[path = "storage_tail_poller.rs"]
mod storage_tail_poller;
#[path = "strip_connection_listed.rs"]
mod strip_connection_listed;
#[path = "strip_well_known.rs"]
mod strip_well_known;
#[path = "subscription_preference_qa_regression.rs"]
mod subscription_preference_qa_regression;
#[path = "subscription_preference_v11_preview.rs"]
mod subscription_preference_v11_preview;
#[path = "subscription_preference_v8_regression.rs"]
mod subscription_preference_v8_regression;
#[path = "subscription_quota_checkpoint_writer.rs"]
mod subscription_quota_checkpoint_writer;
#[path = "task_23_no_hop_by_hop.rs"]
mod task_23_no_hop_by_hop;
#[path = "tower_layer_round_trip.rs"]
mod tower_layer_round_trip;
#[path = "upstream_affinity.rs"]
mod upstream_affinity;
#[path = "upstream_rate_limit_cache.rs"]
mod upstream_rate_limit_cache;
