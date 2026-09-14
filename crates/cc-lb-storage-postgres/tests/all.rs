#![allow(non_snake_case)]

#[path = "../../cc-lb-storage-conformance/src/postgres_fixture.rs"]
mod postgres_fixture;

#[path = "audit.rs"]
mod audit;
#[path = "migration_0037_warmup.rs"]
mod migration_0037_warmup;
#[path = "api_key_usage.rs"]
mod t3_postgres__api_key_usage;
#[path = "cache_keepalive_session_reads.rs"]
mod t3_postgres__cache_keepalive_session_reads;
#[path = "cache_keepalive_sessions.rs"]
mod t3_postgres__cache_keepalive_sessions;
#[path = "crash_recovery.rs"]
mod t3_postgres__crash_recovery;
#[path = "managed_keys_adapter.rs"]
mod t3_postgres__managed_keys_adapter;
#[path = "migration_0040_wasm_registry_wire_version.rs"]
mod t3_postgres__migration_0040_wasm_registry_wire_version;
#[path = "plan_tier_concurrency.rs"]
mod t3_postgres__plan_tier_concurrency;
#[path = "plugin_registry.rs"]
mod t3_postgres__plugin_registry;
#[path = "principal_terminal_strategy.rs"]
mod t3_postgres__principal_terminal_strategy;
#[path = "prompt_cache_observation.rs"]
mod t3_postgres__prompt_cache_observation;
#[path = "quota_aggregates.rs"]
mod t3_postgres__quota_aggregates;
#[path = "request_event_cost_components.rs"]
mod t3_postgres__request_event_cost_components;
#[path = "request_event_projections.rs"]
mod t3_postgres__request_event_projections;
#[path = "request_events_cursor.rs"]
mod t3_postgres__request_events_cursor;
#[path = "request_events_reasoning_effort.rs"]
mod t3_postgres__request_events_reasoning_effort;
#[path = "request_events_thinking_budget_tokens.rs"]
mod t3_postgres__request_events_thinking_budget_tokens;
#[path = "router_singleton_dropped.rs"]
mod t3_postgres__router_singleton_dropped;
#[path = "upstream_affinity.rs"]
mod t3_postgres__upstream_affinity;
#[path = "hydrate_50k_under_500ms.rs"]
mod tx__hydrate_50k_under_500ms;
