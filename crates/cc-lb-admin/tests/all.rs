#[path = "admin_test_common.rs"]
mod admin_test_common;
#[path = "admin_warmup_endpoints.rs"]
mod admin_warmup_endpoints;
#[path = "audit_pagination.rs"]
mod audit_pagination;
#[path = "auth_required.rs"]
mod auth_required;
#[path = "config_admin_common/mod.rs"]
mod config_admin_common;
#[path = "config_apply.rs"]
mod config_apply;
#[path = "config_diff.rs"]
mod config_diff;
#[path = "config_draft.rs"]
mod config_draft;
#[path = "config_draft_persists.rs"]
mod config_draft_persists;
#[path = "config_history.rs"]
mod config_history;
#[path = "config_schema.rs"]
mod config_schema;
#[path = "config_validate.rs"]
mod config_validate;
#[path = "credentials.rs"]
mod credentials;
#[path = "dashboard_summary.rs"]
mod dashboard_summary;
#[path = "dashboard_usage.rs"]
mod dashboard_usage;
#[path = "disable_enable_key.rs"]
mod disable_enable_key;
#[path = "e2e_pkce_enrollment.rs"]
mod e2e_pkce_enrollment;
#[path = "events_delta_rest.rs"]
mod events_delta_rest;
#[path = "events_recent.rs"]
mod events_recent;
#[path = "events_request_log_contracts.rs"]
mod events_request_log_contracts;
#[path = "events_stream.rs"]
mod events_stream;
#[path = "export_supported_slots.rs"]
mod export_supported_slots;
#[path = "insert_chain_validates_wire_version.rs"]
mod insert_chain_validates_wire_version;
#[path = "internal_partials.rs"]
mod internal_partials;
#[path = "killswitch_persists.rs"]
mod killswitch_persists;
#[path = "new_admin_modules_smoke.rs"]
mod new_admin_modules_smoke;
#[path = "oauth_no_authn_fallback.rs"]
mod oauth_no_authn_fallback;
#[path = "principal_crud.rs"]
mod principal_crud;
#[path = "principal_keys.rs"]
mod principal_keys;
#[path = "principal_limits.rs"]
mod principal_limits;
#[path = "principal_usage.rs"]
mod principal_usage;
#[path = "principals_cache_keepalive_roundtrip.rs"]
mod principals_cache_keepalive_roundtrip;
#[path = "reorder_rebalance_revalidates_slot.rs"]
mod reorder_rebalance_revalidates_slot;
#[path = "scheduler_admin.rs"]
mod scheduler_admin;
#[path = "snapshot_audit_query.rs"]
mod snapshot_audit_query;
#[path = "snapshot_health.rs"]
mod snapshot_health;
#[path = "static_assets_cache.rs"]
mod static_assets_cache;
#[path = "static_assets_served.rs"]
mod static_assets_served;
#[path = "status.rs"]
mod status;
#[path = "subscription_quotas.rs"]
mod subscription_quotas;
#[path = "upstream_health.rs"]
mod upstream_health;
#[path = "v1_oauth.rs"]
mod v1_oauth;
#[path = "v1_plugins.rs"]
mod v1_plugins;
#[path = "v1_principals.rs"]
mod v1_principals;
#[path = "v1_router_preview.rs"]
mod v1_router_preview;
#[path = "v1_router_terminal.rs"]
mod v1_router_terminal;
#[path = "v1_status.rs"]
mod v1_status;
#[path = "v1_synchronous_rebind.rs"]
mod v1_synchronous_rebind;
#[path = "v1_upstreams.rs"]
mod v1_upstreams;
#[path = "wasm_upload.rs"]
mod wasm_upload;
