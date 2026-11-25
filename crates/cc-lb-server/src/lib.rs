#![forbid(unsafe_code)]

// ## Exit Code Reference (for runbook/operators)
//
// 1: generic fatal / unhandled error
// 2: startup validation/preflight/storage kind fatal errors

#[cfg(feature = "admin")]
pub(crate) mod admin_ports;
pub mod app;
pub mod build_meta;
pub(crate) mod cache_keepalive_enqueuer;
pub(crate) mod cache_keepalive_payload;
pub mod chaos;
pub mod cli;
pub mod dynamic_view_builder;
pub mod notify_listener;
mod plugin_manifest;
pub mod preflight;
pub mod prompt_cache_observation_sink;
pub mod reconcile;
pub mod refresh;
pub use cc_lb_runtime_sync::replica;
pub(crate) mod revision_hash;
pub(crate) mod scheduler_dispatch;
#[cfg(any(feature = "storage-sqlite", feature = "storage-postgres"))]
pub use cc_lb_scheduler::factory as scheduler_factory;
pub mod signal;
pub mod state_machine;
pub use cc_lb_runtime_sync::storage_factory;
pub mod subscription_quota_cache;
pub mod tls;
pub mod validate;
pub mod version;
pub(crate) mod vertex_status;
pub(crate) mod vertex_transport;
pub mod warmup;
pub mod wasm_host;

#[cfg(feature = "admin")]
pub use app::{App, build_app, build_app_with_path, run_serve};
pub use app::{
    BuildError, ProxyApp, ProxyRole, ProxyServeOptions, ServeError, build_proxy_app_with_storage,
    run_proxy_serve, serve_error_exit_code,
};
pub(crate) use plugin_manifest::PluginManifest;
#[cfg(any(feature = "storage-sqlite", feature = "storage-postgres"))]
pub use scheduler_factory::{
    OpenedScheduler, SchedulerBackend, SchedulerFactoryError, SchedulerPoolRole,
    open_scheduler_storage, open_scheduler_storage_with_role,
};
pub use subscription_quota_cache::SubscriptionQuotaCache;
