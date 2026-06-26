#![forbid(unsafe_code)]

// ## Exit Code Reference (for runbook/operators)
//
// 1: generic fatal / unhandled error
// 2: startup validation/preflight/storage kind fatal errors

pub(crate) mod admin_plugins;
pub(crate) mod admin_security;
pub mod app;
pub mod bootstrap;
pub mod build_meta;
pub mod builtins;
pub mod chaos;
pub mod cli;
pub mod drain;
pub mod dynamic_view_builder;
pub mod notify_listener;
pub mod preflight;
pub mod prompt_cache_observation_cache;
pub mod prompt_cache_observation_sink;
pub mod reconcile;
pub mod refresh;
pub mod reload;
pub mod replica;
pub(crate) mod revision_hash;
pub(crate) mod scheduler_dispatch;
pub mod scheduler_factory;
pub mod signal;
pub mod startup_handshake;
pub mod state_machine;
pub mod storage_factory;
pub mod subscription_quota_cache;
pub mod tls;
pub mod validate;
pub mod version;
pub mod warmup;

pub use app::{App, BuildError, build_app, build_app_with_path, run_serve};
#[cfg(any(feature = "sqlite", feature = "postgres"))]
pub use scheduler_factory::{
    LeaderConnectionHandle, OpenedScheduler, SchedulerBackend, SchedulerFactoryError,
    open_scheduler_storage,
};
pub use subscription_quota_cache::{MergedQuotaSnapshot, MergedSource, SubscriptionQuotaCache};
