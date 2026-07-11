#![recursion_limit = "512"]

#[cfg(feature = "dto-roundtrip")]
#[path = "dto_roundtrip.rs"]
mod dto_roundtrip;
#[path = "dyn_compat.rs"]
mod dyn_compat;
#[path = "plugin_registry_metadata.rs"]
mod plugin_registry_metadata;
#[path = "principal_terminal_strategy.rs"]
mod principal_terminal_strategy;
#[path = "request_cache_breakpoint_compat.rs"]
mod request_cache_breakpoint_compat;
#[path = "request_event_backward_compat.rs"]
mod request_event_backward_compat;
#[path = "subscription_quota_checkpoint.rs"]
mod subscription_quota_checkpoint;
