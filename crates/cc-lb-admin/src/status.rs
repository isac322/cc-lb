use std::collections::BTreeMap;

use axum::{Json, extract::State};
use cc_lb_config::Config;
use cc_lb_core::{BreakerRegistry, BulkheadRegistry, DrainController};
use cc_lb_storage_api::{RequestEventUpstream, Storage, StorageError};
use serde::Serialize;

use crate::{AdminState, LastReloadStatus};

#[derive(Debug, Clone, Serialize)]
pub struct UpstreamHealthResponse {
    pub name: String,
    pub kind: RequestEventUpstream,
    pub breaker: BreakerHealth,
    pub bulkhead: BulkheadHealth,
    pub drain: DrainHealth,
    pub killswitch: bool,
    pub last_probe_unix_secs: Option<u64>,
    pub error_count_recent: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BreakerHealth {
    pub state: &'static str,
    pub failure_count: u32,
    pub half_open_in_flight: u32,
    pub observed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BulkheadHealth {
    pub max_conns: u32,
    pub available_permits: u32,
    pub observed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DrainHealth {
    pub draining: bool,
    pub in_flight: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginsStatusResponse {
    pub plugins: Vec<PluginStatusEntry>,
    pub principals: BTreeMap<String, PrincipalPluginsStatus>,
    pub last_reload_status: Option<LastReloadStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrincipalPluginsStatus {
    pub router_plugin: Option<PluginRefRedacted>,
    pub observability_hooks: Vec<PluginRefRedacted>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginRefRedacted {
    pub name: String,
    pub wasm_path: Option<String>,
    pub config_hash: String,
}

#[derive(Debug, Clone)]
pub struct PluginRuntimeEntryStatus {
    pub loaded: bool,
    pub disabled: bool,
    pub failure_count: u64,
    pub last_error: Option<String>,
}

pub trait PluginRuntimeStatus {
    fn plugin_status(&self, name: &str) -> Option<PluginRuntimeEntryStatus>;
}

#[derive(Debug, Clone, Serialize)]
pub struct PluginStatusEntry {
    pub slot: &'static str,
    pub name: String,
    pub wasm_path: String,
    pub loaded: bool,
    pub disabled: bool,
    pub failure_count: u64,
    pub last_error: Option<String>,
    pub sse_per_event: Option<bool>,
    pub batched_events_per_flush: Option<u64>,
    pub batched_flush_ms: Option<u64>,
}

pub async fn handler(State(state): State<AdminState>) -> Json<PluginsStatusResponse> {
    let config = state.config.current_config();
    Json(build_status_response(
        &config,
        None,
        state.config.last_reload_status(),
    ))
}

#[derive(Debug)]
pub enum StatusBuildError {
    UnknownUpstream,
    Storage(StorageError),
    Crypto,
    Json,
}

pub async fn build_upstream_health(
    _storage: &dyn Storage,
    _config: &Config,
    _breaker_registry: Option<&BreakerRegistry>,
    _bulkhead_registry: Option<&BulkheadRegistry>,
    _drain_controller: Option<&DrainController>,
    _upstream_name: &str,
    _now_unix_secs: u64,
) -> Result<UpstreamHealthResponse, StatusBuildError> {
    Err(StatusBuildError::UnknownUpstream)
}

pub fn build_plugins_status(
    config: &Config,
    runtime_status: Option<&dyn PluginRuntimeStatus>,
) -> PluginsStatusResponse {
    build_status_response(config, runtime_status, None)
}

pub fn build_status_response(
    _config: &Config,
    _runtime_status: Option<&dyn PluginRuntimeStatus>,
    last_reload_status: Option<LastReloadStatus>,
) -> PluginsStatusResponse {
    PluginsStatusResponse {
        plugins: Vec::new(),
        principals: BTreeMap::new(),
        last_reload_status,
    }
}

impl StatusBuildError {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UnknownUpstream => "unknown_upstream",
            Self::Storage(_) => "storage_error",
            Self::Crypto => "crypto_error",
            Self::Json => "json_error",
        }
    }
}

impl From<StorageError> for StatusBuildError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
