use std::collections::BTreeMap;

use axum::{Json, extract::State};
use cc_lb_aead::AeadService;
use cc_lb_config::{Config, PluginRef, UpstreamKind};
use cc_lb_core::{BreakerRegistry, BreakerState, BulkheadRegistry, DrainController};
use cc_lb_storage_api::{
    OAuthCredentials, RequestEventUpstream, Storage, StorageError, UsageRollupResolution,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};

use crate::{AdminState, LastReloadStatus};

const RECENT_ERROR_WINDOW_SECS: u64 = 15 * 60;
const EXPIRING_SOON_SECS: u64 = 300;
const ANTHROPIC_OAUTH_PROVIDER: &str = "anthropic_oauth";

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

#[derive(Debug, Clone, Serialize)]
pub struct OAuthStatusResponse {
    pub credentials: Vec<OAuthCredentialStatus>,
    pub observed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct OAuthCredentialStatus {
    pub principal_id: String,
    pub provider: String,
    pub has_credentials: bool,
    pub expires_at_unix_secs: Option<u64>,
    pub refresh_token_present: bool,
    pub last_updated_unix_secs: Option<u64>,
    pub status: &'static str,
    pub scopes: Vec<String>,
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
    storage: &dyn Storage,
    config: &Config,
    breaker_registry: Option<&BreakerRegistry>,
    bulkhead_registry: Option<&BulkheadRegistry>,
    drain_controller: Option<&DrainController>,
    upstream_name: &str,
    now_unix_secs: u64,
) -> Result<UpstreamHealthResponse, StatusBuildError> {
    let upstream = config
        .upstreams
        .get(upstream_name)
        .ok_or(StatusBuildError::UnknownUpstream)?;
    let kind = request_event_upstream_kind(&upstream.kind);

    Ok(UpstreamHealthResponse {
        name: upstream_name.to_owned(),
        kind,
        breaker: build_breaker_health(breaker_registry, upstream_name),
        bulkhead: build_bulkhead_health(config, bulkhead_registry, upstream_name),
        drain: DrainHealth {
            draining: drain_controller.is_some_and(DrainController::is_draining),
            in_flight: drain_controller
                .map(DrainController::in_flight)
                .map(saturating_u32)
                .unwrap_or(0),
        },
        killswitch: storage.killswitch_enabled().await?,
        last_probe_unix_secs: None,
        error_count_recent: recent_error_count(storage, upstream_name, kind, now_unix_secs).await?,
    })
}

pub fn build_plugins_status(
    config: &Config,
    runtime_status: Option<&dyn PluginRuntimeStatus>,
) -> PluginsStatusResponse {
    build_status_response(config, runtime_status, None)
}

pub fn build_status_response(
    config: &Config,
    runtime_status: Option<&dyn PluginRuntimeStatus>,
    last_reload_status: Option<LastReloadStatus>,
) -> PluginsStatusResponse {
    let mut plugins = Vec::new();
    if let Some(plugin) = &config.plugins.router_plugin {
        plugins.push(plugin_entry("router", plugin, runtime_status));
    }
    for plugin in &config.plugins.observability_hooks {
        plugins.push(plugin_entry("observability", plugin, runtime_status));
    }

    PluginsStatusResponse {
        plugins,
        principals: build_principals_status(config),
        last_reload_status,
    }
}

pub async fn build_oauth_status(
    storage: &dyn Storage,
    aead: &AeadService,
    config: &Config,
    now_unix_secs: u64,
) -> Result<OAuthStatusResponse, StatusBuildError> {
    let mut refs = config
        .principals
        .iter()
        .filter_map(|(principal_id, principal)| {
            principal
                .credentials_ref
                .as_deref()
                .and_then(oauth_provider_from_credentials_ref)
                .map(|provider| (principal_id.clone(), provider))
        })
        .collect::<Vec<_>>();
    refs.sort();

    if refs.is_empty() {
        return Ok(OAuthStatusResponse {
            credentials: Vec::new(),
            observed: false,
        });
    }

    let mut credentials = Vec::with_capacity(refs.len());
    for (principal_id, provider) in refs {
        let stored = match storage
            .get_oauth_ciphertext(&principal_id, &provider)
            .await?
        {
            Some(ciphertext) => Some(decrypt_json::<OAuthCredentials>(
                aead,
                &ciphertext,
                &oauth_aad(&principal_id, &provider),
            )?),
            None => None,
        };
        credentials.push(match stored {
            Some(creds) => OAuthCredentialStatus {
                principal_id,
                provider,
                has_credentials: true,
                expires_at_unix_secs: Some(creds.expires_at),
                refresh_token_present: !creds.refresh_token.is_empty(),
                last_updated_unix_secs: None,
                status: oauth_status_label(true, Some(creds.expires_at), now_unix_secs),
                scopes: creds.scopes,
            },
            None => OAuthCredentialStatus {
                principal_id,
                provider,
                has_credentials: false,
                expires_at_unix_secs: None,
                refresh_token_present: false,
                last_updated_unix_secs: None,
                status: oauth_status_label(false, None, now_unix_secs),
                scopes: Vec::new(),
            },
        });
    }

    Ok(OAuthStatusResponse {
        credentials,
        observed: true,
    })
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

fn build_breaker_health(
    breaker_registry: Option<&BreakerRegistry>,
    upstream_name: &str,
) -> BreakerHealth {
    match breaker_registry.and_then(|registry| registry.get(upstream_name)) {
        Some(breaker) => BreakerHealth {
            state: breaker_state_label(breaker.current_state()),
            failure_count: breaker.failure_count(),
            half_open_in_flight: breaker.half_open_in_flight(),
            observed: true,
        },
        None => BreakerHealth {
            state: "unobserved",
            failure_count: 0,
            half_open_in_flight: 0,
            observed: false,
        },
    }
}

fn build_bulkhead_health(
    config: &Config,
    bulkhead_registry: Option<&BulkheadRegistry>,
    upstream_name: &str,
) -> BulkheadHealth {
    match bulkhead_registry.and_then(|registry| registry.get(upstream_name)) {
        Some(bulkhead) => BulkheadHealth {
            max_conns: bulkhead.config.max_conns_per_upstream,
            available_permits: saturating_u32(bulkhead.semaphore.available_permits()),
            observed: true,
        },
        None => BulkheadHealth {
            max_conns: config.bulkhead.max_conns_per_upstream,
            available_permits: config.bulkhead.max_conns_per_upstream,
            observed: false,
        },
    }
}

async fn recent_error_count(
    storage: &dyn Storage,
    upstream_name: &str,
    kind: RequestEventUpstream,
    now_unix_secs: u64,
) -> Result<u64, StorageError> {
    let window_start = now_unix_secs.saturating_sub(RECENT_ERROR_WINDOW_SECS);
    let kind_label = request_event_upstream_label(kind);
    Ok(storage
        .query_usage_rollups_in_range(UsageRollupResolution::Minute, window_start, now_unix_secs)
        .await?
        .into_iter()
        .filter(|rollup| rollup.upstream == upstream_name || rollup.upstream == kind_label)
        .map(|rollup| rollup.error_count)
        .sum())
}

fn plugin_entry(
    slot: &'static str,
    plugin: &PluginRef,
    runtime_status: Option<&dyn PluginRuntimeStatus>,
) -> PluginStatusEntry {
    let status = runtime_status.and_then(|runtime| runtime.plugin_status(&plugin.name));
    PluginStatusEntry {
        slot,
        name: plugin.name.clone(),
        wasm_path: plugin
            .wasm_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_default(),
        loaded: status.as_ref().map(|status| status.loaded).unwrap_or(true),
        disabled: status
            .as_ref()
            .map(|status| status.disabled)
            .unwrap_or(false),
        failure_count: status
            .as_ref()
            .map(|status| status.failure_count)
            .unwrap_or(0),
        last_error: status.and_then(|status| status.last_error),
        sse_per_event: Some(plugin.sse_per_event),
        batched_events_per_flush: Some(u64::from(plugin.batched_events_per_flush)),
        batched_flush_ms: Some(plugin.batched_flush_ms),
    }
}

fn build_principals_status(config: &Config) -> BTreeMap<String, PrincipalPluginsStatus> {
    config
        .principals
        .iter()
        .map(|(principal_id, principal)| {
            (
                principal_id.clone(),
                PrincipalPluginsStatus {
                    router_plugin: principal.router_plugin.as_ref().map(redact_plugin_ref),
                    observability_hooks: principal
                        .observability_hooks
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(redact_plugin_ref)
                        .collect(),
                },
            )
        })
        .collect()
}

fn redact_plugin_ref(plugin: &PluginRef) -> PluginRefRedacted {
    PluginRefRedacted {
        name: plugin.name.clone(),
        wasm_path: plugin
            .wasm_path
            .as_ref()
            .map(|path| path.display().to_string()),
        config_hash: config_hash(&plugin.config),
    }
}

fn config_hash(config: &serde_json::Value) -> String {
    let serialized = serde_json::to_string(config).expect("serde_json::Value serializes");
    let digest = Sha256::digest(serialized.as_bytes());
    digest[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decrypt_json<T: DeserializeOwned>(
    aead: &AeadService,
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<T, StatusBuildError> {
    let plaintext = aead
        .decrypt(ciphertext, aad)
        .map_err(|_| StatusBuildError::Crypto)?;
    serde_json::from_slice(&plaintext).map_err(|_| StatusBuildError::Json)
}

fn oauth_aad(principal_id: &str, provider: &str) -> Vec<u8> {
    format!("oauth:{principal_id}:{provider}").into_bytes()
}

fn request_event_upstream_kind(kind: &UpstreamKind) -> RequestEventUpstream {
    match kind {
        UpstreamKind::AnthropicDirect => RequestEventUpstream::AnthropicDirect,
        UpstreamKind::Custom => RequestEventUpstream::CustomAnthropicSpec,
    }
}

fn request_event_upstream_label(kind: RequestEventUpstream) -> &'static str {
    match kind {
        RequestEventUpstream::AnthropicDirect => "anthropic_direct",
        RequestEventUpstream::CustomAnthropicSpec => "custom_anthropic_spec",
    }
}

fn breaker_state_label(state: BreakerState) -> &'static str {
    match state {
        BreakerState::Closed => "closed",
        BreakerState::Open => "open",
        BreakerState::HalfOpen => "half_open",
    }
}

fn oauth_provider_from_credentials_ref(reference: &str) -> Option<String> {
    let reference = reference.trim();
    if reference == ANTHROPIC_OAUTH_PROVIDER {
        return Some(reference.to_owned());
    }
    reference
        .strip_prefix("oauth:")
        .map(str::trim)
        .filter(|provider| !provider.is_empty())
        .map(ToOwned::to_owned)
}

fn oauth_status_label(
    has_credentials: bool,
    expires_at_unix_secs: Option<u64>,
    now_unix_secs: u64,
) -> &'static str {
    let Some(expires_at) = expires_at_unix_secs.filter(|_| has_credentials) else {
        return "missing";
    };
    if expires_at <= now_unix_secs {
        "expired"
    } else if expires_at <= now_unix_secs.saturating_add(EXPIRING_SOON_SECS) {
        "expiring_soon"
    } else {
        "valid"
    }
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
