use std::collections::BTreeMap;

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use cc_lb_config::RestartRequiredField;
use cc_lb_core::{ApplyStatus, ReplicaIdentity};
use cc_lb_storage_api::principal::Limit;
use cc_lb_storage_api::upstream::UpstreamKind;
use cc_lb_storage_api::{
    PluginChainEntry, PluginRegistryStore, PluginSlot, PrincipalKind, PrincipalRecord,
    PrincipalStore, Storage, StorageError, UpstreamRecord, UpstreamStore, WasmRegistryEntry,
};
use serde::Serialize;
use serde_json::{Value, json};

use crate::{AdminState, LastReloadStatus};

const STORE_PAGE_LIMIT: usize = 1000;
const EXPORT_SCHEMA_VERSION: u64 = 1;

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/v1/status", get(status))
        .route("/admin/v1/export", get(export))
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    version: &'static str,
    git_sha: &'static str,
    uptime_secs: u64,
    build: BuildInfo,
    replica_id: Option<String>,
    generation: u64,
    upstreams: Vec<StatusUpstream>,
    principals: Vec<StatusPrincipal>,
    plugin_chain_summary: PluginChainSummary,
    killswitch: bool,
    last_reload_status: Option<LastReloadStatus>,
    restart_required_changes: Vec<RestartRequiredField>,
}

#[derive(Debug, Serialize)]
struct BuildInfo {
    rust_version: &'static str,
    profile: &'static str,
    target: &'static str,
}

#[derive(Debug, Serialize)]
struct StatusUpstream {
    id: String,
    name: String,
    status: &'static str,
    last_apply_at_unix_secs: u64,
    last_apply_error: Option<String>,
}

#[derive(Debug, Serialize)]
struct StatusPrincipal {
    id: String,
    name: String,
    enabled: bool,
    last_apply_error: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct PluginChainSummary {
    principal_count_with_chain: usize,
    total_entries: usize,
}

#[derive(Debug, Serialize)]
struct ExportResponse {
    exported_at_unix_secs: u64,
    schema_version: u64,
    upstreams: Vec<ExportUpstream>,
    principals: Vec<ExportPrincipal>,
    plugins: ExportPlugins,
}

#[derive(Debug, Serialize)]
struct ExportUpstream {
    name: String,
    kind: UpstreamKind,
    enabled: bool,
    oauth_credentials_present: bool,
    expires_at_unix_secs: Option<u64>,
    revision: u64,
}

#[derive(Debug, Serialize)]
struct ExportPrincipal {
    name: String,
    kind: PrincipalKind,
    allowed_models: Vec<String>,
    default_limits: Vec<Limit>,
    enabled: bool,
    revision: u64,
}

#[derive(Debug, Serialize)]
struct ExportPlugins {
    registry: Vec<ExportRegistryEntry>,
    chains: BTreeMap<String, ExportPrincipalChains>,
}

#[derive(Debug, Serialize)]
struct ExportRegistryEntry {
    sha256_hex: String,
    name: String,
    original_filename: String,
    label: Option<String>,
    size_bytes: u64,
    refcount: i64,
    supported_slots: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ExportPrincipalChains {
    #[serde(rename = "Router")]
    router: Vec<ExportChainEntry>,
    #[serde(rename = "ObservabilityHook")]
    observability_hook: Vec<ExportChainEntry>,
    #[serde(rename = "Shape", skip_serializing_if = "Vec::is_empty", default)]
    shape: Vec<ExportChainEntry>,
}

#[derive(Debug, Serialize)]
struct ExportChainEntry {
    wasm_registry_name: String,
    order: i64,
    config: Value,
    sse_per_event: bool,
    batched_events_per_flush: u32,
    batched_flush_ms: u64,
}

async fn status(State(state): State<AdminState>) -> axum::response::Response {
    match build_status(&state).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => status_error_response(error),
    }
}

async fn export(State(state): State<AdminState>) -> axum::response::Response {
    match build_export(&state, unix_now_secs()).await {
        Ok(response) => Json(response).into_response(),
        Err(error) => status_error_response(error),
    }
}

async fn build_status(state: &AdminState) -> Result<StatusResponse, StatusError> {
    let storage = storage(state)?;
    let view = state.dynamic_view.load();
    let upstream_records = all_upstreams(storage).await?;
    let upstreams_by_name = upstream_records
        .iter()
        .map(|record| (record.name.clone(), record.id.to_string()))
        .collect::<BTreeMap<_, _>>();
    let mut upstreams = view
        .upstream_status_snapshot
        .entries
        .iter()
        .map(|(name, entry)| StatusUpstream {
            id: upstreams_by_name.get(name).cloned().unwrap_or_default(),
            name: name.clone(),
            status: apply_status_label(entry.status),
            last_apply_at_unix_secs: entry.last_apply_at_unix_secs,
            last_apply_error: entry.last_apply_error.clone(),
        })
        .collect::<Vec<_>>();
    upstreams.sort_by(|left, right| left.name.cmp(&right.name));

    let principals = all_principals(storage).await?;
    let chain_summary = plugin_chain_summary(storage, &principals).await?;
    let replica_identity = state
        .lifecycle
        .as_ref()
        .and_then(|lifecycle| lifecycle.replica_identity());

    Ok(StatusResponse {
        version: env!("CARGO_PKG_VERSION"),
        git_sha: option_env!("CC_LB_GIT_SHA").unwrap_or("unknown"),
        uptime_secs: state.start_time.elapsed().as_secs(),
        build: build_info(),
        replica_id: replica_identity.map(replica_id),
        generation: view.generation,
        upstreams,
        principals: principals.into_iter().map(status_principal).collect(),
        plugin_chain_summary: chain_summary,
        killswitch: storage.killswitch_enabled().await?,
        last_reload_status: state.config.last_reload_status(),
        restart_required_changes: state.config.restart_required_changes(),
    })
}

async fn build_export(
    state: &AdminState,
    exported_at_unix_secs: u64,
) -> Result<ExportResponse, StatusError> {
    let storage = storage(state)?;
    let upstreams = all_upstreams(storage).await?;
    let principals = all_principals(storage).await?;
    let registry = all_registry(storage).await?;
    let registry_export = export_registry(storage, &registry).await?;
    let registry_by_id = registry
        .iter()
        .map(|entry| (entry.id, entry.name.clone()))
        .collect::<BTreeMap<_, _>>();
    let chains = export_chains(storage, &principals, &registry_by_id).await?;

    Ok(ExportResponse {
        exported_at_unix_secs,
        schema_version: EXPORT_SCHEMA_VERSION,
        upstreams: upstreams.into_iter().map(export_upstream).collect(),
        principals: principals.into_iter().map(export_principal).collect(),
        plugins: ExportPlugins {
            registry: registry_export,
            chains,
        },
    })
}

fn storage(state: &AdminState) -> Result<&dyn Storage, StatusError> {
    state
        .storage
        .as_deref()
        .ok_or(StatusError::StorageUnavailable)
}

async fn all_upstreams(storage: &dyn Storage) -> Result<Vec<UpstreamRecord>, StorageError> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = UpstreamStore::list(storage, after, STORE_PAGE_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        let page_len = page.len();
        all.extend(page);
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    all.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(all)
}

async fn all_principals(storage: &dyn Storage) -> Result<Vec<PrincipalRecord>, StorageError> {
    let mut all = Vec::new();
    let mut offset = 0;
    loop {
        let page = PrincipalStore::list(storage, offset, STORE_PAGE_LIMIT, false).await?;
        if page.is_empty() {
            break;
        }
        offset += page.len();
        let page_len = page.len();
        all.extend(page);
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    all.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(all)
}

async fn all_registry(storage: &dyn Storage) -> Result<Vec<WasmRegistryEntry>, StorageError> {
    let mut all = Vec::new();
    let mut after = None;
    loop {
        let page = PluginRegistryStore::list_registry(storage, after, STORE_PAGE_LIMIT).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|entry| entry.id);
        let page_len = page.len();
        all.extend(page);
        if page_len < STORE_PAGE_LIMIT {
            break;
        }
    }
    all.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(all)
}

async fn plugin_chain_summary(
    storage: &dyn Storage,
    principals: &[PrincipalRecord],
) -> Result<PluginChainSummary, StorageError> {
    let mut summary = PluginChainSummary::default();
    for principal in principals {
        let router = storage
            .list_chain_for_principal(principal.id, PluginSlot::Router)
            .await?;
        let hooks = storage
            .list_chain_for_principal(principal.id, PluginSlot::ObservabilityHook)
            .await?;
        let shape = storage
            .list_chain_for_principal(principal.id, PluginSlot::Shape)
            .await?;
        let count = router.len() + hooks.len() + shape.len();
        if count > 0 {
            summary.principal_count_with_chain += 1;
            summary.total_entries += count;
        }
    }
    Ok(summary)
}

async fn export_chains(
    storage: &dyn Storage,
    principals: &[PrincipalRecord],
    registry_by_id: &BTreeMap<uuid::Uuid, String>,
) -> Result<BTreeMap<String, ExportPrincipalChains>, StorageError> {
    let mut chains = BTreeMap::new();
    for principal in principals {
        let router = storage
            .list_chain_for_principal(principal.id, PluginSlot::Router)
            .await?;
        let observability_hook = storage
            .list_chain_for_principal(principal.id, PluginSlot::ObservabilityHook)
            .await?;
        let shape = storage
            .list_chain_for_principal(principal.id, PluginSlot::Shape)
            .await?;
        if router.is_empty() && observability_hook.is_empty() && shape.is_empty() {
            continue;
        }
        chains.insert(
            principal.name.clone(),
            ExportPrincipalChains {
                router: export_chain_entries(router, registry_by_id),
                observability_hook: export_chain_entries(observability_hook, registry_by_id),
                shape: export_chain_entries(shape, registry_by_id),
            },
        );
    }
    Ok(chains)
}

fn export_chain_entries(
    entries: Vec<PluginChainEntry>,
    registry_by_id: &BTreeMap<uuid::Uuid, String>,
) -> Vec<ExportChainEntry> {
    let mut exported = entries
        .into_iter()
        .map(|entry| ExportChainEntry {
            wasm_registry_name: registry_by_id
                .get(&entry.wasm_registry_id)
                .cloned()
                .unwrap_or_default(),
            order: entry.order,
            config: normalize_json(entry.config),
            sse_per_event: entry.sse_per_event,
            batched_events_per_flush: entry.batched_events_per_flush,
            batched_flush_ms: entry.batched_flush_ms,
        })
        .collect::<Vec<_>>();
    exported.sort_by_key(|entry| entry.order);
    exported
}

fn status_principal(record: PrincipalRecord) -> StatusPrincipal {
    StatusPrincipal {
        id: record.id.to_string(),
        name: record.name,
        enabled: record.enabled,
        last_apply_error: record.last_apply_error,
    }
}

fn export_upstream(record: UpstreamRecord) -> ExportUpstream {
    ExportUpstream {
        name: record.name,
        kind: record.kind,
        enabled: record.enabled,
        oauth_credentials_present: record.oauth_credentials.is_some(),
        // UpstreamRecord does not expose OAuth expiry as plaintext metadata; do not decrypt it here.
        expires_at_unix_secs: None,
        revision: record.revision,
    }
}

fn export_principal(record: PrincipalRecord) -> ExportPrincipal {
    ExportPrincipal {
        name: record.name,
        kind: record.kind,
        allowed_models: record.allowed_models,
        default_limits: record.default_limits,
        enabled: record.enabled,
        revision: record.revision,
    }
}

async fn export_registry(
    storage: &dyn Storage,
    registry: &[WasmRegistryEntry],
) -> Result<Vec<ExportRegistryEntry>, StorageError> {
    let mut exported = Vec::with_capacity(registry.len());
    for entry in registry {
        let size_bytes = storage
            .get_blob_bytes(entry.sha256)
            .await?
            .map(|bytes| bytes.len() as u64)
            .unwrap_or(0);
        exported.push(export_registry_entry(entry.clone(), size_bytes));
    }
    Ok(exported)
}

fn export_registry_entry(entry: WasmRegistryEntry, size_bytes: u64) -> ExportRegistryEntry {
    ExportRegistryEntry {
        sha256_hex: hex_sha256(entry.sha256),
        name: entry.name,
        original_filename: entry.original_filename,
        label: entry.label,
        size_bytes,
        refcount: entry.refcount,
        supported_slots: entry
            .supported_slots
            .into_iter()
            .map(|slot| slot.as_str().to_owned())
            .collect(),
    }
}

fn build_info() -> BuildInfo {
    BuildInfo {
        rust_version: option_env!("CC_LB_RUSTC").unwrap_or("unknown"),
        profile: option_env!("PROFILE").unwrap_or("unknown"),
        target: option_env!("TARGET").unwrap_or("unknown"),
    }
}

fn replica_id(identity: ReplicaIdentity) -> String {
    identity.id.to_string()
}

fn apply_status_label(status: ApplyStatus) -> &'static str {
    match status {
        ApplyStatus::Active => "active",
        ApplyStatus::Disabled => "disabled",
        ApplyStatus::Error => "error",
    }
}

fn normalize_json(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted = object
                .into_iter()
                .map(|(key, value)| {
                    let normalized = if sensitive_key(&key) {
                        Value::String("[REDACTED]".to_owned())
                    } else {
                        normalize_json(value)
                    };
                    (key, normalized)
                })
                .collect::<BTreeMap<_, _>>();
            json!(sorted)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(normalize_json).collect()),
        other => other,
    }
}

fn sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("secret") || key.contains("token") || key.contains("password")
}

fn hex_sha256(sha256: [u8; 32]) -> String {
    sha256.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn status_error_response(error: StatusError) -> axum::response::Response {
    match error {
        StatusError::StorageUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "storage_unavailable" })),
        )
            .into_response(),
        StatusError::Storage(source) => {
            tracing::error!(error = %source, "admin v1 status storage operation failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "storage_error" })),
            )
                .into_response()
        }
        StatusError::RedbStorage(source) => {
            tracing::error!(error = %source, "admin v1 status redb operation failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "storage_error" })),
            )
                .into_response()
        }
    }
}

#[derive(Debug)]
enum StatusError {
    StorageUnavailable,
    Storage(StorageError),
    RedbStorage(cc_lb_storage_redb::StorageError),
}

impl From<StorageError> for StatusError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<cc_lb_storage_redb::StorageError> for StatusError {
    fn from(error: cc_lb_storage_redb::StorageError) -> Self {
        Self::RedbStorage(error)
    }
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
