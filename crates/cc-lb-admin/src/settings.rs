use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::path::Path;

use cc_lb_config::Config;
use cc_lb_storage_api::{
    AuditEntry, ConfigDraftState, HistoryEntry, HistorySummary, Storage, StorageError,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{ConfigReloader, CurrentConfig};

const MAX_DIFF_CHANGES: usize = 500;
const INVALID_DRAFT_TTL_SECS: u64 = 24 * 60 * 60;

pub const COVERAGE_CHECKLIST: &[&str] = &[
    "listener",
    "tls",
    "body",
    "timeouts",
    "downstream_auth",
    "api_keys",
    "storage",
    "scheduler",
    "aead",
    "observability",
    "admin",
    "oauth",
    "runtime",
    "circuit_breaker",
    "bulkhead",
    "dns",
    "egress",
    "subscription_quota",
    "prompt_cache_shadow",
    "limit_reservation_ttl",
    "lifecycle_hook_adapter",
    "lifecycle_pricing_subscriber",
    "lifecycle_cache_observation_subscriber",
    "lifecycle_rate_limit_header_subscriber",
    "lifecycle_subscription_quota_subscriber",
    "lifecycle_limit_rejection_audit_subscriber",
    "lifecycle_api_key_metrics_subscriber",
    "lifecycle_cache_hit_miss_subscriber",
    "lifecycle_prompt_cache_drift_subscriber",
    "lifecycle_prompt_cache_observation_subscriber",
    "lifecycle_limit_reconcile_subscriber",
];

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("storage unavailable")]
    StorageUnavailable,
    #[error(transparent)]
    Storage(StorageError),
    #[error("stale draft revision")]
    StaleDraftRevision { current: u64 },
    #[error("unvalidated revision")]
    UnvalidatedRevision,
    #[error("validation failed: {detail}")]
    ValidationFailed { detail: String },
    #[error("config path missing")]
    ConfigPathMissing,
    #[error("config watcher missing")]
    ConfigWatcherMissing,
    #[error("apply write failed: {detail}")]
    ApplyWriteFailed { detail: String },
    #[error("reload failed: {detail}")]
    ReloadFailed { detail: String },
    #[error("unknown config revision {missing}")]
    UnknownRevision { missing: u64 },
    #[error("schema serialization failed: {0}")]
    Schema(#[from] serde_json::Error),
}

impl From<StorageError> for SettingsError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigSchemaResponse {
    pub schema: Value,
    pub coverage_checklist: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDraftResponse {
    pub draft: Option<Value>,
    pub revision: u64,
    pub last_validated_revision: Option<u64>,
    pub last_validation_error: Option<String>,
    pub saved_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutConfigDraftRequest {
    pub draft: Value,
    pub expected_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutConfigDraftResponse {
    pub revision: u64,
    pub saved_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateConfigDraftRequest {
    pub expected_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateConfigDraftResponse {
    pub valid: bool,
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyConfigRequest {
    pub expected_revision: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyConfigResponse {
    pub applied_revision: u64,
    pub applied_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigHistoryResponse {
    pub history: Vec<ConfigHistoryItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigHistoryItem {
    pub revision: u64,
    pub applied_at_unix_secs: u64,
    pub config_summary: HistorySummary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDiffResponse {
    pub from: u64,
    pub to: u64,
    pub diff: Vec<ConfigDiffItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated_changes_count: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigDiffItem {
    pub path: String,
    pub from: Value,
    pub to: Value,
}

pub fn current_config_response(
    current: &dyn CurrentConfig,
    effective_revision_unix_secs: u64,
) -> Result<Value, SettingsError> {
    let config = current.current_config();
    let mut value = serde_json::to_value(&*config)?;
    mask_secret_like_values(&mut value);
    if let Value::Object(object) = &mut value {
        object.insert(
            "effective_revision_unix_secs".to_owned(),
            Value::from(effective_revision_unix_secs),
        );
    }
    Ok(value)
}

pub fn schema_response() -> Result<ConfigSchemaResponse, SettingsError> {
    let mut schema = serde_json::to_value(schemars::schema_for!(cc_lb_config::Config))?;
    crate::management::extend_config_schema(&mut schema);
    strip_schema_defaults(&mut schema);
    Ok(ConfigSchemaResponse {
        schema,
        coverage_checklist: COVERAGE_CHECKLIST
            .iter()
            .map(|field| (*field).to_owned())
            .collect(),
    })
}

fn strip_schema_defaults(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("default");
            for child in object.values_mut() {
                strip_schema_defaults(child);
            }
        }
        Value::Array(values) => {
            for value in values {
                strip_schema_defaults(value);
            }
        }
        _ => {}
    }
}

pub async fn get_draft(
    storage: &dyn Storage,
    clock: &dyn cc_lb_clock::Clock,
) -> Result<ConfigDraftResponse, SettingsError> {
    let state = storage.get_config_draft().await?;
    if invalid_draft_expired(&state, cc_lb_clock::unix_secs(clock.now())) {
        let revision = state.revision;
        let _ = storage
            .put_config_draft(ConfigDraftState::default(), revision)
            .await?;
        return draft_response(storage.get_config_draft().await?);
    }
    draft_response(state)
}

pub async fn put_draft(
    storage: &dyn Storage,
    request: PutConfigDraftRequest,
    saved_at_unix_secs: u64,
) -> Result<PutConfigDraftResponse, SettingsError> {
    let current = storage.get_config_draft().await?;
    if current.revision != request.expected_revision {
        return Err(SettingsError::StaleDraftRevision {
            current: current.revision,
        });
    }
    let state = ConfigDraftState {
        draft: Some(request.draft),
        revision: 0,
        last_validated_revision: None,
        last_validation_error: None,
        saved_at_unix_secs: Some(saved_at_unix_secs),
    };
    let revision = storage
        .put_config_draft(state, request.expected_revision)
        .await?;
    Ok(PutConfigDraftResponse {
        revision,
        saved_at_unix_secs,
    })
}

pub async fn validate_draft(
    storage: &dyn Storage,
    request: ValidateConfigDraftRequest,
) -> Result<ValidateConfigDraftResponse, SettingsError> {
    let state = storage.get_config_draft().await?;
    if state.revision != request.expected_revision {
        return Err(SettingsError::StaleDraftRevision {
            current: state.revision,
        });
    }

    let validation = state
        .draft
        .clone()
        .ok_or_else(|| "draft_missing".to_owned())
        .and_then(deserialize_and_validate_config);

    match validation {
        Ok(_) => {
            storage
                .set_last_validated_revision(state.revision, None)
                .await?;
            Ok(ValidateConfigDraftResponse {
                valid: true,
                revision: state.revision,
                error: None,
            })
        }
        Err(error) => {
            storage
                .set_last_validated_revision(state.revision, Some(error.clone()))
                .await?;
            Ok(ValidateConfigDraftResponse {
                valid: false,
                revision: state.revision,
                error: Some(error),
            })
        }
    }
}

pub async fn apply_config(
    storage: &dyn Storage,
    config_path: Option<&Path>,
    config_watcher: Option<&dyn ConfigReloader>,
    request: ApplyConfigRequest,
    applied_at_unix_secs: u64,
) -> Result<ApplyConfigResponse, SettingsError> {
    let state = storage.get_config_draft().await?;
    if state.revision != request.expected_revision {
        return Err(SettingsError::StaleDraftRevision {
            current: state.revision,
        });
    }
    if state.last_validated_revision != Some(request.expected_revision) {
        return Err(SettingsError::UnvalidatedRevision);
    }

    let draft = state
        .draft
        .clone()
        .ok_or(SettingsError::UnvalidatedRevision)?;
    let config = deserialize_and_validate_config(draft)
        .map_err(|detail| SettingsError::ValidationFailed { detail })?;
    let config_toml =
        toml::to_string_pretty(&config).map_err(|source| SettingsError::ApplyWriteFailed {
            detail: source.to_string(),
        })?;
    let roundtrip: Config =
        toml::from_str(&config_toml).map_err(|source| SettingsError::ValidationFailed {
            detail: source.to_string(),
        })?;
    roundtrip
        .validate()
        .map_err(|source| SettingsError::ValidationFailed {
            detail: source.to_string(),
        })?;

    let config_path = config_path.ok_or(SettingsError::ConfigPathMissing)?;
    write_config_file(config_path, &config_toml).map_err(|source| {
        SettingsError::ApplyWriteFailed {
            detail: source.to_string(),
        }
    })?;

    let config_watcher = config_watcher.ok_or(SettingsError::ConfigWatcherMissing)?;
    config_watcher
        .reload_now()
        .map_err(|detail| SettingsError::ReloadFailed { detail })?;

    storage
        .append_config_history(
            request.expected_revision,
            config_toml,
            applied_at_unix_secs,
            history_summary(&config),
        )
        .await?;
    storage
        .append_audit(&AuditEntry {
            ts: applied_at_unix_secs,
            request_id: format!("config_apply_{}", request.expected_revision),
            principal_id: "admin".to_owned(),
            route: "config_apply".to_owned(),
            upstream: "admin".to_owned(),
            model: None,
            status: 200,
            input_tokens: Some(0),
            output_tokens: Some(0),
            duration_ms: 0,
            agent_label: None,
            api_key_id: None,
            cost_usd_micros: None,
            limit_violation: None,
            admin_action: Some("config_apply".to_owned()),
            actor: Some("admin".to_owned()),
            kind: None,
            payload: None,
        })
        .await?;

    Ok(ApplyConfigResponse {
        applied_revision: request.expected_revision,
        applied_at_unix_secs,
    })
}

pub async fn list_history(
    storage: &dyn Storage,
    limit: usize,
) -> Result<ConfigHistoryResponse, SettingsError> {
    let limit = limit.min(100);
    let history = storage
        .list_config_history(limit)
        .await?
        .into_iter()
        .map(history_item)
        .collect();
    Ok(ConfigHistoryResponse { history })
}

pub async fn diff_history(
    storage: &dyn Storage,
    from_revision: u64,
    to_revision: u64,
) -> Result<ConfigDiffResponse, SettingsError> {
    if from_revision == to_revision {
        if storage.get_config_history(from_revision).await?.is_none() {
            return Err(SettingsError::UnknownRevision {
                missing: from_revision,
            });
        }
        return Ok(ConfigDiffResponse {
            from: from_revision,
            to: to_revision,
            diff: Vec::new(),
            truncated_changes_count: None,
        });
    }

    let from =
        storage
            .get_config_history(from_revision)
            .await?
            .ok_or(SettingsError::UnknownRevision {
                missing: from_revision,
            })?;
    let to =
        storage
            .get_config_history(to_revision)
            .await?
            .ok_or(SettingsError::UnknownRevision {
                missing: to_revision,
            })?;
    let from_json = history_config_json(&from)?;
    let to_json = history_config_json(&to)?;
    let mut diff = Vec::new();
    let mut total_changes = 0_u64;
    collect_diff("", &from_json, &to_json, &mut diff, &mut total_changes);
    let truncated_changes_count = total_changes
        .saturating_sub(diff.len() as u64)
        .checked_sub(0)
        .filter(|count| *count > 0);

    Ok(ConfigDiffResponse {
        from: from_revision,
        to: to_revision,
        diff,
        truncated_changes_count,
    })
}

fn draft_response(state: ConfigDraftState) -> Result<ConfigDraftResponse, SettingsError> {
    let mut draft = state.draft;
    if let Some(draft) = &mut draft {
        mask_secret_like_values(draft);
    }
    Ok(ConfigDraftResponse {
        draft,
        revision: state.revision,
        last_validated_revision: state.last_validated_revision,
        last_validation_error: state.last_validation_error,
        saved_at_unix_secs: state.saved_at_unix_secs,
    })
}

fn invalid_draft_expired(state: &ConfigDraftState, now_unix_secs: u64) -> bool {
    state.draft.is_some()
        && state.last_validation_error.is_some()
        && state
            .saved_at_unix_secs
            .is_some_and(|saved_at| now_unix_secs.saturating_sub(saved_at) > INVALID_DRAFT_TTL_SECS)
}

fn deserialize_and_validate_config(value: Value) -> Result<Config, String> {
    reject_unknown_top_level_keys(&value)?;
    let config: Config = serde_json::from_value(value).map_err(|source| source.to_string())?;
    config.validate().map_err(|source| source.to_string())?;
    Ok(config)
}

fn reject_unknown_top_level_keys(value: &Value) -> Result<(), String> {
    let Some(object) = value.as_object() else {
        return Err("config draft must be a JSON object".to_owned());
    };
    let allowed = [
        "listener",
        "tls",
        "body",
        "timeouts",
        "downstream_auth",
        "api_keys",
        "storage",
        "scheduler",
        "aead",
        "observability",
        "admin",
        "oauth",
        "runtime",
        "circuit_breaker",
        "bulkhead",
        "dns",
        "egress",
        "subscription_quota",
        "prompt_cache_shadow",
        "lifecycle_hook_adapter",
        "lifecycle_pricing_subscriber",
        "lifecycle_cache_observation_subscriber",
        "limit_reservation_ttl",
        "lifecycle_limit_reconcile_subscriber",
        "lifecycle_rate_limit_header_subscriber",
        "lifecycle_subscription_quota_subscriber",
        "lifecycle_limit_rejection_audit_subscriber",
        "lifecycle_api_key_metrics_subscriber",
        "lifecycle_cache_hit_miss_subscriber",
        "lifecycle_prompt_cache_drift_subscriber",
        "lifecycle_prompt_cache_observation_subscriber",
        "event_bus",
        "cluster",
    ];
    let allowed: BTreeSet<&str> = allowed.into_iter().collect();
    let unknown: Vec<&str> = object
        .keys()
        .map(String::as_str)
        .filter(|key| !allowed.contains(key))
        .collect();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unknown top-level config keys: {}",
            unknown.join(", ")
        ))
    }
}

fn history_summary(config: &Config) -> HistorySummary {
    HistorySummary {
        upstreams: 0,
        principals: 0,
        plugin_count: 0,
        tls_enabled: config.tls.is_some() || config.listener.tls.is_some(),
    }
}

fn history_item(entry: HistoryEntry) -> ConfigHistoryItem {
    ConfigHistoryItem {
        revision: entry.revision,
        applied_at_unix_secs: entry.applied_at_unix_secs,
        config_summary: entry.summary,
    }
}

fn history_config_json(entry: &HistoryEntry) -> Result<Value, SettingsError> {
    let config: Config =
        toml::from_str(&entry.config_toml).map_err(|source| SettingsError::ValidationFailed {
            detail: source.to_string(),
        })?;
    config
        .validate()
        .map_err(|source| SettingsError::ValidationFailed {
            detail: source.to_string(),
        })?;
    Ok(serde_json::to_value(config)?)
}

fn collect_diff(
    path: &str,
    from: &Value,
    to: &Value,
    diff: &mut Vec<ConfigDiffItem>,
    total_changes: &mut u64,
) {
    if from == to {
        return;
    }

    match (from, to) {
        (Value::Object(from_object), Value::Object(to_object)) => {
            let keys = from_object
                .keys()
                .chain(to_object.keys())
                .collect::<BTreeSet<_>>();
            for key in keys {
                let child_path = join_object_path(path, key);
                let from_value = from_object.get(key).unwrap_or(&Value::Null);
                let to_value = to_object.get(key).unwrap_or(&Value::Null);
                collect_diff(&child_path, from_value, to_value, diff, total_changes);
            }
        }
        (Value::Array(from_array), Value::Array(to_array)) => {
            let len = from_array.len().max(to_array.len());
            for index in 0..len {
                let child_path = format!("{path}[{index}]");
                let from_value = from_array.get(index).unwrap_or(&Value::Null);
                let to_value = to_array.get(index).unwrap_or(&Value::Null);
                collect_diff(&child_path, from_value, to_value, diff, total_changes);
            }
        }
        _ => {
            *total_changes = total_changes.saturating_add(1);
            if diff.len() < MAX_DIFF_CHANGES {
                diff.push(ConfigDiffItem {
                    path: path.to_owned(),
                    from: diff_value(path, from),
                    to: diff_value(path, to),
                });
            }
        }
    }
}

fn join_object_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_owned()
    } else {
        format!("{parent}.{key}")
    }
}

fn diff_value(path: &str, value: &Value) -> Value {
    if path.split(['.', '[', ']']).any(is_secret_like_key) {
        Value::String("<redacted>".to_owned())
    } else {
        value.clone()
    }
}

fn mask_secret_like_values(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                if is_secret_like_key(key) {
                    *child = Value::String("[REDACTED]".to_owned());
                } else {
                    mask_secret_like_values(child);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                mask_secret_like_values(value);
            }
        }
        _ => {}
    }
}

fn is_secret_like_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower == "token"
        || lower == "token_env"
        || lower.ends_with("_token")
        || lower.ends_with("_token_env")
        || lower == "secret"
        || lower.ends_with("_secret")
        || lower.ends_with("_secret_env")
        || lower == "api_key"
        || lower.ends_with("_api_key")
        || lower == "aead_master_key"
        || lower == "oauth_aead_key_env"
}

fn write_config_file(path: &Path, contents: &str) -> Result<(), std::io::Error> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let temp_path = parent.join(temp_file_name(
        path.file_name().unwrap_or_else(|| OsStr::new("config")),
    ));
    fs::write(&temp_path, contents)?;
    match fs::rename(&temp_path, path) {
        Ok(()) => Ok(()),
        Err(error) => {
            let _ = fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

fn temp_file_name(file_name: &OsStr) -> OsString {
    let mut temp = OsString::from(".");
    temp.push(file_name);
    temp.push(format!(".{}.tmp", std::process::id()));
    temp
}
