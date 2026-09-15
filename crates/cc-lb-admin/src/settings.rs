use cc_lb_config::Config;
use cc_lb_storage_api::{ConfigDraftState, HistoryEntry, Storage, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::CurrentConfig;

const INVALID_DRAFT_TTL_SECS: u64 = 24 * 60 * 60;

pub const COVERAGE_CHECKLIST: &[&str] = &[
    "listener",
    "body",
    "timeouts",
    "request_event_retention_days",
    "price_catalog",
    "storage",
    "scheduler",
    "upstream_affinity",
    "aead",
    "observability",
    "admin",
    "event_bus",
    "cluster",
    "oauth",
    "subscription_quota",
    "runtime",
    "circuit_breaker",
    "bulkhead",
    "prompt_cache_shadow",
    "limit_reservation_ttl",
];

#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("storage unavailable")]
    StorageUnavailable,
    #[error(transparent)]
    Storage(StorageError),
    #[error("stale draft revision")]
    StaleDraftRevision { current: u64 },
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
pub struct ConfigHistoryResponse {
    pub history: Vec<ConfigHistoryItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigHistoryItem {
    pub revision: u64,
    pub applied_at_unix_secs: u64,
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
    let config: Config = serde_json::from_value(value).map_err(|source| source.to_string())?;
    config.validate().map_err(|source| source.to_string())?;
    Ok(config)
}

fn history_item(entry: HistoryEntry) -> ConfigHistoryItem {
    ConfigHistoryItem {
        revision: entry.revision,
        applied_at_unix_secs: entry.applied_at_unix_secs,
    }
}

pub(crate) fn mask_secret_like_values(value: &mut Value) {
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
