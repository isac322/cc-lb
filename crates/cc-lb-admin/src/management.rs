use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cc_lb_config::PrincipalSpec;
use cc_lb_core::AuditEntry;
use cc_lb_core::api_keys::key_store::{CreateParams, KeyStore, KeyStoreError};
use cc_lb_core::api_keys::secret;
use cc_lb_core::api_keys::types::{
    Limit as PrincipalLimit, LimitKind as PrincipalLimitKind, PrincipalType as CorePrincipalType,
};
use cc_lb_storage_api::types::{
    ApiKeyMutation, KeyStatus, Limit, LimitKind, PrincipalKindLite, StoredApiKeyRecord,
    UpstreamKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::AdminState;

pub type Result<T> = std::result::Result<T, ManagementError>;

#[derive(Debug, thiserror::Error)]
pub enum ManagementError {
    #[error("storage unavailable")]
    StorageUnavailable,
    #[error(transparent)]
    Settings(#[from] crate::settings::SettingsError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("principal exists")]
    PrincipalExists,
    #[error("unknown principal")]
    UnknownPrincipal,
    #[error("unknown api key")]
    UnknownApiKey,
    #[error("invalid draft principal")]
    InvalidDraftPrincipal,
    #[error("invalid request: {message}")]
    InvalidRequest { message: String },
    #[error("conflict: {0}")]
    Conflict(String),
    #[error(transparent)]
    Storage(#[from] cc_lb_storage_redb::StorageError),
    #[error("limit_overrides invalid: {message}")]
    LimitOverridesInvalid { message: String },
    #[error(transparent)]
    KeyStore(#[from] KeyStoreError),
}

impl IntoResponse for ManagementError {
    fn into_response(self) -> Response {
        match self {
            ManagementError::StorageUnavailable => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            ManagementError::Settings(error) => settings_management_error_response(error),
            ManagementError::Json(error) => {
                tracing::error!(error = %error, "admin principal management json operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            ManagementError::PrincipalExists => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": "principal_exists" })),
            )
                .into_response(),
            ManagementError::UnknownPrincipal => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": "unknown_principal" })),
            )
                .into_response(),
            ManagementError::UnknownApiKey => StatusCode::NOT_FOUND.into_response(),
            ManagementError::InvalidDraftPrincipal => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({ "error": "invalid_draft_principal" })),
            )
                .into_response(),
            ManagementError::InvalidRequest { message } => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "type": "error",
                    "error": {
                        "type": "invalid_request_error",
                        "message": message,
                    }
                })),
            )
                .into_response(),
            ManagementError::Conflict(message) => (
                StatusCode::CONFLICT,
                Json(serde_json::json!({ "error": message })),
            )
                .into_response(),
            ManagementError::Storage(error) => {
                tracing::error!(error = %error, "admin api key management storage operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
            ManagementError::LimitOverridesInvalid { message } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({
                    "code": "limit_overrides_invalid",
                    "field": "limit_overrides",
                    "message": message,
                })),
            )
                .into_response(),
            ManagementError::KeyStore(error) => {
                tracing::error!(error = %error, "admin api key management operation failed");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePrincipalRequest {
    pub id: String,
    pub spec: PrincipalSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePrincipalRequest {
    pub spec: PrincipalSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowedModelsRequest {
    pub allowed_models: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalMutationResponse {
    pub revision: u64,
    pub principal_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrincipalAllowedModelsResponse {
    pub revision: u64,
    pub principal_id: String,
    pub allowed_models: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueKeyRequest {
    pub label: Option<String>,
    pub upstream_kind: UpstreamKind,
    pub upstream_credential_ref: String,
    pub description: Option<String>,
    pub expires_at_unix_secs: Option<u64>,
    pub limit_overrides: Option<Vec<Limit>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyRecord {
    pub key_id: String,
    pub label: Option<String>,
    pub issued_at_unix_secs: u64,
    pub revoked_at_unix_secs: Option<u64>,
    pub status: KeyStatus,
    pub last_4: String,
    pub expires_at_unix_secs: Option<u64>,
    pub upstream_kind: UpstreamKind,
    pub upstream_credential_ref: String,
    pub limit_overrides: Vec<Limit>,
    pub description: Option<String>,
    pub principal_kind: PrincipalKindLite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssueKeyResponse {
    pub principal_id: String,
    pub key_id: String,
    pub plaintext_key: String,
    pub issued_at_unix_secs: u64,
    pub last_4: String,
    pub status: KeyStatus,
    pub label: Option<String>,
    pub revoked_at_unix_secs: Option<u64>,
    pub expires_at_unix_secs: Option<u64>,
    pub upstream_kind: UpstreamKind,
    pub upstream_credential_ref: String,
    pub limit_overrides: Vec<Limit>,
    pub description: Option<String>,
    pub principal_kind: PrincipalKindLite,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyListResponse {
    pub keys: Vec<ApiKeyRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeKeyResponse {
    pub key_id: String,
    pub revoked_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKeyRequest {
    pub label: Option<String>,
    pub description: Option<String>,
    #[serde(default, deserialize_with = "double_option")]
    pub expires_at_unix_secs: Option<Option<u64>>,
    pub limit_overrides: Option<Vec<Limit>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKeyResponse {
    pub key_id: String,
    pub updated_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct KeyStatusResponse {
    pub key_id: String,
    pub status: KeyStatus,
    pub updated_at_unix_secs: u64,
}

pub async fn create_principal(
    state: &AdminState,
    request: CreatePrincipalRequest,
    now_unix_secs: u64,
) -> Result<PrincipalMutationResponse> {
    let storage = state
        .storage
        .as_deref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let principal_id = request.id;
    let spec = request.spec;
    let current_exists = state
        .config
        .current_config()
        .principals
        .contains_key(&principal_id);
    let principal_for_audit = principal_id.clone();
    let (revision, response) = apply_principal_change(
        storage,
        state.config.as_ref(),
        now_unix_secs,
        move |principals| {
            let principals = principals_object(principals)?;
            if current_exists || principals.contains_key(&principal_id) {
                return Err(ManagementError::PrincipalExists);
            }
            principals.insert(principal_id.clone(), serde_json::to_value(&spec)?);
            Ok(PrincipalMutationResponse {
                revision: 0,
                principal_id: principal_id.clone(),
            })
        },
    )
    .await?;
    enqueue_principal_admin_audit(state, &principal_for_audit, "principal_create");
    Ok(PrincipalMutationResponse {
        revision,
        ..response
    })
}

pub async fn update_principal(
    state: &AdminState,
    principal_id: String,
    request: UpdatePrincipalRequest,
    now_unix_secs: u64,
) -> Result<PrincipalMutationResponse> {
    let storage = state
        .storage
        .as_deref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let spec = request.spec;
    let principal_for_audit = principal_id.clone();
    let (revision, response) = apply_principal_change(
        storage,
        state.config.as_ref(),
        now_unix_secs,
        move |principals| {
            let principals = principals_object(principals)?;
            if !principals.contains_key(&principal_id) {
                return Err(ManagementError::UnknownPrincipal);
            }
            principals.insert(principal_id.clone(), serde_json::to_value(&spec)?);
            Ok(PrincipalMutationResponse {
                revision: 0,
                principal_id: principal_id.clone(),
            })
        },
    )
    .await?;
    enqueue_principal_admin_audit(state, &principal_for_audit, "principal_update");
    Ok(PrincipalMutationResponse {
        revision,
        ..response
    })
}

pub async fn set_principal_disabled(
    state: &AdminState,
    principal_id: String,
    disabled: bool,
    now_unix_secs: u64,
) -> Result<PrincipalMutationResponse> {
    let storage = state
        .storage
        .as_deref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let current_config = state.config.current_config();
    let current_spec = current_config.principals.get(&principal_id).cloned();
    let principal_for_audit = principal_id.clone();
    let audit_kind = if disabled {
        "principal_disable"
    } else {
        "principal_enable"
    };
    let (revision, response) = apply_principal_change(
        storage,
        state.config.as_ref(),
        now_unix_secs,
        move |principals| {
            let principals = principals_object(principals)?;
            ensure_principal_value(principals, &principal_id, current_spec.as_ref())?;
            let principal = principals
                .get_mut(&principal_id)
                .and_then(Value::as_object_mut)
                .ok_or(ManagementError::InvalidDraftPrincipal)?;
            principal.insert("disabled".to_owned(), Value::Bool(disabled));
            Ok(PrincipalMutationResponse {
                revision: 0,
                principal_id: principal_id.clone(),
            })
        },
    )
    .await?;
    enqueue_principal_admin_audit(state, &principal_for_audit, audit_kind);
    Ok(PrincipalMutationResponse {
        revision,
        ..response
    })
}

pub async fn update_allowed_models(
    state: &AdminState,
    principal_id: String,
    request: AllowedModelsRequest,
    now_unix_secs: u64,
) -> Result<PrincipalAllowedModelsResponse> {
    let storage = state
        .storage
        .as_deref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let current_config = state.config.current_config();
    let current_spec = current_config.principals.get(&principal_id).cloned();
    let allowed_models = request.allowed_models;
    let principal_for_audit = principal_id.clone();
    let (revision, response) = apply_principal_change(
        storage,
        state.config.as_ref(),
        now_unix_secs,
        move |principals| {
            let principals = principals_object(principals)?;
            ensure_principal_value(principals, &principal_id, current_spec.as_ref())?;
            let principal = principals
                .get_mut(&principal_id)
                .and_then(Value::as_object_mut)
                .ok_or(ManagementError::InvalidDraftPrincipal)?;
            principal.insert(
                "allowed_models".to_owned(),
                Value::Array(allowed_models.iter().cloned().map(Value::String).collect()),
            );
            Ok(PrincipalAllowedModelsResponse {
                revision: 0,
                principal_id: principal_id.clone(),
                allowed_models: allowed_models.clone(),
            })
        },
    )
    .await?;
    enqueue_principal_admin_audit(
        state,
        &principal_for_audit,
        "principal_allowed_models_update",
    );
    Ok(PrincipalAllowedModelsResponse {
        revision,
        ..response
    })
}

pub fn issue_principal_key(
    state: &AdminState,
    principal_id: String,
    request: IssueKeyRequest,
) -> Result<IssueKeyResponse> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let key_store = KeyStore::new(storage.clone());
    if !principal_exists_in_current_or_draft(state, &principal_id) {
        return Err(ManagementError::UnknownPrincipal);
    }

    let principal_view = state.principal_view.load();
    let principal = principal_view
        .get(&principal_id)
        .ok_or(ManagementError::UnknownPrincipal)?;
    let default_limits = principal_view.default_limits(&principal_id);
    let limit_overrides = request.limit_overrides.unwrap_or_default();
    validate_limit_overrides(&limit_overrides, default_limits)?;

    let label = request.label.unwrap_or_default();
    let principal_kind = principal_kind_lite(principal.principal_type());
    let (record, plaintext) = key_store.create(
        &principal_id,
        CreateParams {
            upstream_kind: request.upstream_kind,
            upstream_credential_ref: request.upstream_credential_ref,
            label,
            description: request.description,
            expires_at_unix_secs: request.expires_at_unix_secs,
            limit_overrides: limit_overrides.clone(),
            principal_kind,
        },
    )?;
    let (key_id, _) =
        secret::parse(plaintext.expose()).map_err(|_| KeyStoreError::IndexInconsistency {
            reason: "generated api key secret could not be parsed".to_owned(),
        })?;

    enqueue_admin_audit(state, &principal_id, &key_id, "api_key_issue");

    Ok(IssueKeyResponse {
        principal_id,
        key_id,
        plaintext_key: plaintext.expose().to_owned(),
        issued_at_unix_secs: record.issued_at_unix_secs,
        last_4: record.last_4,
        status: record.status,
        label: if record.label.is_empty() {
            None
        } else {
            Some(record.label)
        },
        revoked_at_unix_secs: record.revoked_at_unix_secs,
        expires_at_unix_secs: record.expires_at_unix_secs,
        upstream_kind: record.upstream_kind,
        upstream_credential_ref: record.upstream_credential_ref,
        limit_overrides: record.limit_overrides,
        description: record.description,
        principal_kind: record.principal_kind,
    })
}

pub fn get_principal_key(
    state: &AdminState,
    principal_id: String,
    key_id: String,
) -> Result<ApiKeyRecord> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let key_store = KeyStore::new(storage.clone());
    if !principal_exists_in_current_or_draft(state, &principal_id) {
        return Err(ManagementError::UnknownPrincipal);
    }

    let record = find_key_by_id(&key_store, &principal_id, &key_id)?
        .ok_or(ManagementError::UnknownApiKey)?;

    Ok(ApiKeyRecord {
        key_id,
        label: if record.label.is_empty() {
            None
        } else {
            Some(record.label)
        },
        issued_at_unix_secs: record.issued_at_unix_secs,
        revoked_at_unix_secs: record.revoked_at_unix_secs,
        status: record.status,
        last_4: record.last_4,
        expires_at_unix_secs: record.expires_at_unix_secs,
        upstream_kind: record.upstream_kind,
        upstream_credential_ref: record.upstream_credential_ref,
        limit_overrides: record.limit_overrides,
        description: record.description,
        principal_kind: record.principal_kind,
    })
}

pub fn list_principal_keys(state: &AdminState, principal_id: String) -> Result<KeyListResponse> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let key_store = KeyStore::new(storage.clone());
    if !principal_exists_in_current_or_draft(state, &principal_id) {
        return Err(ManagementError::UnknownPrincipal);
    }

    let mut keys = Vec::new();
    for record in key_store.list_by_principal(&principal_id)? {
        let key_id = key_id_for_record(&key_store, &record)?;
        keys.push(ApiKeyRecord {
            key_id,
            label: if record.label.is_empty() {
                None
            } else {
                Some(record.label)
            },
            issued_at_unix_secs: record.issued_at_unix_secs,
            revoked_at_unix_secs: record.revoked_at_unix_secs,
            status: record.status,
            last_4: record.last_4,
            expires_at_unix_secs: record.expires_at_unix_secs,
            upstream_kind: record.upstream_kind,
            upstream_credential_ref: record.upstream_credential_ref,
            limit_overrides: record.limit_overrides,
            description: record.description,
            principal_kind: record.principal_kind,
        });
    }

    Ok(KeyListResponse { keys })
}

pub fn revoke_principal_key(
    state: &AdminState,
    principal_id: String,
    key_id: String,
) -> Result<RevokeKeyResponse> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let key_store = KeyStore::new(storage.clone());
    if !principal_exists_in_current_or_draft(state, &principal_id) {
        return Err(ManagementError::UnknownPrincipal);
    }

    if find_key_by_id(&key_store, &principal_id, &key_id)?.is_none() {
        return Err(ManagementError::UnknownApiKey);
    }

    key_store.revoke(&principal_id, &key_id)?;
    enqueue_admin_audit(state, &principal_id, &key_id, "api_key_revoke");

    Ok(RevokeKeyResponse {
        key_id,
        revoked_at_unix_secs: unix_now_secs(),
    })
}

pub fn update_principal_key(
    state: &AdminState,
    principal_id: String,
    key_id: String,
    request: UpdateKeyRequest,
) -> Result<UpdateKeyResponse> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let key_store = KeyStore::new(storage.clone());
    if !principal_exists_in_current_or_draft(state, &principal_id) {
        return Err(ManagementError::UnknownPrincipal);
    }

    let Some(existing_record) = storage
        .get_api_key(&principal_id, &key_id)
        .map_err(KeyStoreError::from)?
    else {
        return Err(ManagementError::UnknownApiKey);
    };
    if existing_record.status == KeyStatus::Revoked {
        return Err(ManagementError::Conflict(
            "key revoked; cannot patch".to_owned(),
        ));
    }

    if find_key_by_id(&key_store, &principal_id, &key_id)?.is_none() {
        return Err(ManagementError::UnknownApiKey);
    }

    if let Some(limit_overrides) = request.limit_overrides.as_ref() {
        let principal_view = state.principal_view.load();
        let default_limits = principal_view.default_limits(&principal_id);
        validate_limit_overrides(limit_overrides, default_limits)?;
    }

    key_store.patch(
        &principal_id,
        &key_id,
        ApiKeyMutation {
            label: request.label,
            description: request.description.map(Some),
            expires_at_unix_secs: request.expires_at_unix_secs,
            limit_overrides: request.limit_overrides,
            ..Default::default()
        },
    )?;
    enqueue_admin_audit(state, &principal_id, &key_id, "api_key_patch");

    Ok(UpdateKeyResponse {
        key_id,
        updated_at_unix_secs: unix_now_secs(),
    })
}

pub fn disable_principal_key(
    state: &AdminState,
    principal_id: String,
    key_id: String,
) -> Result<KeyStatusResponse> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let key_store = KeyStore::new(storage.clone());
    if !principal_exists_in_current_or_draft(state, &principal_id) {
        return Err(ManagementError::UnknownPrincipal);
    }

    if find_key_by_id(&key_store, &principal_id, &key_id)?.is_none() {
        return Err(ManagementError::UnknownApiKey);
    }

    key_store.disable(&principal_id, &key_id)?;
    enqueue_admin_audit(state, &principal_id, &key_id, "api_key_disable");

    Ok(KeyStatusResponse {
        key_id,
        status: KeyStatus::Disabled,
        updated_at_unix_secs: unix_now_secs(),
    })
}

pub fn enable_principal_key(
    state: &AdminState,
    principal_id: String,
    key_id: String,
) -> Result<KeyStatusResponse> {
    let storage = state
        .storage
        .as_ref()
        .ok_or(ManagementError::StorageUnavailable)?;
    let key_store = KeyStore::new(storage.clone());
    if !principal_exists_in_current_or_draft(state, &principal_id) {
        return Err(ManagementError::UnknownPrincipal);
    }

    if storage.get_api_key(&principal_id, &key_id)?.is_none() {
        return Err(ManagementError::UnknownApiKey);
    }

    match key_store.enable(&principal_id, &key_id) {
        Ok(()) => {
            enqueue_admin_audit(state, &principal_id, &key_id, "api_key_enable");
            Ok(KeyStatusResponse {
                key_id,
                status: KeyStatus::Active,
                updated_at_unix_secs: unix_now_secs(),
            })
        }
        Err(KeyStoreError::KeyAlreadyRevoked { .. }) => Err(ManagementError::Conflict(
            "key revoked; cannot enable".to_owned(),
        )),
        Err(error) => Err(error.into()),
    }
}

async fn apply_principal_change<T, F>(
    storage: &cc_lb_storage_redb::Storage,
    current: &dyn crate::CurrentConfig,
    now_unix_secs: u64,
    transform: F,
) -> Result<(u64, T)>
where
    F: FnMut(&mut Value) -> Result<T>,
{
    crate::settings::apply_draft_principal_change(storage, current, now_unix_secs, transform).await?
}

fn principals_object(principals: &mut Value) -> Result<&mut serde_json::Map<String, Value>> {
    principals
        .as_object_mut()
        .ok_or(ManagementError::InvalidDraftPrincipal)
}

fn ensure_principal_value(
    principals: &mut serde_json::Map<String, Value>,
    principal_id: &str,
    current_spec: Option<&PrincipalSpec>,
) -> Result<()> {
    if principals.contains_key(principal_id) {
        return Ok(());
    }
    let Some(current_spec) = current_spec else {
        return Err(ManagementError::UnknownPrincipal);
    };
    principals.insert(principal_id.to_owned(), serde_json::to_value(current_spec)?);
    Ok(())
}

fn principal_exists_in_current_or_draft(state: &AdminState, principal_id: &str) -> bool {
    if state.principal_view.load().get(principal_id).is_some() {
        return true;
    }
    state
        .storage
        .as_ref()
        .and_then(|storage| storage.get_config_draft().ok())
        .and_then(|draft| draft.draft)
        .and_then(|draft| draft.get("principals").cloned())
        .and_then(|principals| principals.as_object().cloned())
        .is_some_and(|principals| principals.contains_key(principal_id))
}

fn double_option<'de, T, D>(deserializer: D) -> std::result::Result<Option<Option<T>>, D::Error>
where
    T: serde::Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

fn key_id_for_record(key_store: &KeyStore, record: &StoredApiKeyRecord) -> Result<String> {
    if record.index_hash == [0; 32] {
        return Ok(String::new());
    }

    Ok(key_store
        .lookup_by_index_hash(&record.index_hash)?
        .map(|(_, key_id, _)| key_id)
        .unwrap_or_default())
}

fn find_key_by_id(
    key_store: &KeyStore,
    principal_id: &str,
    key_id: &str,
) -> Result<Option<StoredApiKeyRecord>> {
    for record in key_store.list_by_principal(principal_id)? {
        let Some((_, current_key_id, current_record)) =
            key_store.lookup_by_index_hash(&record.index_hash)?
        else {
            continue;
        };
        if current_key_id == key_id {
            return Ok(Some(current_record));
        }
    }

    Ok(None)
}

fn validate_limit_overrides(overrides: &[Limit], defaults: &[PrincipalLimit]) -> Result<()> {
    for override_limit in overrides {
        let Some(default_limit) = defaults.iter().find(|default_limit| {
            limit_kind_matches(override_limit.kind, default_limit.kind)
                && override_limit.window_secs == default_limit.window.as_secs()
        }) else {
            return Err(ManagementError::LimitOverridesInvalid {
                message: "limit_overrides must match a principal default limit by kind and window"
                    .to_owned(),
            });
        };

        if override_limit.cap_micros > default_limit.cap_micros {
            return Err(ManagementError::LimitOverridesInvalid {
                message: "limit_overrides must be a subset of principal default limits".to_owned(),
            });
        }
    }

    Ok(())
}

fn limit_kind_matches(left: LimitKind, right: PrincipalLimitKind) -> bool {
    matches!(
        (left, right),
        (LimitKind::Requests, PrincipalLimitKind::Requests)
            | (LimitKind::InputTokens, PrincipalLimitKind::InputTokens)
            | (LimitKind::OutputTokens, PrincipalLimitKind::OutputTokens)
            | (LimitKind::TotalTokens, PrincipalLimitKind::TotalTokens)
            | (LimitKind::CostUsd, PrincipalLimitKind::CostUsd)
            | (LimitKind::Concurrent, PrincipalLimitKind::Concurrent)
    )
}

fn principal_kind_lite(kind: CorePrincipalType) -> PrincipalKindLite {
    match kind {
        CorePrincipalType::Human => PrincipalKindLite::Human,
        CorePrincipalType::Machine => PrincipalKindLite::Machine,
    }
}

fn settings_management_error_response(error: crate::settings::SettingsError) -> Response {
    match error {
        crate::settings::SettingsError::StorageUnavailable => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        crate::settings::SettingsError::Storage(source) => {
            tracing::error!(error = %source, "admin principal management storage operation failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        crate::settings::SettingsError::StaleDraftRevision { current } => (
            StatusCode::CONFLICT,
            Json(
                serde_json::json!({ "error": "stale_draft_revision", "current_revision": current }),
            ),
        )
            .into_response(),
        crate::settings::SettingsError::ValidationFailed { detail } => (
            StatusCode::CONFLICT,
            Json(serde_json::json!({ "error": "validation_failed", "detail": detail })),
        )
            .into_response(),
        crate::settings::SettingsError::UnknownRevision { missing } => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "unknown_revision", "missing": missing })),
        )
            .into_response(),
        crate::settings::SettingsError::Schema(source) => {
            tracing::error!(error = %source, "admin principal management schema operation failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        other => {
            tracing::error!(error = %other, "unexpected admin principal settings error");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn enqueue_principal_admin_audit(state: &AdminState, principal_id: &str, action: &str) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = unix_now_secs();
    let _ = audit_sink.try_enqueue(AuditEntry {
        ts,
        request_id: format!("admin-{action}-{principal_id}-{ts}"),
        principal_id: principal_id.to_owned(),
        route: "admin_principal".to_owned(),
        upstream: String::new(),
        model: None,
        status: 200,
        input_tokens: None,
        output_tokens: None,
        duration_ms: 0,
        agent_label: None,
        api_key_id: None,
        cost_usd_micros: None,
        limit_violation: None,
        admin_action: Some(action.to_owned()),
        actor: Some("admin".to_owned()),
    });
}

fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn enqueue_admin_audit(state: &AdminState, principal_id: &str, key_id: &str, action: &str) {
    let Some(audit_sink) = &state.audit_sink else {
        return;
    };
    let ts = unix_now_secs();
    let _ = audit_sink.try_enqueue(AuditEntry {
        ts,
        request_id: format!("admin-{action}-{principal_id}-{key_id}-{ts}"),
        principal_id: principal_id.to_owned(),
        route: "admin_api_key".to_owned(),
        upstream: String::new(),
        model: None,
        status: 200,
        input_tokens: None,
        output_tokens: None,
        duration_ms: 0,
        agent_label: None,
        api_key_id: Some(key_id.to_owned()),
        cost_usd_micros: None,
        limit_violation: None,
        admin_action: Some(action.to_owned()),
        actor: Some("admin".to_owned()),
    });
}
