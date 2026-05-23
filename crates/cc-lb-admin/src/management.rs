use std::collections::HashMap;

use cc_lb_config::{Config, PrincipalSpec};
use cc_lb_storage_redb::{ApiKeyRecord, AuditEntry, Storage, StorageError};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;

use crate::{CurrentConfig, settings};

const EXPIRING_SOON_SECS: u64 = 300;
const DEFAULT_API_KEY_PROVIDER: &str = "api_key";
const ANTHROPIC_OAUTH_PROVIDER: &str = "anthropic_oauth";

#[derive(Debug, Error)]
pub enum ManagementError {
    #[error("storage unavailable")]
    StorageUnavailable,
    #[error(transparent)]
    Settings(#[from] settings::SettingsError),
    #[error("storage error: {0}")]
    Storage(StorageError),
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
    #[error("rotate unsupported for {kind}")]
    RotateUnsupported { kind: &'static str },
}

impl From<StorageError> for ManagementError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::UnknownApiKey { .. } => Self::UnknownApiKey,
            other => Self::Storage(other),
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
pub struct IssueKeyRequest {
    #[serde(default)]
    pub label: Option<String>,
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
pub struct IssueKeyResponse {
    pub principal_id: String,
    pub key_id: String,
    pub plaintext_key: String,
    pub issued_at_unix_secs: u64,
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
pub struct CredentialsResponse {
    pub credentials: Vec<CredentialEntry>,
    pub observed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialEntry {
    pub principal_id: String,
    pub provider: String,
    pub kind: String,
    pub identity: String,
    pub associated_principals: Vec<String>,
    pub has_credentials: bool,
    pub expires_at_unix_secs: Option<u64>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RotateCredentialResponse {
    pub principal_id: String,
    pub provider: String,
    pub kind: String,
    pub new_key_id: String,
    pub revoked_key_id: Option<String>,
    pub plaintext_key: String,
    pub issued_at_unix_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokeCredentialResponse {
    pub principal_id: String,
    pub provider: String,
    pub kind: String,
    pub revoked_keys: Vec<String>,
}

pub fn create_principal(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    request: CreatePrincipalRequest,
    now_unix_secs: u64,
) -> Result<PrincipalMutationResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    let principal_id = request.id;
    let spec = request.spec;
    let current_config = current.current_config();
    let current_exists = current_config.principals.contains_key(&principal_id);
    let principal_for_audit = principal_id.clone();
    let (revision, response) =
        apply_principal_change(Some(storage), current, now_unix_secs, move |principals| {
            let principals = principals_object(principals)?;
            if current_exists || principals.contains_key(&principal_id) {
                return Err(ManagementError::PrincipalExists);
            }
            principals.insert(principal_id.clone(), serde_json::to_value(&spec)?);
            Ok(PrincipalMutationResponse {
                revision: 0,
                principal_id: principal_id.clone(),
            })
        })?;
    append_admin_audit(
        storage,
        now_unix_secs,
        &principal_for_audit,
        "principal_create",
        json!({ "principal_id": principal_for_audit }),
    )?;
    Ok(PrincipalMutationResponse {
        revision,
        ..response
    })
}

pub fn update_principal(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    principal_id: String,
    request: UpdatePrincipalRequest,
    now_unix_secs: u64,
) -> Result<PrincipalMutationResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    let spec = request.spec;
    let principal_for_audit = principal_id.clone();
    let (revision, response) =
        apply_principal_change(Some(storage), current, now_unix_secs, move |principals| {
            let principals = principals_object(principals)?;
            if !principals.contains_key(&principal_id) {
                return Err(ManagementError::UnknownPrincipal);
            }
            principals.insert(principal_id.clone(), serde_json::to_value(&spec)?);
            Ok(PrincipalMutationResponse {
                revision: 0,
                principal_id: principal_id.clone(),
            })
        })?;
    append_admin_audit(
        storage,
        now_unix_secs,
        &principal_for_audit,
        "principal_update",
        json!({ "principal_id": principal_for_audit }),
    )?;
    Ok(PrincipalMutationResponse {
        revision,
        ..response
    })
}

pub fn set_principal_disabled(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    principal_id: String,
    disabled: bool,
    now_unix_secs: u64,
) -> Result<PrincipalMutationResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    let current_config = current.current_config();
    let current_spec = current_config.principals.get(&principal_id).cloned();
    let audit_kind = if disabled {
        "principal_disable"
    } else {
        "principal_enable"
    };
    let principal_for_audit = principal_id.clone();
    let (revision, response) =
        apply_principal_change(Some(storage), current, now_unix_secs, move |principals| {
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
        })?;
    append_admin_audit(
        storage,
        now_unix_secs,
        &principal_for_audit,
        audit_kind,
        json!({ "principal_id": principal_for_audit, "disabled": disabled }),
    )?;
    Ok(PrincipalMutationResponse {
        revision,
        ..response
    })
}

pub fn update_allowed_models(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    principal_id: String,
    request: AllowedModelsRequest,
    now_unix_secs: u64,
) -> Result<PrincipalAllowedModelsResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    let current_config = current.current_config();
    let current_spec = current_config.principals.get(&principal_id).cloned();
    let allowed_models = request.allowed_models;
    let principal_for_audit = principal_id.clone();
    let models_for_audit = allowed_models.len();
    let (revision, response) =
        apply_principal_change(Some(storage), current, now_unix_secs, move |principals| {
            let principals = principals_object(principals)?;
            ensure_principal_value(principals, &principal_id, current_spec.as_ref())?;
            let principal = principals
                .get_mut(&principal_id)
                .and_then(Value::as_object_mut)
                .ok_or(ManagementError::InvalidDraftPrincipal)?;
            principal.insert(
                "allowed_models".to_owned(),
                Value::Array(
                    allowed_models
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect::<Vec<_>>(),
                ),
            );
            Ok(PrincipalAllowedModelsResponse {
                revision: 0,
                principal_id: principal_id.clone(),
                allowed_models: allowed_models.clone(),
            })
        })?;
    append_admin_audit(
        storage,
        now_unix_secs,
        &principal_for_audit,
        "principal_allowed_models_update",
        json!({ "principal_id": principal_for_audit, "allowed_models_count": models_for_audit }),
    )?;
    Ok(PrincipalAllowedModelsResponse {
        revision,
        ..response
    })
}

pub fn issue_principal_key(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    principal_id: String,
    request: IssueKeyRequest,
    now_unix_secs: u64,
) -> Result<IssueKeyResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    if !principal_exists_in_current_or_draft(storage, current, &principal_id)? {
        return Err(ManagementError::UnknownPrincipal);
    }
    let label = request.label;
    let issued = storage.issue_api_key(&principal_id, label.clone())?;
    let key_id = issued.key_id.clone();
    append_admin_audit(
        storage,
        now_unix_secs,
        &principal_id,
        "api_key_issue",
        json!({ "principal_id": principal_id.clone(), "key_id": key_id, "label": label }),
    )?;
    Ok(IssueKeyResponse {
        principal_id,
        key_id: issued.key_id,
        plaintext_key: issued.plaintext,
        issued_at_unix_secs: issued.issued_at_unix_secs,
    })
}

pub fn list_principal_keys(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    principal_id: String,
) -> Result<KeyListResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    if !principal_exists_in_current_or_draft(storage, current, &principal_id)? {
        return Err(ManagementError::UnknownPrincipal);
    }
    Ok(KeyListResponse {
        keys: storage.list_api_keys(&principal_id)?,
    })
}

pub fn revoke_principal_key(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    principal_id: String,
    key_id: String,
    now_unix_secs: u64,
) -> Result<RevokeKeyResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    if !principal_exists_in_current_or_draft(storage, current, &principal_id)? {
        return Err(ManagementError::UnknownPrincipal);
    }
    let record = storage.revoke_api_key(&principal_id, &key_id)?;
    let revoked_at_unix_secs = record
        .revoked_at_unix_secs
        .ok_or(ManagementError::UnknownApiKey)?;
    append_admin_audit(
        storage,
        now_unix_secs,
        &principal_id,
        "api_key_revoke",
        json!({ "principal_id": principal_id.clone(), "key_id": key_id }),
    )?;
    Ok(RevokeKeyResponse {
        key_id: record.key_id,
        revoked_at_unix_secs,
    })
}

pub fn list_credentials(
    storage: Option<&Storage>,
    config: &Config,
    now_unix_secs: u64,
) -> Result<CredentialsResponse, ManagementError> {
    let Some(storage) = storage else {
        return Ok(CredentialsResponse {
            credentials: Vec::new(),
            observed: false,
        });
    };

    let associations = associated_principals(config);
    let mut principal_ids = config.principals.keys().cloned().collect::<Vec<_>>();
    principal_ids.sort();
    let mut credentials = Vec::new();
    for principal_id in principal_ids {
        let principal = config
            .principals
            .get(&principal_id)
            .ok_or(ManagementError::UnknownPrincipal)?;
        if let Some(provider) = principal
            .credentials_ref
            .as_deref()
            .and_then(oauth_provider_from_credentials_ref)
        {
            let stored = storage.get_oauth(&principal_id, &provider)?;
            credentials.push(oauth_credential_entry(
                &principal_id,
                &provider,
                stored.as_ref(),
                association_for(
                    &associations,
                    &provider,
                    principal.credentials_ref.as_deref(),
                    &principal_id,
                ),
                now_unix_secs,
            ));
        }

        let keys = storage.list_api_keys(&principal_id)?;
        if !keys.is_empty() || api_key_credentials_ref(principal).is_some() {
            let provider = api_key_provider(principal);
            credentials.push(api_key_credential_entry(
                &principal_id,
                &provider,
                &keys,
                association_for(
                    &associations,
                    &provider,
                    principal.credentials_ref.as_deref(),
                    &principal_id,
                ),
            ));
        }
    }

    credentials.sort_by(|left, right| {
        left.principal_id
            .cmp(&right.principal_id)
            .then_with(|| left.provider.cmp(&right.provider))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    Ok(CredentialsResponse {
        credentials,
        observed: true,
    })
}

pub fn rotate_credential(
    storage: Option<&Storage>,
    config: &Config,
    principal_id: String,
    provider: String,
    now_unix_secs: u64,
) -> Result<RotateCredentialResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    let principal = config
        .principals
        .get(&principal_id)
        .ok_or(ManagementError::UnknownPrincipal)?;
    if credential_kind(principal, &provider) == CredentialKind::Oauth {
        return Err(ManagementError::RotateUnsupported { kind: "oauth" });
    }

    let old_key_id = storage
        .list_api_keys(&principal_id)?
        .into_iter()
        .filter(|record| record.revoked_at_unix_secs.is_none())
        .max_by(|left, right| {
            left.issued_at_unix_secs
                .cmp(&right.issued_at_unix_secs)
                .then_with(|| left.key_id.cmp(&right.key_id))
        })
        .map(|record| record.key_id);
    let issued = storage.issue_api_key(&principal_id, Some(format!("rotate:{provider}")))?;
    if let Some(key_id) = old_key_id.as_deref() {
        storage.revoke_api_key(&principal_id, key_id)?;
    }
    let new_key_id = issued.key_id.clone();
    append_admin_audit(
        storage,
        now_unix_secs,
        &principal_id,
        "credential_rotate",
        json!({
            "principal_id": principal_id.clone(),
            "provider": provider.clone(),
            "kind": "api_key",
            "new_key_id": new_key_id,
            "revoked_key_id": old_key_id.clone(),
        }),
    )?;
    Ok(RotateCredentialResponse {
        principal_id,
        provider,
        kind: "api_key".to_owned(),
        new_key_id: issued.key_id,
        revoked_key_id: old_key_id,
        plaintext_key: issued.plaintext,
        issued_at_unix_secs: issued.issued_at_unix_secs,
    })
}

pub fn revoke_credential(
    storage: Option<&Storage>,
    config: &Config,
    principal_id: String,
    provider: String,
    now_unix_secs: u64,
) -> Result<RevokeCredentialResponse, ManagementError> {
    let storage = storage.ok_or(ManagementError::StorageUnavailable)?;
    let principal = config
        .principals
        .get(&principal_id)
        .ok_or(ManagementError::UnknownPrincipal)?;
    match credential_kind(principal, &provider) {
        CredentialKind::Oauth => {
            storage.delete_oauth(&principal_id, &provider)?;
            append_admin_audit(
                storage,
                now_unix_secs,
                &principal_id,
                "credential_revoke",
                json!({ "principal_id": principal_id.clone(), "provider": provider.clone(), "kind": "oauth" }),
            )?;
            Ok(RevokeCredentialResponse {
                principal_id,
                provider,
                kind: "oauth".to_owned(),
                revoked_keys: Vec::new(),
            })
        }
        CredentialKind::ApiKey => {
            let active_keys = storage
                .list_api_keys(&principal_id)?
                .into_iter()
                .filter(|record| record.revoked_at_unix_secs.is_none())
                .map(|record| record.key_id)
                .collect::<Vec<_>>();
            for key_id in &active_keys {
                storage.revoke_api_key(&principal_id, key_id)?;
            }
            append_admin_audit(
                storage,
                now_unix_secs,
                &principal_id,
                "credential_revoke",
                json!({ "principal_id": principal_id.clone(), "provider": provider.clone(), "kind": "api_key", "revoked_keys": active_keys.clone() }),
            )?;
            Ok(RevokeCredentialResponse {
                principal_id,
                provider,
                kind: "api_key".to_owned(),
                revoked_keys: active_keys,
            })
        }
    }
}

fn apply_principal_change<T, F>(
    storage: Option<&Storage>,
    current: &dyn CurrentConfig,
    now_unix_secs: u64,
    transform: F,
) -> Result<(u64, T), ManagementError>
where
    F: FnMut(&mut Value) -> Result<T, ManagementError>,
{
    settings::apply_draft_principal_change(storage, current, now_unix_secs, transform)?
}

fn principals_object(
    principals: &mut Value,
) -> Result<&mut serde_json::Map<String, Value>, ManagementError> {
    principals
        .as_object_mut()
        .ok_or(ManagementError::InvalidDraftPrincipal)
}

fn ensure_principal_value(
    principals: &mut serde_json::Map<String, Value>,
    principal_id: &str,
    current_spec: Option<&PrincipalSpec>,
) -> Result<(), ManagementError> {
    if principals.contains_key(principal_id) {
        return Ok(());
    }
    let Some(current_spec) = current_spec else {
        return Err(ManagementError::UnknownPrincipal);
    };
    principals.insert(principal_id.to_owned(), serde_json::to_value(current_spec)?);
    Ok(())
}

fn principal_exists_in_current_or_draft(
    storage: &Storage,
    current: &dyn CurrentConfig,
    principal_id: &str,
) -> Result<bool, ManagementError> {
    if current
        .current_config()
        .principals
        .contains_key(principal_id)
    {
        return Ok(true);
    }
    let state = storage.get_config_draft()?;
    Ok(state
        .draft
        .as_ref()
        .and_then(|draft| draft.get("principals"))
        .and_then(Value::as_object)
        .is_some_and(|principals| principals.contains_key(principal_id)))
}

fn append_admin_audit(
    storage: &Storage,
    now_unix_secs: u64,
    principal_id: &str,
    kind: &str,
    payload: Value,
) -> Result<(), ManagementError> {
    storage.append_audit(&AuditEntry {
        ts: now_unix_secs,
        request_id: format!("admin_{kind}_{principal_id}_{now_unix_secs}"),
        principal_id: principal_id.to_owned(),
        route: kind.to_owned(),
        upstream: "admin".to_owned(),
        model: None,
        status: 200,
        input_tokens: 0,
        output_tokens: 0,
        duration_ms: 0,
        agent_label: None,
        kind: Some(kind.to_owned()),
        payload: Some(payload),
    })?;
    Ok(())
}

fn oauth_credential_entry(
    principal_id: &str,
    provider: &str,
    stored: Option<&cc_lb_storage_redb::OAuthCredentials>,
    associated_principals: Vec<String>,
    now_unix_secs: u64,
) -> CredentialEntry {
    let expires_at = stored.map(|creds| creds.expires_at);
    CredentialEntry {
        principal_id: principal_id.to_owned(),
        provider: provider.to_owned(),
        kind: "oauth".to_owned(),
        identity: format!("{principal_id}/{provider}"),
        associated_principals,
        has_credentials: stored.is_some(),
        expires_at_unix_secs: expires_at,
        status: oauth_status_label(stored.is_some(), expires_at, now_unix_secs).to_owned(),
    }
}

fn api_key_credential_entry(
    principal_id: &str,
    provider: &str,
    keys: &[ApiKeyRecord],
    associated_principals: Vec<String>,
) -> CredentialEntry {
    let active_count = keys
        .iter()
        .filter(|record| record.revoked_at_unix_secs.is_none())
        .count();
    let status = if active_count > 0 {
        "valid"
    } else if keys
        .iter()
        .any(|record| record.revoked_at_unix_secs.is_some())
    {
        "revoked"
    } else {
        "missing"
    };
    CredentialEntry {
        principal_id: principal_id.to_owned(),
        provider: provider.to_owned(),
        kind: "api_key".to_owned(),
        identity: format!("{principal_id}/{provider}"),
        associated_principals,
        has_credentials: active_count > 0,
        expires_at_unix_secs: None,
        status: status.to_owned(),
    }
}

fn associated_principals(config: &Config) -> HashMap<(String, String), Vec<String>> {
    let mut associations: HashMap<(String, String), Vec<String>> = HashMap::new();
    for (principal_id, principal) in &config.principals {
        let Some(reference) = principal.credentials_ref.as_deref() else {
            continue;
        };
        let provider = oauth_provider_from_credentials_ref(reference)
            .unwrap_or_else(|| reference.trim().to_owned());
        associations
            .entry((provider, reference.trim().to_owned()))
            .or_default()
            .push(principal_id.clone());
    }
    for principals in associations.values_mut() {
        principals.sort();
    }
    associations
}

fn association_for(
    associations: &HashMap<(String, String), Vec<String>>,
    provider: &str,
    reference: Option<&str>,
    principal_id: &str,
) -> Vec<String> {
    reference
        .map(str::trim)
        .and_then(|reference| associations.get(&(provider.to_owned(), reference.to_owned())))
        .cloned()
        .unwrap_or_else(|| vec![principal_id.to_owned()])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CredentialKind {
    ApiKey,
    Oauth,
}

fn credential_kind(principal: &PrincipalSpec, provider: &str) -> CredentialKind {
    if principal
        .credentials_ref
        .as_deref()
        .and_then(oauth_provider_from_credentials_ref)
        .is_some_and(|oauth_provider| oauth_provider == provider)
        || provider == ANTHROPIC_OAUTH_PROVIDER
    {
        CredentialKind::Oauth
    } else {
        CredentialKind::ApiKey
    }
}

fn api_key_provider(principal: &PrincipalSpec) -> String {
    api_key_credentials_ref(principal).unwrap_or_else(|| DEFAULT_API_KEY_PROVIDER.to_owned())
}

fn api_key_credentials_ref(principal: &PrincipalSpec) -> Option<String> {
    principal
        .credentials_ref
        .as_deref()
        .map(str::trim)
        .filter(|reference| !reference.is_empty())
        .filter(|reference| oauth_provider_from_credentials_ref(reference).is_none())
        .map(ToOwned::to_owned)
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
