use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use cc_lb_aead::AeadService;
use cc_lb_storage_api::{OAuthCredentials, PrincipalRecord, PrincipalStore, Storage};
use serde::Serialize;
use serde_json::json;

use crate::AdminState;

const ANTHROPIC_PROVIDER: &str = "anthropic";
const STATUS_ACTIVE: &str = "active";
const STATUS_EXPIRED: &str = "expired";
const STATUS_MISSING: &str = "missing";
const STATUS_CORRUPTED: &str = "corrupted";

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/admin/credentials", get(list_credentials))
        .route("/admin/oauth/status", get(list_oauth_status))
        .route(
            "/admin/credentials/{principal_id}/{provider}/rotate",
            post(rotate_credential),
        )
        .route(
            "/admin/credentials/{principal_id}/{provider}/revoke",
            post(revoke_credential),
        )
}

#[derive(Debug, Serialize)]
struct CredentialsResponse {
    credentials: Vec<CredentialEntry>,
    observed: bool,
}

#[derive(Debug, Serialize)]
struct CredentialEntry {
    principal_id: String,
    provider: String,
    kind: String,
    identity: String,
    associated_principals: Vec<String>,
    has_credentials: bool,
    expires_at_unix_secs: Option<u64>,
    status: String,
}

#[derive(Debug, Serialize)]
struct OAuthStatusResponse {
    credentials: Vec<OAuthStatusEntry>,
    observed: bool,
}

#[derive(Debug, Serialize)]
struct OAuthStatusEntry {
    principal_id: String,
    provider: String,
    has_credentials: bool,
    expires_at_unix_secs: Option<u64>,
    refresh_token_present: bool,
    last_updated_unix_secs: Option<u64>,
    status: String,
    scopes: Vec<String>,
}

async fn list_credentials(State(state): State<AdminState>) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let principals = match list_all_principals(storage.as_ref()).await {
        Ok(records) => records,
        Err(error) => {
            tracing::error!(%error, "credentials principal list failed");
            return internal_error("storage_error");
        }
    };
    let now = now_unix_secs();
    let mut credentials = Vec::new();
    for principal in &principals {
        let principal_id = principal.id.to_string();
        let lookup = load_oauth(&state.aead, storage.as_ref(), &principal_id).await;
        let entry = build_credential_entry(principal, principal_id, lookup, now);
        if entry.has_credentials {
            credentials.push(entry);
        }
    }
    let observed = !credentials.is_empty();
    Json(CredentialsResponse {
        credentials,
        observed,
    })
    .into_response()
}

async fn list_oauth_status(State(state): State<AdminState>) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let principals = match list_all_principals(storage.as_ref()).await {
        Ok(records) => records,
        Err(error) => {
            tracing::error!(%error, "oauth status principal list failed");
            return internal_error("storage_error");
        }
    };
    let now = now_unix_secs();
    let mut credentials = Vec::new();
    for principal in &principals {
        let principal_id = principal.id.to_string();
        let lookup = load_oauth(&state.aead, storage.as_ref(), &principal_id).await;
        match lookup {
            OAuthLookup::Found(creds) => {
                credentials.push(OAuthStatusEntry {
                    principal_id,
                    provider: ANTHROPIC_PROVIDER.to_owned(),
                    has_credentials: true,
                    expires_at_unix_secs: Some(creds.expires_at),
                    refresh_token_present: !creds.refresh_token.is_empty(),
                    last_updated_unix_secs: principal.last_apply_at_unix_secs,
                    status: status_for_expiry(Some(creds.expires_at), now),
                    scopes: creds.scopes,
                });
            }
            OAuthLookup::Missing => {}
            OAuthLookup::Corrupted => {
                credentials.push(OAuthStatusEntry {
                    principal_id,
                    provider: ANTHROPIC_PROVIDER.to_owned(),
                    has_credentials: true,
                    expires_at_unix_secs: None,
                    refresh_token_present: false,
                    last_updated_unix_secs: principal.last_apply_at_unix_secs,
                    status: STATUS_CORRUPTED.to_owned(),
                    scopes: Vec::new(),
                });
            }
        }
    }
    let observed = !credentials.is_empty();
    Json(OAuthStatusResponse {
        credentials,
        observed,
    })
    .into_response()
}

async fn revoke_credential(
    State(state): State<AdminState>,
    Path((principal_id, provider)): Path<(String, String)>,
) -> Response {
    let Some(storage) = state.storage.as_ref() else {
        return service_unavailable("storage_unavailable");
    };
    let deleted = match storage.delete_oauth(&principal_id, &provider).await {
        Ok(deleted) => deleted,
        Err(error) => {
            tracing::error!(%error, "delete_oauth failed");
            return internal_error("storage_error");
        }
    };
    let revoked_keys = if deleted {
        vec![format!("{provider}:oauth")]
    } else {
        Vec::new()
    };
    Json(json!({
        "principal_id": principal_id,
        "provider": provider,
        "kind": "oauth",
        "revoked_keys": revoked_keys,
    }))
    .into_response()
}

async fn rotate_credential(
    State(_state): State<AdminState>,
    Path((principal_id, provider)): Path<(String, String)>,
) -> Response {
    // OAuth credentials cannot be rotated without re-running the PKCE flow.
    // Anthropic API key rotation isn't yet wired through this path either.
    // Surface a structured 501 so the SPA can show a meaningful message
    // instead of a generic "Not Found".
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "rotate_unsupported",
            "principal_id": principal_id,
            "provider": provider,
            "message": "OAuth rotation requires the upstream PKCE flow; trigger it from the Upstreams page.",
        })),
    )
        .into_response()
}

enum OAuthLookup {
    Found(OAuthCredentials),
    Missing,
    Corrupted,
}

async fn load_oauth(aead: &AeadService, storage: &dyn Storage, principal_id: &str) -> OAuthLookup {
    let ciphertext = match storage
        .get_oauth_ciphertext(principal_id, ANTHROPIC_PROVIDER)
        .await
    {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return OAuthLookup::Missing,
        Err(error) => {
            tracing::warn!(%error, principal_id, "get_oauth_ciphertext failed");
            return OAuthLookup::Missing;
        }
    };
    let aad = format!("oauth:{principal_id}:{ANTHROPIC_PROVIDER}").into_bytes();
    let plaintext = match aead.decrypt(&ciphertext, &aad) {
        Ok(plaintext) => plaintext,
        Err(error) => {
            tracing::warn!(%error, principal_id, "oauth credential decrypt failed");
            return OAuthLookup::Corrupted;
        }
    };
    match serde_json::from_slice::<OAuthCredentials>(&plaintext) {
        Ok(creds) => OAuthLookup::Found(creds),
        Err(error) => {
            tracing::warn!(%error, principal_id, "oauth credential decode failed");
            OAuthLookup::Corrupted
        }
    }
}

fn build_credential_entry(
    principal: &PrincipalRecord,
    principal_id: String,
    lookup: OAuthLookup,
    now: u64,
) -> CredentialEntry {
    match lookup {
        OAuthLookup::Found(creds) => CredentialEntry {
            principal_id: principal_id.clone(),
            provider: ANTHROPIC_PROVIDER.to_owned(),
            kind: "oauth".to_owned(),
            identity: principal.name.clone(),
            associated_principals: vec![principal_id],
            has_credentials: true,
            expires_at_unix_secs: Some(creds.expires_at),
            status: status_for_expiry(Some(creds.expires_at), now),
        },
        OAuthLookup::Missing => CredentialEntry {
            principal_id: principal_id.clone(),
            provider: ANTHROPIC_PROVIDER.to_owned(),
            kind: "oauth".to_owned(),
            identity: principal.name.clone(),
            associated_principals: vec![principal_id],
            has_credentials: false,
            expires_at_unix_secs: None,
            status: STATUS_MISSING.to_owned(),
        },
        OAuthLookup::Corrupted => CredentialEntry {
            principal_id: principal_id.clone(),
            provider: ANTHROPIC_PROVIDER.to_owned(),
            kind: "oauth".to_owned(),
            identity: principal.name.clone(),
            associated_principals: vec![principal_id],
            has_credentials: true,
            expires_at_unix_secs: None,
            status: STATUS_CORRUPTED.to_owned(),
        },
    }
}

fn status_for_expiry(expires_at: Option<u64>, now: u64) -> String {
    match expires_at {
        Some(expiry) if expiry <= now => STATUS_EXPIRED.to_owned(),
        Some(_) => STATUS_ACTIVE.to_owned(),
        None => STATUS_MISSING.to_owned(),
    }
}

async fn list_all_principals(
    storage: &dyn Storage,
) -> Result<Vec<PrincipalRecord>, cc_lb_storage_api::StorageError> {
    let mut all = Vec::new();
    let mut offset = 0usize;
    let page_size = 200usize;
    loop {
        let page = PrincipalStore::list(storage, offset, page_size, false).await?;
        if page.is_empty() {
            break;
        }
        let len = page.len();
        all.extend(page);
        offset += len;
        if len < page_size {
            break;
        }
    }
    Ok(all)
}

fn service_unavailable(error: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": error })),
    )
        .into_response()
}

fn internal_error(error: &str) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": error })),
    )
        .into_response()
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
