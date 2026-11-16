//! Operator-only bootstrap of local Codex credentials.
use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use cc_lb_control::api_keys::session_token::{self, Claims, MintedToken};
use cc_lb_storage_api::{
    StorageError,
    account_owner::AccountOwnerStore,
    codex_sessions::{CodexSessionStore, CredentialPurpose, IssueSession, SessionCredential, SessionFamily, SessionStoreError},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{AdminState, audit::{AdminAuditEvent, record_admin_audit}, auth::AdminIdentity};
use super::post_commit::{PostCommitAborted, complete_after_commit};

pub fn router() -> Router<AdminState> {
    Router::new().route("/admin/v1/principals/{id}/codex-sessions", post(issue_session))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IssueRequest {
    parent_key_id: String,
    owner_binding_id: String,
    workspace_id: String,
    origin: String,
    access_ttl_seconds: u64,
    session_ttl_seconds: u64,
}

// Deliberately no Debug: these two fields are bearer plaintext.
#[derive(Serialize)]
struct IssueResponse<'a> {
    session_id: &'a str,
    workspace_generation: u64,
    access_token: &'a str,
    renewal_token: &'a str,
    token_type: &'static str,
    expires_at: u64,
    session_expires_at: u64,
    origin: &'a str,
    account_id: &'a str,
    user_id: &'a str,
}

async fn issue_session(
    State(state): State<AdminState>,
    Extension(identity): Extension<AdminIdentity>,
    Path(principal_id): Path<String>,
    Json(request): Json<IssueRequest>,
) -> Response {
    // The enclosing require_admin_auth layer checks Write permission before
    // Axum polls JSON. Never mount this router outside that protected layer.
    let Some(storage) = state.storage.clone() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable");
    };
    let now = cc_lb_clock::unix_secs(state.clock.now());
    let Some((origin, access_expires_at, session_expires_at)) = validate_request(&request, now) else {
        return error(StatusCode::BAD_REQUEST, "invalid_session_request");
    };
    // This read locates server-owned metadata, not authority. issue_session
    // revalidates principal, parent and this owner revision in its transaction.
    let binding = match AccountOwnerStore::get_owner_binding(storage.as_ref(), &request.owner_binding_id).await {
        Ok(Some(binding)) => binding,
        Ok(None) => return error(StatusCode::BAD_REQUEST, "owner_binding_not_found"),
        Err(source) => return storage_error(source),
    };
    if binding.principal_id != principal_id || binding.revoked_at_unix_secs.is_some()
        || !binding.allowed_workspaces.contains(&request.workspace_id)
    {
        return error(StatusCode::BAD_REQUEST, "owner_binding_conflict");
    }
    let family_id = uuid::Uuid::new_v4().to_string();
    let access_id = uuid::Uuid::new_v4().to_string();
    let renewal_id = uuid::Uuid::new_v4().to_string();
    let family = SessionFamily {
        family_id,
        principal_id: principal_id.clone(),
        parent_key_id: request.parent_key_id,
        owner_binding_id: binding.binding_id,
        provider: binding.provider,
        owner_generation: binding.owner_generation,
        binding_revision: binding.revision,
        workspace_id: request.workspace_id,
        workspace_generation: 1,
        access_generation: 1,
        local_user_id: binding.local_user_id,
        // Binding has no authenticated plan metadata.
        plan_type: "unknown".to_owned(),
        origin,
        access_ttl_secs: request.access_ttl_seconds,
        created_at: now,
        expires_at: session_expires_at,
        revoked_at: None,
    };
    let access = match session_token::mint_access(&Claims {
        issuer: family.origin.clone(), local_user_id: family.local_user_id.clone(),
        workspace_id: family.workspace_id.clone(), family_id: family.family_id.clone(),
        access_id: access_id.clone(), issued_at: now, expires_at: access_expires_at,
        plan: family.plan_type.clone(),
    }) {
        Ok(access) => access,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid_session_metadata"),
    };
    let renewal = session_token::mint_renewal();
    let command = IssueSession {
        access: credential(&access, access_id.clone(), &family, CredentialPurpose::SessionAccess, access_expires_at),
        renewal: credential(&renewal, renewal_id.clone(), &family, CredentialPurpose::SessionRenewal, session_expires_at),
        family: family.clone(),
    };
    // Own the write as well as the continuation so cancellation cannot land
    // between transaction commit and scheduling audit/compensation.
    match complete_after_commit(async move {
        if let Err(source) = CodexSessionStore::issue_session(storage.as_ref(), command).await {
            return session_error(source);
        }
        let route = format!("/admin/v1/principals/{principal_id}/codex-sessions");
        if record_admin_audit(&state, AdminAuditEvent {
            identity: Some(&identity), system_component: None,
            action: "codex_session_issue", route: &route,
            target_principal_id: Some(&principal_id), target_upstream: None,
            api_key_id: Some(&family.parent_key_id), status: StatusCode::CREATED.as_u16(),
            payload: Some(json!({"session_id": family.family_id, "owner_binding_id": family.owner_binding_id,
                "workspace_id": family.workspace_id, "access_credential_id": access_id,
                "renewal_credential_id": renewal_id})),
        }).await.is_err() {
            if CodexSessionStore::revoke_session_family(storage.as_ref(), &family.family_id, &renewal_id, cc_lb_clock::unix_secs(state.clock.now())).await.is_err() {
                tracing::error!(session_id = %family.family_id, "session issuance audit and compensation failed");
            }
            return error(StatusCode::INTERNAL_SERVER_ERROR, "audit_write_failed");
        }
        let body = IssueResponse {
            session_id: &family.family_id, workspace_generation: family.workspace_generation,
            access_token: access.plaintext.expose(), renewal_token: renewal.plaintext.expose(),
            token_type: "Bearer", expires_at: access_expires_at,
            session_expires_at: family.expires_at, origin: &family.origin,
            account_id: &family.workspace_id, user_id: &family.local_user_id,
        };
        let mut response = (StatusCode::CREATED, Json(body)).into_response();
        response.headers_mut().insert(header::CACHE_CONTROL, "no-store".parse().expect("static header"));
        response
    }).await {
        Ok(response) => response,
        Err(PostCommitAborted) => error(StatusCode::INTERNAL_SERVER_ERROR, "post_commit_aborted"),
    }
}

fn validate_request(request: &IssueRequest, now: u64) -> Option<(String, u64, u64)> {
    if request.parent_key_id.is_empty() || request.owner_binding_id.is_empty() || request.workspace_id.is_empty()
        || request.access_ttl_seconds == 0 || request.access_ttl_seconds > request.session_ttl_seconds
    { return None; }
    let access = now.checked_add(request.access_ttl_seconds)?;
    let session = now.checked_add(request.session_ttl_seconds)?;
    // Both supported durable adapters store signed BIGINT/INTEGER seconds.
    if session > i64::MAX as u64 { return None; }
    let parsed = url::Url::parse(&request.origin).ok()?;
    if parsed.scheme() != "https" || parsed.host_str().is_none()
        || !parsed.username().is_empty() || parsed.password().is_some()
        || parsed.query().is_some() || parsed.fragment().is_some() || parsed.path() != "/"
    { return None; }
    // URL parsing can erase empty userinfo and normalize path traversal. Reject
    // them from the input too; accepted input is an origin, not a URL reference.
    let authority = request.origin.strip_prefix("https://")?;
    if authority.contains('@') || authority.contains('\\') || authority.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return None;
    }
    let (_, path) = authority.split_once('/').unwrap_or((authority, ""));
    if !path.is_empty() { return None; }
    Some((parsed.origin().ascii_serialization(), access, session))
}

fn credential(token: &MintedToken, credential_id: String, family: &SessionFamily, purpose: CredentialPurpose, expires_at: u64) -> SessionCredential {
    SessionCredential {
        credential_id, family_id: family.family_id.clone(), purpose,
        index_hash: token.index_hash, verify_hash: token.verify_hash, salt: token.secret_salt,
        workspace_generation: 1, access_generation: 1, issued_at: family.created_at,
        expires_at, revoked_at: None,
    }
}

fn storage_error(source: StorageError) -> Response {
    match source {
        StorageError::Conflict { .. } | StorageError::InvalidInput { .. } => error(StatusCode::BAD_REQUEST, "owner_binding_conflict"),
        _ => error(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable"),
    }
}

fn session_error(source: SessionStoreError) -> Response {
    match source {
        SessionStoreError::CredentialRejected => error(StatusCode::UNAUTHORIZED, "parent_key_rejected"),
        SessionStoreError::PrincipalDisabled => error(StatusCode::FORBIDDEN, "principal_disabled"),
        SessionStoreError::OwnerConflict => error(StatusCode::BAD_REQUEST, "owner_binding_conflict"),
        SessionStoreError::InvalidOperation => error(StatusCode::BAD_REQUEST, "invalid_session_request"),
        SessionStoreError::Storage(source) => storage_error(source),
    }
}

fn error(status: StatusCode, code: &'static str) -> Response {
    (status, Json(json!({"error": code}))).into_response()
}
