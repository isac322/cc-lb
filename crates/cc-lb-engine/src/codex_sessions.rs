//! Local session lifecycle behind the existing header authentication proof.

use std::sync::Arc;

use cc_lb_clock::{ClockHandle, unix_secs};
use cc_lb_control::api_keys::{
    builtin_authn::BuiltinAuthError,
    session_token::{self, Claims, MintedToken, RedactedToken},
};
use cc_lb_storage_api::{
    StorageError,
    account_owner::{AccountOwnerBinding, AccountOwnerStore, AccountOwnerWorkspaceTarget, OwnerBindingExpectation},
    codex_sessions::{CodexSessionStore, CredentialPurpose, ReplaceSessionAccess, SessionCredential, SessionFamily, SessionStoreError},
};
use crate::authn_rail::Authenticated;

/// Reserved provenance is never accepted from clients, providers, or plugins.
pub fn strip_untrusted_provenance(headers: &mut http::HeaderMap) {
    headers.remove("cc-lb-error-origin");
    headers.remove("cc-lb-auth-reason");
}

/// Call only on a response actually received from an authenticated dispatch.
pub fn mark_upstream_response(headers: &mut http::HeaderMap) {
    strip_untrusted_provenance(headers);
    headers.insert("cc-lb-error-origin", http::HeaderValue::from_static("upstream"));
}

pub fn session_error_response(
    error: SessionError,
    observer: Option<&crate::LifecycleContext>,
) -> http::Response<axum::body::Body> {
    let (status, origin, code) = match error {
        SessionError::Unauthenticated(error) => (error.http_status(), "principal", "session_rejected"),
        SessionError::OwnerConflict => (400, "proxy", "owner_conflict"),
        SessionError::InvalidRequest => (400, "proxy", "invalid_session_request"),
        SessionError::Unavailable => (503, "proxy", "session_unavailable"),
    };
    let mut response = http::Response::new(axum::body::Body::from(
        serde_json::json!({"error": code}).to_string(),
    ));
    *response.status_mut() = http::StatusCode::from_u16(status).expect("fixed HTTP status");
    response.headers_mut().insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/json"));
    response.headers_mut().insert("cc-lb-error-origin", http::HeaderValue::from_static(origin));
    let classification = crate::TerminalClassification::local(response.status());
    response.extensions_mut().insert(classification);
    if let Some(observer) = observer { observer.terminate_local(classification); }
    response
}

/// Apply this to Axum's registered matched path before any request body is polled.
pub fn authorize_registered_purpose(
    auth: &Authenticated,
    method: &http::Method,
    matched_path: &str,
) -> Result<(), BuiltinAuthError> {
    let required = match (method.as_str(), matched_path) {
        ("GET" | "POST", "/backend-api/codex/responses") => Some(CredentialPurpose::SessionAccess),
        ("POST", "/oauth/token" | "/oauth/revoke") => Some(CredentialPurpose::SessionRenewal),
        ("POST", "/backend-api/wham/remote/control/server/pair"
            | "/backend-api/wham/remote/control/server/pair/status")
        | ("GET", "/backend-api/wham/remote/control/server") => Some(CredentialPurpose::RemoteRelay),
        ("POST", "/backend-api/wham/remote/control/server/enroll"
            | "/backend-api/wham/remote/control/server/refresh")
        | ("GET", "/backend-api/wham/remote/control/environments/{environment_id}/clients")
        | ("DELETE", "/backend-api/wham/remote/control/environments/{environment_id}/clients/{client_id}") => Some(CredentialPurpose::SessionAccess),
        _ => None,
    };
    let allowed = match required {
        Some(required) => auth.purpose() == required,
        None => matches!(auth.purpose(), CredentialPurpose::PrincipalKey | CredentialPurpose::SessionAccess),
    };
    if allowed { Ok(()) } else { Err(BuiltinAuthError::InvalidFormat) }
}

/// A server-created owner fence, never constructed from caller claims.
#[derive(Clone)]
pub struct SessionOwnerConstraint {
    upstream_id: uuid::Uuid,
    principal_id: String,
    snapshot: cc_lb_storage_api::codex_sessions::SessionSnapshot,
    store: Arc<dyn CodexSessionStore>,
    clock: ClockHandle,
}

impl SessionOwnerConstraint {
    pub fn upstream_id(&self) -> uuid::Uuid { self.upstream_id }

    pub async fn validate(&self, auth: &Authenticated) -> Result<(), SessionError> {
        if auth.principal_id() != self.principal_id
            || auth.session() != Some(&self.snapshot)
        {
            return Err(SessionError::OwnerConflict);
        }
        self.store.fence_session_dispatch(
            &self.principal_id, &self.snapshot, unix_secs(self.clock.now()),
        ).await.map_err(Into::into)
    }
}

pub struct CodexSessionService {
    store: Arc<dyn CodexSessionStore>,
    owners: Arc<dyn AccountOwnerStore>,
    clock: ClockHandle,
}

pub struct SessionGrant {
    pub session_id: String,
    pub access_token: RedactedToken,
    pub renewal_token: Option<RedactedToken>,
    pub expires_at: u64,
    pub session_expires_at: u64,
    pub origin: String,
    pub account_id: String,
    pub user_id: String,
    pub workspace_generation: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionWorkspaceDiscovery {
    pub session_id: String,
    pub workspace_generation: u64,
    pub selected_workspace_id: String,
    pub workspaces: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error("session authentication failed: {0}")]
    Unauthenticated(BuiltinAuthError),
    #[error("session owner authority changed")]
    OwnerConflict,
    #[error("invalid session request")]
    InvalidRequest,
    #[error("session service unavailable")]
    Unavailable,
}

impl From<SessionStoreError> for SessionError {
    fn from(error: SessionStoreError) -> Self {
        match error {
            SessionStoreError::CredentialRejected => Self::Unauthenticated(BuiltinAuthError::KeyRevoked),
            SessionStoreError::PrincipalDisabled => Self::Unauthenticated(BuiltinAuthError::PrincipalDisabled),
            SessionStoreError::OwnerConflict => Self::OwnerConflict,
            SessionStoreError::InvalidOperation => Self::InvalidRequest,
            SessionStoreError::Storage(error) => map_owner_error(error),
        }
    }
}

fn map_owner_error(error: StorageError) -> SessionError {
    match error {
        StorageError::Conflict { .. } => SessionError::OwnerConflict,
        StorageError::InvalidInput { .. } => SessionError::InvalidRequest,
        _ => SessionError::Unavailable,
    }
}

impl CodexSessionService {
    pub async fn session_owner_constraint(
        &self,
        auth: &Authenticated,
    ) -> Result<SessionOwnerConstraint, SessionError> {
        let binding = self.fence_session_dispatch(auth).await?;
        Ok(SessionOwnerConstraint {
            upstream_id: binding.upstream_id,
            principal_id: auth.principal_id().to_owned(),
            snapshot: auth.session().ok_or(SessionError::InvalidRequest)?.clone(),
            store: self.store.clone(),
            clock: self.clock.clone(),
        })
    }

    pub fn new(
        store: Arc<dyn CodexSessionStore>,
        owners: Arc<dyn AccountOwnerStore>,
        clock: ClockHandle,
    ) -> Self {
        Self { store, owners, clock }
    }

    pub async fn renew_session(
        &self,
        auth: &Authenticated,
        expected_workspace_generation: u64,
    ) -> Result<SessionGrant, SessionError> {
        let family = self.family(auth, CredentialPurpose::SessionRenewal).await?;
        if family.workspace_generation != expected_workspace_generation {
            return Err(SessionError::InvalidRequest);
        }
        self.replace_access(auth, family, None).await
    }

    pub async fn switch_session_workspace(
        &self,
        auth: &Authenticated,
        target_workspace_id: &str,
    ) -> Result<SessionGrant, SessionError> {
        if target_workspace_id.is_empty() {
            return Err(SessionError::InvalidRequest);
        }
        let family = self.family(auth, CredentialPurpose::SessionAccess).await?;
        let binding = self.owners.find_owner_binding_for_workspace(&AccountOwnerWorkspaceTarget {
            principal_id: family.principal_id.clone(),
            provider: family.provider.clone(),
            local_user_id: family.local_user_id.clone(),
            workspace_id: target_workspace_id.to_owned(),
        }).await.map_err(map_owner_error)?;
        let target = OwnerBindingExpectation {
            binding_id: binding.binding_id,
            principal_id: family.principal_id.clone(),
            provider: family.provider.clone(),
            owner_generation: binding.owner_generation,
            binding_revision: binding.revision,
            workspace_id: target_workspace_id.to_owned(),
        };
        // This lookup only locates the candidate. Storage locks and validates both
        // bindings with the source family in the replacement transaction.
        self.replace_access(auth, family, Some(target)).await
    }
    pub async fn list_session_workspaces(
        &self,
        auth: &Authenticated,
    ) -> Result<SessionWorkspaceDiscovery, SessionError> {
        let family = self.family(auth, CredentialPurpose::SessionAccess).await?;
        let mut workspaces = self.owners.list_owner_workspace_targets(
            &family.principal_id,
            &family.provider,
            &family.local_user_id,
        ).await.map_err(map_owner_error)?;
        workspaces.sort();
        workspaces.dedup();
        if !workspaces.iter().any(|workspace| workspace == &family.workspace_id) {
            return Err(SessionError::OwnerConflict);
        }
        Ok(SessionWorkspaceDiscovery {
            session_id: family.family_id,
            workspace_generation: family.workspace_generation,
            selected_workspace_id: family.workspace_id,
            workspaces,
        })
    }

    pub async fn revoke_session(&self, auth: &Authenticated) -> Result<(), SessionError> {
        let family = self.family(auth, CredentialPurpose::SessionRenewal).await?;
        let snapshot = auth.session().ok_or(SessionError::InvalidRequest)?;
        self.store.revoke_session_family(
            &family.family_id,
            &snapshot.credential_id,
            unix_secs(self.clock.now()),
        ).await.map_err(Into::into)
    }

    pub async fn fence_session_dispatch(
        &self,
        auth: &Authenticated,
    ) -> Result<AccountOwnerBinding, SessionError> {
        let snapshot = auth.session().ok_or(SessionError::InvalidRequest)?;
        if snapshot.purpose != CredentialPurpose::SessionAccess {
            return Err(SessionError::Unauthenticated(BuiltinAuthError::InvalidFormat));
        }
        self.store.fence_session_dispatch(auth.principal_id(), snapshot, unix_secs(self.clock.now())).await?;
        self.owners.resolve_owner_binding(&OwnerBindingExpectation {
            binding_id: snapshot.owner_binding_id.clone(),
            principal_id: auth.principal_id().to_owned(),
            provider: snapshot.provider.clone(),
            owner_generation: snapshot.owner_generation,
            binding_revision: snapshot.binding_revision,
            workspace_id: snapshot.workspace_id.clone(),
        }).await.map_err(map_owner_error)
    }

    async fn family(
        &self,
        auth: &Authenticated,
        purpose: CredentialPurpose,
    ) -> Result<SessionFamily, SessionError> {
        let snapshot = auth.session().ok_or(SessionError::Unauthenticated(BuiltinAuthError::InvalidFormat))?;
        if snapshot.purpose != purpose {
            return Err(SessionError::Unauthenticated(BuiltinAuthError::InvalidFormat));
        }
        let family = self.store.get_session_family(&snapshot.family_id).await?
            .ok_or(SessionError::Unauthenticated(BuiltinAuthError::NotFound))?;
        if family.principal_id != auth.principal_id()
            || family.parent_key_id != auth.key_id()
            || family.workspace_generation != snapshot.workspace_generation
        {
            return Err(SessionError::Unauthenticated(BuiltinAuthError::KeyRevoked));
        }
        Ok(family)
    }

    async fn replace_access(
        &self,
        auth: &Authenticated,
        family: SessionFamily,
        target_owner: Option<OwnerBindingExpectation>,
    ) -> Result<SessionGrant, SessionError> {
        let snapshot = auth.session().ok_or(SessionError::InvalidRequest)?;
        let now = unix_secs(self.clock.now());
        let expires_at = now.checked_add(family.access_ttl_secs)
            .ok_or(SessionError::InvalidRequest)?.min(family.expires_at);
        if expires_at <= now {
            return Err(SessionError::Unauthenticated(BuiltinAuthError::KeyRevoked));
        }
        let workspace_generation = if target_owner.is_some() {
            family.workspace_generation.checked_add(1).ok_or(SessionError::InvalidRequest)?
        } else { family.workspace_generation };
        let access_generation = family.access_generation.checked_add(1).ok_or(SessionError::InvalidRequest)?;
        let workspace_id = target_owner.as_ref().map_or(family.workspace_id.as_str(), |target| target.workspace_id.as_str());
        let access_id = uuid::Uuid::new_v4().to_string();
        let minted = session_token::mint_access(&Claims {
            issuer: family.origin.clone(),
            local_user_id: family.local_user_id.clone(),
            workspace_id: workspace_id.to_owned(),
            family_id: family.family_id.clone(),
            access_id: access_id.clone(),
            issued_at: now,
            expires_at,
            plan: if target_owner.is_some() { "unknown".to_owned() } else { family.plan_type.clone() },
        }).map_err(|_| SessionError::InvalidRequest)?;
        let access = credential(&minted, access_id, &family.family_id, workspace_generation, access_generation, now, expires_at);
        let updated = self.store.replace_session_access(ReplaceSessionAccess {
            expected: family,
            authorizing_credential_id: snapshot.credential_id.clone(),
            access,
            target_owner,
        }, now).await?;
        Ok(SessionGrant {
            session_id: updated.family_id,
            access_token: minted.plaintext,
            renewal_token: None,
            expires_at,
            session_expires_at: updated.expires_at,
            origin: updated.origin,
            account_id: updated.workspace_id,
            user_id: updated.local_user_id,
            workspace_generation: updated.workspace_generation,
        })
    }
}

fn credential(
    token: &MintedToken,
    credential_id: String,
    family_id: &str,
    workspace_generation: u64,
    access_generation: u64,
    issued_at: u64,
    expires_at: u64,
) -> SessionCredential {
    SessionCredential {
        credential_id,
        family_id: family_id.to_owned(),
        purpose: CredentialPurpose::SessionAccess,
        index_hash: token.index_hash,
        verify_hash: token.verify_hash,
        salt: token.secret_salt,
        workspace_generation,
        access_generation,
        issued_at,
        expires_at,
        revoked_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_cannot_assert_local_expiry() {
        let mut headers = http::HeaderMap::new();
        headers.append("cc-lb-error-origin", http::HeaderValue::from_static("principal"));
        headers.append("cc-lb-error-origin", http::HeaderValue::from_static("renewal"));
        headers.insert("cc-lb-auth-reason", http::HeaderValue::from_static("expired"));
        headers.insert("retry-after", http::HeaderValue::from_static("7"));
        headers.insert("x-request-id", http::HeaderValue::from_static("provider-request"));
        mark_upstream_response(&mut headers);
        assert_eq!(headers.get_all("cc-lb-error-origin").iter().count(), 1);
        assert_eq!(headers["cc-lb-error-origin"], "upstream");
        assert!(!headers.contains_key("cc-lb-auth-reason"));
        assert_eq!(headers["retry-after"], "7");
        assert_eq!(headers["x-request-id"], "provider-request");
    }

    #[test]
    fn client_provenance_is_not_forwarded() {
        let mut headers = http::HeaderMap::new();
        headers.insert("cc-lb-error-origin", http::HeaderValue::from_static("principal"));
        headers.insert("cc-lb-auth-reason", http::HeaderValue::from_static("expired"));
        headers.insert("content-type", http::HeaderValue::from_static("application/json"));
        strip_untrusted_provenance(&mut headers);
        assert!(!headers.contains_key("cc-lb-error-origin"));
        assert!(!headers.contains_key("cc-lb-auth-reason"));
        assert_eq!(headers["content-type"], "application/json");
    }
}
