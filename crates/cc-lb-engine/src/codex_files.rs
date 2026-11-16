//! Owner-bound Codex Files lifecycle domain.
//!
//! This module owns no authentication or routes. Callers supply the authenticated
//! owner-binding expectation produced by C-AUTH/F-OWNER. Every protected operation
//! resolves that expectation before consulting the durable Files ledger.

use std::fmt;

use crate::authn_rail::Authenticated;
use cc_lb_storage_api::{
    AccountOwnerStore, CodexFileResource, CodexFileStore, OwnerBindingExpectation, StorageError,
    StorageResult,
};
use url::Url;

/// A failure before a provider response could be relayed.
///
/// Signer and transport error text is deliberately not exposed: it may contain
/// credentials or URLs. Session errors retain the existing authentication path.
#[derive(Debug)]
pub enum FilesDispatchError {
    Session(crate::codex_sessions::SessionError),
    OwnerUnavailable,
    SigningFailed,
    DispatchFailed,
}

/// Relay Files bytes to the current session owner without routing or retries.
pub async fn dispatch(
    lifecycle: &crate::Lifecycle,
    auth: &Authenticated,
    owner: &crate::codex_sessions::SessionOwnerConstraint,
    request: http::Request<bytes::Bytes>,
    observer: Option<&crate::LifecycleContext>,
) -> Result<http::Response<axum::body::Body>, FilesDispatchError> {
    lifecycle.dispatch_codex_file(auth, owner, request, observer).await
}

/// Finish the already-buffered provider response without inventing usage.
pub fn complete_response(
    _auth: &Authenticated,
    status: http::StatusCode,
    body_bytes: usize,
    observer: Option<&crate::LifecycleContext>,
) {
    if let Some(observer) = observer {
        observer.emit_lifecycle(cc_lb_lifecycle::LifecycleEvent::StreamCompleted {
            event_id: observer.event_id().to_owned(),
            result: Ok(cc_lb_lifecycle::StreamSuccess {
                body_bytes: Some(body_bytes as u64),
                ..Default::default()
            }),
        });
        if status.is_client_error() {
            observer.set_upstream_error(status, crate::terminal_observer::UpstreamErrorCode::Upstream4xx);
        } else if status.is_server_error() {
            observer.set_upstream_error(status, crate::terminal_observer::UpstreamErrorCode::Upstream5xx);
        } else {
            observer.set_success_status(status);
        }
        observer.finish();
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SignedUrlMetadata {
    url: String,
    origin: String,
}

impl fmt::Debug for SignedUrlMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignedUrlMetadata")
            .field("origin", &self.origin)
            .field("url", &"[redacted]")
            .finish()
    }
}

impl SignedUrlMetadata {
    pub fn parse(url: impl Into<String>) -> StorageResult<Self> {
        let url = url.into();
        let parsed = Url::parse(&url).map_err(|error| StorageError::Conflict {
            message: format!("signed file URL is invalid: {error}"),
        })?;
        if parsed.scheme() != "https"
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(StorageError::Conflict {
                message: "signed file URL must be HTTPS without userinfo or fragment".into(),
            });
        }
        let origin = parsed.origin().ascii_serialization();
        Ok(Self { url, origin })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }
}

pub struct CodexFiles<'a, S: ?Sized> {
    storage: &'a S,
}

impl<'a, S: ?Sized> CodexFiles<'a, S>
where
    S: AccountOwnerStore + CodexFileStore,
{
    pub const fn new(storage: &'a S) -> Self {
        Self { storage }
    }

    pub async fn create_success(
        &self,
        auth: &Authenticated,
        expected: &OwnerBindingExpectation,
        file_id: String,
        kind: String,
        created_at_unix_secs: u64,
        expires_at_unix_secs: Option<u64>,
    ) -> StorageResult<CodexFileResource> {
        ensure_principal(auth, expected)?;
        let binding = self.storage.resolve_owner_binding(expected).await?;
        let resource = CodexFileResource::bind(
            file_id,
            kind,
            &binding,
            expected.workspace_id.clone(),
            created_at_unix_secs,
            expires_at_unix_secs,
        );
        self.storage
            .create_codex_file(expected, resource.clone())
            .await?;
        Ok(resource)
    }

    pub async fn finalize(
        &self,
        auth: &Authenticated,
        expected: &OwnerBindingExpectation,
        kind: &str,
        file_id: &str,
        reference: String,
        expires_at_unix_secs: Option<u64>,
    ) -> StorageResult<Option<CodexFileResource>> {
        ensure_principal(auth, expected)?;
        self.storage.resolve_owner_binding(expected).await?;
        self.storage
            .finalize_codex_file(expected, kind, file_id, reference, expires_at_unix_secs)
            .await
    }

    pub async fn reference(
        &self,
        auth: &Authenticated,
        expected: &OwnerBindingExpectation,
        kind: &str,
        file_id: &str,
    ) -> StorageResult<Option<CodexFileResource>> {
        ensure_principal(auth, expected)?;
        self.storage.resolve_owner_binding(expected).await?;
        self.storage
            .resolve_codex_file(expected, kind, file_id)
            .await
    }
}

fn ensure_principal(auth: &Authenticated, expected: &OwnerBindingExpectation) -> StorageResult<()> {
    if auth.principal_id() != expected.principal_id {
        return Err(StorageError::Conflict {
            message: "file owner principal differs from authenticated principal".into(),
        });
    }
    Ok(())
}
