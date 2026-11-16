//! Owner-bound Codex Files lifecycle domain.
//!
//! This module owns no authentication or routes. Callers supply the authenticated
//! owner-binding expectation produced by C-AUTH/F-OWNER. Every protected operation
//! resolves that expectation before consulting the durable Files ledger.

use std::fmt;

use cc_lb_storage_api::{
    AccountOwnerStore, CodexFileResource, CodexFileStore, OwnerBindingExpectation, StorageError,
    StorageResult,
};
use url::Url;

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
            .field("requires_proxy_fetch", &false)
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
            || parsed.username().is_empty() == false
            || parsed.password().is_some()
            || parsed.fragment().is_some()
        {
            return Err(StorageError::Conflict {
                message: "signed file URL must be HTTPS without userinfo or fragment".into(),
            });
        }
        let origin = match parsed.port() {
            Some(port) => format!("{}://{}:{}", parsed.scheme(), parsed.host_str().unwrap_or_default(), port),
            None => format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or_default()),
        };
        Ok(Self { url, origin })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn origin(&self) -> &str {
        &self.origin
    }

    pub const fn requires_proxy_fetch(&self) -> bool {
        false
    }

    pub const fn sends_credentials(&self) -> bool {
        false
    }
}

pub struct CodexFiles<'a, S> {
    storage: &'a S,
}

impl<'a, S> CodexFiles<'a, S>
where
    S: AccountOwnerStore + CodexFileStore,
{
    pub const fn new(storage: &'a S) -> Self {
        Self { storage }
    }

    pub async fn create_success(
        &self,
        expected: &OwnerBindingExpectation,
        file_id: String,
        kind: String,
        created_at_unix_secs: u64,
        expires_at_unix_secs: Option<u64>,
    ) -> StorageResult<CodexFileResource> {
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
        expected: &OwnerBindingExpectation,
        file_id: &str,
        reference: String,
        expires_at_unix_secs: Option<u64>,
    ) -> StorageResult<CodexFileResource> {
        self.storage.resolve_owner_binding(expected).await?;
        self.storage
            .finalize_codex_file(expected, file_id, reference, expires_at_unix_secs)
            .await
    }

    pub async fn reference(
        &self,
        expected: &OwnerBindingExpectation,
        file_id: &str,
    ) -> StorageResult<CodexFileResource> {
        self.storage.resolve_owner_binding(expected).await?;
        self.storage.resolve_codex_file(expected, file_id).await
    }
}
