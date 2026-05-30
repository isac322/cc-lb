#![forbid(unsafe_code)]

mod http_client;
mod pkce;
mod refresh;
mod single_flight;

use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_plugin_api::{
    ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, Upstream, UpstreamError,
};
use cc_lb_storage_api::{OAuthCredentialStore, OAuthCredentials, StorageError, UpstreamStore};
use dashmap::DashMap;
use http::header::{AUTHORIZATION, HeaderValue};
use oauth2::ClientId;
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

const OAUTH_EXPIRY_SKEW_SECS: u64 = 30;

pub use http_client::{
    HyperOAuthHttpClient, OAuthHttpClient, OAuthHttpError, OAuthTokenRequest, OAuthTokenResponse,
};
pub use pkce::{PkceHandshake, PkceHandshakeState, complete_pkce_flow, start_pkce_flow};
pub use refresh::{
    BREAKER_FAILURE_THRESHOLD, BreakerMap, CircuitBreakerState, REFRESH_BUFFER_SECS, RefreshError,
};
pub use single_flight::{RefreshLocks, new_refresh_locks};

#[derive(Debug, thiserror::Error)]
pub enum LazyRefreshError {
    #[error("refresh failed: {reason}")]
    Failed { reason: String },
}

#[async_trait]
pub trait LazyRefreshHandle: Send + Sync {
    async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError>;
}

#[derive(Clone)]
pub struct AnthropicOAuthSignerFactoryWithLazyRefresh<Inner: SignerFactory + Clone> {
    inner: Inner,
    refresh_handle: Arc<dyn LazyRefreshHandle>,
    upstream_id: Uuid,
}

impl<Inner> AnthropicOAuthSignerFactoryWithLazyRefresh<Inner>
where
    Inner: SignerFactory + Clone,
{
    pub fn new(
        inner: Inner,
        refresh_handle: Arc<dyn LazyRefreshHandle>,
        upstream_id: Uuid,
    ) -> Self {
        Self {
            inner,
            refresh_handle,
            upstream_id,
        }
    }
}

impl<Inner> fmt::Debug for AnthropicOAuthSignerFactoryWithLazyRefresh<Inner>
where
    Inner: SignerFactory + Clone + fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicOAuthSignerFactoryWithLazyRefresh")
            .field("inner", &self.inner)
            .field("refresh_handle", &"LazyRefreshHandle")
            .field("upstream_id", &self.upstream_id)
            .finish()
    }
}

#[async_trait]
impl<Inner> SignerFactory for AnthropicOAuthSignerFactoryWithLazyRefresh<Inner>
where
    Inner: SignerFactory + Clone + Send + Sync + 'static,
{
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        match self.inner.build(upstream).await {
            Ok(signer) => Ok(Arc::new(LazyRefreshSigner {
                signer,
                inner: self.inner.clone(),
                refresh_handle: self.refresh_handle.clone(),
                upstream_id: self.upstream_id,
                upstream: upstream.clone(),
            })),
            Err(original @ SignerError::ExpiredToken { .. }) => {
                if self
                    .refresh_handle
                    .refresh_one(self.upstream_id)
                    .await
                    .is_err()
                {
                    return Err(original);
                }
                self.inner.build(upstream).await
            }
            Err(error) => Err(error),
        }
    }
}

struct LazyRefreshSigner<Inner: SignerFactory + Clone> {
    signer: Arc<dyn Signer>,
    inner: Inner,
    refresh_handle: Arc<dyn LazyRefreshHandle>,
    upstream_id: Uuid,
    upstream: Upstream,
}

impl<Inner> fmt::Debug for LazyRefreshSigner<Inner>
where
    Inner: SignerFactory + Clone,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LazyRefreshSigner")
            .field("signer", &"Signer")
            .field("refresh_handle", &"LazyRefreshHandle")
            .field("upstream_id", &self.upstream_id)
            .finish()
    }
}

#[async_trait]
impl<Inner> Signer for LazyRefreshSigner<Inner>
where
    Inner: SignerFactory + Clone + Send + Sync + 'static,
{
    async fn sign(
        &self,
        shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let retry_shape = shaped.clone();
        match self.signer.sign(shaped, capability).await {
            Ok(signed) => Ok(signed),
            Err(original @ SignerError::ExpiredToken { .. }) => {
                if self
                    .refresh_handle
                    .refresh_one(self.upstream_id)
                    .await
                    .is_err()
                {
                    return Err(original);
                }
                let refreshed = self.inner.build(&self.upstream).await?;
                refreshed.sign(retry_shape, capability).await
            }
            Err(error) => Err(error),
        }
    }

    async fn on_unauthorized(&self, err: &UpstreamError) -> RetryDecision {
        self.signer.on_unauthorized(err).await
    }
}

#[derive(Clone, Debug)]
pub struct AnthropicOAuthSharedState {
    pub http: Arc<dyn OAuthHttpClient>,
    pub refresh_locks: RefreshLocks,
    pub breaker_state: BreakerMap,
}

#[derive(Clone)]
pub struct AnthropicOAuthSigner {
    pub principal_id: String,
    pub provider: String,
    pub storage: Arc<dyn OAuthCredentialStore>,
    pub aead: Arc<AeadService>,
    pub refresh_locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
    pub breaker_state: BreakerMap,
    pub http: Arc<dyn OAuthHttpClient>,
    pub token_url: Url,
    pub client_id: ClientId,
    pub metrics_principal: String,
    pub metrics_provider: String,
    signed_access_token: Arc<Mutex<Option<SecretString>>>,
}

impl AnthropicOAuthSigner {
    pub fn new(
        principal_id: impl Into<String>,
        provider: impl Into<String>,
        storage: Arc<dyn OAuthCredentialStore>,
        aead: Arc<AeadService>,
        token_url: Url,
        client_id: ClientId,
    ) -> Self {
        Self::with_http(
            principal_id,
            provider,
            storage,
            aead,
            token_url,
            client_id,
            Arc::new(HyperOAuthHttpClient::new()),
        )
    }

    pub fn with_http(
        principal_id: impl Into<String>,
        provider: impl Into<String>,
        storage: Arc<dyn OAuthCredentialStore>,
        aead: Arc<AeadService>,
        token_url: Url,
        client_id: ClientId,
        http: Arc<dyn OAuthHttpClient>,
    ) -> Self {
        let principal_id = principal_id.into();
        let provider = provider.into();
        Self {
            metrics_principal: principal_id.clone(),
            metrics_provider: provider.clone(),
            principal_id,
            provider,
            storage,
            aead,
            refresh_locks: new_refresh_locks(),
            breaker_state: refresh::new_breaker_map(),
            http,
            token_url,
            client_id,
            signed_access_token: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_shared_state(
        principal_id: impl Into<String>,
        provider: impl Into<String>,
        storage: Arc<dyn OAuthCredentialStore>,
        aead: Arc<AeadService>,
        token_url: Url,
        client_id: ClientId,
        shared: AnthropicOAuthSharedState,
    ) -> Self {
        let principal_id = principal_id.into();
        let provider = provider.into();
        Self {
            metrics_principal: principal_id.clone(),
            metrics_provider: provider.clone(),
            principal_id,
            provider,
            storage,
            aead,
            refresh_locks: shared.refresh_locks,
            breaker_state: shared.breaker_state,
            http: shared.http,
            token_url,
            client_id,
            signed_access_token: Arc::new(Mutex::new(None)),
        }
    }

    async fn credentials_for_signing(&self) -> Result<OAuthCredentials, SignerError> {
        let creds = self
            .load_credentials()
            .await
            .map_err(refresh_error_to_signer)?
            .ok_or_else(|| SignerError::MissingCredentials {
                reason: "oauth credentials not found".to_owned(),
            })?;

        if refresh::is_expiring(&creds, now_epoch_secs()) {
            return self
                .refresh_under_single_flight(creds)
                .await
                .map_err(refresh_error_to_signer);
        }

        Ok(creds)
    }

    async fn refresh_under_single_flight(
        &self,
        fallback: OAuthCredentials,
    ) -> Result<OAuthCredentials, RefreshError> {
        let lock = single_flight::lock_for(&self.refresh_locks, &self.principal_id, &self.provider);
        let _guard = lock.lock().await;

        let current = self.load_credentials().await?.unwrap_or(fallback);
        let now = now_epoch_secs();
        if !refresh::is_expiring(&current, now) {
            return Ok(current);
        }

        self.refresh_current_credentials(&current, now).await
    }

    async fn on_unauthorized_refresh(&self) -> Result<OAuthCredentials, RefreshError> {
        let observed_access_token = self.signed_access_token.lock().await.clone();
        let lock = single_flight::lock_for(&self.refresh_locks, &self.principal_id, &self.provider);
        let _guard = lock.lock().await;
        let current = self
            .load_credentials()
            .await?
            .ok_or(RefreshError::MissingCredentials)?;

        if self.storage_has_newer_usable_token(&current, observed_access_token.as_ref()) {
            return Ok(current);
        }

        self.refresh_current_credentials(&current, now_epoch_secs())
            .await
    }

    fn storage_has_newer_usable_token(
        &self,
        current: &OAuthCredentials,
        observed_access_token: Option<&SecretString>,
    ) -> bool {
        if refresh::is_expiring(current, now_epoch_secs()) {
            return false;
        }

        observed_access_token
            .map(|token| token.expose_secret() != current.access_token)
            .unwrap_or(true)
    }

    async fn refresh_current_credentials(
        &self,
        current: &OAuthCredentials,
        now: u64,
    ) -> Result<OAuthCredentials, RefreshError> {
        let breaker = refresh::breaker_for(&self.breaker_state, &self.principal_id, &self.provider);
        if breaker.is_open() {
            return Err(RefreshError::CircuitOpen);
        }

        match refresh::refresh_credentials(
            self.http.as_ref(),
            &self.token_url,
            self.client_id.as_str(),
            current,
            now,
        )
        .await
        {
            Ok(new_creds) => {
                self.store_credentials(new_creds.clone()).await?;
                breaker.reset();
                refresh::increment_refresh_metric(
                    &self.metrics_principal,
                    &self.metrics_provider,
                    "success",
                );
                Ok(new_creds)
            }
            Err(error) => {
                breaker.record_failure();
                refresh::increment_refresh_metric(
                    &self.metrics_principal,
                    &self.metrics_provider,
                    "failure",
                );
                Err(error)
            }
        }
    }

    async fn load_credentials(&self) -> Result<Option<OAuthCredentials>, RefreshError> {
        let ciphertext = self
            .storage
            .get_oauth_ciphertext(&self.principal_id, &self.provider)
            .await?;
        let Some(ciphertext) = ciphertext else {
            return Ok(None);
        };

        let aad = oauth_credentials_aad(&self.principal_id, &self.provider);
        let plaintext = self
            .aead
            .decrypt(&ciphertext, &aad)
            .map_err(storage_aead_error)?;
        let creds = serde_json::from_slice(&plaintext).map_err(storage_json_error)?;
        Ok(Some(creds))
    }

    async fn store_credentials(&self, creds: OAuthCredentials) -> Result<(), RefreshError> {
        let plaintext = serde_json::to_vec(&creds).map_err(storage_json_error)?;
        let aad = oauth_credentials_aad(&self.principal_id, &self.provider);
        let ciphertext = self
            .aead
            .encrypt(&plaintext, &aad)
            .map_err(storage_aead_error)?;
        self.storage
            .put_oauth_ciphertext(&self.principal_id, &self.provider, &ciphertext)
            .await?;
        Ok(())
    }

    async fn remember_signed_access_token(&self, access_token: &str) {
        let mut signed = self.signed_access_token.lock().await;
        *signed = Some(SecretString::new(access_token.to_owned().into_boxed_str()));
    }
}

impl fmt::Debug for AnthropicOAuthSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicOAuthSigner")
            .field("principal_id", &self.principal_id)
            .field("provider", &self.provider)
            .field("storage", &"Storage")
            .field("refresh_locks", &"RefreshLocks")
            .field("breaker_state", &"BreakerMap")
            .field("http", &self.http)
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("metrics_principal", &self.metrics_principal)
            .field("metrics_provider", &self.metrics_provider)
            .field("signed_access_token", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl Signer for AnthropicOAuthSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let creds = self.credentials_for_signing().await?;
        let access_token = SecretString::new(creds.access_token.into_boxed_str());
        let header_value = bearer_header_value(access_token.expose_secret())?;
        shaped.headers_mut().insert(AUTHORIZATION, header_value);
        self.remember_signed_access_token(access_token.expose_secret())
            .await;
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        match self.on_unauthorized_refresh().await {
            Ok(creds) => {
                self.remember_signed_access_token(&creds.access_token).await;
                RetryDecision::Refresh {
                    new_signer: Arc::new(self.clone()),
                }
            }
            Err(_) => RetryDecision::Fail,
        }
    }
}

#[derive(Clone)]
pub struct AnthropicOAuthSignerFactory {
    store: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    upstream_name: Option<String>,
}

impl AnthropicOAuthSignerFactory {
    pub fn new(store: Arc<dyn UpstreamStore>, aead: Arc<AeadService>) -> Self {
        Self {
            store,
            aead,
            upstream_name: None,
        }
    }

    pub fn for_upstream_name(
        store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        upstream_name: impl Into<String>,
    ) -> Self {
        Self {
            aead,
            store,
            upstream_name: Some(upstream_name.into()),
        }
    }

    async fn load_tokens(&self, upstream: &Upstream) -> Result<OAuthTokenBundle, SignerError> {
        let record = if let Some(name) = &self.upstream_name {
            self.store
                .get_by_name(name)
                .await
                .map_err(storage_error_to_signer)?
                .ok_or_else(|| SignerError::MissingCredentials {
                    reason: "oauth upstream not found".to_owned(),
                })?
        } else {
            self.resolve_without_name(upstream).await?
        };

        if !record.enabled {
            return Err(SignerError::MissingCredentials {
                reason: "oauth upstream disabled".to_owned(),
            });
        }
        let encrypted =
            record
                .oauth_credentials
                .as_ref()
                .ok_or_else(|| SignerError::MissingCredentials {
                    reason: "oauth credentials not found".to_owned(),
                })?;
        encrypted
            .decrypt(&self.aead, record.id.as_bytes())
            .map_err(|source| SignerError::SigningFailed {
                reason: source.to_string(),
            })
    }

    async fn resolve_without_name(
        &self,
        _upstream: &Upstream,
    ) -> Result<cc_lb_storage_api::UpstreamRecord, SignerError> {
        let mut after = None;
        loop {
            let page = self
                .store
                .list(after, 100)
                .await
                .map_err(storage_error_to_signer)?;
            if page.is_empty() {
                return Err(SignerError::MissingCredentials {
                    reason: "oauth upstream not found".to_owned(),
                });
            }
            after = page.last().map(|record| record.id);
            if let Some(record) = page.into_iter().find(|record| {
                record.kind == cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth
                    && record.deleted_at_unix_secs.is_none()
            }) {
                return Ok(record);
            }
        }
    }
}

impl fmt::Debug for AnthropicOAuthSignerFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicOAuthSignerFactory")
            .field("store", &"UpstreamStore")
            .field("aead", &"AeadService")
            .field("upstream_name", &self.upstream_name)
            .finish()
    }
}

impl ApiKeyAwareSignerFactory for AnthropicOAuthSignerFactory {
    fn with_router_choice(
        &self,
        _api_key: String,
        router_chosen_upstream_name: String,
    ) -> Arc<dyn SignerFactory> {
        let mut factory = self.clone();
        factory.upstream_name = Some(router_chosen_upstream_name);
        Arc::new(factory)
    }
}

#[async_trait]
impl SignerFactory for AnthropicOAuthSignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let tokens = self.load_tokens(upstream).await?;
        Ok(Arc::new(PersistedAnthropicOAuthSigner {
            access_token: SecretString::new(tokens.access_token.into_boxed_str()),
            expires_at_unix_secs: tokens.expires_at_unix_secs,
        }))
    }
}

#[derive(Clone)]
struct PersistedAnthropicOAuthSigner {
    access_token: SecretString,
    expires_at_unix_secs: u64,
}

impl fmt::Debug for PersistedAnthropicOAuthSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistedAnthropicOAuthSigner")
            .field("access_token", &"[REDACTED]")
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .finish()
    }
}

#[async_trait]
impl Signer for PersistedAnthropicOAuthSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        if self.expires_at_unix_secs <= now_epoch_secs().saturating_add(OAUTH_EXPIRY_SKEW_SECS) {
            return Err(SignerError::ExpiredToken {
                reason: "oauth access token expired".to_owned(),
            });
        }
        let header_value = bearer_header_value(self.access_token.expose_secret())?;
        shaped.headers_mut().insert(AUTHORIZATION, header_value);
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

fn oauth_credentials_aad(principal_id: &str, provider: &str) -> Vec<u8> {
    format!("oauth:{principal_id}:{provider}").into_bytes()
}

fn storage_aead_error(source: cc_lb_aead::AeadError) -> RefreshError {
    RefreshError::Storage {
        source: StorageError::Aead(source.to_string()),
    }
}

fn storage_json_error(source: serde_json::Error) -> RefreshError {
    RefreshError::Storage {
        source: StorageError::Serialization(source),
    }
}

fn refresh_error_to_signer(error: RefreshError) -> SignerError {
    match error {
        RefreshError::MissingCredentials => SignerError::MissingCredentials {
            reason: "oauth credentials not found".to_owned(),
        },
        RefreshError::Storage {
            source: StorageError::Unavailable { .. },
        } => SignerError::StorageUnavailable {
            reason: "storage unavailable".to_owned(),
        },
        other => SignerError::SigningFailed {
            reason: other.to_string(),
        },
    }
}

fn storage_error_to_signer(source: StorageError) -> SignerError {
    if source.is_retryable() {
        SignerError::StorageUnavailable {
            reason: "storage unavailable".to_owned(),
        }
    } else {
        SignerError::SigningFailed {
            reason: source.to_string(),
        }
    }
}

fn bearer_header_value(token: &str) -> Result<HeaderValue, SignerError> {
    let mut value = Vec::with_capacity("Bearer ".len() + token.len());
    value.extend_from_slice(b"Bearer ");
    value.extend_from_slice(token.as_bytes());
    HeaderValue::from_bytes(&value).map_err(|source| SignerError::SigningFailed {
        reason: source.to_string(),
    })
}

fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use bytes::Bytes;
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_plugin_api::{
        RequestContext, Upstream, UpstreamDialect, shape_request, sign_request,
    };
    use cc_lb_storage_api::upstream::UpstreamKind;
    use cc_lb_storage_api::{
        StorageError, StorageResult, UpstreamCreate, UpstreamRecord, UpstreamRecordId,
        UpstreamStore, UpstreamUpdate, validate_identifier,
    };
    use http::header::{AUTHORIZATION, USER_AGENT};
    use http::{HeaderMap, HeaderValue, Method, StatusCode};
    use tokio::sync::Mutex;
    use url::Url;

    use super::*;

    #[derive(Default)]
    struct MemoryUpstreamStore {
        records: Mutex<Vec<UpstreamRecord>>,
    }

    #[async_trait]
    impl UpstreamStore for MemoryUpstreamStore {
        async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
            validate_identifier("upstream.name", &create.name)?;
            let mut records = self.records.lock().await;
            if records.iter().any(|record| record.name == create.name) {
                return Err(conflict("upstream name already exists"));
            }
            let now = now_secs();
            let record = UpstreamRecord {
                id: UpstreamRecordId::new_v4(),
                name: create.name,
                kind: create.kind,
                base_url: create.base_url,
                enabled: true,
                oauth_credentials: None,
                api_key_ciphertext: create.api_key_ciphertext,
                refresh_lease_holder: None,
                refresh_lease_until_unix_secs: None,
                last_apply_error: None,
                last_apply_at_unix_secs: None,
                deleted_at_unix_secs: None,
                revision: 1,
                created_at_unix_secs: now,
                updated_at_unix_secs: now,
            };
            records.push(record.clone());
            Ok(record)
        }

        async fn get_by_name(&self, name: &str) -> StorageResult<Option<UpstreamRecord>> {
            Ok(self
                .records
                .lock()
                .await
                .iter()
                .find(|record| record.name == name)
                .cloned())
        }

        async fn get_by_id(&self, id: UpstreamRecordId) -> StorageResult<Option<UpstreamRecord>> {
            Ok(self
                .records
                .lock()
                .await
                .iter()
                .find(|record| record.id == id)
                .cloned())
        }

        async fn list(
            &self,
            after: Option<UpstreamRecordId>,
            limit: usize,
        ) -> StorageResult<Vec<UpstreamRecord>> {
            let mut records = self.records.lock().await.clone();
            records.sort_by_key(|record| record.id);
            let start = after
                .and_then(|id| {
                    records
                        .iter()
                        .position(|record| record.id == id)
                        .map(|index| index + 1)
                })
                .unwrap_or(0);
            Ok(records.into_iter().skip(start).take(limit).collect())
        }

        async fn update(
            &self,
            id: UpstreamRecordId,
            expected_revision: u64,
            update: UpstreamUpdate,
        ) -> StorageResult<UpstreamRecord> {
            self.mutate(id, Some(expected_revision), |record| {
                if let Some(name) = update.name {
                    record.name = name;
                }
                if let Some(base_url) = update.base_url {
                    record.base_url = Some(base_url);
                }
                if let Some(api_key_ciphertext) = update.api_key_ciphertext {
                    record.api_key_ciphertext = Some(api_key_ciphertext);
                }
                Ok(())
            })
            .await
        }

        async fn set_enabled(
            &self,
            id: UpstreamRecordId,
            expected_revision: u64,
            enabled: bool,
        ) -> StorageResult<UpstreamRecord> {
            self.mutate(id, Some(expected_revision), |record| {
                record.enabled = enabled;
                Ok(())
            })
            .await
        }

        async fn store_oauth_tokens(
            &self,
            id: UpstreamRecordId,
            expected_revision: u64,
            tokens: EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            self.mutate(id, Some(expected_revision), |record| {
                record.oauth_credentials = Some(tokens);
                Ok(())
            })
            .await
        }

        async fn claim_refresh_lease(
            &self,
            _id: UpstreamRecordId,
            _holder: UpstreamRecordId,
            _ttl_secs: u64,
        ) -> StorageResult<bool> {
            Ok(false)
        }

        async fn complete_refresh(
            &self,
            id: UpstreamRecordId,
            _holder: UpstreamRecordId,
            tokens: EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            self.mutate(id, None, |record| {
                record.oauth_credentials = Some(tokens);
                Ok(())
            })
            .await
        }

        async fn release_lease_on_failure(
            &self,
            _id: UpstreamRecordId,
            _holder: UpstreamRecordId,
            _reason: String,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn set_last_apply_error(
            &self,
            _id: UpstreamRecordId,
            _error: Option<String>,
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn soft_delete(
            &self,
            id: UpstreamRecordId,
            expected_revision: u64,
        ) -> StorageResult<()> {
            self.mutate(id, Some(expected_revision), |record| {
                record.deleted_at_unix_secs = Some(now_secs());
                Ok(())
            })
            .await?;
            Ok(())
        }

        async fn hard_delete(&self, id: UpstreamRecordId) -> StorageResult<()> {
            self.records.lock().await.retain(|record| record.id != id);
            Ok(())
        }
    }

    impl MemoryUpstreamStore {
        async fn mutate<F>(
            &self,
            id: UpstreamRecordId,
            expected_revision: Option<u64>,
            mutate: F,
        ) -> StorageResult<UpstreamRecord>
        where
            F: FnOnce(&mut UpstreamRecord) -> StorageResult<()>,
        {
            let mut records = self.records.lock().await;
            let record = records
                .iter_mut()
                .find(|record| record.id == id)
                .ok_or_else(|| conflict("upstream not found"))?;
            if expected_revision.is_some_and(|expected| record.revision != expected) {
                return Err(conflict("stale upstream revision"));
            }
            mutate(record)?;
            record.revision += 1;
            record.updated_at_unix_secs = now_secs();
            Ok(record.clone())
        }
    }

    #[tokio::test]
    async fn build_missing_returns_missing_credentials() {
        let store = Arc::new(MemoryUpstreamStore::default());
        let factory = AnthropicOAuthSignerFactory::for_upstream_name(store, aead(), "missing");

        let error = build_error(&factory).await;

        assert!(matches!(error, SignerError::MissingCredentials { .. }));
    }

    #[tokio::test]
    async fn build_disabled_returns_missing_credentials() {
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs() + 600),
            )
            .await
            .unwrap();
        let record = store.get_by_name("primary").await.unwrap().unwrap();
        store
            .set_enabled(record.id, record.revision, false)
            .await
            .unwrap();
        let factory = AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary");

        let error = build_error(&factory).await;

        assert!(matches!(error, SignerError::MissingCredentials { .. }));
    }

    #[tokio::test]
    async fn build_expired_token_returns_expired_token_error() {
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs()),
            )
            .await
            .unwrap();
        let factory = AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary");
        let signer = factory.build(&Upstream::AnthropicDirect).await.unwrap();

        let error = sign_request(signer.as_ref(), shaped_request())
            .await
            .unwrap_err();

        assert!(matches!(error, SignerError::ExpiredToken { .. }));
    }

    #[tokio::test]
    async fn decryption_failure_with_wrong_aad_returns_aead_error() {
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        let wrong_record = create_upstream(&store, "wrong-aad").await;
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, wrong_record.id, now_secs() + 600),
            )
            .await
            .unwrap();
        let factory = AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary");

        let error = build_error(&factory).await;

        assert!(
            matches!(error, SignerError::SigningFailed { reason } if reason.contains("decryption failed"))
        );
    }

    #[tokio::test]
    async fn signer_sets_bearer_header_with_access_token() {
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs() + 600),
            )
            .await
            .unwrap();
        let factory = AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary");
        let signer = factory.build(&Upstream::AnthropicDirect).await.unwrap();

        let signed = sign_request(signer.as_ref(), shaped_request())
            .await
            .unwrap();

        assert_eq!(
            signed
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer access-token")
        );
    }

    async fn create_upstream(store: &MemoryUpstreamStore, name: &str) -> UpstreamRecord {
        store
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
            })
            .await
            .unwrap()
    }

    fn encrypted_tokens(
        aead: &AeadService,
        upstream_id: UpstreamRecordId,
        expires_at_unix_secs: u64,
    ) -> EncryptedOAuthTokens {
        EncryptedOAuthTokens::encrypt(
            aead,
            &OAuthTokenBundle {
                access_token: "access-token".to_owned(),
                refresh_token: "refresh-token".to_owned(),
                expires_at_unix_secs,
                scopes: vec!["messages".to_owned()],
            },
            upstream_id.as_bytes(),
        )
        .unwrap()
    }

    fn shaped_request() -> ShapedRequest {
        let ctx = RequestContext {
            request_id: "req-1".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::from_static(b"{}"),
        };
        let principal = cc_lb_plugin_api::Principal {
            id: "principal".to_owned(),
            kind: cc_lb_plugin_api::PrincipalKind::OAuthSubject,
            claims: serde_json::Map::new(),
        };
        shape_request(&DirectDialect, &ctx, &Upstream::AnthropicDirect, &principal).unwrap()
    }

    struct DirectDialect;

    impl UpstreamDialect for DirectDialect {
        fn shape(
            &self,
            _ctx: &RequestContext,
            _upstream: &Upstream,
            _principal: &cc_lb_plugin_api::Principal,
            builder: &mut cc_lb_plugin_api::ShapedRequestBuilder,
        ) -> Result<ShapedRequest, cc_lb_plugin_api::DialectError> {
            let mut headers = HeaderMap::new();
            headers.insert(USER_AGENT, HeaderValue::from_static("test-agent"));
            Ok(builder.shaped_request(
                Url::parse("https://api.anthropic.com/v1/messages").unwrap(),
                Method::POST,
                headers,
                Bytes::from_static(b"{}"),
            ))
        }

        fn normalize_error(&self, _status: StatusCode, _body: &Bytes) -> Option<Bytes> {
            None
        }
    }

    fn aead() -> Arc<AeadService> {
        Arc::new(AeadService::from_master_key([7; 32]))
    }

    async fn build_error(factory: &AnthropicOAuthSignerFactory) -> SignerError {
        match factory.build(&Upstream::AnthropicDirect).await {
            Ok(_) => panic!("expected signer build error"),
            Err(error) => error,
        }
    }

    fn conflict(message: &str) -> StorageError {
        StorageError::Conflict {
            message: message.to_owned(),
        }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}
