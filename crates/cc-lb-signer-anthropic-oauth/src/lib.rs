#![forbid(unsafe_code)]

mod http_client;
mod pkce;
mod refresh;
mod single_flight;

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_clock::{Clock, ClockHandle, unix_secs};
use cc_lb_domain::Upstream;
use cc_lb_plugin_api::{
    ApiKeyAwareSignerFactory, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, UpstreamError,
};
use cc_lb_storage_api::{
    OAuthCredentialStore, OAuthCredentials, StorageError, UpstreamRecord, UpstreamStore,
};
use dashmap::DashMap;
use http::HeaderMap;
use http::header::{AUTHORIZATION, HeaderValue};
use oauth2::ClientId;
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

const OAUTH_EXPIRY_SKEW_SECS: u64 = 30;
const ANTHROPIC_OAUTH_BETA: &str = "oauth-2025-04-20";

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

    async fn enqueue_only(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
        self.refresh_one(upstream_id).await
    }
}

pub trait LazyRefreshConfigurable: SignerFactory + Clone {
    fn with_lazy_refresh(
        &self,
        refresh_handle: Arc<dyn LazyRefreshHandle>,
        upstream_id: Uuid,
        refresh_locks: RefreshLocks,
    ) -> Self;
}

#[derive(Clone)]
pub struct AnthropicOAuthSignerFactoryWithLazyRefresh<Inner: LazyRefreshConfigurable + Clone> {
    inner: Inner,
    refresh_handle: Arc<dyn LazyRefreshHandle>,
    refresh_locks: RefreshLocks,
    upstream_id: Uuid,
}

impl<Inner> AnthropicOAuthSignerFactoryWithLazyRefresh<Inner>
where
    Inner: LazyRefreshConfigurable + Clone,
{
    pub fn new(
        inner: Inner,
        refresh_handle: Arc<dyn LazyRefreshHandle>,
        upstream_id: Uuid,
    ) -> Self {
        Self {
            inner,
            refresh_handle,
            refresh_locks: new_refresh_locks(),
            upstream_id,
        }
    }
}

impl<Inner> fmt::Debug for AnthropicOAuthSignerFactoryWithLazyRefresh<Inner>
where
    Inner: LazyRefreshConfigurable + Clone + fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicOAuthSignerFactoryWithLazyRefresh")
            .field("inner", &self.inner)
            .field("refresh_handle", &"LazyRefreshHandle")
            .field("refresh_locks", &"RefreshLocks")
            .field("upstream_id", &self.upstream_id)
            .finish()
    }
}

#[async_trait]
impl<Inner> SignerFactory for AnthropicOAuthSignerFactoryWithLazyRefresh<Inner>
where
    Inner: LazyRefreshConfigurable + Clone + Send + Sync + 'static,
{
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let inner = self.inner.with_lazy_refresh(
            self.refresh_handle.clone(),
            self.upstream_id,
            self.refresh_locks.clone(),
        );
        match inner.build(upstream).await {
            Ok(signer) => Ok(Arc::new(LazyRefreshSigner {
                signer,
                inner,
                refresh_handle: self.refresh_handle.clone(),
                refresh_locks: self.refresh_locks.clone(),
                upstream_id: self.upstream_id,
                upstream: upstream.clone(),
            })),
            Err(original @ SignerError::ExpiredToken { .. }) => {
                if lazy_refresh_under_single_flight(
                    &self.refresh_locks,
                    &self.refresh_handle,
                    self.upstream_id,
                )
                .await
                .is_err()
                {
                    return Err(original);
                }
                inner.build(upstream).await
            }
            Err(error) => Err(error),
        }
    }
}

struct LazyRefreshSigner<Inner: LazyRefreshConfigurable + Clone> {
    signer: Arc<dyn Signer>,
    inner: Inner,
    refresh_handle: Arc<dyn LazyRefreshHandle>,
    refresh_locks: RefreshLocks,
    upstream_id: Uuid,
    upstream: Upstream,
}

impl<Inner> fmt::Debug for LazyRefreshSigner<Inner>
where
    Inner: LazyRefreshConfigurable + Clone,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LazyRefreshSigner")
            .field("signer", &"Signer")
            .field("refresh_handle", &"LazyRefreshHandle")
            .field("refresh_locks", &"RefreshLocks")
            .field("upstream_id", &self.upstream_id)
            .finish()
    }
}

#[async_trait]
impl<Inner> Signer for LazyRefreshSigner<Inner>
where
    Inner: LazyRefreshConfigurable + Clone + Send + Sync + 'static,
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
                if lazy_refresh_under_single_flight(
                    &self.refresh_locks,
                    &self.refresh_handle,
                    self.upstream_id,
                )
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

async fn lazy_refresh_under_single_flight(
    refresh_locks: &RefreshLocks,
    refresh_handle: &Arc<dyn LazyRefreshHandle>,
    upstream_id: Uuid,
) -> Result<(), LazyRefreshError> {
    let upstream_key = upstream_id.to_string();
    let lock = single_flight::lock_for(refresh_locks, "upstream", &upstream_key);
    let _guard = lock.lock().await;
    refresh_handle.refresh_one(upstream_id).await
}

#[derive(Clone, Debug)]
pub struct AnthropicOAuthSharedState {
    pub http: Arc<dyn OAuthHttpClient>,
    pub refresh_locks: RefreshLocks,
    pub breaker_state: BreakerMap,
}

pub struct AnthropicOAuthSignerHttpParams {
    pub principal_id: String,
    pub provider: String,
    pub storage: Arc<dyn OAuthCredentialStore>,
    pub aead: Arc<AeadService>,
    pub token_url: Url,
    pub client_id: ClientId,
    pub http: Arc<dyn OAuthHttpClient>,
    pub clock: ClockHandle,
}

pub struct AnthropicOAuthSignerSharedParams {
    pub principal_id: String,
    pub provider: String,
    pub storage: Arc<dyn OAuthCredentialStore>,
    pub aead: Arc<AeadService>,
    pub token_url: Url,
    pub client_id: ClientId,
    pub shared: AnthropicOAuthSharedState,
    pub clock: ClockHandle,
}

#[derive(Clone)]
pub struct AnthropicOAuthSigner {
    pub principal_id: String,
    pub provider: String,
    pub storage: Arc<dyn OAuthCredentialStore>,
    pub aead: Arc<AeadService>,
    pub clock: ClockHandle,
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
        clock: ClockHandle,
    ) -> Self {
        Self::with_http(AnthropicOAuthSignerHttpParams {
            principal_id: principal_id.into(),
            provider: provider.into(),
            storage,
            aead,
            token_url,
            client_id,
            http: Arc::new(HyperOAuthHttpClient::new()),
            clock,
        })
    }

    pub fn with_http(params: AnthropicOAuthSignerHttpParams) -> Self {
        Self {
            metrics_principal: params.principal_id.clone(),
            metrics_provider: params.provider.clone(),
            principal_id: params.principal_id,
            provider: params.provider,
            storage: params.storage,
            aead: params.aead,
            clock: params.clock,
            refresh_locks: new_refresh_locks(),
            breaker_state: refresh::new_breaker_map(),
            http: params.http,
            token_url: params.token_url,
            client_id: params.client_id,
            signed_access_token: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_shared_state(params: AnthropicOAuthSignerSharedParams) -> Self {
        Self {
            metrics_principal: params.principal_id.clone(),
            metrics_provider: params.provider.clone(),
            principal_id: params.principal_id,
            provider: params.provider,
            storage: params.storage,
            aead: params.aead,
            clock: params.clock,
            refresh_locks: params.shared.refresh_locks,
            breaker_state: params.shared.breaker_state,
            http: params.shared.http,
            token_url: params.token_url,
            client_id: params.client_id,
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

        if refresh::is_expiring(&creds, unix_secs(self.clock.now())) {
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
        let now = unix_secs(self.clock.now());
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

        self.refresh_current_credentials(&current, unix_secs(self.clock.now()))
            .await
    }

    fn storage_has_newer_usable_token(
        &self,
        current: &OAuthCredentials,
        observed_access_token: Option<&SecretString>,
    ) -> bool {
        if refresh::is_expiring(current, unix_secs(self.clock.now())) {
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
        let headers = shaped.headers_mut();
        headers.remove("x-api-key");
        headers.insert(AUTHORIZATION, header_value);
        merge_anthropic_beta(headers)?;
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
    clock: ClockHandle,
    upstream_name: Option<String>,
    refresh_handle: Option<Arc<dyn LazyRefreshHandle>>,
    refresh_locks: RefreshLocks,
    refresh_upstream_id: Option<Uuid>,
    allow_disabled_upstream: bool,
}

impl AnthropicOAuthSignerFactory {
    pub fn new(store: Arc<dyn UpstreamStore>, aead: Arc<AeadService>, clock: ClockHandle) -> Self {
        Self {
            store,
            aead,
            clock,
            upstream_name: None,
            refresh_handle: None,
            refresh_locks: new_refresh_locks(),
            refresh_upstream_id: None,
            allow_disabled_upstream: false,
        }
    }

    pub fn for_upstream_name(
        store: Arc<dyn UpstreamStore>,
        aead: Arc<AeadService>,
        upstream_name: impl Into<String>,
        clock: ClockHandle,
    ) -> Self {
        Self {
            aead,
            store,
            clock,
            upstream_name: Some(upstream_name.into()),
            refresh_handle: None,
            refresh_locks: new_refresh_locks(),
            refresh_upstream_id: None,
            allow_disabled_upstream: false,
        }
    }

    #[must_use]
    pub fn allow_disabled_upstream(mut self, allow: bool) -> Self {
        self.allow_disabled_upstream = allow;
        self
    }

    async fn load_record_and_tokens(
        &self,
        upstream: &Upstream,
    ) -> Result<(UpstreamRecord, OAuthTokenBundle), SignerError> {
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

        ensure_oauth_upstream_usable(&record, self.allow_disabled_upstream)?;
        let tokens = decrypt_upstream_tokens(&self.aead, &record)?;
        Ok((record, tokens))
    }

    async fn resolve_without_name(
        &self,
        _upstream: &Upstream,
    ) -> Result<UpstreamRecord, SignerError> {
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
            .field("clock", &"Clock")
            .field("upstream_name", &self.upstream_name)
            .field(
                "refresh_handle",
                &self.refresh_handle.as_ref().map(|_| "LazyRefreshHandle"),
            )
            .field("refresh_locks", &"RefreshLocks")
            .field("refresh_upstream_id", &self.refresh_upstream_id)
            .finish()
    }
}

impl LazyRefreshConfigurable for AnthropicOAuthSignerFactory {
    fn with_lazy_refresh(
        &self,
        refresh_handle: Arc<dyn LazyRefreshHandle>,
        upstream_id: Uuid,
        refresh_locks: RefreshLocks,
    ) -> Self {
        let mut factory = self.clone();
        factory.refresh_handle = Some(refresh_handle);
        factory.refresh_locks = refresh_locks;
        factory.refresh_upstream_id = Some(upstream_id);
        factory
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
        let (record, tokens) = self.load_record_and_tokens(upstream).await?;
        Ok(Arc::new(PersistedAnthropicOAuthSigner {
            access_token: SecretString::new(tokens.access_token.into_boxed_str()),
            expires_at_unix_secs: tokens.expires_at_unix_secs,
            store: self.store.clone(),
            aead: self.aead.clone(),
            clock: self.clock.clone(),
            upstream_id: self.refresh_upstream_id.unwrap_or(record.id),
            refresh_handle: self.refresh_handle.clone(),
            refresh_locks: self.refresh_locks.clone(),
            allow_disabled_upstream: self.allow_disabled_upstream,
        }))
    }
}

#[derive(Clone)]
struct PersistedAnthropicOAuthSigner {
    access_token: SecretString,
    expires_at_unix_secs: u64,
    store: Arc<dyn UpstreamStore>,
    aead: Arc<AeadService>,
    clock: ClockHandle,
    upstream_id: Uuid,
    refresh_handle: Option<Arc<dyn LazyRefreshHandle>>,
    refresh_locks: RefreshLocks,
    allow_disabled_upstream: bool,
}

impl fmt::Debug for PersistedAnthropicOAuthSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PersistedAnthropicOAuthSigner")
            .field("access_token", &"[REDACTED]")
            .field("expires_at_unix_secs", &self.expires_at_unix_secs)
            .field("store", &"UpstreamStore")
            .field("aead", &"AeadService")
            .field("clock", &"Clock")
            .field("upstream_id", &self.upstream_id)
            .field(
                "refresh_handle",
                &self.refresh_handle.as_ref().map(|_| "LazyRefreshHandle"),
            )
            .field("refresh_locks", &"RefreshLocks")
            .finish()
    }
}

impl PersistedAnthropicOAuthSigner {
    async fn on_unauthorized_refresh(&self) -> Result<OAuthTokenBundle, SignerError> {
        let upstream_key = self.upstream_id.to_string();
        let lock = single_flight::lock_for(&self.refresh_locks, "upstream", &upstream_key);
        let _guard = lock.lock().await;

        let current = self.load_current_tokens().await?;
        if self.storage_has_newer_usable_token(&current) {
            return Ok(current);
        }

        let refresh_handle =
            self.refresh_handle
                .as_ref()
                .ok_or_else(|| SignerError::SigningFailed {
                    reason: "oauth refresh handle unavailable".to_owned(),
                })?;
        refresh_handle
            .refresh_one(self.upstream_id)
            .await
            .map_err(|source| SignerError::SigningFailed {
                reason: source.to_string(),
            })?;

        let refreshed = self.load_current_tokens().await?;
        if persisted_token_is_usable(refreshed.expires_at_unix_secs, self.clock.as_ref()) {
            Ok(refreshed)
        } else {
            Err(SignerError::ExpiredToken {
                reason: "oauth access token expired".to_owned(),
            })
        }
    }

    async fn load_current_tokens(&self) -> Result<OAuthTokenBundle, SignerError> {
        let record = self
            .store
            .get_by_id(self.upstream_id)
            .await
            .map_err(storage_error_to_signer)?
            .ok_or_else(|| SignerError::MissingCredentials {
                reason: "oauth upstream not found".to_owned(),
            })?;
        ensure_oauth_upstream_usable(&record, self.allow_disabled_upstream)?;
        decrypt_upstream_tokens(&self.aead, &record)
    }

    fn storage_has_newer_usable_token(&self, current: &OAuthTokenBundle) -> bool {
        persisted_token_is_usable(current.expires_at_unix_secs, self.clock.as_ref())
            && current.access_token != self.access_token.expose_secret()
    }

    fn with_tokens(&self, tokens: OAuthTokenBundle) -> Self {
        Self {
            access_token: SecretString::new(tokens.access_token.into_boxed_str()),
            expires_at_unix_secs: tokens.expires_at_unix_secs,
            store: self.store.clone(),
            aead: self.aead.clone(),
            clock: self.clock.clone(),
            upstream_id: self.upstream_id,
            refresh_handle: self.refresh_handle.clone(),
            refresh_locks: self.refresh_locks.clone(),
            allow_disabled_upstream: self.allow_disabled_upstream,
        }
    }

    fn maybe_trigger_soft_refresh(&self, now_unix_secs: u64) {
        if self
            .expires_at_unix_secs
            .saturating_sub(refresh::REFRESH_SOFT_BUFFER_SECS)
            > now_unix_secs
        {
            return;
        }
        let Some(handle) = self.refresh_handle.clone() else {
            return;
        };
        let lock = single_flight::lock_for(
            &self.refresh_locks,
            "soft_refresh",
            &self.upstream_id.to_string(),
        );
        let Ok(guard) = lock.try_lock_owned() else {
            return;
        };
        let upstream_id = self.upstream_id;
        tokio::spawn(async move {
            let _guard = guard;
            if let Err(error) = handle.enqueue_only(upstream_id).await {
                tracing::warn!(
                    upstream_id = %upstream_id,
                    error = %error,
                    "oauth soft-window background refresh enqueue failed",
                );
            }
        });
    }
}

#[async_trait]
impl Signer for PersistedAnthropicOAuthSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let now = unix_secs(self.clock.now());
        if self.expires_at_unix_secs <= now.saturating_add(OAUTH_EXPIRY_SKEW_SECS) {
            return Err(SignerError::ExpiredToken {
                reason: "oauth access token expired".to_owned(),
            });
        }
        self.maybe_trigger_soft_refresh(now);
        let header_value = bearer_header_value(self.access_token.expose_secret())?;
        let headers = shaped.headers_mut();
        headers.remove("x-api-key");
        headers.insert(AUTHORIZATION, header_value);
        merge_anthropic_beta(headers)?;
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        match self.on_unauthorized_refresh().await {
            Ok(tokens) => RetryDecision::Refresh {
                new_signer: Arc::new(self.with_tokens(tokens)),
            },
            Err(_) => RetryDecision::Fail,
        }
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

fn ensure_oauth_upstream_usable(
    record: &UpstreamRecord,
    allow_disabled: bool,
) -> Result<(), SignerError> {
    if record.kind != cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth
        || record.deleted_at_unix_secs.is_some()
    {
        return Err(SignerError::MissingCredentials {
            reason: "oauth upstream not found".to_owned(),
        });
    }
    if !record.enabled && !allow_disabled {
        return Err(SignerError::MissingCredentials {
            reason: "oauth upstream disabled".to_owned(),
        });
    }
    Ok(())
}

fn decrypt_upstream_tokens(
    aead: &AeadService,
    record: &UpstreamRecord,
) -> Result<OAuthTokenBundle, SignerError> {
    let encrypted =
        record
            .oauth_credentials
            .as_ref()
            .ok_or_else(|| SignerError::MissingCredentials {
                reason: "oauth credentials not found".to_owned(),
            })?;
    encrypted
        .decrypt(aead, record.id.as_bytes())
        .map_err(|source| SignerError::SigningFailed {
            reason: source.to_string(),
        })
}

fn persisted_token_is_usable(expires_at_unix_secs: u64, clock: &dyn Clock) -> bool {
    expires_at_unix_secs > unix_secs(clock.now()).saturating_add(OAUTH_EXPIRY_SKEW_SECS)
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

fn merge_anthropic_beta(headers: &mut HeaderMap) -> Result<(), SignerError> {
    let mut flags = Vec::new();
    for value in headers.get_all("anthropic-beta") {
        let value = value
            .to_str()
            .map_err(|source| SignerError::SigningFailed {
                reason: source.to_string(),
            })?;
        for flag in value
            .split(',')
            .map(str::trim)
            .filter(|flag| !flag.is_empty())
        {
            if !flags.iter().any(|existing| existing == flag) {
                flags.push(flag.to_owned());
            }
        }
    }

    if !flags.iter().any(|flag| flag == ANTHROPIC_OAUTH_BETA) {
        flags.push(ANTHROPIC_OAUTH_BETA.to_owned());
    }

    headers.remove("anthropic-beta");
    let value = flags.join(", ");
    let header_value =
        HeaderValue::from_str(&value).map_err(|source| SignerError::SigningFailed {
            reason: source.to_string(),
        })?;
    headers.insert("anthropic-beta", header_value);
    Ok(())
}

fn bearer_header_value(token: &str) -> Result<HeaderValue, SignerError> {
    let mut value = Vec::with_capacity("Bearer ".len() + token.len());
    value.extend_from_slice(b"Bearer ");
    value.extend_from_slice(token.as_bytes());
    HeaderValue::from_bytes(&value).map_err(|source| SignerError::SigningFailed {
        reason: source.to_string(),
    })
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use async_trait::async_trait;
    use bytes::Bytes;
    use cc_lb_aead::{AeadService, EncryptedOAuthTokens, OAuthTokenBundle};
    use cc_lb_clock::TestClock;
    use cc_lb_plugin_api::{
        RequestContext, Upstream, UpstreamDialect, shape_request, sign_request,
    };
    use cc_lb_storage_api::upstream::{UpstreamKind, UpstreamStatusUpdate};
    use cc_lb_storage_api::{
        OAuthCredentialStore, StorageError, StorageResult, UpstreamCreate, UpstreamRecord,
        UpstreamRecordId, UpstreamStore, UpstreamUpdate, validate_identifier,
    };
    use http::header::{AUTHORIZATION, USER_AGENT};
    use http::{HeaderMap, HeaderValue, Method, StatusCode};
    use oauth2::ClientId;
    use tokio::sync::Mutex;
    use url::Url;

    use super::*;

    struct MemoryUpstreamStore {
        clock: ClockHandle,
        records: Mutex<Vec<UpstreamRecord>>,
        oauth: Mutex<HashMap<(String, String), Vec<u8>>>,
    }

    impl Default for MemoryUpstreamStore {
        fn default() -> Self {
            Self {
                clock: Arc::new(TestClock::new_at_secs(1_700_000_000)),
                records: Mutex::new(Vec::new()),
                oauth: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl UpstreamStore for MemoryUpstreamStore {
        async fn create(&self, create: UpstreamCreate) -> StorageResult<UpstreamRecord> {
            validate_identifier("upstream.name", &create.name)?;
            let mut records = self.records.lock().await;
            if records.iter().any(|record| record.name == create.name) {
                return Err(conflict("upstream name already exists"));
            }
            let now = now_secs(self.clock.as_ref());
            let record = UpstreamRecord {
                id: UpstreamRecordId::new_v4(),
                name: create.name,
                kind: create.kind,
                base_url: create.base_url,
                enabled: true,
                oauth_credentials: None,
                api_key_ciphertext: create.api_key_ciphertext,
                last_apply_error: None,
                last_apply_at_unix_secs: None,
                deleted_at_unix_secs: None,
                revision: 1,
                oauth_token_generation: create.oauth_token_generation.unwrap_or_default(),
                created_at_unix_secs: now,
                updated_at_unix_secs: now,
                warmup_enabled: create.warmup_enabled,
                warmup_dialect_plugin: None,
                last_warmup_at_unix_secs: None,
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

        async fn update_spec(
            &self,
            id: UpstreamRecordId,
            expected_revision: u64,
            update: UpstreamUpdate,
        ) -> StorageResult<UpstreamRecord> {
            self.update(id, expected_revision, update).await
        }

        async fn update_api_key_secret(
            &self,
            id: UpstreamRecordId,
            api_key_ciphertext: Option<Vec<u8>>,
        ) -> StorageResult<UpstreamRecord> {
            self.mutate(id, None, |record| {
                record.api_key_ciphertext = api_key_ciphertext;
                Ok(())
            })
            .await
        }

        async fn update_oauth_token(
            &self,
            id: UpstreamRecordId,
            tokens: EncryptedOAuthTokens,
        ) -> StorageResult<UpstreamRecord> {
            self.mutate(id, None, |record| {
                record.oauth_credentials = Some(tokens);
                Ok(())
            })
            .await
        }

        async fn set_status(
            &self,
            id: UpstreamRecordId,
            status: UpstreamStatusUpdate,
        ) -> StorageResult<()> {
            self.mutate(id, None, |record| {
                if let Some(value) = status.last_apply_error {
                    record.last_apply_error = value;
                }
                if let Some(value) = status.last_apply_at_unix_secs {
                    record.last_apply_at_unix_secs = value;
                }
                if let Some(value) = status.last_warmup_at_unix_secs {
                    record.last_warmup_at_unix_secs = value;
                }
                Ok(())
            })
            .await?;
            Ok(())
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
                record.deleted_at_unix_secs = Some(now_secs(self.clock.as_ref()));
                Ok(())
            })
            .await?;
            Ok(())
        }

        async fn hard_delete(&self, id: UpstreamRecordId) -> StorageResult<()> {
            self.records.lock().await.retain(|record| record.id != id);
            Ok(())
        }

        async fn clear_warmup_dialect_plugin(
            &self,
            _id: Uuid,
            _expected_revision: u64,
        ) -> StorageResult<Option<UpstreamRecord>> {
            Ok(None)
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
            record.updated_at_unix_secs = now_secs(self.clock.as_ref());
            Ok(record.clone())
        }
    }

    #[async_trait]
    impl OAuthCredentialStore for MemoryUpstreamStore {
        async fn put_oauth_ciphertext(
            &self,
            principal_id: &str,
            provider: &str,
            ciphertext: &[u8],
        ) -> StorageResult<()> {
            self.oauth.lock().await.insert(
                (principal_id.to_owned(), provider.to_owned()),
                ciphertext.to_vec(),
            );
            Ok(())
        }

        async fn get_oauth_ciphertext(
            &self,
            principal_id: &str,
            provider: &str,
        ) -> StorageResult<Option<Vec<u8>>> {
            Ok(self
                .oauth
                .lock()
                .await
                .get(&(principal_id.to_owned(), provider.to_owned()))
                .cloned())
        }

        async fn delete_oauth(&self, principal_id: &str, provider: &str) -> StorageResult<bool> {
            Ok(self
                .oauth
                .lock()
                .await
                .remove(&(principal_id.to_owned(), provider.to_owned()))
                .is_some())
        }

        async fn put_anthropic_api_key_ciphertext(
            &self,
            _storage_key: &str,
            _ciphertext: &[u8],
        ) -> StorageResult<()> {
            Ok(())
        }

        async fn get_anthropic_api_key_ciphertext(
            &self,
            _storage_key: &str,
        ) -> StorageResult<Option<Vec<u8>>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn build_missing_returns_missing_credentials() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, aead(), "missing", clock);

        let error = build_error(&factory).await;

        assert!(matches!(error, SignerError::MissingCredentials { .. }));
    }

    #[tokio::test]
    async fn build_disabled_returns_missing_credentials() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs(clock.as_ref()) + 600),
            )
            .await
            .unwrap();
        let record = store.get_by_name("primary").await.unwrap().unwrap();
        store
            .set_enabled(record.id, record.revision, false)
            .await
            .unwrap();
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock);

        let error = build_error(&factory).await;

        assert!(matches!(error, SignerError::MissingCredentials { .. }));
    }

    #[tokio::test]
    async fn build_disabled_with_allow_disabled_upstream_succeeds() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs(clock.as_ref()) + 600),
            )
            .await
            .unwrap();
        let record = store.get_by_name("primary").await.unwrap().unwrap();
        store
            .set_enabled(record.id, record.revision, false)
            .await
            .unwrap();
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock)
                .allow_disabled_upstream(true);

        let signer = factory
            .build(&Upstream::AnthropicDirect { base_url: None })
            .await
            .expect("disabled upstream should sign when allow_disabled_upstream is true");

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

    #[tokio::test]
    async fn build_deleted_with_allow_disabled_upstream_still_rejects() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs(clock.as_ref()) + 600),
            )
            .await
            .unwrap();
        let record = store.get_by_name("primary").await.unwrap().unwrap();
        store.soft_delete(record.id, record.revision).await.unwrap();
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock)
                .allow_disabled_upstream(true);

        let error = build_error(&factory).await;

        assert!(matches!(error, SignerError::MissingCredentials { .. }));
    }

    #[tokio::test]
    async fn build_expired_token_returns_expired_token_error() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs(clock.as_ref())),
            )
            .await
            .unwrap();
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock);
        let signer = factory
            .build(&Upstream::AnthropicDirect { base_url: None })
            .await
            .unwrap();

        let error = sign_request(signer.as_ref(), shaped_request())
            .await
            .unwrap_err();

        assert!(matches!(error, SignerError::ExpiredToken { .. }));
    }

    #[tokio::test]
    async fn decryption_failure_with_wrong_aad_returns_aead_error() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        let wrong_record = create_upstream(&store, "wrong-aad").await;
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, wrong_record.id, now_secs(clock.as_ref()) + 600),
            )
            .await
            .unwrap();
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock);

        let error = build_error(&factory).await;

        assert!(
            matches!(error, SignerError::SigningFailed { reason } if reason.contains("decryption failed"))
        );
    }

    #[tokio::test]
    async fn signer_sets_bearer_header_with_access_token() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs(clock.as_ref()) + 600),
            )
            .await
            .unwrap();
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock);
        let signer = factory
            .build(&Upstream::AnthropicDirect { base_url: None })
            .await
            .unwrap();

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

    #[tokio::test]
    async fn merge_anthropic_beta_preserves_client_flags_active_signer() {
        let signer = active_signer().await;

        let signed = sign_request(
            &signer,
            shaped_request_with_beta(Some(CLIENT_BETA_FLAGS_WITHOUT_OAUTH)),
        )
        .await
        .unwrap();

        assert_eq!(anthropic_beta(&signed), MERGED_CLIENT_BETA_FLAGS);
    }

    #[tokio::test]
    async fn merge_anthropic_beta_adds_oauth_when_absent_active_signer() {
        let signer = active_signer().await;

        let signed = sign_request(&signer, shaped_request_with_beta(None))
            .await
            .unwrap();

        assert_eq!(anthropic_beta(&signed), ANTHROPIC_OAUTH_BETA);
    }

    #[tokio::test]
    async fn merge_anthropic_beta_no_duplicate_when_oauth_already_present_active_signer() {
        let signer = active_signer().await;

        let signed = sign_request(
            &signer,
            shaped_request_with_beta(Some("claude-code-20250219, oauth-2025-04-20")),
        )
        .await
        .unwrap();

        assert_eq!(
            anthropic_beta(&signed),
            "claude-code-20250219, oauth-2025-04-20"
        );
    }

    #[tokio::test]
    async fn merge_anthropic_beta_preserves_client_flags_persisted_signer() {
        let signer = persisted_signer().await;

        let signed = sign_request(
            signer.as_ref(),
            shaped_request_with_beta(Some(CLIENT_BETA_FLAGS_WITHOUT_OAUTH)),
        )
        .await
        .unwrap();

        assert_eq!(anthropic_beta(&signed), MERGED_CLIENT_BETA_FLAGS);
    }

    #[tokio::test]
    async fn merge_anthropic_beta_adds_oauth_when_absent_persisted_signer() {
        let signer = persisted_signer().await;

        let signed = sign_request(signer.as_ref(), shaped_request_with_beta(None))
            .await
            .unwrap();

        assert_eq!(anthropic_beta(&signed), ANTHROPIC_OAUTH_BETA);
    }

    #[tokio::test]
    async fn merge_anthropic_beta_no_duplicate_when_oauth_already_present_persisted_signer() {
        let signer = persisted_signer().await;

        let signed = sign_request(
            signer.as_ref(),
            shaped_request_with_beta(Some("claude-code-20250219, oauth-2025-04-20")),
        )
        .await
        .unwrap();

        assert_eq!(
            anthropic_beta(&signed),
            "claude-code-20250219, oauth-2025-04-20"
        );
    }

    const CLIENT_BETA_FLAGS_WITHOUT_OAUTH: &str = "claude-code-20250219, interleaved-thinking-2025-05-14, context-management-2025-06-27, prompt-caching-scope-2026-01-05, effort-2025-11-24, structured-outputs-2025-12-15";
    const MERGED_CLIENT_BETA_FLAGS: &str = "claude-code-20250219, interleaved-thinking-2025-05-14, context-management-2025-06-27, prompt-caching-scope-2026-01-05, effort-2025-11-24, structured-outputs-2025-12-15, oauth-2025-04-20";

    async fn active_signer() -> AnthropicOAuthSigner {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let service = aead();
        store_live_credentials(&store, &service, now_secs(clock.as_ref()) + 600).await;
        AnthropicOAuthSigner::new(
            "principal",
            "anthropic_oauth",
            store,
            service,
            Url::parse("https://platform.claude.com/v1/oauth/token").unwrap(),
            ClientId::new("client-test".to_owned()),
            clock,
        )
    }

    async fn persisted_signer() -> Arc<dyn Signer> {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs(clock.as_ref()) + 600),
            )
            .await
            .unwrap();
        let factory =
            AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock);
        factory
            .build(&Upstream::AnthropicDirect { base_url: None })
            .await
            .unwrap()
    }

    async fn store_live_credentials(
        store: &MemoryUpstreamStore,
        aead: &AeadService,
        expires_at_unix_secs: u64,
    ) {
        let plaintext = serde_json::to_vec(&OAuthCredentials {
            access_token: "access-token".to_owned(),
            refresh_token: "refresh-token".to_owned(),
            expires_at: expires_at_unix_secs,
            scopes: vec!["messages".to_owned()],
        })
        .unwrap();
        let ciphertext = aead
            .encrypt(
                &plaintext,
                &oauth_credentials_aad("principal", "anthropic_oauth"),
            )
            .unwrap();
        store
            .put_oauth_ciphertext("principal", "anthropic_oauth", &ciphertext)
            .await
            .unwrap();
    }

    fn shaped_request_with_beta(value: Option<&str>) -> ShapedRequest {
        let mut shaped = shaped_request();
        if let Some(value) = value {
            shaped
                .headers_mut()
                .insert("anthropic-beta", value.parse().unwrap());
        }
        shaped
    }

    fn anthropic_beta(signed: &SignedRequest) -> &str {
        signed
            .headers()
            .get("anthropic-beta")
            .and_then(|value| value.to_str().ok())
            .unwrap()
    }

    #[tokio::test]
    async fn persisted_signer_refreshes_on_unauthorized() {
        let clock = test_clock();
        let store = Arc::new(MemoryUpstreamStore::default());
        let record = create_upstream(&store, "primary").await;
        let service = aead();
        store
            .store_oauth_tokens(
                record.id,
                record.revision,
                encrypted_tokens(&service, record.id, now_secs(clock.as_ref()) + 600),
            )
            .await
            .unwrap();
        let refresh_handle = Arc::new(RefreshingLazyHandle {
            store: store.clone(),
            aead: service.clone(),
            clock: clock.clone(),
            calls: AtomicU32::new(0),
        });
        let base = AnthropicOAuthSignerFactory::for_upstream_name(store, service, "primary", clock);
        let factory = AnthropicOAuthSignerFactoryWithLazyRefresh::new(
            base,
            refresh_handle.clone(),
            record.id,
        );
        let signer = factory
            .build(&Upstream::AnthropicDirect { base_url: None })
            .await
            .unwrap();

        let new_signer = match signer.on_unauthorized(&unauthorized_error()).await {
            RetryDecision::Refresh { new_signer } => new_signer,
            RetryDecision::Fail => panic!("persisted signer did not refresh on unauthorized"),
        };
        let signed = sign_request(new_signer.as_ref(), shaped_request())
            .await
            .unwrap();

        assert_eq!(refresh_handle.call_count(), 1);
        assert_eq!(
            signed
                .headers()
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer refreshed-access-token")
        );
    }

    struct RefreshingLazyHandle {
        store: Arc<MemoryUpstreamStore>,
        aead: Arc<AeadService>,
        clock: ClockHandle,
        calls: AtomicU32,
    }

    impl RefreshingLazyHandle {
        fn call_count(&self) -> u32 {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl LazyRefreshHandle for RefreshingLazyHandle {
        async fn refresh_one(&self, upstream_id: Uuid) -> Result<(), LazyRefreshError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let record = self
                .store
                .get_by_id(upstream_id)
                .await
                .map_err(lazy_refresh_error)?
                .ok_or_else(|| LazyRefreshError::Failed {
                    reason: "oauth upstream not found".to_owned(),
                })?;
            self.store
                .store_oauth_tokens(
                    record.id,
                    record.revision,
                    encrypted_tokens_with_access_token(
                        &self.aead,
                        record.id,
                        "refreshed-access-token",
                        now_secs(self.clock.as_ref()) + 600,
                    ),
                )
                .await
                .map_err(lazy_refresh_error)?;
            Ok(())
        }
    }

    fn lazy_refresh_error(source: StorageError) -> LazyRefreshError {
        LazyRefreshError::Failed {
            reason: source.to_string(),
        }
    }

    fn unauthorized_error() -> UpstreamError {
        UpstreamError::Unauthorized {
            status: StatusCode::UNAUTHORIZED,
            body: None,
        }
    }

    async fn create_upstream(store: &MemoryUpstreamStore, name: &str) -> UpstreamRecord {
        store
            .create(UpstreamCreate {
                name: name.to_owned(),
                kind: UpstreamKind::AnthropicOauth,
                base_url: None,
                api_key_ciphertext: None,
                oauth_token_generation: None,
                warmup_enabled: false,
                warmup_dialect_plugin: None,
            })
            .await
            .unwrap()
    }

    fn encrypted_tokens(
        aead: &AeadService,
        upstream_id: UpstreamRecordId,
        expires_at_unix_secs: u64,
    ) -> EncryptedOAuthTokens {
        encrypted_tokens_with_access_token(aead, upstream_id, "access-token", expires_at_unix_secs)
    }

    fn encrypted_tokens_with_access_token(
        aead: &AeadService,
        upstream_id: UpstreamRecordId,
        access_token: &str,
        expires_at_unix_secs: u64,
    ) -> EncryptedOAuthTokens {
        EncryptedOAuthTokens::encrypt(
            aead,
            &OAuthTokenBundle {
                access_token: access_token.to_owned(),
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
            thread_id: None,
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::from_static(b"{}"),
            cache_breakpoints: Vec::new(),
            canonical_model_id: String::new(),
            cache_pricing: cc_lb_domain::CachePricingSummary::default(),
        };
        let principal = cc_lb_domain::Principal {
            id: "principal".to_owned(),
            kind: cc_lb_domain::PrincipalKind::OAuthSubject,
            claims: serde_json::Map::new(),
        };
        shape_request(
            &DirectDialect,
            &ctx,
            &Upstream::AnthropicDirect { base_url: None },
            &principal,
        )
        .unwrap()
    }

    struct DirectDialect;

    impl UpstreamDialect for DirectDialect {
        fn shape(
            &self,
            _ctx: &RequestContext,
            _upstream: &Upstream,
            _principal: &cc_lb_domain::Principal,
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
    }

    fn aead() -> Arc<AeadService> {
        Arc::new(AeadService::from_master_key([7; 32]))
    }

    fn test_clock() -> ClockHandle {
        Arc::new(TestClock::new_at_secs(1_700_000_000))
    }

    async fn build_error(factory: &AnthropicOAuthSignerFactory) -> SignerError {
        match factory
            .build(&Upstream::AnthropicDirect { base_url: None })
            .await
        {
            Ok(_) => panic!("expected signer build error"),
            Err(error) => error,
        }
    }

    fn conflict(message: &str) -> StorageError {
        StorageError::Conflict {
            message: message.to_owned(),
        }
    }

    fn now_secs(clock: &dyn Clock) -> u64 {
        unix_secs(clock.now())
    }
}
