#![forbid(unsafe_code)]

mod http_client;
mod pkce;
mod refresh;
mod single_flight;

use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use cc_lb_plugin_api::{
    AuthStrategy, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, Upstream, UpstreamError,
};
use cc_lb_storage_redb::{OAuthCredentials, Storage};
use dashmap::DashMap;
use http::header::{HeaderValue, AUTHORIZATION};
use oauth2::{ClientId, TokenUrl};
use secrecy::{ExposeSecret, SecretString};
use tokio::sync::Mutex;
use url::Url;

pub use http_client::{
    HyperOAuthHttpClient, OAuthHttpClient, OAuthHttpError, OAuthTokenRequest, OAuthTokenResponse,
};
pub use pkce::{complete_pkce_flow, start_pkce_flow, PkceHandshake, PkceHandshakeState};
pub use refresh::{
    BreakerMap, CircuitBreakerState, RefreshError, BREAKER_FAILURE_THRESHOLD, REFRESH_BUFFER_SECS,
};
pub use single_flight::{new_refresh_locks, RefreshLocks};

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
    pub storage: Arc<Storage>,
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
        storage: Arc<Storage>,
        token_url: Url,
        client_id: ClientId,
    ) -> Self {
        Self::with_http(
            principal_id,
            provider,
            storage,
            token_url,
            client_id,
            Arc::new(HyperOAuthHttpClient::new()),
        )
    }

    pub fn with_http(
        principal_id: impl Into<String>,
        provider: impl Into<String>,
        storage: Arc<Storage>,
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
        storage: Arc<Storage>,
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
        let storage = self.storage.clone();
        let principal_id = self.principal_id.clone();
        let provider = self.provider.clone();
        tokio::task::spawn_blocking(move || storage.get_oauth(&principal_id, &provider))
            .await
            .map_err(|source| RefreshError::StorageTask {
                reason: source.to_string(),
            })?
            .map_err(|source| RefreshError::Storage {
                reason: source.to_string(),
            })
    }

    async fn store_credentials(&self, creds: OAuthCredentials) -> Result<(), RefreshError> {
        let storage = self.storage.clone();
        let principal_id = self.principal_id.clone();
        let provider = self.provider.clone();
        tokio::task::spawn_blocking(move || storage.put_oauth(&principal_id, &provider, &creds))
            .await
            .map_err(|source| RefreshError::StorageTask {
                reason: source.to_string(),
            })?
            .map_err(|source| RefreshError::Storage {
                reason: source.to_string(),
            })
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
    auth_strategy: AuthStrategy,
    principal_id: String,
    provider: String,
    storage: Arc<Storage>,
    token_url: Url,
    client_id: ClientId,
    http: Arc<dyn OAuthHttpClient>,
    refresh_locks: RefreshLocks,
    breaker_state: BreakerMap,
}

impl AnthropicOAuthSignerFactory {
    pub fn new(
        principal_id: impl Into<String>,
        provider: impl Into<String>,
        storage: Arc<Storage>,
        token_url: TokenUrl,
        client_id: ClientId,
    ) -> Self {
        Self::with_http(
            principal_id,
            provider,
            storage,
            token_url,
            client_id,
            Arc::new(HyperOAuthHttpClient::new()),
        )
    }

    pub fn with_http(
        principal_id: impl Into<String>,
        provider: impl Into<String>,
        storage: Arc<Storage>,
        token_url: TokenUrl,
        client_id: ClientId,
        http: Arc<dyn OAuthHttpClient>,
    ) -> Self {
        Self {
            auth_strategy: AuthStrategy::OAuth,
            principal_id: principal_id.into(),
            provider: provider.into(),
            storage,
            token_url: token_url.url().clone(),
            client_id,
            http,
            refresh_locks: new_refresh_locks(),
            breaker_state: refresh::new_breaker_map(),
        }
    }

    pub fn with_strategy(mut self, auth_strategy: AuthStrategy) -> Self {
        self.auth_strategy = auth_strategy;
        self
    }
}

impl fmt::Debug for AnthropicOAuthSignerFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicOAuthSignerFactory")
            .field("auth_strategy", &self.auth_strategy)
            .field("principal_id", &self.principal_id)
            .field("provider", &self.provider)
            .field("storage", &"Storage")
            .field("token_url", &self.token_url)
            .field("client_id", &self.client_id)
            .field("http", &self.http)
            .field("refresh_locks", &"RefreshLocks")
            .field("breaker_state", &"BreakerMap")
            .finish()
    }
}

#[async_trait]
impl SignerFactory for AnthropicOAuthSignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        if self.auth_strategy != AuthStrategy::OAuth {
            return Err(SignerError::WrongStrategy {
                strategy: self.auth_strategy.clone(),
            });
        }

        Ok(Arc::new(AnthropicOAuthSigner::with_shared_state(
            self.principal_id.clone(),
            self.provider.clone(),
            self.storage.clone(),
            self.token_url.clone(),
            self.client_id.clone(),
            AnthropicOAuthSharedState {
                http: self.http.clone(),
                refresh_locks: self.refresh_locks.clone(),
                breaker_state: self.breaker_state.clone(),
            },
        )))
    }
}

fn refresh_error_to_signer(error: RefreshError) -> SignerError {
    match error {
        RefreshError::MissingCredentials => SignerError::MissingCredentials {
            reason: "oauth credentials not found".to_owned(),
        },
        other => SignerError::SigningFailed {
            reason: other.to_string(),
        },
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
