#![forbid(unsafe_code)]

//! GCP OAuth bearer signer for Vertex AI requests.

pub mod adc;
pub mod single_flight;
pub mod token;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use cc_lb_plugin_api::{
    AuthStrategy, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, Upstream, UpstreamError,
};
use http::header::{HeaderValue, AUTHORIZATION};
use metrics::Unit;
use secrecy::ExposeSecret;

pub use adc::AdcTokenProvider;
pub use single_flight::{new_single_flight_locks, GcpSingleFlightLocks};
pub use token::{GcpToken, GcpTokenError, GcpTokenProvider, StaticGcpTokenProvider};

const DEFAULT_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const SUCCESS_OUTCOME: &str = "success";
const FAILURE_OUTCOME: &str = "failure";

static METRICS_ONCE: Once = Once::new();
static REFRESH_SUCCESS_TOTAL: AtomicU64 = AtomicU64::new(0);
static REFRESH_FAILURE_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Registers GCP signer metrics once for the current process.
pub fn register_metrics() {
    METRICS_ONCE.call_once(|| {
        metrics::describe_counter!(
            "cc_lb_gcp_token_refresh_total",
            Unit::Count,
            "GCP OAuth token refresh attempts by outcome."
        );
    });
}

/// Process-local count of GCP token refresh outcomes observed by this signer.
pub fn gcp_token_refresh_total(outcome: &str) -> u64 {
    match outcome {
        SUCCESS_OUTCOME => REFRESH_SUCCESS_TOTAL.load(Ordering::Relaxed),
        FAILURE_OUTCOME => REFRESH_FAILURE_TOTAL.load(Ordering::Relaxed),
        _ => 0,
    }
}

/// OAuth bearer signer for Google Vertex AI Anthropic publisher requests.
#[derive(Clone)]
pub struct GcpOAuthSigner {
    /// OAuth scopes requested from the token provider.
    pub scopes: Vec<String>,
    /// Token provider used for signing and refresh.
    pub provider: Arc<dyn GcpTokenProvider>,
    /// Per-scope single-flight locks used for proactive and 401 refreshes.
    pub single_flight: GcpSingleFlightLocks,
}

impl GcpOAuthSigner {
    /// Creates a signer using the default Vertex cloud-platform scope.
    pub fn new(provider: Arc<dyn GcpTokenProvider>) -> Self {
        Self::with_scopes(default_scopes(), provider)
    }

    /// Creates a signer with explicit OAuth scopes.
    pub fn with_scopes(scopes: Vec<String>, provider: Arc<dyn GcpTokenProvider>) -> Self {
        Self::with_single_flight(scopes, provider, new_single_flight_locks())
    }

    /// Creates a signer with explicit scopes and shared single-flight locks.
    pub fn with_single_flight(
        scopes: Vec<String>,
        provider: Arc<dyn GcpTokenProvider>,
        single_flight: GcpSingleFlightLocks,
    ) -> Self {
        Self {
            scopes: single_flight::scope_key(&scopes),
            provider,
            single_flight,
        }
    }

    async fn token_for_signing(&self) -> Result<GcpToken, SignerError> {
        let token = self
            .provider
            .get_token(&self.scopes)
            .await
            .map_err(token_error_to_signer)?;

        if token.expires_within(token::REFRESH_BUFFER) {
            return self.refresh_under_single_flight(false).await;
        }

        Ok(token)
    }

    async fn refresh_under_single_flight(&self, force: bool) -> Result<GcpToken, SignerError> {
        let lock = single_flight::lock_for(&self.single_flight, &self.scopes);
        let _guard = lock.lock().await;

        if !force {
            let current = self
                .provider
                .get_token(&self.scopes)
                .await
                .map_err(token_error_to_signer)?;
            if !current.expires_within(token::REFRESH_BUFFER) {
                return Ok(current);
            }
        }

        self.provider
            .refresh_token(&self.scopes)
            .await
            .map_err(token_error_to_signer)
    }
}

impl fmt::Debug for GcpOAuthSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GcpOAuthSigner")
            .field("scopes", &self.scopes)
            .field("provider", &"GcpTokenProvider")
            .field("single_flight", &"GcpSingleFlightLocks")
            .finish()
    }
}

#[async_trait]
impl Signer for GcpOAuthSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let token = self.token_for_signing().await?;
        let header_value = bearer_header_value(token.value.expose_secret())?;

        shaped.headers_mut().insert(AUTHORIZATION, header_value);
        tracing::debug!(scopes = ?self.scopes, "gcp oauth bearer applied");
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        match self.refresh_under_single_flight(true).await {
            Ok(_) => {
                increment_refresh_metric(SUCCESS_OUTCOME);
                RetryDecision::Refresh {
                    new_signer: Arc::new(self.clone()),
                }
            }
            Err(_) => {
                increment_refresh_metric(FAILURE_OUTCOME);
                RetryDecision::Fail
            }
        }
    }
}

/// Factory for Vertex GCP OAuth bearer signers.
#[derive(Clone)]
pub struct GcpOAuthSignerFactory {
    auth_strategy: AuthStrategy,
    scopes: Vec<String>,
    provider: Arc<dyn GcpTokenProvider>,
    single_flight: GcpSingleFlightLocks,
}

impl GcpOAuthSignerFactory {
    /// Creates a factory backed by Application Default Credentials.
    pub fn new() -> Self {
        if let Ok(token) = std::env::var("CC_LB_GCP_ACCESS_TOKEN") {
            if !token.trim().is_empty() {
                let expires_at = SystemTime::now() + Duration::from_secs(3600);
                let token = GcpToken::new(token, expires_at, default_scopes());
                return Self::with_provider(Arc::new(StaticGcpTokenProvider::new(token)));
            }
        }
        Self::with_provider(Arc::new(AdcTokenProvider::new()))
    }

    /// Creates a factory with an injectable token provider.
    pub fn with_provider(provider: Arc<dyn GcpTokenProvider>) -> Self {
        Self {
            auth_strategy: AuthStrategy::GcpOAuth,
            scopes: default_scopes(),
            provider,
            single_flight: new_single_flight_locks(),
        }
    }

    /// Overrides OAuth scopes.
    pub fn with_scopes(mut self, scopes: Vec<String>) -> Self {
        self.scopes = single_flight::scope_key(&scopes);
        self
    }

    /// Overrides the selected authentication strategy, primarily for factory tests.
    pub fn with_strategy(mut self, auth_strategy: AuthStrategy) -> Self {
        self.auth_strategy = auth_strategy;
        self
    }
}

impl Default for GcpOAuthSignerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for GcpOAuthSignerFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GcpOAuthSignerFactory")
            .field("auth_strategy", &self.auth_strategy)
            .field("scopes", &self.scopes)
            .field("provider", &"GcpTokenProvider")
            .field("single_flight", &"GcpSingleFlightLocks")
            .finish()
    }
}

#[async_trait]
impl SignerFactory for GcpOAuthSignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        if self.auth_strategy != AuthStrategy::GcpOAuth {
            return Err(SignerError::WrongStrategy {
                strategy: self.auth_strategy.clone(),
            });
        }

        if !matches!(upstream, Upstream::Vertex { .. }) {
            return Err(SignerError::WrongStrategy {
                strategy: self.auth_strategy.clone(),
            });
        }

        Ok(Arc::new(GcpOAuthSigner::with_single_flight(
            self.scopes.clone(),
            self.provider.clone(),
            self.single_flight.clone(),
        )))
    }
}

fn default_scopes() -> Vec<String> {
    vec![DEFAULT_CLOUD_PLATFORM_SCOPE.to_owned()]
}

fn token_error_to_signer(error: GcpTokenError) -> SignerError {
    match error {
        GcpTokenError::MissingCredentials { reason } => SignerError::MissingCredentials { reason },
        GcpTokenError::InvalidCredentials { reason } => SignerError::InvalidCredentials { reason },
        GcpTokenError::Provider { reason } => SignerError::SigningFailed { reason },
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

fn increment_refresh_metric(outcome: &'static str) {
    register_metrics();
    match outcome {
        SUCCESS_OUTCOME => {
            REFRESH_SUCCESS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        FAILURE_OUTCOME => {
            REFRESH_FAILURE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
    metrics::counter!("cc_lb_gcp_token_refresh_total", "outcome" => outcome).increment(1);
}
