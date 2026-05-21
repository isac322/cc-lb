#![forbid(unsafe_code)]

//! AWS SigV4 signer for Bedrock Runtime requests.

pub mod credentials;

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Once};
use std::time::SystemTime;

use async_trait::async_trait;
use aws_credential_types::Credentials as SdkCredentials;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, SignableBody, SignableRequest, SignatureLocation,
    SigningInstructions, SigningSettings,
};
use aws_sigv4::sign::v4;
use cc_lb_plugin_api::{
    AuthStrategy, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, Upstream, UpstreamError,
};
use credentials::{
    AwsCredentials, AwsCredentialsError, AwsCredentialsProvider, ChainCredentialsProvider,
};
use http::header::{HeaderName, HeaderValue};
use metrics::Unit;
use secrecy::ExposeSecret;

pub use credentials::{
    EnvCredentialsProvider, ProfileCredentialsProvider, StaticCredentialsProvider,
};

static METRICS_ONCE: Once = Once::new();
static CLOCK_SKEW_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Clock source used by the signer.
pub trait Clock: fmt::Debug + Send + Sync {
    /// Returns the current signing time.
    fn now(&self) -> SystemTime;
}

/// System clock implementation.
#[derive(Clone, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Registers AWS signer metrics once for the process.
pub fn register_metrics() {
    METRICS_ONCE.call_once(|| {
        metrics::describe_counter!(
            "cc_lb_aws_clock_skew_total",
            Unit::Count,
            "AWS Bedrock clock-skew responses observed by the SigV4 signer."
        );
    });
}

/// Process-local count of observed AWS clock-skew responses.
pub fn clock_skew_total() -> u64 {
    CLOCK_SKEW_TOTAL.load(Ordering::Relaxed)
}

/// AWS SigV4 signer for Bedrock Runtime requests.
#[derive(Clone)]
pub struct AwsSigV4Signer {
    /// AWS region used in the SigV4 credential scope.
    pub region: String,
    /// AWS service signing name. Bedrock Runtime uses `bedrock`.
    pub service: String,
    /// Credentials provider used for each signing operation.
    pub credentials: Arc<dyn AwsCredentialsProvider>,
    /// Clock used to populate `X-Amz-Date`.
    pub clock: Arc<dyn Clock>,
}

impl AwsSigV4Signer {
    /// Creates a Bedrock SigV4 signer using the system clock.
    pub fn new(region: impl Into<String>, credentials: Arc<dyn AwsCredentialsProvider>) -> Self {
        Self::with_clock(region, "bedrock", credentials, Arc::new(SystemClock))
    }

    /// Creates a SigV4 signer with an explicit service and clock.
    pub fn with_clock(
        region: impl Into<String>,
        service: impl Into<String>,
        credentials: Arc<dyn AwsCredentialsProvider>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            region: region.into(),
            service: service.into(),
            credentials,
            clock,
        }
    }
}

impl fmt::Debug for AwsSigV4Signer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsSigV4Signer")
            .field("region", &self.region)
            .field("service", &self.service)
            .field("credentials", &self.credentials)
            .field("clock", &self.clock)
            .finish()
    }
}

#[async_trait]
impl Signer for AwsSigV4Signer {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        register_metrics();
        let credentials = self
            .credentials
            .resolve()
            .await
            .map_err(credentials_error)?;
        let identity = sdk_credentials(&credentials).into();
        let mut settings = SigningSettings::default();
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        settings.signature_location = SignatureLocation::Headers;

        let signing_params = v4::SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(self.clock.now())
            .settings(settings)
            .build()
            .map_err(|source| SignerError::SigningFailed {
                reason: source.to_string(),
            })?
            .into();

        let method = shaped.method().as_str().to_owned();
        let uri = shaped.url().as_str().to_owned();
        let headers = signable_headers(shaped.headers())?;
        let body = SignableBody::Bytes(shaped.body());
        let request = SignableRequest::new(
            &method,
            &uri,
            headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str())),
            body,
        )
        .map_err(|source| SignerError::SigningFailed {
            reason: source.to_string(),
        })?;

        let (instructions, _signature) = sign(request, &signing_params)
            .map_err(|source| SignerError::SigningFailed {
                reason: source.to_string(),
            })?
            .into_parts();
        apply_instructions(shaped.headers_mut(), &instructions)?;

        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, err: &UpstreamError) -> RetryDecision {
        if contains_clock_skew(err) {
            increment_clock_skew();
        }
        RetryDecision::Fail
    }
}

/// Factory for Bedrock Runtime SigV4 signers.
#[derive(Clone)]
pub struct AwsSigV4SignerFactory {
    auth_strategy: AuthStrategy,
    credentials: Arc<dyn AwsCredentialsProvider>,
    service: String,
    clock: Arc<dyn Clock>,
}

impl AwsSigV4SignerFactory {
    /// Creates a factory using the standard environment, explicit-config, profile chain.
    pub fn new() -> Self {
        Self::with_credentials_provider(ChainCredentialsProvider::standard(None, "default"))
    }

    /// Creates a factory with explicit static credentials in the standard chain.
    pub fn with_static_credentials(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        let credentials =
            AwsCredentials::new(access_key_id, secret_access_key, session_token, None);
        Self::with_credentials_provider(ChainCredentialsProvider::standard(
            Some(credentials),
            "default",
        ))
    }

    /// Creates a factory from an already-composed credentials provider.
    pub fn with_credentials_provider(credentials: Arc<dyn AwsCredentialsProvider>) -> Self {
        Self {
            auth_strategy: AuthStrategy::AwsSigV4,
            credentials,
            service: "bedrock".to_owned(),
            clock: Arc::new(SystemClock),
        }
    }

    /// Overrides the selected authentication strategy, primarily for factory tests.
    pub fn with_strategy(mut self, auth_strategy: AuthStrategy) -> Self {
        self.auth_strategy = auth_strategy;
        self
    }

    /// Overrides the service signing name.
    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = service.into();
        self
    }

    /// Overrides the signing clock.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
}

impl Default for AwsSigV4SignerFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for AwsSigV4SignerFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AwsSigV4SignerFactory")
            .field("auth_strategy", &self.auth_strategy)
            .field("credentials", &self.credentials)
            .field("service", &self.service)
            .field("clock", &self.clock)
            .finish()
    }
}

#[async_trait]
impl SignerFactory for AwsSigV4SignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        if self.auth_strategy != AuthStrategy::AwsSigV4 {
            return Err(SignerError::WrongStrategy {
                strategy: self.auth_strategy.clone(),
            });
        }

        let Upstream::BedrockRuntime { region } = upstream else {
            return Err(SignerError::WrongStrategy {
                strategy: self.auth_strategy.clone(),
            });
        };

        Ok(Arc::new(AwsSigV4Signer::with_clock(
            region.clone(),
            self.service.clone(),
            self.credentials.clone(),
            self.clock.clone(),
        )))
    }
}

fn sdk_credentials(credentials: &AwsCredentials) -> SdkCredentials {
    SdkCredentials::new(
        credentials.access_key_id.clone(),
        credentials.secret_access_key.expose_secret().to_owned(),
        credentials
            .session_token
            .as_ref()
            .map(|token| token.expose_secret().to_owned()),
        credentials.expires_at,
        "cc-lb-signer-aws",
    )
}

fn signable_headers(headers: &http::HeaderMap) -> Result<Vec<(String, String)>, SignerError> {
    headers
        .iter()
        .map(|(name, value)| {
            let value = value
                .to_str()
                .map_err(|source| SignerError::SigningFailed {
                    reason: source.to_string(),
                })?;
            Ok((name.as_str().to_owned(), value.to_owned()))
        })
        .collect()
}

fn apply_instructions(
    headers: &mut http::HeaderMap,
    instructions: &SigningInstructions,
) -> Result<(), SignerError> {
    if !instructions.params().is_empty() {
        return Err(SignerError::SigningFailed {
            reason: "header signing unexpectedly returned query parameters".to_owned(),
        });
    }

    for (name, value) in instructions.headers() {
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|source| {
            SignerError::SigningFailed {
                reason: source.to_string(),
            }
        })?;
        let value = HeaderValue::from_str(value).map_err(|source| SignerError::SigningFailed {
            reason: source.to_string(),
        })?;
        headers.insert(name, value);
    }

    Ok(())
}

fn credentials_error(error: AwsCredentialsError) -> SignerError {
    match error {
        AwsCredentialsError::NotFound { location } => {
            SignerError::MissingCredentials { reason: location }
        }
        AwsCredentialsError::Invalid { reason } => SignerError::InvalidCredentials { reason },
        AwsCredentialsError::Io { path, reason } => SignerError::SigningFailed {
            reason: format!("{}: {reason}", path.display()),
        },
    }
}

fn contains_clock_skew(err: &UpstreamError) -> bool {
    let body = match err {
        UpstreamError::Unauthorized { body, .. }
        | UpstreamError::Retryable { body, .. }
        | UpstreamError::Failed { body, .. } => body.as_ref(),
    };

    body.and_then(|body| serde_json::from_slice::<serde_json::Value>(body).ok())
        .and_then(|value| {
            value
                .get("__type")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .map(|error_type| error_type.ends_with("RequestTimeTooSkewed"))
        .unwrap_or(false)
}

fn increment_clock_skew() {
    register_metrics();
    CLOCK_SKEW_TOTAL.fetch_add(1, Ordering::Relaxed);
    metrics::counter!("cc_lb_aws_clock_skew_total").increment(1);
}
