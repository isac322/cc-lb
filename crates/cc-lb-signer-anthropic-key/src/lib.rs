#![forbid(unsafe_code)]

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_domain::{CredentialStrategy, Upstream};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError,
};
use http::HeaderValue;
use secrecy::{ExposeSecret, SecretString};

#[derive(Clone)]
pub struct AnthropicKeySigner {
    api_key: SecretString,
}

impl AnthropicKeySigner {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: SecretString::new(api_key.into().into_boxed_str()),
        }
    }
}

impl fmt::Debug for AnthropicKeySigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicKeySigner")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl Signer for AnthropicKeySigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let header_value =
            HeaderValue::from_str(self.api_key.expose_secret()).map_err(|source| {
                SignerError::SigningFailed {
                    reason: source.to_string(),
                }
            })?;
        shaped.headers_mut().insert("x-api-key", header_value);
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }
}

#[derive(Clone)]
pub struct AnthropicKeySignerFactory {
    auth_strategy: CredentialStrategy,
    api_key: SecretString,
}

impl AnthropicKeySignerFactory {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            auth_strategy: CredentialStrategy::ApiKey,
            api_key: SecretString::new(api_key.into().into_boxed_str()),
        }
    }

    pub fn with_strategy(auth_strategy: CredentialStrategy, api_key: impl Into<String>) -> Self {
        Self {
            auth_strategy,
            api_key: SecretString::new(api_key.into().into_boxed_str()),
        }
    }
}

impl fmt::Debug for AnthropicKeySignerFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicKeySignerFactory")
            .field("auth_strategy", &self.auth_strategy)
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl SignerFactory for AnthropicKeySignerFactory {
    async fn build(&self, upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let _ = upstream;
        if self.auth_strategy != CredentialStrategy::ApiKey {
            return Err(SignerError::WrongStrategy {
                strategy: self.auth_strategy.clone(),
            });
        }

        Ok(Arc::new(AnthropicKeySigner {
            api_key: self.api_key.clone(),
        }))
    }
}
