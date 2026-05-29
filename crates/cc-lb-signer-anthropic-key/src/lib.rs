#![forbid(unsafe_code)]

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_aead::AeadService;
use cc_lb_plugin_api::{
    CredentialStrategy, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, Upstream, UpstreamError,
};
use cc_lb_storage_api::{AnthropicApiKeyCredential, OAuthCredentialStore};
use http::header::{AUTHORIZATION, HeaderValue};
use secrecy::{ExposeSecret, SecretString};

#[derive(Clone)]
enum AnthropicKeyCredentialSource {
    Static(SecretString),
    Storage {
        storage_key: String,
        storage: Arc<dyn OAuthCredentialStore>,
        aead: Arc<AeadService>,
    },
}

impl AnthropicKeyCredentialSource {
    async fn api_key(&self) -> Result<SecretString, SignerError> {
        match self {
            Self::Static(api_key) => Ok(api_key.clone()),
            Self::Storage {
                storage_key,
                storage,
                aead,
            } => load_stored_api_key(storage.clone(), aead.clone(), storage_key.clone()).await,
        }
    }

    fn strip_authorization(&self) -> bool {
        matches!(self, Self::Storage { .. })
    }
}

#[derive(Clone)]
pub struct AnthropicKeySigner {
    credential: AnthropicKeyCredentialSource,
}

impl AnthropicKeySigner {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            credential: AnthropicKeyCredentialSource::Static(SecretString::new(
                api_key.into().into_boxed_str(),
            )),
        }
    }

    pub fn from_storage(
        storage_key: impl Into<String>,
        storage: Arc<dyn OAuthCredentialStore>,
        aead: Arc<AeadService>,
    ) -> Self {
        Self {
            credential: AnthropicKeyCredentialSource::Storage {
                storage_key: storage_key.into(),
                storage,
                aead,
            },
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
        let api_key = self.credential.api_key().await?;
        let header_value = HeaderValue::from_str(api_key.expose_secret()).map_err(|source| {
            SignerError::SigningFailed {
                reason: source.to_string(),
            }
        })?;
        if self.credential.strip_authorization() {
            shaped.headers_mut().remove(AUTHORIZATION);
        }
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
    credential: AnthropicKeyCredentialSource,
}

impl AnthropicKeySignerFactory {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            auth_strategy: CredentialStrategy::ApiKey,
            credential: AnthropicKeyCredentialSource::Static(SecretString::new(
                api_key.into().into_boxed_str(),
            )),
        }
    }

    pub fn with_strategy(auth_strategy: CredentialStrategy, api_key: impl Into<String>) -> Self {
        Self {
            auth_strategy,
            credential: AnthropicKeyCredentialSource::Static(SecretString::new(
                api_key.into().into_boxed_str(),
            )),
        }
    }

    pub fn from_storage(
        storage_key: impl Into<String>,
        storage: Arc<dyn OAuthCredentialStore>,
        aead: Arc<AeadService>,
    ) -> Self {
        Self {
            auth_strategy: CredentialStrategy::ApiKey,
            credential: AnthropicKeyCredentialSource::Storage {
                storage_key: storage_key.into(),
                storage,
                aead,
            },
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
            credential: self.credential.clone(),
        }))
    }
}

async fn load_stored_api_key(
    storage: Arc<dyn OAuthCredentialStore>,
    aead: Arc<AeadService>,
    storage_key: String,
) -> Result<SecretString, SignerError> {
    let loaded = storage
        .get_anthropic_api_key_ciphertext(&storage_key)
        .await
        .map_err(signing_failed)?;
    let ciphertext = loaded.ok_or_else(|| SignerError::MissingCredentials {
        reason: "real Anthropic API key not found in storage".to_owned(),
    })?;
    let plaintext = aead
        .decrypt(&ciphertext, &anthropic_api_key_aad(&storage_key))
        .map_err(signing_failed)?;
    let credential: AnthropicApiKeyCredential =
        serde_json::from_slice(&plaintext).map_err(signing_failed)?;
    let api_key = credential.anthropic_api_key;
    if api_key.trim().is_empty() {
        return Err(SignerError::MissingCredentials {
            reason: "real Anthropic API key is empty".to_owned(),
        });
    }
    Ok(SecretString::new(api_key.into_boxed_str()))
}

fn anthropic_api_key_aad(storage_key: &str) -> Vec<u8> {
    format!("anthropic-api-key:{storage_key}").into_bytes()
}

fn signing_failed(source: impl fmt::Display) -> SignerError {
    SignerError::SigningFailed {
        reason: source.to_string(),
    }
}
