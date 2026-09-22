#![forbid(unsafe_code)]

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_domain::{CredentialStrategy, Upstream};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError,
};
use http::header::{AUTHORIZATION, HeaderValue};
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
        let headers = shaped.headers_mut();
        // The Anthropic direct dialect clones downstream headers verbatim and
        // `authorization` is not hop-by-hop, so a downstream
        // `Authorization: Bearer sk-cclb-…` would otherwise be forwarded to the
        // upstream alongside the configured key.
        headers.remove(AUTHORIZATION);
        headers.insert("x-api-key", header_value);
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cc_lb_upstream::{
        DialectError, DialectShapeContext, ShapedRequestBuilder, UpstreamDialect, shape_request,
        sign_request,
    };
    use http::header::AUTHORIZATION;
    use http::{HeaderMap, HeaderValue, Method};

    use super::*;

    fn shaped_request() -> ShapedRequest {
        let ctx = DialectShapeContext {
            request_id: "req-1".to_owned(),
            downstream_headers: HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::from_static(b"{}"),
        };
        let principal = cc_lb_domain::Principal {
            id: "principal".to_owned(),
            kind: cc_lb_domain::PrincipalKind::ApiKey,
            claims: serde_json::Map::new(),
        };
        shape_request(
            &DirectDialect,
            &ctx,
            &Upstream::AnthropicDirect { base_url: None },
            &principal,
        )
        .expect("shape request")
    }

    struct DirectDialect;

    impl UpstreamDialect for DirectDialect {
        fn shape(
            &self,
            _context: &DialectShapeContext,
            _upstream: &Upstream,
            _principal: &cc_lb_domain::Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            Ok(builder.shaped_request(
                "https://api.anthropic.com/v1/messages"
                    .parse()
                    .expect("url"),
                Method::POST,
                HeaderMap::new(),
                Bytes::from_static(b"{}"),
            ))
        }
    }

    #[tokio::test]
    async fn sign_replaces_downstream_credentials_with_configured_key() {
        let mut shaped = shaped_request();
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("sk-cclb-downstream"));
        shaped.headers_mut().insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer sk-cclb-downstream"),
        );
        let signer = AnthropicKeySigner::new("sk-ant-configured");

        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(
            signed
                .headers()
                .get("x-api-key")
                .and_then(|value| value.to_str().ok()),
            Some("sk-ant-configured")
        );
        assert!(!signed.headers().contains_key(AUTHORIZATION));
    }

    #[tokio::test]
    async fn sign_preserves_unrelated_headers() {
        let mut shaped = shaped_request();
        shaped.headers_mut().insert(
            "anthropic-beta",
            HeaderValue::from_static("prompt-caching-2024-07-31"),
        );
        let signer = AnthropicKeySigner::new("sk-ant-configured");

        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(
            signed
                .headers()
                .get("anthropic-beta")
                .and_then(|value| value.to_str().ok()),
            Some("prompt-caching-2024-07-31")
        );
    }
}
