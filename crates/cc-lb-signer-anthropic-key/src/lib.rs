#![forbid(unsafe_code)]

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use cc_lb_domain::{ANTHROPIC_WORKSPACE_ID_HEADER, Upstream};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError, scrub_secret,
};
use http::header::{AUTHORIZATION, HeaderValue};
use secrecy::{ExposeSecret, SecretString};

const API_KEY_HEADER: &str = "x-api-key";

/// Signs requests for one Anthropic API-key upstream.
///
/// Both outbound header values are built once, at construction; `sign` only
/// clones them, so signing never parses, formats, or decrypts.
#[derive(Clone)]
pub struct AnthropicKeySigner {
    api_key: SecretString,
    api_key_header: HeaderValue,
    workspace_header: Option<HeaderValue>,
}

impl AnthropicKeySigner {
    /// Builds a signer for `api_key`, acting in `workspace_id` when one is set.
    pub fn new(api_key: &str, workspace_id: Option<&str>) -> Result<Self, SignerError> {
        let mut api_key_header =
            HeaderValue::from_str(api_key).map_err(|_| SignerError::SigningFailed {
                reason: "anthropic api key is not a valid header value".to_owned(),
            })?;
        api_key_header.set_sensitive(true);
        let workspace_header = workspace_id
            .map(|id| {
                HeaderValue::from_str(id).map_err(|_| SignerError::SigningFailed {
                    reason: "anthropic workspace id is not a valid header value".to_owned(),
                })
            })
            .transpose()?;
        Ok(Self {
            api_key: SecretString::new(Box::from(api_key)),
            api_key_header,
            workspace_header,
        })
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
        let headers = shaped.headers_mut();
        // The Anthropic direct dialect clones downstream headers verbatim and
        // `authorization` is not hop-by-hop, so a downstream
        // `Authorization: Bearer sk-cclb-…` would otherwise be forwarded to the
        // upstream alongside the configured key.
        headers.remove(AUTHORIZATION);
        headers.insert(API_KEY_HEADER, self.api_key_header.clone());
        // The workspace is upstream configuration: a downstream caller can
        // never choose it. `insert` replaces every downstream value.
        match &self.workspace_header {
            Some(workspace) => {
                headers.insert(ANTHROPIC_WORKSPACE_ID_HEADER, workspace.clone());
            }
            None => {
                headers.remove(ANTHROPIC_WORKSPACE_ID_HEADER);
            }
        }
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> Cow<'a, str> {
        scrub_secret(self.api_key.expose_secret(), text)
    }
}

/// Hands out one prebuilt [`AnthropicKeySigner`]; `build` is a refcount bump.
#[derive(Clone)]
pub struct AnthropicKeySignerFactory {
    signer: Arc<AnthropicKeySigner>,
}

impl AnthropicKeySignerFactory {
    /// Builds the signer for `api_key`, acting in `workspace_id` when one is set.
    pub fn new(api_key: &str, workspace_id: Option<&str>) -> Result<Self, SignerError> {
        Ok(Self {
            signer: Arc::new(AnthropicKeySigner::new(api_key, workspace_id)?),
        })
    }
}

impl fmt::Debug for AnthropicKeySignerFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnthropicKeySignerFactory")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl SignerFactory for AnthropicKeySignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let signer: Arc<dyn Signer> = self.signer.clone();
        Ok(signer)
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

    const KEY: &str = "sk-ant-api03-SECRETVALUE1234-AAAA";
    const WORKSPACE: &str = "wrkspc_P";

    fn header_values<'a>(signed: &'a SignedRequest, name: &str) -> Vec<&'a str> {
        signed
            .headers()
            .get_all(name)
            .iter()
            .map(|value| value.to_str().expect("ascii header"))
            .collect()
    }

    /// A downstream request that tries to pick its own credential and workspace.
    fn hostile_shaped_request() -> ShapedRequest {
        let mut shaped = shaped_request();
        let headers = shaped.headers_mut();
        headers.insert("x-api-key", HeaderValue::from_static("sk-cclb-downstream"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer sk-cclb-downstream"),
        );
        headers.append(
            ANTHROPIC_WORKSPACE_ID_HEADER,
            HeaderValue::from_static("wrkspc_EVIL"),
        );
        headers.append(
            ANTHROPIC_WORKSPACE_ID_HEADER,
            HeaderValue::from_static("wrkspc_EVIL2"),
        );
        shaped
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
        let signer = AnthropicKeySigner::new("sk-ant-configured", None).expect("signer");

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
        let signer = AnthropicKeySigner::new("sk-ant-configured", None).expect("signer");

        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(
            signed
                .headers()
                .get("anthropic-beta")
                .and_then(|value| value.to_str().ok()),
            Some("prompt-caching-2024-07-31")
        );
    }

    /// SGN-01 (T0) + SGN-02 (b): a workspace upstream sends exactly one
    /// configured `x-api-key`, exactly one configured workspace header, and no
    /// `authorization`, whatever the downstream sent.
    #[tokio::test]
    async fn sgn_01_sign_sets_configured_workspace_over_downstream_values() {
        let signer = AnthropicKeySigner::new(KEY, Some(WORKSPACE)).expect("signer");

        let signed = sign_request(&signer, hostile_shaped_request())
            .await
            .expect("sign request");

        assert_eq!(header_values(&signed, "x-api-key"), vec![KEY]);
        assert_eq!(
            header_values(&signed, ANTHROPIC_WORKSPACE_ID_HEADER),
            vec![WORKSPACE]
        );
        assert!(!signed.headers().contains_key(AUTHORIZATION));
    }

    /// SGN-02 (a): an upstream without a workspace strips every downstream
    /// workspace header.
    #[tokio::test]
    async fn sgn_02_sign_without_workspace_strips_downstream_workspace() {
        let signer = AnthropicKeySigner::new(KEY, None).expect("signer");

        let signed = sign_request(&signer, hostile_shaped_request())
            .await
            .expect("sign request");

        assert_eq!(header_values(&signed, "x-api-key"), vec![KEY]);
        assert!(
            !signed.headers().contains_key(ANTHROPIC_WORKSPACE_ID_HEADER),
            "{:?}",
            signed.headers()
        );
    }

    /// SGN-07: header values are built once. Two requests signed by one
    /// signer, and two signers built by one factory, share the same bytes.
    #[tokio::test]
    async fn sgn_07_sign_reuses_prebuilt_header_bytes() {
        let factory = AnthropicKeySignerFactory::new(KEY, Some(WORKSPACE)).expect("factory");
        let upstream = Upstream::AnthropicDirect { base_url: None };
        let first_signer = factory.build(&upstream).await.expect("first build");
        let second_signer = factory.build(&upstream).await.expect("second build");
        assert!(
            std::ptr::addr_eq(Arc::as_ptr(&first_signer), Arc::as_ptr(&second_signer)),
            "build hands out the prebuilt signer"
        );

        let first = sign_request(first_signer.as_ref(), shaped_request())
            .await
            .expect("first sign");
        let second = sign_request(first_signer.as_ref(), shaped_request())
            .await
            .expect("second sign");

        for name in ["x-api-key", ANTHROPIC_WORKSPACE_ID_HEADER] {
            let first_value = first.headers().get(name).expect("first header");
            let second_value = second.headers().get(name).expect("second header");
            assert_eq!(first_value, second_value);
            assert_eq!(
                first_value.as_bytes().as_ptr(),
                second_value.as_bytes().as_ptr(),
                "{name} reuses the prebuilt bytes"
            );
        }
    }

    /// SEC-02: the key header is marked sensitive, so a `Debug` dump of the
    /// signed headers never prints it.
    #[tokio::test]
    async fn sec_02_signed_key_header_is_sensitive() {
        let signer = AnthropicKeySigner::new(KEY, Some(WORKSPACE)).expect("signer");

        let signed = sign_request(&signer, shaped_request())
            .await
            .expect("sign request");

        let key_header = signed.headers().get("x-api-key").expect("key header");
        assert!(key_header.is_sensitive());
        let rendered = format!("{:?}", signed.headers());
        assert!(!rendered.contains("SECRETVALUE"), "{rendered}");
        assert!(rendered.contains("Sensitive"), "{rendered}");
    }

    #[test]
    fn invalid_header_values_fail_without_echoing_input() {
        for (key, workspace) in [
            ("sk-ant-api03-SECRETVALUE1234\n", None),
            (KEY, Some("wrkspc_SECRETVALUE1234\r\nx-evil: 1")),
        ] {
            let error = AnthropicKeySignerFactory::new(key, workspace).expect_err("rejected");
            assert!(matches!(error, SignerError::SigningFailed { .. }));
            assert!(!error.to_string().contains("SECRETVALUE"), "{error}");
        }
    }

    /// SEC-14 (unit): the override is reachable through the factory's
    /// `Arc<dyn Signer>`, masks the stored key, and borrows when nothing
    /// matched.
    #[tokio::test]
    async fn scrub_secrets_masks_stored_key_and_borrows_clean_text() {
        let signer = AnthropicKeySignerFactory::new(KEY, None)
            .expect("factory")
            .build(&Upstream::AnthropicDirect { base_url: None })
            .await
            .expect("build");

        let echoed = format!("invalid x-api-key: {KEY}");
        let scrubbed = signer.scrub_secrets(&echoed);
        assert_eq!(scrubbed, "invalid x-api-key: [REDACTED]");

        let clean = "invalid x-api-key";
        assert!(matches!(signer.scrub_secrets(clean), Cow::Borrowed(_)));
    }

    /// SEC-09: neither signer type prints the configured key.
    #[test]
    fn sec_09_debug_redacts_signer_and_factory() {
        let secret = "sk-ant-api03-SECRETVALUE1234";
        for rendered in [
            format!(
                "{:?}",
                AnthropicKeySigner::new(secret, Some(WORKSPACE)).expect("signer")
            ),
            format!(
                "{:?}",
                AnthropicKeySignerFactory::new(secret, Some(WORKSPACE)).expect("factory")
            ),
        ] {
            assert!(!rendered.contains("SECRETVALUE"), "{rendered}");
            assert!(rendered.contains("[REDACTED]"), "{rendered}");
        }
    }
}
