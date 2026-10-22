#![forbid(unsafe_code)]

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_clock::{Clock, ClockHandle, unix_secs};
use cc_lb_domain::{ANTHROPIC_WORKSPACE_ID_HEADER, Upstream};
use cc_lb_upstream::{
    RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError, SignerFactory,
    SigningCapability, UpstreamError, scrub_secret,
};
use http::header::{AUTHORIZATION, HeaderValue};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};

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
/// Provider API-key header strategy for native OpenAI- and Gemini-compatible
/// upstreams.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderKeyKind {
    /// Sends `Authorization: Bearer <key>`.
    Openai,
    /// Sends `x-goog-api-key: <key>`.
    Gemini,
}

/// Signs requests for one native provider API-key upstream.
///
/// The downstream credential headers are removed before the configured
/// provider header is inserted. The request body and all unrelated headers
/// are preserved.
#[derive(Clone)]
pub struct ProviderKeySigner {
    api_key: SecretString,
    header_name: &'static str,
    header_value: HeaderValue,
}

impl ProviderKeySigner {
    /// Builds a provider signer with a prebuilt, sensitive header value.
    pub fn new(kind: ProviderKeyKind, api_key: &str) -> Result<Self, SignerError> {
        let (header_name, value) = match kind {
            ProviderKeyKind::Openai => ("authorization", format!("Bearer {api_key}")),
            ProviderKeyKind::Gemini => ("x-goog-api-key", api_key.to_owned()),
        };
        let mut header_value =
            HeaderValue::from_str(&value).map_err(|_| SignerError::SigningFailed {
                reason: "provider api key is not a valid header value".to_owned(),
            })?;
        header_value.set_sensitive(true);
        Ok(Self {
            api_key: SecretString::new(Box::from(api_key)),
            header_name,
            header_value,
        })
    }
}

impl fmt::Debug for ProviderKeySigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderKeySigner")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl Signer for ProviderKeySigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        let headers = shaped.headers_mut();
        headers.remove(AUTHORIZATION);
        headers.remove("x-api-key");
        headers.insert(self.header_name, self.header_value.clone());
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> Cow<'a, str> {
        scrub_secret(self.api_key.expose_secret(), text)
    }
}

/// Hands out one prebuilt [`ProviderKeySigner`].
#[derive(Clone)]
pub struct ProviderKeySignerFactory {
    signer: Arc<ProviderKeySigner>,
}

impl ProviderKeySignerFactory {
    /// Builds a provider signer factory for `api_key`.
    pub fn new(kind: ProviderKeyKind, api_key: &str) -> Result<Self, SignerError> {
        Ok(Self {
            signer: Arc::new(ProviderKeySigner::new(kind, api_key)?),
        })
    }
}

impl fmt::Debug for ProviderKeySignerFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderKeySignerFactory")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl SignerFactory for ProviderKeySignerFactory {
    async fn build(&self, _upstream: &Upstream) -> Result<Arc<dyn Signer>, SignerError> {
        let signer: Arc<dyn Signer> = self.signer.clone();
        Ok(signer)
    }
}

fn check_access_token_expiry(
    expires_at_unix_secs: Option<u64>,
    clock: &dyn Clock,
) -> Result<(), SignerError> {
    if expires_at_unix_secs.is_some_and(|deadline| deadline <= unix_secs(clock.now())) {
        return Err(SignerError::SigningFailed {
            reason: "oauth access token expired; reauthorization required".to_owned(),
        });
    }
    Ok(())
}

/// Signs requests for an OpenAI ChatGPT subscription upstream.
///
/// The configured access token replaces downstream credential headers. The
/// request body and unrelated headers are preserved.
#[derive(Clone)]
pub struct OpenaiSubscriptionSigner {
    access_token: SecretString,
    authorization_header: HeaderValue,
    account_header: Option<HeaderValue>,
    expires_at_unix_secs: Option<u64>,
    clock: ClockHandle,
}

impl OpenaiSubscriptionSigner {
    /// Access-only imports supply their expiry; refreshing credentials keep
    /// their existing refresh policy by leaving it unset.
    pub fn new(
        access_token: &str,
        account_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        clock: ClockHandle,
    ) -> Result<Self, SignerError> {
        let mut authorization_header = HeaderValue::from_str(&format!("Bearer {access_token}"))
            .map_err(|_| SignerError::SigningFailed {
                reason: "openai access token is not a valid header value".to_owned(),
            })?;
        authorization_header.set_sensitive(true);
        let account_header = account_id
            .map(|account_id| {
                HeaderValue::from_str(account_id).map_err(|_| SignerError::SigningFailed {
                    reason: "openai account id is not a valid header value".to_owned(),
                })
            })
            .transpose()?;
        Ok(Self {
            access_token: SecretString::new(Box::from(access_token)),
            authorization_header,
            account_header,
            expires_at_unix_secs,
            clock,
        })
    }
}

impl fmt::Debug for OpenaiSubscriptionSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenaiSubscriptionSigner")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl Signer for OpenaiSubscriptionSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        check_access_token_expiry(self.expires_at_unix_secs, self.clock.as_ref())?;
        let headers = shaped.headers_mut();
        headers.remove(AUTHORIZATION);
        headers.remove(API_KEY_HEADER);
        headers.insert(AUTHORIZATION, self.authorization_header.clone());
        match &self.account_header {
            Some(account) => {
                headers.insert("ChatGPT-Account-ID", account.clone());
            }
            None => {
                headers.remove("ChatGPT-Account-ID");
            }
        }
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> Cow<'a, str> {
        scrub_secret(self.access_token.expose_secret(), text)
    }
}

/// Signs requests for a Gemini Code Assist subscription upstream.
///
/// The configured access token replaces downstream credential headers. Gemini
/// generate-content requests are wrapped in the Code Assist envelope here so
/// API-key signers can continue to pass through native request bytes.
#[derive(Clone)]
pub struct GeminiSubscriptionSigner {
    access_token: SecretString,
    authorization_header: HeaderValue,
    project_id: Option<String>,
    expires_at_unix_secs: Option<u64>,
    clock: ClockHandle,
}

impl GeminiSubscriptionSigner {
    /// Access-only imports supply their expiry; refreshing credentials keep
    /// their existing refresh policy by leaving it unset.
    pub fn new(
        access_token: &str,
        project_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        clock: ClockHandle,
    ) -> Result<Self, SignerError> {
        let mut authorization_header = HeaderValue::from_str(&format!("Bearer {access_token}"))
            .map_err(|_| SignerError::SigningFailed {
                reason: "gemini access token is not a valid header value".to_owned(),
            })?;
        authorization_header.set_sensitive(true);
        Ok(Self {
            access_token: SecretString::new(Box::from(access_token)),
            authorization_header,
            project_id: project_id.map(str::to_owned),
            expires_at_unix_secs,
            clock,
        })
    }
}

fn wrap_gemini_code_assist_request(
    shaped: &mut ShapedRequest,
    project_id: Option<&str>,
) -> Result<(), SignerError> {
    let path = shaped.url().path();
    let is_stream =
        path == "/v1internal:streamGenerateContent" || path.ends_with(":streamGenerateContent");
    let is_generate = path == "/v1internal:generateContent" || path.ends_with(":generateContent");
    if !is_stream && !is_generate {
        return Ok(());
    }

    let original =
        serde_json::from_slice::<Value>(shaped.body()).map_err(|_| SignerError::SigningFailed {
            reason: "Gemini Code Assist request body must be valid JSON".to_owned(),
        })?;
    let already_wrapped = original.get("request").is_some() && original.get("model").is_some();
    let model = path
        .strip_prefix("/v1beta/models/")
        .and_then(|model_path| {
            model_path
                .strip_suffix(":generateContent")
                .or_else(|| model_path.strip_suffix(":streamGenerateContent"))
        })
        .map(str::to_owned)
        .or_else(|| {
            original
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| SignerError::SigningFailed {
            reason: "Gemini Code Assist request has no model".to_owned(),
        })?;

    let envelope = if already_wrapped {
        let mut envelope = original;
        if let Some(project_id) = project_id {
            envelope["project"] = Value::String(project_id.to_owned());
        }
        envelope
    } else {
        let project = project_id
            .map(|project_id| Value::String(project_id.to_owned()))
            .or_else(|| original.get("project").cloned());
        match project {
            Some(project) => json!({
                "model": model,
                "project": project,
                "request": original,
            }),
            None => json!({
                "model": model,
                "request": original,
            }),
        }
    };
    let body = serde_json::to_vec(&envelope).map_err(|_| SignerError::SigningFailed {
        reason: "Gemini Code Assist request could not be serialized".to_owned(),
    })?;
    let mut url = shaped.url().clone();
    url.set_path(if is_stream {
        "/v1internal:streamGenerateContent"
    } else {
        "/v1internal:generateContent"
    });
    shaped.set_url(url);
    shaped.set_body(Bytes::from(body));
    Ok(())
}

impl fmt::Debug for GeminiSubscriptionSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeminiSubscriptionSigner")
            .field("credential", &"[REDACTED]")
            .finish()
    }
}

#[async_trait]
impl Signer for GeminiSubscriptionSigner {
    async fn sign(
        &self,
        mut shaped: ShapedRequest,
        capability: &mut SigningCapability,
    ) -> Result<SignedRequest, SignerError> {
        check_access_token_expiry(self.expires_at_unix_secs, self.clock.as_ref())?;
        wrap_gemini_code_assist_request(&mut shaped, self.project_id.as_deref())?;
        let headers = shaped.headers_mut();
        headers.remove("x-goog-api-key");
        headers.remove(AUTHORIZATION);
        headers.insert(AUTHORIZATION, self.authorization_header.clone());
        Ok(SignedRequest::from_shaped(shaped, capability))
    }

    async fn on_unauthorized(&self, _err: &UpstreamError) -> RetryDecision {
        RetryDecision::Fail
    }

    fn scrub_secrets<'a>(&self, text: &'a str) -> Cow<'a, str> {
        scrub_secret(self.access_token.expose_secret(), text)
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

    #[tokio::test]
    async fn provider_signers_replace_only_credential_headers_and_preserve_body() {
        let mut shaped = shaped_request();
        shaped
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer downstream"));
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("downstream"));
        shaped
            .headers_mut()
            .insert("x-request-id", HeaderValue::from_static("request"));
        let body = shaped.body().clone();
        let signer =
            ProviderKeySigner::new(ProviderKeyKind::Openai, "openai-secret").expect("signer");
        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(signed.body(), &body);
        assert_eq!(
            signed.headers().get(AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer openai-secret"))
        );
        assert!(!signed.headers().contains_key("x-api-key"));
        assert_eq!(
            signed.headers().get("x-request-id"),
            Some(&HeaderValue::from_static("request"))
        );

        let mut shaped = shaped_request();
        shaped
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer downstream"));
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("downstream"));
        let signer =
            ProviderKeySigner::new(ProviderKeyKind::Gemini, "gemini-secret").expect("signer");
        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(
            signed.headers().get("x-goog-api-key"),
            Some(&HeaderValue::from_static("gemini-secret"))
        );
        assert!(!signed.headers().contains_key(AUTHORIZATION));
        assert!(!signed.headers().contains_key("x-api-key"));
    }

    #[test]
    fn provider_signer_debug_redacts_key() {
        let secret = "provider-secret-value";
        let rendered = format!(
            "{:?}",
            ProviderKeySignerFactory::new(ProviderKeyKind::Gemini, secret).expect("factory")
        );
        assert!(!rendered.contains(secret));
        assert!(rendered.contains("[REDACTED]"));
    }
    #[tokio::test]
    async fn openai_subscription_signer_sets_bearer_optional_account_and_preserves_body() {
        let mut shaped = shaped_request();
        shaped
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer downstream"));
        shaped
            .headers_mut()
            .insert("x-api-key", HeaderValue::from_static("downstream"));
        shaped
            .headers_mut()
            .insert("x-request-id", HeaderValue::from_static("request"));
        let body = shaped.body().clone();

        let signer = OpenaiSubscriptionSigner::new(
            "openai-access-token",
            Some("account-123"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer");
        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(signed.body(), &body);
        assert_eq!(
            signed.headers().get(AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer openai-access-token"))
        );
        assert_eq!(
            signed.headers().get("ChatGPT-Account-ID"),
            Some(&HeaderValue::from_static("account-123"))
        );
        assert!(!signed.headers().contains_key("x-api-key"));
        assert_eq!(
            signed.headers().get("x-request-id"),
            Some(&HeaderValue::from_static("request"))
        );

        let signer = OpenaiSubscriptionSigner::new(
            "openai-access-token",
            None,
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer without account id");
        let signed = sign_request(&signer, shaped_request())
            .await
            .expect("sign request without account id");
        assert_eq!(
            signed.headers().get(AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer openai-access-token"))
        );
        assert!(!signed.headers().contains_key("ChatGPT-Account-ID"));
        assert!(!signed.headers().contains_key("x-api-key"));
    }
    #[tokio::test]
    async fn gemini_subscription_signer_sets_bearer_and_preserves_request() {
        let mut shaped = shaped_request();
        shaped
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer downstream"));
        shaped
            .headers_mut()
            .insert("x-goog-api-key", HeaderValue::from_static("downstream"));
        shaped
            .headers_mut()
            .insert("x-request-id", HeaderValue::from_static("request"));
        let body = shaped.body().clone();

        let signer = GeminiSubscriptionSigner::new(
            "gemini-access-token",
            None,
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer");
        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(signed.body(), &body);
        assert_eq!(
            signed.headers().get(AUTHORIZATION),
            Some(&HeaderValue::from_static("Bearer gemini-access-token"))
        );
        assert!(!signed.headers().contains_key("x-goog-api-key"));
        assert_eq!(
            signed.headers().get("x-request-id"),
            Some(&HeaderValue::from_static("request"))
        );
    }

    #[tokio::test]
    async fn gemini_subscription_signer_wraps_request_with_project_and_rewrites_path() {
        let mut shaped = shaped_request();
        shaped.set_url(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent"
                .parse()
                .expect("url"),
        );
        shaped.set_body(Bytes::from_static(br#"{"contents":[]}"#));

        let signer = GeminiSubscriptionSigner::new(
            "gemini-access-token",
            Some("project-123"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer");
        let signed = sign_request(&signer, shaped).await.expect("sign request");
        let body: Value = serde_json::from_slice(signed.body()).expect("json body");

        assert_eq!(signed.url().path(), "/v1internal:generateContent");
        assert_eq!(body["model"], "gemini-2.5-pro");
        assert_eq!(body["project"], "project-123");
        assert_eq!(body["request"], serde_json::json!({"contents": []}));
    }
}
