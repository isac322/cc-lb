#![forbid(unsafe_code)]

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_clock::{Clock, ClockHandle, unix_secs};
use cc_lb_domain::{ANTHROPIC_WORKSPACE_ID_HEADER, Upstream};
use cc_lb_upstream::{
    GeminiEnvelopeRequest, RetryDecision, ShapedRequest, SignedRequest, Signer, SignerError,
    SignerFactory, SigningCapability, UpstreamError, scrub_secret, scrub_url_query_api_key,
};
use http::header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, USER_AGENT};
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

const API_KEY_HEADER: &str = "x-api-key";
const LEGACY_DAILY_USER_AGENT: &str =
    "antigravity/hub/2.19.1 (aidev_client; os_type=darwin; arch=arm64; cl=963137146)";

fn strip_provider_credential_headers(headers: &mut http::HeaderMap) {
    for name in ["authorization", "x-api-key", "x-goog-api-key", "api-key"] {
        headers.remove(name);
    }
}

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
        let mut url = shaped.url().clone();
        scrub_url_query_api_key(&mut url);
        shaped.set_url(url);
        let headers = shaped.headers_mut();
        // provider credential below is the only credential sent upstream.
        strip_provider_credential_headers(headers);
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
    kind: ProviderKeyKind,
    api_key: SecretString,
    header_name: &'static str,
    header_value: HeaderValue,
    gemini_bearer_value: Option<HeaderValue>,
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
        let gemini_bearer_value = if kind == ProviderKeyKind::Gemini {
            let mut value = HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|_| {
                SignerError::SigningFailed {
                    reason: "provider api key is not a valid bearer header value".to_owned(),
                }
            })?;
            value.set_sensitive(true);
            Some(value)
        } else {
            None
        };
        Ok(Self {
            kind,
            api_key: SecretString::new(Box::from(api_key)),
            header_name,
            header_value,
            gemini_bearer_value,
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
        let mut url = shaped.url().clone();
        scrub_url_query_api_key(&mut url);
        shaped.set_url(url);
        if self.kind == ProviderKeyKind::Gemini {
            ensure_gemini_stream_alt_sse(&mut shaped);
        }
        let gemini_openai_compat = shaped.url().path().starts_with("/v1beta/openai/");
        let headers = shaped.headers_mut();
        strip_provider_credential_headers(headers);
        if let Some(value) = self
            .gemini_bearer_value
            .as_ref()
            .filter(|_| gemini_openai_compat)
        {
            headers.insert("authorization", value.clone());
        } else {
            headers.insert(self.header_name, self.header_value.clone());
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
    never_refresh: bool,
    clock: &dyn Clock,
) -> Result<(), SignerError> {
    let skew = if never_refresh { 0 } else { 30 };
    if expires_at_unix_secs
        .is_some_and(|deadline| deadline <= unix_secs(clock.now()).saturating_add(skew))
    {
        return Err(if never_refresh {
            SignerError::SigningFailed {
                reason: "oauth access token expired; reauthorization required".to_owned(),
            }
        } else {
            SignerError::ExpiredToken {
                reason: "oauth access token expired".to_owned(),
            }
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
    never_refresh: bool,
    clock: ClockHandle,
}

impl OpenaiSubscriptionSigner {
    /// Builds a signer for an access-only credential.
    pub fn new(
        access_token: &str,
        account_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        clock: ClockHandle,
    ) -> Result<Self, SignerError> {
        Self::new_with_policy(access_token, account_id, expires_at_unix_secs, true, clock)
    }

    /// Builds a signer for a rotating OAuth credential.
    pub fn new_refreshing(
        access_token: &str,
        account_id: Option<&str>,
        expires_at_unix_secs: u64,
        clock: ClockHandle,
    ) -> Result<Self, SignerError> {
        Self::new_with_policy(
            access_token,
            account_id,
            Some(expires_at_unix_secs),
            false,
            clock,
        )
    }

    fn new_with_policy(
        access_token: &str,
        account_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        never_refresh: bool,
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
            never_refresh,
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
        check_access_token_expiry(
            self.expires_at_unix_secs,
            self.never_refresh,
            self.clock.as_ref(),
        )?;
        let headers = shaped.headers_mut();
        strip_provider_credential_headers(headers);
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
    never_refresh: bool,
    clock: ClockHandle,
    antigravity: Option<AntigravityRequestProfile>,
}

#[derive(Clone)]
struct AntigravityRequestProfile {
    base_url: Url,
    user_agent: HeaderValue,
    content_type: HeaderValue,
}

impl GeminiSubscriptionSigner {
    /// Access-only imports supply their expiry; rotating credentials use
    /// [`Self::new_refreshing`] so expiry is checked with refresh skew.
    pub fn new(
        access_token: &str,
        project_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        clock: ClockHandle,
    ) -> Result<Self, SignerError> {
        Self::new_with_policy(access_token, project_id, expires_at_unix_secs, true, clock)
    }

    pub fn new_refreshing(
        access_token: &str,
        project_id: Option<&str>,
        expires_at_unix_secs: u64,
        clock: ClockHandle,
    ) -> Result<Self, SignerError> {
        Self::new_with_policy(
            access_token,
            project_id,
            Some(expires_at_unix_secs),
            false,
            clock,
        )
    }

    fn new_with_policy(
        access_token: &str,
        project_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        never_refresh: bool,
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
            never_refresh,
            clock,
            antigravity: None,
        })
    }

    /// Builds the Antigravity profile with endpoint and headers parsed once.
    pub fn new_antigravity(
        access_token: &str,
        project_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        clock: ClockHandle,
        code_assist_url: &str,
        user_agent: &str,
    ) -> Result<Self, SignerError> {
        Self::new_antigravity_with_policy(
            access_token,
            project_id,
            expires_at_unix_secs,
            true,
            clock,
            code_assist_url,
            user_agent,
        )
    }

    pub fn new_antigravity_refreshing(
        access_token: &str,
        project_id: Option<&str>,
        expires_at_unix_secs: u64,
        clock: ClockHandle,
        code_assist_url: &str,
        user_agent: &str,
    ) -> Result<Self, SignerError> {
        Self::new_antigravity_with_policy(
            access_token,
            project_id,
            Some(expires_at_unix_secs),
            false,
            clock,
            code_assist_url,
            user_agent,
        )
    }

    fn new_antigravity_with_policy(
        access_token: &str,
        project_id: Option<&str>,
        expires_at_unix_secs: Option<u64>,
        never_refresh: bool,
        clock: ClockHandle,
        code_assist_url: &str,
        user_agent: &str,
    ) -> Result<Self, SignerError> {
        let base_url = Url::parse(code_assist_url).map_err(|_| SignerError::SigningFailed {
            reason: "antigravity Code Assist URL is invalid".to_owned(),
        })?;
        let user_agent =
            HeaderValue::from_str(user_agent).map_err(|_| SignerError::SigningFailed {
                reason: "antigravity user agent is not a valid header value".to_owned(),
            })?;
        let content_type = HeaderValue::from_static("application/json");
        let mut signer = Self::new_with_policy(
            access_token,
            project_id,
            expires_at_unix_secs,
            never_refresh,
            clock,
        )?;
        signer.antigravity = Some(AntigravityRequestProfile {
            base_url,
            user_agent,
            content_type,
        });
        Ok(signer)
    }
}

fn ensure_gemini_stream_alt_sse(shaped: &mut ShapedRequest) {
    let path = shaped.url().path();
    if path != "/v1internal:streamGenerateContent" && !path.ends_with(":streamGenerateContent") {
        return;
    }
    let has_alt = shaped.url().query().is_some_and(|q| {
        q.split('&')
            .any(|pair| pair == "alt" || pair.starts_with("alt="))
    });
    if !has_alt {
        let mut url = shaped.url().clone();
        let next_query = match url.query() {
            Some(existing) if !existing.is_empty() => format!("{existing}&alt=sse"),
            _ => "alt=sse".to_owned(),
        };
        url.set_query(Some(&next_query));
        shaped.set_url(url);
    }
}

fn antigravity_thinking_tier(request: &Value) -> Option<&'static str> {
    let thinking = request.get("generationConfig")?.get("thinkingConfig")?;
    let level = thinking
        .get("thinkingLevel")
        .or_else(|| thinking.get("thinking_level"))?
        .as_str()?;
    ["low", "medium", "high", "xhigh"]
        .into_iter()
        .find(|tier| level.eq_ignore_ascii_case(tier))
}

fn qualify_antigravity_tiered_model(raw_model: &str, request: &Value) -> Option<String> {
    let candidate = raw_model.strip_prefix("models/").unwrap_or(raw_model);
    if !ANTIGRAVITY_TIERED_FAMILIES.contains(&candidate) {
        return None;
    }
    let tier = antigravity_thinking_tier(request)?;
    Some(format!("{candidate}-{tier}"))
}

const ANTIGRAVITY_TIERED_FAMILIES: [&str; 7] = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.6-flash",
    "gemini-3.1-pro",
    "gemini-4-carbon",
    "gemini-4-argon",
    "gemini-4-hydrogen",
];

fn wrap_gemini_code_assist_request(
    shaped: &mut ShapedRequest,
    project_id: Option<&str>,
    antigravity: Option<&AntigravityRequestProfile>,
) -> Result<bool, SignerError> {
    let mut url = shaped.url().clone();
    scrub_url_query_api_key(&mut url);
    shaped.set_url(url);

    let path = shaped.url().path();
    let is_stream =
        path == "/v1internal:streamGenerateContent" || path.ends_with(":streamGenerateContent");
    let is_generate = path == "/v1internal:generateContent" || path.ends_with(":generateContent");
    let is_count = path == "/v1internal:countTokens" || path.ends_with(":countTokens");
    if !is_stream && !is_generate && !is_count {
        return Ok(false);
    }

    let original =
        serde_json::from_slice::<Value>(shaped.body()).map_err(|_| SignerError::SigningFailed {
            reason: "Gemini Code Assist request body must be valid JSON".to_owned(),
        })?;
    let model = path
        .strip_prefix("/v1beta/models/")
        .and_then(|model_path| {
            model_path
                .strip_suffix(":generateContent")
                .or_else(|| model_path.strip_suffix(":streamGenerateContent"))
                .or_else(|| model_path.strip_suffix(":countTokens"))
        })
        .or_else(|| original.get("model").and_then(Value::as_str))
        .or_else(|| {
            original
                .get("generateContentRequest")
                .and_then(|request| request.get("model"))
                .and_then(Value::as_str)
        })
        .map(|model| model.strip_prefix("models/").unwrap_or(model).to_owned())
        .ok_or_else(|| SignerError::SigningFailed {
            reason: "Gemini Code Assist request has no model".to_owned(),
        })?;

    let mut envelope = if is_count {
        // Cloud Code Assist `/v1internal:countTokens` accepts only `{ "request": { "model": "models/<id>", ... } }`
        // and rejects top-level `model` or `project` fields with HTTP 400 INVALID_ARGUMENT.
        let model = Value::String(format!("models/{model}"));
        let request = if original.get("model").is_none()
            && let Some(request) = original.get("request").and_then(Value::as_object)
        {
            let mut request = request.clone();
            request.insert("model".to_owned(), model);
            Value::Object(request)
        } else if let Some(inner) = original
            .get("generateContentRequest")
            .and_then(Value::as_object)
        {
            let mut inner = inner.clone();
            inner.insert("model".to_owned(), model);
            Value::Object(inner)
        } else if let Some(mut object) = original.as_object().cloned() {
            object.remove("project");
            object.insert("model".to_owned(), model);
            Value::Object(object)
        } else {
            return Err(SignerError::SigningFailed {
                reason: "Gemini Code Assist countTokens request must be a JSON object".to_owned(),
            });
        };
        json!({ "request": request })
    } else {
        let already_wrapped = original.get("request").is_some() && original.get("model").is_some();
        if already_wrapped {
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
        }
    };
    if antigravity.is_some()
        && !is_count
        && let Some(envelope) = envelope.as_object_mut()
    {
        if let Some(model) = envelope.get("model").and_then(Value::as_str)
            && let Some(request) = envelope.get("request")
            && let Some(qualified_model) = qualify_antigravity_tiered_model(model, request)
        {
            envelope.insert("model".to_owned(), Value::String(qualified_model));
        }
        envelope.insert(
            "userAgent".to_owned(),
            Value::String("antigravity".to_owned()),
        );
        let has_request_id = envelope
            .get("requestId")
            .and_then(Value::as_str)
            .is_some_and(|id| !id.is_empty());
        if !has_request_id {
            envelope.insert(
                "requestId".to_owned(),
                Value::String(format!("agent-{}", Uuid::new_v4())),
            );
        }
    }
    let body = serde_json::to_vec(&envelope).map_err(|_| SignerError::SigningFailed {
        reason: "Gemini Code Assist request could not be serialized".to_owned(),
    })?;
    let is_antigravity_host = shaped.url().host_str() == Some("daily-cloudcode-pa.googleapis.com");
    let mut url = antigravity
        .map(|profile| {
            let mut url = profile.base_url.clone();
            url.set_query(shaped.url().query());
            url
        })
        .unwrap_or_else(|| shaped.url().clone());
    url.set_path(if is_stream {
        "/v1internal:streamGenerateContent"
    } else if is_count {
        "/v1internal:countTokens"
    } else {
        "/v1internal:generateContent"
    });
    shaped.set_url(url);
    if is_stream {
        ensure_gemini_stream_alt_sse(shaped);
    }
    shaped.set_body(Bytes::from(body));
    let headers = shaped.headers_mut();
    headers.remove(CONTENT_LENGTH);
    if antigravity.is_none() && is_antigravity_host {
        let keep_existing_ua = headers
            .get(USER_AGENT)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|ua| ua.starts_with("antigravity/") || ua.starts_with("GeminiCLI/"));
        if !keep_existing_ua {
            headers.insert(
                USER_AGENT,
                HeaderValue::from_static(LEGACY_DAILY_USER_AGENT),
            );
        }
    }
    Ok(true)
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
        let mut url = shaped.url().clone();
        scrub_url_query_api_key(&mut url);
        shaped.set_url(url);
        check_access_token_expiry(
            self.expires_at_unix_secs,
            self.never_refresh,
            self.clock.as_ref(),
        )?;
        let wrapped = wrap_gemini_code_assist_request(
            &mut shaped,
            self.project_id.as_deref(),
            self.antigravity.as_ref(),
        )?;
        if wrapped && let Some(hook) = shaped.take_gemini_envelope_hook() {
            let result = hook.shape_envelope_gemini(GeminiEnvelopeRequest {
                url: shaped.url(),
                headers: shaped.headers(),
                envelope_body: shaped.body(),
            })?;
            if let Some(body) = result.envelope_body {
                shaped.set_body(body);
                shaped.headers_mut().remove(CONTENT_LENGTH);
            }
            if let Some(mut headers) = result.headers {
                headers.remove(CONTENT_LENGTH);
                *shaped.headers_mut() = headers;
            }
        }
        let headers = shaped.headers_mut();
        strip_provider_credential_headers(headers);
        headers.insert(AUTHORIZATION, self.authorization_header.clone());
        if let Some(profile) = &self.antigravity {
            headers.remove("x-goog-api-client");
            headers.remove("client-metadata");
            headers.insert(USER_AGENT, profile.user_agent.clone());
            headers.insert(CONTENT_TYPE, profile.content_type.clone());
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
            .insert("x-goog-api-key", HeaderValue::from_static("downstream"));
        shaped
            .headers_mut()
            .insert("api-key", HeaderValue::from_static("downstream"));
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
        assert!(!signed.headers().contains_key("x-goog-api-key"));
        assert!(!signed.headers().contains_key("api-key"));
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
        shaped
            .headers_mut()
            .insert("api-key", HeaderValue::from_static("downstream"));
        let signer =
            ProviderKeySigner::new(ProviderKeyKind::Gemini, "gemini-secret").expect("signer");
        let signed = sign_request(&signer, shaped).await.expect("sign request");

        assert_eq!(
            signed.headers().get("x-goog-api-key"),
            Some(&HeaderValue::from_static("gemini-secret"))
        );
        assert!(!signed.headers().contains_key(AUTHORIZATION));
        assert!(!signed.headers().contains_key("x-api-key"));
        assert!(!signed.headers().contains_key("api-key"));
    }

    #[tokio::test]
    async fn gemini_openai_compat_uses_sensitive_bearer_without_rewriting_path_or_body() {
        let signer =
            ProviderKeySigner::new(ProviderKeyKind::Gemini, "gemini-secret").expect("signer");
        for path in [
            "/v1beta/openai/chat/completions",
            "/v1beta/openai/embeddings",
            "/v1beta/openai/images/generations",
            "/v1beta/openai/models",
            "/v1beta/openai/models/gemini-test",
        ] {
            let mut shaped = hostile_shaped_request();
            let body = shaped.body().clone();
            shaped.set_url(
                format!("https://generativelanguage.googleapis.com{path}?key=downstream&keep=1")
                    .parse()
                    .expect("URL"),
            );
            let signed = sign_request(&signer, shaped).await.expect("sign request");
            assert_eq!(signed.url().path(), path);
            assert_eq!(signed.url().query(), Some("keep=1"));
            assert_eq!(signed.body(), &body);
            assert_eq!(
                signed.headers().get(AUTHORIZATION),
                Some(&HeaderValue::from_static("Bearer gemini-secret"))
            );
            assert!(signed.headers()[AUTHORIZATION].is_sensitive());
            assert!(!signed.headers().contains_key("x-goog-api-key"));
            assert!(!signed.headers().contains_key("x-api-key"));
        }
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
    async fn refreshing_subscription_signers_reject_near_expiry_with_refreshable_error() {
        let clock = Arc::new(cc_lb_clock::SystemClock);
        let deadline = unix_secs(clock.now()).saturating_add(1);
        let signer = OpenaiSubscriptionSigner::new_refreshing(
            "openai-access-token",
            Some("account-123"),
            deadline,
            clock,
        )
        .expect("signer");
        let error = sign_request(&signer, shaped_request())
            .await
            .expect_err("near-expiry rotating token must request refresh");
        assert!(matches!(error, SignerError::ExpiredToken { .. }));
    }

    #[tokio::test]
    async fn refreshing_gemini_signer_rejects_near_expiry_with_refreshable_error() {
        let clock = Arc::new(cc_lb_clock::SystemClock);
        let deadline = unix_secs(clock.now()).saturating_add(1);
        let signer = GeminiSubscriptionSigner::new_refreshing(
            "gemini-access-token",
            Some("project-123"),
            deadline,
            clock,
        )
        .expect("signer");
        let error = sign_request(&signer, shaped_request())
            .await
            .expect_err("near-expiry rotating token must request refresh");
        assert!(matches!(error, SignerError::ExpiredToken { .. }));
    }

    #[tokio::test]
    async fn oauth_expiry_skew_boundary_is_exact_and_access_only_has_no_skew() {
        let now = 1_700_000_000;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(now));

        for deadline in [now + 30, now + 31] {
            let signer = OpenaiSubscriptionSigner::new_refreshing(
                "openai-access-token",
                Some("account-123"),
                deadline,
                clock.clone(),
            )
            .expect("OpenAI signer");
            let result = sign_request(&signer, shaped_request()).await;
            if deadline == now + 30 {
                assert!(matches!(result, Err(SignerError::ExpiredToken { .. })));
            } else {
                assert!(result.is_ok(), "one second beyond skew must be usable");
            }
        }

        let access_only = OpenaiSubscriptionSigner::new(
            "access-only-token",
            Some("account-123"),
            Some(now + 30),
            clock.clone(),
        )
        .expect("access-only signer");
        assert!(sign_request(&access_only, shaped_request()).await.is_ok());
        clock.set_unix_secs(now + 30);
        assert!(matches!(
            sign_request(&access_only, shaped_request()).await,
            Err(SignerError::SigningFailed { .. })
        ));
    }

    #[tokio::test]
    async fn antigravity_refreshing_constructor_uses_refresh_skew_boundary() {
        let now = 1_700_000_000;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(now));
        for deadline in [now + 30, now + 31] {
            let signer = GeminiSubscriptionSigner::new_antigravity_refreshing(
                "antigravity-access-token",
                Some("project-123"),
                deadline,
                clock.clone(),
                "https://daily-cloudcode-pa.googleapis.com",
                "antigravity/test",
            )
            .expect("Antigravity signer");
            let mut shaped = shaped_request();
            shaped.set_url(
                "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:generateContent"
                    .parse()
                    .expect("URL"),
            );
            shaped.set_body(Bytes::from_static(br#"{"contents":[]}"#));
            let result = sign_request(&signer, shaped).await;
            if deadline == now + 30 {
                assert!(matches!(result, Err(SignerError::ExpiredToken { .. })));
            } else {
                let signed = result.expect("one second beyond skew must sign");
                assert_eq!(
                    signed.headers().get(USER_AGENT),
                    Some(&HeaderValue::from_static("antigravity/test"))
                );
                let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
                assert_eq!(body["userAgent"], "antigravity");
                assert!(
                    !body
                        .as_object()
                        .expect("envelope object")
                        .contains_key("requestType")
                );
                let request_id = body["requestId"].as_str().expect("request ID");
                let uuid = request_id.strip_prefix("agent-").expect("agent prefix");
                assert_eq!(Uuid::parse_str(uuid).expect("UUID").get_version_num(), 4);
            }
        }
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
            .insert("x-goog-api-key", HeaderValue::from_static("downstream"));
        shaped
            .headers_mut()
            .insert("api-key", HeaderValue::from_static("downstream"));
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
        assert!(!signed.headers().contains_key("x-goog-api-key"));
        assert!(!signed.headers().contains_key("api-key"));
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
            .insert("x-api-key", HeaderValue::from_static("downstream"));
        shaped
            .headers_mut()
            .insert("api-key", HeaderValue::from_static("downstream"));
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
        assert!(!signed.headers().contains_key("x-api-key"));
        assert!(!signed.headers().contains_key("api-key"));
        assert_eq!(
            signed.headers().get("x-request-id"),
            Some(&HeaderValue::from_static("request"))
        );
    }

    struct EnvelopeProbe {
        calls: std::sync::atomic::AtomicUsize,
        fail: bool,
        replace: bool,
    }

    impl cc_lb_upstream::GeminiEnvelopeHook for EnvelopeProbe {
        fn shape_envelope_gemini(
            &self,
            request: GeminiEnvelopeRequest<'_>,
        ) -> Result<cc_lb_upstream::GeminiEnvelopeResult, SignerError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            assert_eq!(request.url.host_str(), Some("cloudcode-pa.googleapis.com"));
            assert_eq!(request.url.path(), "/v1internal:generateContent");
            assert_eq!(request.url.query(), Some("keep=1"));
            let body: Value =
                serde_json::from_slice(request.envelope_body).expect("wrapped envelope");
            assert_eq!(body["model"], "gemini-2.5-pro");
            assert_eq!(body["project"], "stored-project");
            assert_eq!(body["request"], json!({"contents":[]}));
            assert_ne!(
                request.headers.get(AUTHORIZATION),
                Some(&HeaderValue::from_static("Bearer stored-access-token"))
            );
            if self.fail {
                return Err(SignerError::SigningFailed {
                    reason: "envelope hook rejected request".to_owned(),
                });
            }
            if !self.replace {
                return Ok(cc_lb_upstream::GeminiEnvelopeResult::default());
            }
            let mut headers = HeaderMap::new();
            headers.insert("x-envelope-hook", HeaderValue::from_static("applied"));
            headers.insert(CONTENT_LENGTH, HeaderValue::from_static("999"));
            for name in ["authorization", "x-api-key", "x-goog-api-key", "api-key"] {
                headers.insert(name, HeaderValue::from_static("plugin-credential"));
            }
            Ok(cc_lb_upstream::GeminiEnvelopeResult {
                envelope_body: Some(Bytes::from_static(br#"{"request":{"hook":true}}"#)),
                headers: Some(headers),
            })
        }
    }

    fn gemini_envelope_request() -> ShapedRequest {
        let mut shaped = shaped_request();
        shaped.set_url(
            "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent?key=downstream&keep=1"
                .parse()
                .expect("request URL"),
        );
        shaped.set_body(Bytes::from_static(br#"{"contents":[]}"#));
        shaped
    }

    #[tokio::test]
    async fn gemini_envelope_hook_runs_after_wrapping_before_signing_without_url_control() {
        let signer = GeminiSubscriptionSigner::new(
            "stored-access-token",
            Some("stored-project"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer");
        for replace in [false, true] {
            let hook = Arc::new(EnvelopeProbe {
                calls: std::sync::atomic::AtomicUsize::new(0),
                fail: false,
                replace,
            });
            let mut shaped = gemini_envelope_request();
            shaped.set_gemini_envelope_hook(hook.clone());
            let signed = sign_request(&signer, shaped)
                .await
                .expect("signed envelope");
            assert_eq!(hook.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
            assert_eq!(
                signed.url().as_str(),
                "https://cloudcode-pa.googleapis.com/v1internal:generateContent?keep=1"
            );
            assert_eq!(
                signed.headers().get(AUTHORIZATION),
                Some(&HeaderValue::from_static("Bearer stored-access-token"))
            );
            for name in ["x-api-key", "x-goog-api-key", "api-key", "content-length"] {
                assert!(!signed.headers().contains_key(name), "{name}");
            }
            if replace {
                assert_eq!(signed.body().as_ref(), br#"{"request":{"hook":true}}"#);
                assert_eq!(signed.headers()["x-envelope-hook"], "applied");
            } else {
                assert_eq!(
                    signed.body().as_ref(),
                    br#"{"model":"gemini-2.5-pro","project":"stored-project","request":{"contents":[]}}"#
                );
            }
        }
    }

    #[tokio::test]
    async fn gemini_envelope_hook_is_optional_and_ignored_for_unwrapped_or_api_key_requests() {
        let hook = Arc::new(EnvelopeProbe {
            calls: std::sync::atomic::AtomicUsize::new(0),
            fail: true,
            replace: false,
        });
        let signer = GeminiSubscriptionSigner::new(
            "stored-access-token",
            Some("stored-project"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer");
        let signed = sign_request(&signer, gemini_envelope_request())
            .await
            .expect("absent hook preserves legacy signing");
        assert_eq!(
            signed.body().as_ref(),
            br#"{"model":"gemini-2.5-pro","project":"stored-project","request":{"contents":[]}}"#
        );
        let mut unwrapped = shaped_request();
        unwrapped.set_gemini_envelope_hook(hook.clone());
        sign_request(&signer, unwrapped)
            .await
            .expect("unrelated operation skips envelope hook");
        let key_signer =
            ProviderKeySigner::new(ProviderKeyKind::Gemini, "stored-key").expect("key signer");
        let mut native = gemini_envelope_request();
        native.set_gemini_envelope_hook(hook.clone());
        let signed = sign_request(&key_signer, native)
            .await
            .expect("API-key signing skips envelope hook");
        assert_eq!(signed.body().as_ref(), br#"{"contents":[]}"#);
        assert_eq!(hook.calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn gemini_envelope_hook_failure_aborts_signing() {
        let hook = Arc::new(EnvelopeProbe {
            calls: std::sync::atomic::AtomicUsize::new(0),
            fail: true,
            replace: false,
        });
        let mut shaped = gemini_envelope_request();
        shaped.set_gemini_envelope_hook(hook.clone());
        let signer = GeminiSubscriptionSigner::new(
            "stored-access-token",
            Some("stored-project"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer");
        let error = sign_request(&signer, shaped)
            .await
            .expect_err("failed envelope must not be sent");
        assert!(matches!(
            error,
            SignerError::SigningFailed { reason } if reason == "envelope hook rejected request"
        ));
        assert_eq!(hook.calls.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn gemini_subscription_signer_wraps_request_with_project_and_rewrites_path() {
        let mut shaped = shaped_request();
        shaped.set_url(
            "https://daily-cloudcode-pa.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent"
                .parse()
                .expect("url"),
        );
        shaped.set_body(Bytes::from_static(br#"{"contents":[]}"#));
        shaped
            .headers_mut()
            .insert(CONTENT_LENGTH, HeaderValue::from_static("15"));
        shaped
            .headers_mut()
            .insert(USER_AGENT, HeaderValue::from_static("node"));

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
        assert!(!signed.headers().contains_key(CONTENT_LENGTH));
        assert_eq!(
            signed.headers().get(USER_AGENT),
            Some(&HeaderValue::from_static(LEGACY_DAILY_USER_AGENT))
        );
    }

    #[tokio::test]
    async fn antigravity_signer_uses_profile_endpoint_headers_and_generation_metadata() {
        let signer = GeminiSubscriptionSigner::new_antigravity(
            "antigravity-access-token",
            Some("project-123"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
            "https://daily-cloudcode-pa.googleapis.com",
            "antigravity/hub/2.24.0 (aidev_client; os_type=darwin; arch=arm64; cl=1000335682)",
        )
        .expect("antigravity signer");
        let mut request_ids = Vec::new();
        for operation in ["generateContent", "streamGenerateContent"] {
            let mut shaped = shaped_request();
            shaped.set_url(
                format!("https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-pro:{operation}?key=downstream&keep=1")
                    .parse()
                    .expect("request URL"),
            );
            let original = json!({
                "contents": [],
                "tools": [{"functionDeclarations": [{
                    "name": "read_file",
                    "parameters": {"type": "OBJECT"}
                }]}]
            });
            shaped.set_body(Bytes::from(
                serde_json::to_vec(&original).expect("request body"),
            ));
            for name in ["authorization", "x-api-key", "x-goog-api-key", "api-key"] {
                shaped
                    .headers_mut()
                    .insert(name, HeaderValue::from_static("downstream"));
            }
            shaped
                .headers_mut()
                .insert(USER_AGENT, HeaderValue::from_static("GeminiCLI/downstream"));
            shaped
                .headers_mut()
                .insert("x-goog-api-client", HeaderValue::from_static("downstream"));
            shaped
                .headers_mut()
                .insert("client-metadata", HeaderValue::from_static("downstream"));
            shaped
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from_static("15"));

            let signed = sign_request(&signer, shaped).await.expect("sign request");
            assert_eq!(
                signed.url().host_str(),
                Some("daily-cloudcode-pa.googleapis.com")
            );
            assert_eq!(signed.url().path(), format!("/v1internal:{operation}"));
            assert_eq!(
                signed.url().query(),
                Some(if operation == "streamGenerateContent" {
                    "keep=1&alt=sse"
                } else {
                    "keep=1"
                }),
            );
            assert_eq!(
                signed.headers().get(AUTHORIZATION),
                Some(&HeaderValue::from_static("Bearer antigravity-access-token"))
            );
            assert_eq!(
                signed.headers().get(USER_AGENT),
                Some(&HeaderValue::from_static(
                    "antigravity/hub/2.24.0 (aidev_client; os_type=darwin; arch=arm64; cl=1000335682)"
                ))
            );
            assert_eq!(
                signed.headers().get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json"))
            );
            for name in [
                "x-api-key",
                "x-goog-api-key",
                "api-key",
                "x-goog-api-client",
                "client-metadata",
                "content-length",
            ] {
                assert!(!signed.headers().contains_key(name), "{name}");
            }
            let body: Value = serde_json::from_slice(signed.body()).expect("JSON envelope");
            assert_eq!(body["model"], "gemini-3-pro");
            assert_eq!(body["project"], "project-123");
            assert_eq!(body["request"], original);
            assert_eq!(body["userAgent"], "antigravity");
            assert!(
                !body
                    .as_object()
                    .expect("envelope object")
                    .contains_key("requestType")
            );
            let request_id = body["requestId"].as_str().expect("request ID");
            let uuid = request_id.strip_prefix("agent-").expect("agent prefix");
            assert_eq!(Uuid::parse_str(uuid).expect("UUID").get_version_num(), 4);
            request_ids.push(request_id.to_owned());
        }
        assert_ne!(request_ids.first(), request_ids.last());
    }

    fn antigravity_test_signer() -> GeminiSubscriptionSigner {
        GeminiSubscriptionSigner::new_antigravity(
            "antigravity-access-token",
            Some("project-123"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
            "https://daily-cloudcode-pa.googleapis.com",
            "antigravity/test",
        )
        .expect("Antigravity signer")
    }

    fn gemini_generation_request(path: &str, body: &Value) -> ShapedRequest {
        let mut shaped = shaped_request();
        shaped.set_url(
            format!("https://cloudcode-pa.googleapis.com{path}")
                .parse()
                .expect("generation URL"),
        );
        shaped.set_body(Bytes::from(serde_json::to_vec(body).expect("request body")));
        shaped
    }

    #[tokio::test]
    async fn antigravity_already_wrapped_preserves_caller_request_id_and_omits_request_type() {
        let signer = antigravity_test_signer();
        for path in [
            "/v1beta/models/gemini-3-pro:generateContent",
            "/v1internal:generateContent",
        ] {
            for caller_id in ["caller-req-123", "   "] {
                let original = json!({
                    "model": "gemini-3-pro",
                    "request": {"contents": []},
                    "requestId": caller_id,
                    "userAgent": "other"
                });
                let mut current = original;
                for pass in 1..=2 {
                    let signed = sign_request(&signer, gemini_generation_request(path, &current))
                        .await
                        .unwrap_or_else(|error| {
                            panic!("{path}, {caller_id:?}, pass {pass}: {error:?}")
                        });
                    let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
                    assert_eq!(body["requestId"], caller_id, "{path}, pass {pass}");
                    assert_eq!(body["userAgent"], "antigravity", "{path}, pass {pass}");
                    assert!(
                        !body
                            .as_object()
                            .expect("envelope object")
                            .contains_key("requestType"),
                        "{path}, pass {pass}: {body}"
                    );
                    current = body;
                }
            }

            let original = json!({
                "model": "gemini-3-pro",
                "request": {"contents": []},
                "requestId": "caller-req-123",
                "requestType": "custom",
                "userAgent": "other"
            });
            let signed = sign_request(&signer, gemini_generation_request(path, &original))
                .await
                .unwrap_or_else(|error| panic!("{path}, explicit requestType: {error:?}"));
            let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
            assert_eq!(body["requestType"], "custom", "{path}");
            assert_eq!(body["requestId"], "caller-req-123", "{path}");
            assert_eq!(body["userAgent"], "antigravity", "{path}");

            let mut generated_ids = std::collections::HashSet::new();
            for (case, request_id) in [
                ("absent", None),
                ("empty", Some(json!(""))),
                ("null", Some(Value::Null)),
                ("number", Some(json!(123))),
            ] {
                let mut original = json!({
                    "model": "gemini-3-pro",
                    "request": {"contents": []},
                    "userAgent": "other"
                });
                if let Some(request_id) = request_id {
                    original["requestId"] = request_id;
                }
                for attempt in 1..=2 {
                    let signed = sign_request(&signer, gemini_generation_request(path, &original))
                        .await
                        .unwrap_or_else(|error| {
                            panic!("{path}, {case}, attempt {attempt}: {error:?}")
                        });
                    let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
                    let request_id = body["requestId"].as_str().expect("generated request ID");
                    let uuid = request_id.strip_prefix("agent-").expect("agent prefix");
                    assert_eq!(
                        Uuid::parse_str(uuid).expect("UUID").get_version_num(),
                        4,
                        "{path}, {case}, attempt {attempt}"
                    );
                    assert!(
                        generated_ids.insert(request_id.to_owned()),
                        "{path}, {case}: repeated generated ID"
                    );
                    assert_eq!(body["userAgent"], "antigravity", "{path}, {case}");
                    assert!(
                        !body
                            .as_object()
                            .expect("envelope object")
                            .contains_key("requestType"),
                        "{path}, {case}: {body}"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn antigravity_qualifies_bare_tiered_models_from_thinking_level() {
        let signer = antigravity_test_signer();
        for (family, level, expected) in [
            ("gemini-3.8-flash", "LOW", "gemini-3.8-flash-low"),
            ("gemini-3.8-flash", "MEDIUM", "gemini-3.8-flash-medium"),
            ("gemini-3.8-flash", "HIGH", "gemini-3.8-flash-high"),
            ("gemini-3.8-flash", "XHIGH", "gemini-3.8-flash-xhigh"),
            ("gemini-3.7-flash", "low", "gemini-3.7-flash-low"),
            ("gemini-3.7-flash", "mEdIuM", "gemini-3.7-flash-medium"),
            ("gemini-3.7-flash", "high", "gemini-3.7-flash-high"),
            ("gemini-3.7-flash", "xHiGh", "gemini-3.7-flash-xhigh"),
            ("gemini-3.6-flash", "Low", "gemini-3.6-flash-low"),
            ("gemini-3.6-flash", "MEDIUM", "gemini-3.6-flash-medium"),
            ("gemini-3.6-flash", "HIGH", "gemini-3.6-flash-high"),
            ("gemini-3.6-flash", "XHIGH", "gemini-3.6-flash-xhigh"),
            ("gemini-3.1-pro", "LOW", "gemini-3.1-pro-low"),
            ("gemini-3.1-pro", "mEdIuM", "gemini-3.1-pro-medium"),
            ("gemini-3.1-pro", "high", "gemini-3.1-pro-high"),
            ("gemini-3.1-pro", "xHiGh", "gemini-3.1-pro-xhigh"),
            ("gemini-4-carbon", "Low", "gemini-4-carbon-low"),
            ("gemini-4-carbon", "MEDIUM", "gemini-4-carbon-medium"),
            ("gemini-4-carbon", "HIGH", "gemini-4-carbon-high"),
            ("gemini-4-carbon", "xHiGh", "gemini-4-carbon-xhigh"),
            ("gemini-4-argon", "low", "gemini-4-argon-low"),
            ("gemini-4-argon", "mEdIuM", "gemini-4-argon-medium"),
            ("gemini-4-argon", "high", "gemini-4-argon-high"),
            ("gemini-4-argon", "XHIGH", "gemini-4-argon-xhigh"),
            ("gemini-4-hydrogen", "LOW", "gemini-4-hydrogen-low"),
            ("gemini-4-hydrogen", "MEDIUM", "gemini-4-hydrogen-medium"),
            ("gemini-4-hydrogen", "HIGH", "gemini-4-hydrogen-high"),
            ("gemini-4-hydrogen", "xHiGh", "gemini-4-hydrogen-xhigh"),
        ] {
            let original = json!({
                "contents": [],
                "generationConfig": {"thinkingConfig": {"thinkingLevel": level}}
            });
            let path = format!("/v1beta/models/{family}:generateContent");
            let signed = sign_request(&signer, gemini_generation_request(&path, &original))
                .await
                .unwrap_or_else(|error| panic!("{family}, {level}: {error:?}"));
            let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
            assert_eq!(body["model"], expected, "{family}, {level}");
            assert_eq!(body["request"], original, "{family}, {level}");
            assert!(
                !body
                    .as_object()
                    .expect("envelope object")
                    .contains_key("requestType"),
                "{family}, {level}: {body}"
            );
        }

        for (case, path, envelope_model, thinking, expected) in [
            (
                "stream",
                "/v1beta/models/gemini-3.8-flash:streamGenerateContent",
                None,
                json!({"thinkingLevel": "HIGH"}),
                "gemini-3.8-flash-high",
            ),
            (
                "internal unwrapped",
                "/v1internal:generateContent",
                None,
                json!({"thinkingLevel": "HIGH"}),
                "gemini-3.8-flash-high",
            ),
            (
                "already wrapped bare",
                "/v1internal:generateContent",
                Some("gemini-3.8-flash"),
                json!({"thinkingLevel": "HIGH"}),
                "gemini-3.8-flash-high",
            ),
            (
                "already wrapped prefixed",
                "/v1beta/models/gemini-3.8-flash:generateContent",
                Some("models/gemini-3.8-flash"),
                json!({"thinkingLevel": "HIGH"}),
                "gemini-3.8-flash-high",
            ),
            (
                "snake case",
                "/v1beta/models/gemini-3.8-flash:generateContent",
                None,
                json!({"thinking_level": "XHIGH"}),
                "gemini-3.8-flash-xhigh",
            ),
            (
                "camel case precedence",
                "/v1beta/models/gemini-3.8-flash:generateContent",
                None,
                json!({"thinkingLevel": "HIGH", "thinking_level": "LOW"}),
                "gemini-3.8-flash-high",
            ),
        ] {
            let mut original = json!({
                "contents": [],
                "generationConfig": {"thinkingConfig": thinking}
            });
            let expected_request = if let Some(model) = envelope_model {
                let request = original.clone();
                original = json!({"model": model, "request": request});
                request
            } else {
                if path == "/v1internal:generateContent" {
                    original["model"] = json!("gemini-3.8-flash");
                }
                original.clone()
            };
            let mut shaped = gemini_generation_request(path, &original);
            shaped
                .headers_mut()
                .insert(CONTENT_LENGTH, HeaderValue::from_static("5"));
            let signed = sign_request(&signer, shaped)
                .await
                .unwrap_or_else(|error| panic!("{case}: {error:?}"));
            let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
            assert_eq!(body["model"], expected, "{case}");
            assert_eq!(body["request"], expected_request, "{case}");
            assert!(
                !body
                    .as_object()
                    .expect("envelope object")
                    .contains_key("requestType"),
                "{case}: {body}"
            );
            assert!(!signed.headers().contains_key(CONTENT_LENGTH), "{case}");
            if case == "stream" {
                assert_eq!(signed.url().query(), Some("alt=sse"), "{case}");
            }
        }
    }

    #[tokio::test]
    async fn antigravity_leaves_suffixed_nontiered_and_unrecognized_thinking_models_unchanged() {
        let signer = antigravity_test_signer();
        for (model, thinking_level) in [
            ("gemini-3.8-flash-low", json!("HIGH")),
            ("gemini-4-hydrogen-xhigh", json!("LOW")),
            ("gemini-3.1-pro-high", json!("LOW")),
            ("gemini-3.1-pro-preview", json!("HIGH")),
            ("gemini-3.8-flash-lite", json!("HIGH")),
            ("GEMINI-3.8-FLASH", json!("HIGH")),
            ("gemini-2.5-flash", json!("HIGH")),
            ("gemini-2.5-flash-lite", json!("HIGH")),
            ("gemini-3-flash", json!("HIGH")),
            ("gemini-3-pro", json!("HIGH")),
            ("gemini-3.1-flash-lite", json!("HIGH")),
            ("gemini-3.5-flash-lite", json!("HIGH")),
            ("claude-sonnet-5-5-low", json!("HIGH")),
            ("gpt-oss-120b-medium", json!("HIGH")),
        ] {
            let original = json!({
                "contents": [],
                "generationConfig": {"thinkingConfig": {"thinkingLevel": thinking_level}}
            });
            let path = format!("/v1beta/models/{model}:generateContent");
            let signed = sign_request(&signer, gemini_generation_request(&path, &original))
                .await
                .unwrap_or_else(|error| panic!("{model}, {thinking_level}: {error:?}"));
            let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
            assert_eq!(body["model"], model, "{model}, {thinking_level}");
            assert_eq!(body["request"], original, "{model}, {thinking_level}");
        }

        for (case, original) in [
            ("no generationConfig", json!({"contents": []})),
            (
                "no thinkingConfig",
                json!({"contents": [], "generationConfig": {}}),
            ),
            (
                "thinkingBudget only",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingBudget": 1024}}}),
            ),
            (
                "null shadows snake case",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": null, "thinking_level": "HIGH"}}}),
            ),
            (
                "number shadows snake case",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": 123, "thinking_level": "HIGH"}}}),
            ),
            (
                "unrecognized shadows snake case",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": "MINIMAL", "thinking_level": "HIGH"}}}),
            ),
            (
                "empty",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": ""}}}),
            ),
            (
                "leading whitespace",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": " HIGH"}}}),
            ),
            (
                "trailing whitespace",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": "HIGH "}}}),
            ),
            (
                "unspecified",
                json!({"contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": "THINKING_LEVEL_UNSPECIFIED"}}}),
            ),
        ] {
            let signed = sign_request(
                &signer,
                gemini_generation_request(
                    "/v1beta/models/gemini-3.8-flash:generateContent",
                    &original,
                ),
            )
            .await
            .unwrap_or_else(|error| panic!("{case}: {error:?}"));
            let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
            assert_eq!(body["model"], "gemini-3.8-flash", "{case}");
            assert_eq!(body["request"], original, "{case}");
        }

        for (case, original, expected_model) in [
            (
                "double models prefix",
                json!({
                    "model": "models/models/gemini-3.8-flash",
                    "request": {
                        "contents": [],
                        "generationConfig": {"thinkingConfig": {"thinkingLevel": "HIGH"}}
                    }
                }),
                "models/models/gemini-3.8-flash",
            ),
            (
                "non-object request",
                json!({"model": "gemini-3.8-flash", "request": "not-an-object"}),
                "gemini-3.8-flash",
            ),
        ] {
            let signed = sign_request(
                &signer,
                gemini_generation_request("/v1internal:generateContent", &original),
            )
            .await
            .unwrap_or_else(|error| panic!("{case}: {error:?}"));
            let body: Value = serde_json::from_slice(signed.body()).expect("envelope");
            assert_eq!(body["model"], expected_model, "{case}");
            assert_eq!(body["request"], original["request"], "{case}");
        }
    }

    struct AntigravityEnvelopeRecorder {
        envelopes: std::sync::Mutex<Vec<Value>>,
        replacement: Option<Bytes>,
    }

    impl cc_lb_upstream::GeminiEnvelopeHook for AntigravityEnvelopeRecorder {
        fn shape_envelope_gemini(
            &self,
            request: GeminiEnvelopeRequest<'_>,
        ) -> Result<cc_lb_upstream::GeminiEnvelopeResult, SignerError> {
            let envelope = serde_json::from_slice(request.envelope_body).expect("hook envelope");
            self.envelopes
                .lock()
                .expect("record envelopes")
                .push(envelope);
            Ok(cc_lb_upstream::GeminiEnvelopeResult {
                envelope_body: self.replacement.clone(),
                headers: None,
            })
        }
    }

    #[tokio::test]
    async fn gemini_envelope_hook_observes_qualified_antigravity_envelope_without_post_processing()
    {
        let signer = antigravity_test_signer();
        let replacement = Bytes::from_static(
            br#"{"model":"gemini-3.8-flash","request":{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}}"#,
        );
        for replacement in [None, Some(replacement)] {
            let hook = Arc::new(AntigravityEnvelopeRecorder {
                envelopes: std::sync::Mutex::new(Vec::new()),
                replacement: replacement.clone(),
            });
            let original = json!({
                "contents": [],
                "generationConfig": {"thinkingConfig": {"thinkingLevel": "HIGH"}}
            });
            let mut shaped = gemini_generation_request(
                "/v1beta/models/gemini-3.8-flash:generateContent",
                &original,
            );
            shaped.set_gemini_envelope_hook(hook.clone());
            let signed = sign_request(&signer, shaped)
                .await
                .expect("hook signs envelope");
            let envelopes = hook.envelopes.lock().expect("recorded envelopes");
            let [body] = envelopes.as_slice() else {
                panic!("hook must observe one envelope, got {envelopes:?}");
            };
            assert_eq!(body["model"], "gemini-3.8-flash-high");
            assert_eq!(body["request"], original);
            assert_eq!(body["userAgent"], "antigravity");
            let request_id = body["requestId"].as_str().expect("request ID");
            let uuid = request_id.strip_prefix("agent-").expect("agent prefix");
            assert_eq!(Uuid::parse_str(uuid).expect("UUID").get_version_num(), 4);
            assert!(
                !body
                    .as_object()
                    .expect("envelope object")
                    .contains_key("requestType")
            );
            if let Some(replacement) = replacement {
                assert_eq!(
                    signed.body(),
                    &replacement,
                    "hook replacement is emitted verbatim"
                );
            } else {
                let emitted: Value =
                    serde_json::from_slice(signed.body()).expect("signed envelope");
                assert_eq!(&emitted, body, "hook observes the final generated envelope");
            }
        }
    }

    #[tokio::test]
    async fn antigravity_signer_preserves_count_tokens_shape_and_custom_endpoint() {
        let signer = GeminiSubscriptionSigner::new_antigravity(
            "access-token",
            Some("project-123"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
            "http://gateway.invalid/custom-prefix",
            "antigravity/custom",
        )
        .expect("antigravity signer");
        let mut shaped = shaped_request();
        shaped.set_url(
            "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3-flash:countTokens"
                .parse()
                .expect("URL"),
        );
        shaped.set_body(Bytes::from_static(
            br#"{"generateContentRequest":{"contents":[]}}"#,
        ));
        let signed = sign_request(&signer, shaped).await.expect("sign request");
        assert_eq!(
            signed.url().as_str(),
            "http://gateway.invalid/v1internal:countTokens"
        );
        assert_eq!(
            signed.body().as_ref(),
            br#"{"request":{"contents":[],"model":"models/gemini-3-flash"}}"#
        );
        assert_eq!(
            signed.headers().get(USER_AGENT),
            Some(&HeaderValue::from_static("antigravity/custom"))
        );

        let mut shaped = shaped_request();
        shaped.set_url(
            "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3.8-flash:countTokens"
                .parse()
                .expect("URL"),
        );
        shaped.set_body(Bytes::from_static(
            br#"{"generateContentRequest":{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}}"#,
        ));
        let signed = sign_request(&signer, shaped)
            .await
            .expect("tiered countTokens signs without qualification");
        assert_eq!(
            signed.body().as_ref(),
            br#"{"request":{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}},"model":"models/gemini-3.8-flash"}}"#
        );
    }

    #[tokio::test]
    async fn gemini_cli_generation_envelope_remains_byte_identical() {
        let signer = GeminiSubscriptionSigner::new(
            "gemini-access-token",
            Some("project-123"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("Gemini CLI signer");
        for (operation, expected_query) in [
            ("generateContent", "keep=1"),
            ("streamGenerateContent", "keep=1&alt=sse"),
        ] {
            let mut shaped = shaped_request();
            shaped.set_url(
                format!("https://cloudcode-pa.googleapis.com/v1beta/models/gemini-2.5-pro:{operation}?keep=1")
                    .parse()
                    .expect("request URL"),
            );
            shaped.set_body(Bytes::from_static(
                br#"{"contents":[],"generationConfig":{"temperature":0.5}}"#,
            ));
            shaped
                .headers_mut()
                .insert(USER_AGENT, HeaderValue::from_static("GeminiCLI/existing"));
            let signed = sign_request(&signer, shaped).await.expect("sign request");
            assert_eq!(signed.url().host_str(), Some("cloudcode-pa.googleapis.com"));
            assert_eq!(signed.url().path(), format!("/v1internal:{operation}"));
            assert_eq!(signed.url().query(), Some(expected_query));
            assert_eq!(signed.body().as_ref(), br#"{"model":"gemini-2.5-pro","project":"project-123","request":{"contents":[],"generationConfig":{"temperature":0.5}}}"#);
            assert_eq!(
                signed.headers().get(USER_AGENT),
                Some(&HeaderValue::from_static("GeminiCLI/existing"))
            );
        }

        let now = 1_700_000_000;
        let clock = Arc::new(cc_lb_clock::TestClock::new_at_secs(now));
        let refreshing = GeminiSubscriptionSigner::new_refreshing(
            "gemini-access-token",
            Some("project-123"),
            now + 31,
            clock,
        )
        .expect("refreshing Gemini CLI signer");
        for (constructor, signer) in [("new", &signer), ("new_refreshing", &refreshing)] {
            for (shape, original, expected) in [
                (
                    "unwrapped",
                    br#"{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}"#.as_slice(),
                    br#"{"model":"gemini-3.8-flash","project":"project-123","request":{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}}"#.as_slice(),
                ),
                (
                    "already wrapped",
                    br#"{"model":"gemini-3.8-flash","project":"project-123","request":{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}},"requestId":"caller-req-123","requestType":"custom"}"#.as_slice(),
                    br#"{"model":"gemini-3.8-flash","project":"project-123","request":{"contents":[],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}},"requestId":"caller-req-123","requestType":"custom"}"#.as_slice(),
                ),
            ] {
                let mut shaped = shaped_request();
                shaped.set_url(
                    "https://cloudcode-pa.googleapis.com/v1beta/models/gemini-3.8-flash:generateContent"
                        .parse()
                        .expect("URL"),
                );
                shaped.set_body(Bytes::from_static(original));
                let signed = sign_request(signer, shaped)
                    .await
                    .unwrap_or_else(|error| panic!("{constructor}, {shape}: {error:?}"));
                assert_eq!(signed.body().as_ref(), expected, "{constructor}, {shape}");
            }
        }
    }

    #[tokio::test]
    async fn gemini_subscription_signer_wraps_count_tokens_requests() {
        for original in [
            serde_json::json!({"contents": []}),
            serde_json::json!({"generateContentRequest": {"contents": []}}),
        ] {
            let mut shaped = shaped_request();
            shaped.set_url(
                "https://daily-cloudcode-pa.googleapis.com/v1beta/models/gemini-2.5-flash:countTokens"
                    .parse()
                    .expect("url"),
            );
            shaped.set_body(Bytes::from(serde_json::to_vec(&original).expect("body")));

            let signer = GeminiSubscriptionSigner::new(
                "gemini-access-token",
                None,
                None,
                Arc::new(cc_lb_clock::SystemClock),
            )
            .expect("signer");
            let signed = sign_request(&signer, shaped).await.expect("sign request");
            let body: Value = serde_json::from_slice(signed.body()).expect("json body");

            assert_eq!(signed.url().path(), "/v1internal:countTokens");
            assert_eq!(
                body,
                serde_json::json!({
                    "request": {
                        "model": "models/gemini-2.5-flash",
                        "contents": []
                    }
                })
            );
        }
    }
    #[tokio::test]
    async fn gemini_signers_append_alt_sse_on_stream_generate_content_when_missing() {
        let mut shaped = shaped_request();
        shaped.set_url(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent?key=leaked&keep=1"
                .parse()
                .expect("url"),
        );
        let key_signer =
            ProviderKeySigner::new(ProviderKeyKind::Gemini, "gemini-secret").expect("signer");
        let signed_key = sign_request(&key_signer, shaped).await.expect("sign key");
        assert_eq!(signed_key.url().query(), Some("keep=1&alt=sse"));

        let mut oauth_shaped = shaped_request();
        oauth_shaped.set_url(
            "https://daily-cloudcode-pa.googleapis.com/v1beta/models/gemini-2.5-pro:streamGenerateContent?key=leaked&keep=1"
                .parse()
                .expect("url"),
        );
        oauth_shaped.set_body(Bytes::from_static(br#"{"contents":[]}"#));
        let oauth_signer = GeminiSubscriptionSigner::new(
            "gemini-access-token",
            Some("project-123"),
            None,
            Arc::new(cc_lb_clock::SystemClock),
        )
        .expect("signer");
        let signed_oauth = sign_request(&oauth_signer, oauth_shaped)
            .await
            .expect("sign oauth");
        assert_eq!(
            signed_oauth.url().path(),
            "/v1internal:streamGenerateContent"
        );
        assert_eq!(signed_oauth.url().query(), Some("keep=1&alt=sse"));
    }
}
