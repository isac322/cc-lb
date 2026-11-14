use super::usage::{
    GeminiQuotaGroup, classify_gemini_summary_bucket, gemini_summary_bucket_usable,
};
use bytes::{Bytes, BytesMut};
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_oauth_protocol::{
    ANTIGRAVITY_OAUTH_USER_AGENT, GeminiOAuthProfile, TokenEndpointResponse,
    parse_token_endpoint_response, refresh_token_form_body, refresh_token_json_body,
};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_storage_api::UpstreamRecord;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/";
const DEFAULT_OPENAI_CODEX_BASE_URL: &str = "https://chatgpt.com/backend-api/codex";
const DEFAULT_GEMINI_BASE_URL: &str = "https://generativelanguage.googleapis.com/";
const DEFAULT_GEMINI_CODE_ASSIST_BASE_URL: &str = "https://cloudcode-pa.googleapis.com/v1internal";
const OPENAI_OAUTH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const OPENAI_CODEX_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const GEMINI_OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const GEMINI_OAUTH_CLIENT_ID: &str =
    "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com";
const GEMINI_OAUTH_CLIENT_SECRET: &str = "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";
/// One poll request returns the quota windows and the `cedar_ember`
/// limit-reset block together. `skip_spend` is deliberately absent so the
/// provider keeps reporting `extra_usage`.
const USAGE_PATH: &str = "api/oauth/usage?cedar_ember=1";
const SCHEDULER_RESPONSE_BODY_CAP_BYTES: usize = 1 << 20;

pub(super) type JsonHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

#[cfg(test)]
tokio::task_local! {
    static DENIED_TOKEN_REFRESH_CALLS: std::sync::Arc<std::sync::atomic::AtomicUsize>;
}

#[cfg(test)]
pub(super) async fn with_denied_token_refresh<F: Future>(
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    future: F,
) -> F::Output {
    DENIED_TOKEN_REFRESH_CALLS.scope(calls, future).await
}

pub(super) struct UsageFetchResponse {
    pub status: StatusCode,
    pub body: Bytes,
}

struct JsonRequest<'a> {
    url: Url,
    method: http::Method,
    body: Bytes,
    content_type: &'a str,
    access_token: &'a str,
    account_id: Option<&'a str>,
    codex: bool,
    user_agent: &'a str,
    anthropic: bool,
    cancel: &'a CancellationToken,
}

#[derive(thiserror::Error)]
pub(super) enum RefreshRequestError {
    #[error("oauth token endpoint returned {status}")]
    Endpoint { status: StatusCode, body: Bytes },
    #[error(transparent)]
    Other(#[from] SchedulerError),
}

impl std::fmt::Debug for RefreshRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Endpoint { status, .. } => formatter
                .debug_struct("Endpoint")
                .field("status", status)
                .finish_non_exhaustive(),
            Self::Other(error) => std::fmt::Debug::fmt(error, formatter),
        }
    }
}
async fn read_bounded_response_body<B>(
    body: B,
    cancel: &CancellationToken,
    timeout_message: &'static str,
) -> SchedulerResult<Bytes>
where
    B: BodyExt + Unpin,
    B::Data: AsRef<[u8]>,
    B::Error: std::fmt::Display,
{
    let collect = async move {
        let mut output = BytesMut::new();
        let mut body = body;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|error| SchedulerError::Job(error.to_string()))?;
            let Ok(data) = frame.into_data() else {
                continue;
            };
            let data = data.as_ref();
            if output.len().saturating_add(data.len()) > SCHEDULER_RESPONSE_BODY_CAP_BYTES {
                return Err(SchedulerError::Job(
                    "scheduler response body exceeded the configured cap".to_owned(),
                ));
            }
            output.extend_from_slice(data);
        }
        Ok(output.freeze())
    };
    tokio::select! {
        _ = cancel.cancelled() => Err(SchedulerError::Job("scheduler response body cancelled".to_owned())),
        result = tokio::time::timeout(Duration::from_secs(30), collect) => {
            result.map_err(|_| SchedulerError::Job(timeout_message.to_owned()))?
        }
    }
}

pub(super) async fn request_refresh(
    http: &JsonHttpClient,
    oauth_cfg: &AnthropicOAuthConfig,
    google_config: &cc_lb_config::Config,
    oauth_kind: cc_lb_storage_api::upstream::UpstreamKind,
    oauth_profile: cc_lb_oauth_protocol::GeminiOAuthProfile,
    cancel: &CancellationToken,
    refresh_token: &str,
) -> Result<TokenEndpointResponse, RefreshRequestError> {
    #[cfg(test)]
    if DENIED_TOKEN_REFRESH_CALLS
        .try_with(|calls| calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
        .is_ok()
    {
        return Err(SchedulerError::Job(
            "token refresh transport denied by scoped test".to_owned(),
        )
        .into());
    }
    let antigravity = google_config.oauth.antigravity.clone().unwrap_or_default();
    let (token_url, content_type, body) = match oauth_kind {
        cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth => (
            oauth_cfg.token_url.as_str(),
            "application/x-www-form-urlencoded",
            refresh_token_form_body(oauth_cfg.client_id.as_str(), refresh_token),
        ),
        cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth => (
            OPENAI_OAUTH_TOKEN_URL,
            "application/json",
            refresh_token_json_body(OPENAI_CODEX_CLIENT_ID, refresh_token),
        ),
        cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth => {
            let (client_id, client_secret, token_url) = match oauth_profile {
                cc_lb_oauth_protocol::GeminiOAuthProfile::Antigravity => {
                    if antigravity.missing_client_credentials().is_some() {
                        return Err(SchedulerError::Job(
                            "oauth_client_not_configured: configure oauth.antigravity.client_id and oauth.antigravity.client_secret before refreshing Antigravity credentials".to_owned(),
                        )
                        .into());
                    }
                    (
                        antigravity.client_id.as_str(),
                        antigravity.client_secret.as_str(),
                        antigravity.token_url.as_str(),
                    )
                }
                cc_lb_oauth_protocol::GeminiOAuthProfile::GeminiCli => (
                    GEMINI_OAUTH_CLIENT_ID,
                    GEMINI_OAUTH_CLIENT_SECRET,
                    GEMINI_OAUTH_TOKEN_URL,
                ),
            };
            (
                token_url,
                "application/x-www-form-urlencoded",
                cc_lb_oauth_protocol::gemini_refresh_token_form_body(
                    client_id,
                    client_secret,
                    refresh_token,
                ),
            )
        }
        _ => {
            return Err(SchedulerError::Job("unsupported oauth upstream kind".to_owned()).into());
        }
    };
    let request = Request::post(token_url)
        .header(http::header::CONTENT_TYPE, content_type)
        .header(http::header::CONTENT_LENGTH, body.len().to_string())
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(SchedulerError::Job("oauth refresh cancelled".to_owned()).into()),
        response = tokio::time::timeout(Duration::from_secs(30), http.request(request)) => response,
    }
    .map_err(|_| SchedulerError::Job("oauth refresh request timed out".to_owned()))?
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let status = response.status();
    if !status.is_success() {
        let body = if status == StatusCode::BAD_REQUEST {
            read_bounded_response_body(
                response.into_body(),
                cancel,
                "oauth refresh response body timed out",
            )
            .await?
        } else {
            Bytes::new()
        };
        return Err(RefreshRequestError::Endpoint { status, body });
    }
    let body = read_bounded_response_body(
        response.into_body(),
        cancel,
        "oauth refresh response body timed out",
    )
    .await?;
    parse_token_endpoint_response(&body)
        .map_err(|error| SchedulerError::Job(error.to_string()).into())
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn fetch_usage(
    http: &JsonHttpClient,
    base_url: &Url,
    kind: cc_lb_storage_api::upstream::UpstreamKind,
    profile: GeminiOAuthProfile,
    access_token: &str,
    account_id: Option<&str>,
    project_id: Option<&str>,
    user_agent: &str,
    cancel: &CancellationToken,
) -> SchedulerResult<UsageFetchResponse> {
    fetch_json_request(
        http,
        usage_request(
            base_url,
            kind,
            profile,
            access_token,
            account_id,
            project_id,
            user_agent,
            cancel,
        )?,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
fn usage_request<'a>(
    base_url: &Url,
    kind: cc_lb_storage_api::upstream::UpstreamKind,
    profile: GeminiOAuthProfile,
    access_token: &'a str,
    account_id: Option<&'a str>,
    project_id: Option<&str>,
    user_agent: &'a str,
    cancel: &'a CancellationToken,
) -> SchedulerResult<JsonRequest<'a>> {
    let (url, method, body, content_type) = match kind {
        cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth => (
            base_url
                .join(USAGE_PATH)
                .map_err(|error| SchedulerError::Job(format!("invalid usage base URL: {error}")))?,
            http::Method::GET,
            Bytes::new(),
            "application/json",
        ),
        cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth => (
            codex_usage_url(base_url),
            http::Method::GET,
            Bytes::new(),
            "application/json",
        ),
        cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth => {
            let project = project_id.unwrap_or_default();
            let body = serde_json::to_vec(&serde_json::json!({ "project": project })).map_err(
                |error| SchedulerError::Job(format!("Gemini quota body encode failed: {error}")),
            )?;
            let url = gemini_summary_url(base_url)?;
            (
                url,
                http::Method::POST,
                Bytes::from(body),
                "application/json",
            )
        }
        _ => {
            return Err(SchedulerError::Job(
                "unsupported usage poll kind".to_owned(),
            ));
        }
    };
    Ok(JsonRequest {
        url,
        method,
        body,
        content_type,
        access_token,
        account_id,
        codex: kind == cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth,
        user_agent: if kind == cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth
            && profile == GeminiOAuthProfile::Antigravity
        {
            ANTIGRAVITY_OAUTH_USER_AGENT
        } else {
            user_agent
        },
        anthropic: kind == cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth,
        cancel,
    })
}

pub(super) async fn fetch_gemini_quota(
    http: &JsonHttpClient,
    base_url: &Url,
    access_token: &str,
    project_id: Option<&str>,
    user_agent: &str,
    cancel: &CancellationToken,
) -> SchedulerResult<UsageFetchResponse> {
    let project = project_id.unwrap_or_default();
    let body = serde_json::to_vec(&serde_json::json!({ "project": project })).map_err(|error| {
        SchedulerError::Job(format!("Gemini quota body encode failed: {error}"))
    })?;
    fetch_json_request(
        http,
        JsonRequest {
            url: gemini_quota_url(base_url)?,
            method: http::Method::POST,
            body: Bytes::from(body),
            content_type: "application/json",
            access_token,
            account_id: None,
            codex: false,
            user_agent,
            anthropic: false,
            cancel,
        },
    )
    .await
}

/// Best-effort display enrichment, using only the configured OAuth metadata
/// endpoints. Never follows a 401 into token refresh or account onboarding.
pub(super) async fn fetch_gemini_account_metadata(
    http: &JsonHttpClient,
    config: &cc_lb_config::Config,
    bundle: &OAuthTokenBundle,
    cancel: &CancellationToken,
) -> (Option<serde_json::Value>, Option<serde_json::Value>) {
    let default_antigravity;
    let default_cli;
    let (code_assist_url, userinfo_url, user_agent, body) = match bundle.oauth_profile {
        GeminiOAuthProfile::Antigravity => {
            let config = match config.oauth.antigravity.as_ref() {
                Some(config) => config,
                None => {
                    default_antigravity = cc_lb_config::AntigravityOAuthConfig::default();
                    &default_antigravity
                }
            };
            (
                &config.code_assist_url,
                &config.userinfo_url,
                config.user_agent.as_str(),
                Bytes::from_static(br#"{"metadata":{"ideType":"ANTIGRAVITY"}}"#),
            )
        }
        GeminiOAuthProfile::GeminiCli => {
            let config = match config.oauth.gemini.as_ref() {
                Some(config) => config,
                None => {
                    default_cli = cc_lb_config::GeminiOAuthConfig::default();
                    &default_cli
                }
            };
            // Match Gemini CLI setup.ts's core metadata, scoped to this
            // credential's already configured project. Never onboard here.
            let body = serde_json::json!({
                "cloudaicompanionProject": bundle.project_id,
                "metadata": {
                    "ideType": "IDE_UNSPECIFIED", "platform": "PLATFORM_UNSPECIFIED",
                    "pluginType": "GEMINI", "duetProject": bundle.project_id,
                }
            });
            let Ok(body) = serde_json::to_vec(&body) else {
                return (None, None);
            };
            (
                &config.code_assist_url,
                &config.userinfo_url,
                "GeminiCLI/cc-lb",
                Bytes::from(body),
            )
        }
    };
    let base = code_assist_url.as_str().trim_end_matches('/');
    let endpoint = format!(
        "{}/v1internal:loadCodeAssist",
        base.trim_end_matches("/v1internal")
    );
    let Ok(load_url) = Url::parse(&endpoint) else {
        return (None, None);
    };
    let load = JsonRequest {
        url: load_url,
        method: http::Method::POST,
        body,
        content_type: "application/json",
        access_token: &bundle.access_token,
        account_id: None,
        codex: false,
        user_agent,
        anthropic: false,
        cancel,
    };
    let userinfo = JsonRequest {
        url: userinfo_url.clone(),
        method: http::Method::GET,
        body: Bytes::new(),
        content_type: "application/json",
        access_token: &bundle.access_token,
        account_id: None,
        codex: false,
        user_agent,
        anthropic: false,
        cancel,
    };
    async fn fetch(http: &JsonHttpClient, request: JsonRequest<'_>) -> Option<serde_json::Value> {
        let response =
            tokio::time::timeout(Duration::from_secs(5), fetch_json_request(http, request))
                .await
                .ok()?
                .ok()?;
        response
            .status
            .is_success()
            .then(|| serde_json::from_slice(&response.body).ok())
            .flatten()
    }
    tokio::join!(fetch(http, load), fetch(http, userinfo))
}

async fn fetch_json_request(
    http: &JsonHttpClient,
    request: JsonRequest<'_>,
) -> SchedulerResult<UsageFetchResponse> {
    let cancel = request.cancel;
    let request = build_json_request(request)?;
    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(SchedulerError::Job("oauth usage poll cancelled".to_owned())),
        response = tokio::time::timeout(Duration::from_secs(30), http.request(request)) => response,
    }
    .map_err(|_| SchedulerError::Job("oauth usage request timed out".to_owned()))?
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let status = response.status();
    let body = read_bounded_response_body(
        response.into_body(),
        cancel,
        "oauth usage response body timed out",
    )
    .await?;
    Ok(UsageFetchResponse { status, body })
}

fn build_json_request(request: JsonRequest<'_>) -> SchedulerResult<Request<Full<Bytes>>> {
    let JsonRequest {
        url,
        method,
        body,
        content_type,
        access_token,
        account_id,
        codex,
        user_agent,
        anthropic,
        cancel: _,
    } = request;
    let mut builder = Request::builder()
        .method(method)
        .uri(url.as_str())
        .header("Authorization", format!("Bearer {access_token}"))
        .header("User-Agent", if codex { "codex-cli" } else { user_agent })
        .header("Accept", "application/json")
        .header(http::header::CONTENT_TYPE, content_type);
    if codex {
        // https://github.com/openai/codex/blob/main/codex-rs/login/src/auth/default_client.rs
        // https://github.com/openai/codex/blob/main/codex-rs/backend-client/src/client.rs
        // Use the backend client's User-Agent fallback and the default Codex originator.
        builder = builder.header("originator", "codex_cli_rs");
        if let Some(account_id) = account_id {
            builder = builder.header("ChatGPT-Account-Id", account_id);
        }
    }
    if anthropic {
        builder = builder.header("anthropic-beta", "oauth-2025-04-20");
    }
    builder
        .body(Full::new(body))
        .map_err(|error| SchedulerError::Job(error.to_string()))
}

fn codex_usage_url(base_url: &Url) -> Url {
    // https://github.com/openai/codex/blob/main/codex-rs/backend-client/src/client.rs
    // PathStyle selects ChatGptApi for /backend-api; rate_limit_resets.rs appends
    // /wham/usage. The configured proxy base ends in /codex, not the backend base.
    // https://github.com/openai/codex/blob/main/codex-rs/backend-client/src/client/rate_limit_resets.rs
    let mut url = base_url.clone();
    let base_path = base_url.path().trim_end_matches('/');
    let backend_path = base_path.strip_suffix("/codex").unwrap_or(base_path);
    let path = if backend_path.contains("/backend-api") {
        format!("{backend_path}/wham/usage")
    } else if matches!(url.host_str(), Some("chatgpt.com" | "chat.openai.com")) {
        format!("{backend_path}/backend-api/wham/usage")
    } else {
        format!("{base_path}/api/codex/usage")
    };
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    url
}

pub(super) fn gemini_summary_groups(body: &[u8]) -> Option<(bool, bool)> {
    let value: serde_json::Value = sonic_rs::from_slice(body).ok()?;
    let groups = value.get("groups")?.as_array()?;
    let mut gemini = false;
    let mut third_party = false;
    for group in groups {
        let display_name = group
            .get("displayName")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_ascii_lowercase();
        let Some(buckets) = group.get("buckets").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for bucket in buckets {
            let group_kind = classify_gemini_summary_bucket(&display_name, bucket);
            if !gemini_summary_bucket_usable(group_kind, bucket) {
                continue;
            }
            match group_kind {
                GeminiQuotaGroup::Gemini => gemini = true,
                GeminiQuotaGroup::ThirdParty => third_party = true,
                GeminiQuotaGroup::Unknown => {}
            }
        }
    }
    Some((gemini, third_party))
}

fn gemini_summary_url(base_url: &Url) -> SchedulerResult<Url> {
    let base_path = base_url.path().trim_end_matches('/');
    let path = if base_path.is_empty() {
        "/v1internal:retrieveUserQuotaSummary".to_owned()
    } else {
        format!("{base_path}:retrieveUserQuotaSummary")
    };
    let mut url = base_url.clone();
    url.set_path(&path);
    Ok(url)
}

fn gemini_quota_url(base_url: &Url) -> SchedulerResult<Url> {
    let base_path = base_url.path().trim_end_matches('/');
    let path = if base_path.is_empty() {
        "/v1internal:retrieveUserQuota".to_owned()
    } else {
        format!("{base_path}:retrieveUserQuota")
    };
    let mut url = base_url.clone();
    url.set_path(&path);
    Ok(url)
}

pub(super) fn decrypt_bundle(
    upstream: &UpstreamRecord,
    aead: &AeadService,
) -> SchedulerResult<OAuthTokenBundle> {
    upstream
        .oauth_credentials
        .as_ref()
        .ok_or_else(|| SchedulerError::Job("missing oauth credentials".to_owned()))?
        .decrypt(aead, upstream.id.as_bytes())
        .map_err(|_| SchedulerError::Job("oauth decrypt failed".to_owned()))
}

pub(super) fn upstream_base_url(upstream: &UpstreamRecord) -> SchedulerResult<Url> {
    if let Some(base_url) = upstream.base_url.clone() {
        return Ok(base_url);
    }
    let default = match upstream.kind {
        cc_lb_storage_api::upstream::UpstreamKind::AnthropicApiKey
        | cc_lb_storage_api::upstream::UpstreamKind::AnthropicOauth => DEFAULT_ANTHROPIC_BASE_URL,
        cc_lb_storage_api::upstream::UpstreamKind::OpenaiOauth => DEFAULT_OPENAI_CODEX_BASE_URL,
        cc_lb_storage_api::upstream::UpstreamKind::OpenaiApiKey => DEFAULT_OPENAI_BASE_URL,
        cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth => {
            DEFAULT_GEMINI_CODE_ASSIST_BASE_URL
        }
        cc_lb_storage_api::upstream::UpstreamKind::GeminiApiKey => DEFAULT_GEMINI_BASE_URL,
    };
    Url::parse(default).map_err(|error| SchedulerError::Job(error.to_string()))
}

pub(super) fn json_http_client() -> JsonHttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

#[cfg(test)]
mod tests {
    use super::{
        SCHEDULER_RESPONSE_BODY_CAP_BYTES, gemini_quota_url, gemini_summary_groups,
        gemini_summary_url,
    };
    use bytes::Bytes;
    use http_body_util::Full;
    use url::Url;
    #[test]
    fn codex_usage_request_matches_backend_path_and_account_headers()
    -> Result<(), Box<dyn std::error::Error>> {
        use super::{JsonRequest, build_json_request, codex_usage_url};
        use bytes::Bytes;
        use tokio_util::sync::CancellationToken;

        let cancel = CancellationToken::new();
        let base = Url::parse("https://chatgpt.com/backend-api/codex")?;
        let request = build_json_request(JsonRequest {
            url: codex_usage_url(&base),
            method: http::Method::GET,
            body: Bytes::new(),
            content_type: "application/json",
            access_token: "test-access",
            account_id: Some("team-account"),
            codex: true,
            user_agent: "claude-code/test",
            anthropic: false,
            cancel: &cancel,
        })?;
        assert_eq!(
            request.uri().to_string(),
            "https://chatgpt.com/backend-api/wham/usage"
        );
        assert_eq!(
            request
                .headers()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer test-access")
        );
        assert_eq!(
            request
                .headers()
                .get("chatgpt-account-id")
                .and_then(|value| value.to_str().ok()),
            Some("team-account")
        );
        assert_eq!(
            request
                .headers()
                .get("user-agent")
                .and_then(|value| value.to_str().ok()),
            Some("codex-cli")
        );
        assert_eq!(
            request
                .headers()
                .get("originator")
                .and_then(|value| value.to_str().ok()),
            Some("codex_cli_rs")
        );
        assert!(!request.headers().contains_key("anthropic-beta"));
        assert_eq!(
            codex_usage_url(&Url::parse("https://codex.example/custom")?).as_str(),
            "https://codex.example/custom/api/codex/usage"
        );
        Ok(())
    }

    #[test]
    fn codex_usage_url_normalizes_chatgpt_backend_variants()
    -> Result<(), Box<dyn std::error::Error>> {
        for base in [
            "https://chatgpt.com",
            "https://chatgpt.com/backend-api",
            "https://chatgpt.com/backend-api/",
            "https://chatgpt.com/backend-api/codex/",
        ] {
            assert_eq!(
                super::codex_usage_url(&Url::parse(base)?).as_str(),
                "https://chatgpt.com/backend-api/wham/usage"
            );
        }
        Ok(())
    }

    #[test]
    fn anthropic_usage_request_keeps_provider_headers() -> Result<(), Box<dyn std::error::Error>> {
        let cancel = tokio_util::sync::CancellationToken::new();
        let request = super::build_json_request(super::JsonRequest {
            url: Url::parse("https://api.anthropic.com/api/oauth/usage?cedar_ember=1")?,
            method: http::Method::GET,
            body: bytes::Bytes::new(),
            content_type: "application/json",
            access_token: "test-access",
            account_id: None,
            codex: false,
            user_agent: "claude-code/test",
            anthropic: true,
            cancel: &cancel,
        })?;
        assert_eq!(
            request
                .headers()
                .get("anthropic-beta")
                .and_then(|value| value.to_str().ok()),
            Some("oauth-2025-04-20")
        );
        assert_eq!(
            request
                .headers()
                .get("user-agent")
                .and_then(|value| value.to_str().ok()),
            Some("claude-code/test")
        );
        assert!(!request.headers().contains_key("originator"));
        assert!(!request.headers().contains_key("chatgpt-account-id"));
        Ok(())
    }

    #[test]
    fn gemini_quota_url_handles_root_custom_base() {
        let base = Url::parse("http://127.0.0.1:18181").unwrap();
        assert_eq!(
            gemini_quota_url(&base).unwrap().as_str(),
            "http://127.0.0.1:18181/v1internal:retrieveUserQuota"
        );
    }

    #[test]
    fn gemini_quota_url_preserves_cloud_code_path() {
        let base = Url::parse("https://cloudcode-pa.googleapis.com/v1internal").unwrap();
        assert_eq!(
            gemini_quota_url(&base).unwrap().as_str(),
            "https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuota"
        );
    }

    #[test]
    fn gemini_summary_url_preserves_cloud_code_path() {
        let base = Url::parse("https://cloudcode-pa.googleapis.com/v1internal").unwrap();
        assert_eq!(
            gemini_summary_url(&base).unwrap().as_str(),
            "https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary"
        );
    }

    #[test]
    fn antigravity_usage_request_uses_daily_quota_summary() -> Result<(), Box<dyn std::error::Error>>
    {
        let cancel = tokio_util::sync::CancellationToken::new();
        let base = Url::parse("https://daily-cloudcode-pa.googleapis.com")?;
        let request = super::build_json_request(super::usage_request(
            &base,
            cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth,
            super::GeminiOAuthProfile::Antigravity,
            "test-access",
            None,
            Some("test-project"),
            "gemini-cli/test",
            &cancel,
        )?)?;
        assert_eq!(
            request.uri().to_string(),
            "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary"
        );
        assert_eq!(request.method(), http::Method::POST);
        assert_eq!(
            request
                .headers()
                .get("user-agent")
                .and_then(|value| value.to_str().ok()),
            Some(super::ANTIGRAVITY_OAUTH_USER_AGENT)
        );
        Ok(())
    }

    #[test]
    fn gemini_summary_groups_detects_only_usable_provider_groups() {
        let body = br#"{"groups":[{"displayName":"Gemini models","buckets":[{"bucketId":"gemini-5h","remainingFraction":0.7,"resetTime":"2026-11-01T13:00:00Z"}]},{"displayName":"Claude and GPT-OSS models","buckets":[{"bucketId":"3p-weekly","remainingFraction":0.4,"resetTime":"2026-11-07T15:00:00Z"}]}]}"#;
        assert_eq!(gemini_summary_groups(body), Some((true, true)));
    }

    #[test]
    fn gemini_summary_groups_does_not_relabel_third_party_bucket_by_group_name() {
        let body = br#"{"groups":[{"displayName":"Gemini models","buckets":[{"bucketId":"3p-5h","remainingFraction":0.7,"resetTime":"2026-11-01T13:00:00Z"}]}]}"#;
        assert_eq!(gemini_summary_groups(body), Some((false, true)));
    }

    #[tokio::test]
    async fn scheduler_body_collector_rejects_oversized_responses() {
        let cancel = tokio_util::sync::CancellationToken::new();
        let body = Full::new(Bytes::from(vec![
            b'x';
            SCHEDULER_RESPONSE_BODY_CAP_BYTES + 1
        ]));
        let error = super::read_bounded_response_body(body, &cancel, "scheduler body timed out")
            .await
            .expect_err("oversized scheduler body must be rejected");
        assert!(
            error
                .to_string()
                .contains("scheduler response body exceeded")
        );
    }
}
