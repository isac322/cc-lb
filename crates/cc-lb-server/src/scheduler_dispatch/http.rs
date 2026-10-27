use bytes::Bytes;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_oauth_protocol::{
    TokenEndpointResponse, parse_token_endpoint_response, refresh_token_form_body,
    refresh_token_json_body,
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

pub(super) async fn request_refresh(
    http: &JsonHttpClient,
    oauth_cfg: &AnthropicOAuthConfig,
    oauth_kind: cc_lb_storage_api::upstream::UpstreamKind,
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
        cc_lb_storage_api::upstream::UpstreamKind::GeminiOauth => (
            GEMINI_OAUTH_TOKEN_URL,
            "application/x-www-form-urlencoded",
            gemini_refresh_token_form_body(refresh_token),
        ),
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
            tokio::select! {
                _ = cancel.cancelled() => return Err(SchedulerError::Job("oauth refresh cancelled".to_owned()).into()),
                body = tokio::time::timeout(Duration::from_secs(30), response.into_body().collect()) => body,
            }
            .map_err(|_| SchedulerError::Job("oauth refresh response body timed out".to_owned()))?
            .map_err(|error| SchedulerError::Job(error.to_string()))?
            .to_bytes()
        } else {
            Bytes::new()
        };
        return Err(RefreshRequestError::Endpoint { status, body });
    }
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| SchedulerError::Job(error.to_string()))?
        .to_bytes();
    parse_token_endpoint_response(&body)
        .map_err(|error| SchedulerError::Job(error.to_string()).into())
}
fn gemini_refresh_token_form_body(refresh_token: &str) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    serializer.append_pair("grant_type", "refresh_token");
    serializer.append_pair("client_id", GEMINI_OAUTH_CLIENT_ID);
    serializer.append_pair("client_secret", GEMINI_OAUTH_CLIENT_SECRET);
    serializer.append_pair("refresh_token", refresh_token);
    serializer.finish()
}

pub(super) async fn fetch_usage(
    http: &JsonHttpClient,
    base_url: &Url,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> SchedulerResult<UsageFetchResponse> {
    let url = base_url
        .join(USAGE_PATH)
        .map_err(|error| SchedulerError::Job(format!("invalid usage base URL: {error}")))?;
    let request = Request::get(url.as_str())
        .header("Authorization", format!("Bearer {access_token}"))
        .header("User-Agent", user_agent)
        .header("anthropic-beta", "oauth-2025-04-20")
        .header("Accept", "application/json")
        .body(Full::new(Bytes::new()))
        .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(SchedulerError::Job("oauth usage poll cancelled".to_owned())),
        response = tokio::time::timeout(Duration::from_secs(30), http.request(request)) => response,
    }
    .map_err(|_| SchedulerError::Job("oauth usage request timed out".to_owned()))?
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let status = response.status();
    let body = response
        .into_body()
        .collect()
        .await
        .map_err(|error| SchedulerError::Job(error.to_string()))?
        .to_bytes();
    Ok(UsageFetchResponse { status, body })
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
