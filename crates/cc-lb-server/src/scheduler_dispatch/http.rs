use bytes::Bytes;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_oauth_protocol::{
    TokenEndpointResponse, parse_token_endpoint_response, refresh_token_form_body,
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
/// One poll request returns the quota windows and the `cedar_ember`
/// limit-reset block together. `skip_spend` is deliberately absent so the
/// provider keeps reporting `extra_usage`.
const USAGE_PATH: &str = "api/oauth/usage?cedar_ember=1";

pub(super) type JsonHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

pub(super) struct UsageFetchResponse {
    pub status: StatusCode,
    pub body: Bytes,
}

pub(super) async fn request_refresh(
    http: &JsonHttpClient,
    oauth_cfg: &AnthropicOAuthConfig,
    cancel: &CancellationToken,
    refresh_token: &str,
) -> SchedulerResult<TokenEndpointResponse> {
    let body = refresh_token_form_body(oauth_cfg.client_id.as_str(), refresh_token);
    let request = Request::post(oauth_cfg.token_url.as_str())
        .header(
            http::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .header(http::header::CONTENT_LENGTH, body.len().to_string())
        .body(Full::new(Bytes::from(body)))
        .map_err(|error| SchedulerError::Job(error.to_string()))?;
    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(SchedulerError::Job("oauth refresh cancelled".to_owned())),
        response = tokio::time::timeout(Duration::from_secs(30), http.request(request)) => response,
    }
    .map_err(|_| SchedulerError::Job("oauth refresh request timed out".to_owned()))?
    .map_err(|error| SchedulerError::Job(error.to_string()))?;
    if !response.status().is_success() {
        return Err(SchedulerError::Job(format!(
            "oauth token endpoint returned {}",
            response.status()
        )));
    }
    let bytes = response
        .into_body()
        .collect()
        .await
        .map_err(|error| SchedulerError::Job(error.to_string()))?
        .to_bytes();
    parse_token_endpoint_response(&bytes).map_err(|error| SchedulerError::Job(error.to_string()))
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
    match upstream.base_url.clone() {
        Some(base_url) => Ok(base_url),
        None => Url::parse(DEFAULT_ANTHROPIC_BASE_URL)
            .map_err(|error| SchedulerError::Job(error.to_string())),
    }
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
