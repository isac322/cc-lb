use bytes::Bytes;
use cc_lb_aead::{AeadService, OAuthTokenBundle};
use cc_lb_config::AnthropicOAuthConfig;
use cc_lb_oauth_protocol::{
    TokenEndpointResponse, parse_token_endpoint_response, refresh_token_form_body,
    terminal_token_endpoint_error_code,
};
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_storage_api::UpstreamRecord;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

const DEFAULT_ANTHROPIC_BASE_URL: &str = "https://api.anthropic.com/";
const USAGE_URL: &str = "https://api.anthropic.com/api/oauth/usage";
const MAX_TOKEN_ENDPOINT_BODY_BYTES: usize = 64 * 1024;

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
    let status = response.status();
    let bytes = tokio::select! {
        _ = cancel.cancelled() => {
            return Err(SchedulerError::Job("oauth refresh cancelled".to_owned()));
        }
        body = tokio::time::timeout(
            Duration::from_secs(30),
            Limited::new(response.into_body(), MAX_TOKEN_ENDPOINT_BODY_BYTES).collect(),
        ) => {
            body
                .map_err(|_| SchedulerError::Job("oauth refresh response body timed out".to_owned()))?
                .map_err(|error| SchedulerError::Job(error.to_string()))?
                .to_bytes()
        }
    };
    if !status.is_success() {
        return Err(token_endpoint_status_error(status, &bytes));
    }
    parse_token_endpoint_response(&bytes).map_err(|error| SchedulerError::Job(error.to_string()))
}

fn token_endpoint_status_error(status: StatusCode, body: &[u8]) -> SchedulerError {
    let message = format!("oauth token endpoint returned {status}");
    let terminal_code = terminal_token_endpoint_error_code(status.as_u16(), body);
    match terminal_code {
        Some(code) => SchedulerError::TerminalJob(code.to_owned()),
        None => SchedulerError::Job(message),
    }
}

pub(super) async fn fetch_usage(
    http: &JsonHttpClient,
    access_token: &str,
    user_agent: &str,
    cancel: &CancellationToken,
) -> SchedulerResult<UsageFetchResponse> {
    let request = Request::get(USAGE_URL)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_endpoint_terminal_codes_are_dead_letter_errors() {
        for code in ["invalid_grant", "invalid_client", "unauthorized_client"] {
            let body = format!(r#"{{"error":"{code}"}}"#);
            assert!(matches!(
                token_endpoint_status_error(StatusCode::BAD_REQUEST, body.as_bytes()),
                SchedulerError::TerminalJob(message) if message.contains(code)
            ));
        }
    }

    #[test]
    fn token_endpoint_unknown_or_absent_codes_are_retryable_errors() {
        for (status, body) in [
            (
                StatusCode::BAD_REQUEST,
                br#"{"error":"unknown_error"}"#.as_slice(),
            ),
            (StatusCode::REQUEST_TIMEOUT, br#"{}"#.as_slice()),
            (StatusCode::TOO_MANY_REQUESTS, br#"not json"#.as_slice()),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"error":"temporarily_unavailable"}"#.as_slice(),
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                br#"{"error":"invalid_grant"}"#.as_slice(),
            ),
            (
                StatusCode::TOO_MANY_REQUESTS,
                br#"{"error":"invalid_grant"}"#.as_slice(),
            ),
        ] {
            assert!(matches!(
                token_endpoint_status_error(status, body),
                SchedulerError::Job(_)
            ));
        }
    }
}
