use std::time::Duration;

use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Empty};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use serde::Deserialize;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::keys::CompatFetcher;

const CLAUDE_CODE_STABLE_URL: &str = "https://downloads.claude.ai/claude-code-releases/stable";
const CLAUDE_CODE_NPM_STABLE_URL: &str =
    "https://registry.npmjs.org/@anthropic-ai/claude-code/stable";
const CLAUDE_CODE_LATEST_URL: &str = "https://downloads.claude.ai/claude-code-releases/latest";
const CLAUDE_CODE_NPM_LATEST_URL: &str =
    "https://registry.npmjs.org/@anthropic-ai/claude-code/latest";
const COMPAT_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

type CompatHttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Empty<Bytes>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompatFetchOutcome {
    pub value: String,
    pub source_url: String,
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CompatFetchError {
    #[error("compatibility fetch HTTP error: {0}")]
    Http(String),
    #[error("compatibility fetch parse error: {0}")]
    Parse(String),
    #[error("compatibility fetch timed out")]
    Timeout,
    #[error("compatibility fetch cancelled")]
    Cancelled,
}

pub async fn run_compat_fetcher(
    fetcher: CompatFetcher,
    cancel: &CancellationToken,
) -> Result<CompatFetchOutcome, CompatFetchError> {
    match fetcher {
        CompatFetcher::ClaudeCodeStableVersion => {
            fetch_claude_code_version(
                "stable",
                CLAUDE_CODE_STABLE_URL,
                CLAUDE_CODE_NPM_STABLE_URL,
                cancel,
            )
            .await
        }
        CompatFetcher::ClaudeCodeLatestVersion => {
            fetch_claude_code_version(
                "latest",
                CLAUDE_CODE_LATEST_URL,
                CLAUDE_CODE_NPM_LATEST_URL,
                cancel,
            )
            .await
        }
    }
}

/// Fetches a Claude Code version channel from the public release endpoint,
/// falling back to the npm registry tag when the primary endpoint errors.
async fn fetch_claude_code_version(
    channel: &str,
    primary_url: &'static str,
    npm_url: &'static str,
    cancel: &CancellationToken,
) -> Result<CompatFetchOutcome, CompatFetchError> {
    let client = compat_http_client();
    match fetch_text(&client, primary_url, cancel).await {
        Ok(body) => {
            let version = body.trim();
            if version.is_empty() {
                Err(CompatFetchError::Parse(format!(
                    "{channel} version response is empty"
                )))
            } else {
                Ok(CompatFetchOutcome {
                    value: version.to_owned(),
                    source_url: primary_url.to_owned(),
                })
            }
        }
        Err(CompatFetchError::Cancelled) => Err(CompatFetchError::Cancelled),
        Err(_primary_error) => {
            fetch_claude_code_version_from_npm(channel, &client, npm_url, cancel).await
        }
    }
}

async fn fetch_claude_code_version_from_npm(
    channel: &str,
    client: &CompatHttpClient,
    npm_url: &'static str,
    cancel: &CancellationToken,
) -> Result<CompatFetchOutcome, CompatFetchError> {
    let body = fetch_text(client, npm_url, cancel).await?;
    let response: NpmVersionResponse =
        serde_json::from_str(&body).map_err(|error| CompatFetchError::Parse(error.to_string()))?;
    let version = response.version.trim();
    if version.is_empty() {
        return Err(CompatFetchError::Parse(format!(
            "npm {channel} version is empty"
        )));
    }
    Ok(CompatFetchOutcome {
        value: version.to_owned(),
        source_url: npm_url.to_owned(),
    })
}

fn compat_http_client() -> CompatHttpClient {
    let connector = HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .enable_http2()
        .build();
    Client::builder(TokioExecutor::new()).build(connector)
}

async fn fetch_text(
    client: &CompatHttpClient,
    url: &str,
    cancel: &CancellationToken,
) -> Result<String, CompatFetchError> {
    let request = Request::get(url)
        .body(Empty::<Bytes>::new())
        .map_err(|error| CompatFetchError::Http(error.to_string()))?;
    let response = tokio::select! {
        _ = cancel.cancelled() => return Err(CompatFetchError::Cancelled),
        response = tokio::time::timeout(COMPAT_FETCH_TIMEOUT, client.request(request)) => {
            response
                .map_err(|_| CompatFetchError::Timeout)?
                .map_err(|error| CompatFetchError::Http(error.to_string()))?
        }
    };
    let status = response.status();
    if !status.is_success() {
        return Err(CompatFetchError::Http(format!("{url} returned {status}")));
    }
    let body = tokio::select! {
        _ = cancel.cancelled() => return Err(CompatFetchError::Cancelled),
        body = tokio::time::timeout(COMPAT_FETCH_TIMEOUT, response.into_body().collect()) => {
            body
                .map_err(|_| CompatFetchError::Timeout)?
                .map_err(|error| CompatFetchError::Http(error.to_string()))?
                .to_bytes()
        }
    };
    String::from_utf8(body.to_vec()).map_err(|error| CompatFetchError::Parse(error.to_string()))
}

#[derive(Deserialize)]
struct NpmVersionResponse {
    version: String,
}
