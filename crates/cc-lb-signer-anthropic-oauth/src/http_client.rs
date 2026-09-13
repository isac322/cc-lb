use async_trait::async_trait;
use bytes::Bytes;
use http::header::{CONTENT_LENGTH, CONTENT_TYPE};
use http::{HeaderValue, Request, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;
use url::Url;

pub const APPLICATION_JSON: &str = "application/json";

#[derive(Clone)]
pub struct OAuthTokenRequest {
    pub endpoint: Url,
    pub form_body: SecretString,
    pub content_type: &'static str,
}

impl std::fmt::Debug for OAuthTokenRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OAuthTokenRequest")
            .field("endpoint", &self.endpoint)
            .field("form_body", &"[REDACTED]")
            .field("content_type", &self.content_type)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct OAuthTokenResponse {
    pub status: StatusCode,
    pub body: Bytes,
}

#[derive(Debug, Error)]
pub enum OAuthHttpError {
    #[error("oauth token request build failed: {reason}")]
    RequestBuild { reason: String },
    #[error("oauth token request failed: {reason}")]
    Request { reason: String },
    #[error("oauth token response body failed: {reason}")]
    ResponseBody { reason: String },
}

#[async_trait]
pub trait OAuthHttpClient: Send + Sync + std::fmt::Debug {
    async fn post_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthHttpError>;
}

#[derive(Clone)]
pub struct HyperOAuthHttpClient {
    client: Client<
        hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
        Full<Bytes>,
    >,
}

impl HyperOAuthHttpClient {
    pub fn new() -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();
        let client = Client::builder(TokioExecutor::new()).build(connector);
        Self { client }
    }
}

impl Default for HyperOAuthHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for HyperOAuthHttpClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("HyperOAuthHttpClient").finish()
    }
}

#[async_trait]
impl OAuthHttpClient for HyperOAuthHttpClient {
    async fn post_token(
        &self,
        request: OAuthTokenRequest,
    ) -> Result<OAuthTokenResponse, OAuthHttpError> {
        let body = request.form_body.expose_secret().to_owned();
        let content_length = HeaderValue::from_str(&body.len().to_string()).map_err(|source| {
            OAuthHttpError::RequestBuild {
                reason: source.to_string(),
            }
        })?;
        let http_request = Request::post(request.endpoint.as_str())
            .header(CONTENT_TYPE, request.content_type)
            .header(CONTENT_LENGTH, content_length)
            .body(Full::new(Bytes::from(body)))
            .map_err(|source| OAuthHttpError::RequestBuild {
                reason: source.to_string(),
            })?;

        let response =
            self.client
                .request(http_request)
                .await
                .map_err(|source| OAuthHttpError::Request {
                    reason: source.to_string(),
                })?;
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .map_err(|source| OAuthHttpError::ResponseBody {
                reason: source.to_string(),
            })?
            .to_bytes();

        Ok(OAuthTokenResponse { status, body })
    }
}
