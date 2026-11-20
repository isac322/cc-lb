use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_vertex_auth::{
    MAX_RESPONSE_BYTES, TokenEndpoint, TokenRequest, TokenResponse, TokenTransport, TransportError,
};
use http::{HeaderName, HeaderValue, Request};
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::{Client, connect::HttpConnector};
use hyper_util::rt::TokioExecutor;
use url::Url;
const OAUTH2_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const IAM_CREDENTIALS_HOST: &str =
    "https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

type HttpClient = Client<hyper_rustls::HttpsConnector<HttpConnector>, Full<Bytes>>;

/// Production HTTPS transport for the fixed Google token endpoints.
#[derive(Clone)]
pub(crate) struct HyperTokenTransport {
    client: HttpClient,
}

impl HyperTokenTransport {
    pub(crate) fn new() -> Self {
        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_only()
            .enable_http1()
            .enable_http2()
            .build();
        Self {
            client: Client::builder(TokioExecutor::new()).build(connector),
        }
    }

    fn endpoint_url(endpoint: TokenEndpoint) -> Result<Url, TransportError> {
        match endpoint {
            TokenEndpoint::OAuth2Token => Url::parse(OAUTH2_TOKEN_URL),
            TokenEndpoint::IamGenerateAccessToken { email } => {
                let mut url = Url::parse(IAM_CREDENTIALS_HOST)
                    .map_err(|_| TransportError::InvalidEndpoint)?;
                url.set_path(&format!(
                    "/v1/projects/-/serviceAccounts/{email}:generateAccessToken"
                ));
                Ok(url)
            }
        }
        .map_err(|_| TransportError::InvalidEndpoint)
    }
}

#[async_trait::async_trait]
impl TokenTransport for HyperTokenTransport {
    async fn post(
        &self,
        endpoint: TokenEndpoint,
        request: TokenRequest,
    ) -> Result<TokenResponse, TransportError> {
        let url = Self::endpoint_url(endpoint)?;
        let mut builder = Request::post(url.as_str())
            .header(http::header::CONTENT_TYPE, request.content_type)
            .header(http::header::CONTENT_LENGTH, request.body.len());
        for (name, value) in request.headers {
            let name = HeaderName::try_from(name).map_err(|_| TransportError::InvalidEndpoint)?;
            let value =
                HeaderValue::try_from(value).map_err(|_| TransportError::InvalidEndpoint)?;
            builder = builder.header(name, value);
        }
        let request = builder
            .body(Full::new(Bytes::from(request.body)))
            .map_err(|_| TransportError::InvalidEndpoint)?;
        let exchange = async {
            let response = self
                .client
                .request(request)
                .await
                .map_err(|_| TransportError::Network)?;
            if response.status().is_redirection() {
                return Err(TransportError::Redirect);
            }
            let status = response.status().as_u16();
            let headers = response
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    Some((name.as_str().to_owned(), value.to_str().ok()?.to_owned()))
                })
                .collect();
            let body =
                match Limited::new(response.into_body(), MAX_RESPONSE_BYTES.saturating_add(1))
                    .collect()
                    .await
                {
                    Ok(collected) => collected.to_bytes().to_vec(),
                    Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => {
                        vec![0; MAX_RESPONSE_BYTES.saturating_add(1)]
                    }
                    Err(_) => return Err(TransportError::Network),
                };
            Ok(TokenResponse {
                status,
                headers,
                body,
            })
        };
        tokio::time::timeout(REQUEST_TIMEOUT, exchange)
            .await
            .map_err(|_| TransportError::Timeout)?
    }
}

pub(crate) fn production_transport() -> Arc<dyn TokenTransport> {
    Arc::new(HyperTokenTransport::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qa_mnt_3_iam_endpoint_uses_single_service_account_segment() {
        let url = HyperTokenTransport::endpoint_url(TokenEndpoint::IamGenerateAccessToken {
            email: "target@project.iam.gserviceaccount.com".to_owned(),
        })
        .expect("IAM endpoint");
        assert_eq!(
            url.path(),
            "/v1/projects/-/serviceAccounts/target@project.iam.gserviceaccount.com:generateAccessToken"
        );
        assert!(
            !url.path()
                .contains("/target@project.iam.gserviceaccount.com/:")
        );
    }
}
