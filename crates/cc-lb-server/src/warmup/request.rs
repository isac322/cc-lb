use bytes::Bytes;
use cc_lb_quota::{UnifiedQuotaObservation, parse_anthropic_unified_headers};
use http::{Method, Request, StatusCode};
use http_body_util::Full;
use hyper_util::client::legacy::Client;
use serde_json::{Value, json};
use url::Url;

pub(crate) const WARMUP_MODEL: &str = "claude-haiku-4-5-20251001";
pub(crate) const WARMUP_MAX_TOKENS: u32 = 1;
const WARMUP_ANTHROPIC_BETA: &str = "oauth-2025-04-20";

pub(crate) type WarmupHttpClient = Client<
    hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>,
    Full<Bytes>,
>;

#[derive(Debug)]
pub enum WarmupRequestAttempt {
    Response {
        status: StatusCode,
        observations: Vec<UnifiedQuotaObservation>,
    },
    RequestBuildFailed {
        error: String,
    },
    NetworkError {
        error: String,
    },
}

/// Builds a minimal warmup request for POST /v1/messages.
pub fn build_warmup_request(
    access_token: &str,
    base_url: &Url,
) -> Result<Request<Full<Bytes>>, String> {
    let url = base_url
        .join("v1/messages")
        .map_err(|e| format!("Failed to build URL: {}", e))?;

    let body_bytes = warmup_body_bytes()?;

    let request = Request::builder()
        .method(Method::POST)
        .uri(url.as_str())
        .header("authorization", format!("Bearer {}", access_token))
        .header("content-type", "application/json")
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", WARMUP_ANTHROPIC_BETA)
        .body(Full::new(Bytes::from(body_bytes)))
        .map_err(|e| format!("Failed to build request: {}", e))?;

    Ok(request)
}

fn warmup_body_bytes() -> Result<Vec<u8>, String> {
    serde_json::to_string(&warmup_body())
        .map(String::into_bytes)
        .map_err(|e| format!("Failed to serialize body: {}", e))
}

fn warmup_body() -> Value {
    json!({
        "model": WARMUP_MODEL,
        "max_tokens": WARMUP_MAX_TOKENS,
        "messages": [{"role": "user", "content": "."}]
    })
}

pub async fn dispatch_warmup_attempt(
    client: &WarmupHttpClient,
    access_token: &str,
    base_url: &Url,
) -> WarmupRequestAttempt {
    let request = match build_warmup_request(access_token, base_url) {
        Ok(req) => req,
        Err(error) => return WarmupRequestAttempt::RequestBuildFailed { error },
    };

    match client.request(request).await {
        Ok(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            let observations = parse_anthropic_unified_headers(&headers);
            WarmupRequestAttempt::Response {
                status,
                observations,
            }
        }
        Err(error) => WarmupRequestAttempt::NetworkError {
            error: error.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cc_lb_domain::{Principal, PrincipalKind, Upstream};
    use cc_lb_upstream::{
        DialectError, DialectShapeContext, ShapedRequest, ShapedRequestBuilder, UpstreamDialect,
        shape_request,
    };
    use http_body_util::BodyExt;

    fn warmup_request() -> Request<Full<Bytes>> {
        let base_url = Url::parse("https://api.anthropic.com/").expect("valid URL");

        build_warmup_request("test-token", &base_url).expect("request builds successfully")
    }

    async fn warmup_body_bytes_from_request() -> Bytes {
        warmup_request()
            .into_body()
            .collect()
            .await
            .expect("body collects")
            .to_bytes()
    }

    async fn warmup_body_from_request() -> serde_json::Value {
        serde_json::from_slice(&warmup_body_bytes_from_request().await).expect("valid json body")
    }

    struct WarmupRequestDialect {
        url: Url,
        method: Method,
        headers: http::HeaderMap,
        body: Bytes,
    }

    impl UpstreamDialect for WarmupRequestDialect {
        fn shape(
            &self,
            _ctx: &DialectShapeContext,
            _upstream: &Upstream,
            _principal: &Principal,
            builder: &mut ShapedRequestBuilder,
        ) -> Result<ShapedRequest, DialectError> {
            Ok(builder.shaped_request(
                self.url.clone(),
                self.method.clone(),
                self.headers.clone(),
                self.body.clone(),
            ))
        }
    }

    #[test]
    fn request_shape_matches_spec() {
        let request = warmup_request();

        assert_eq!(request.uri().path(), "/v1/messages");
        assert_eq!(request.method(), http::Method::POST);
        assert!(request.headers().contains_key("authorization"));
        assert!(request.headers().contains_key("content-type"));
        assert!(request.headers().contains_key("anthropic-version"));
        assert!(request.headers().contains_key("anthropic-beta"));
    }

    #[test]
    fn request_shape_includes_anthropic_version_header() {
        let request = warmup_request();

        assert_eq!(
            request.headers().get("anthropic-version"),
            Some(&http::HeaderValue::from_static("2023-06-01"))
        );
        assert_eq!(
            request.headers().get("anthropic-beta"),
            Some(&http::HeaderValue::from_static(WARMUP_ANTHROPIC_BETA))
        );
    }

    #[tokio::test]
    async fn warmup_intent_contract_dispatch_warmup_attempt_shaped_input() {
        // Given: the exact request constructed before dispatch_warmup_attempt sends it.
        let base_url = Url::parse("https://api.anthropic.com/").expect("valid URL");
        let request =
            build_warmup_request("test-token", &base_url).expect("request builds successfully");
        let (parts, body) = request.into_parts();
        let body = body.collect().await.expect("body collects").to_bytes();
        let dialect = WarmupRequestDialect {
            url: Url::parse(&parts.uri.to_string()).expect("request URI is a valid URL"),
            method: parts.method,
            headers: parts.headers,
            body: body.clone(),
        };
        let request_context = DialectShapeContext {
            request_id: "warmup-intent-contract".to_owned(),
            downstream_headers: http::HeaderMap::new(),
            method: Method::POST,
            path: "/v1/messages".to_owned(),
            query: None,
            body_bytes: Bytes::new(),
        };
        let principal = Principal {
            id: "warmup".to_owned(),
            kind: PrincipalKind::OAuthSubject,
        };

        // When: a sealed ShapedRequest is built from the request's real parts.
        let shaped_request = shape_request(
            &dialect,
            &request_context,
            &Upstream::AnthropicDirect { base_url: None },
            &principal,
        )
        .expect("warmup request shapes successfully");

        // Then: dispatch_warmup_attempt receives the fixed request as rail-shaped input.
        assert_eq!(
            shaped_request.url().as_str(),
            "https://api.anthropic.com/v1/messages"
        );
        assert_eq!(shaped_request.method(), &Method::POST);
        assert_eq!(
            shaped_request.headers().get("authorization"),
            Some(&http::HeaderValue::from_static("Bearer test-token"))
        );
        assert_eq!(
            shaped_request.headers().get("content-type"),
            Some(&http::HeaderValue::from_static("application/json"))
        );
        assert_eq!(
            shaped_request.headers().get("anthropic-version"),
            Some(&http::HeaderValue::from_static("2023-06-01"))
        );
        assert_eq!(shaped_request.body(), &body);

        let shaped_body: Value =
            serde_json::from_slice(shaped_request.body()).expect("shaped body remains valid JSON");
        assert_eq!(shaped_body.get("model"), Some(&json!(WARMUP_MODEL)));
        assert_eq!(
            shaped_body.get("max_tokens"),
            Some(&json!(WARMUP_MAX_TOKENS))
        );
    }

    #[tokio::test]
    async fn request_shape_max_tokens_is_1() {
        let body = warmup_body_from_request().await;

        assert_eq!(body.get("max_tokens"), Some(&json!(1)));
    }

    #[tokio::test]
    async fn request_shape_has_no_system_or_tools_or_stream() {
        let body = warmup_body_from_request().await;

        assert!(body.get("system").is_none());
        assert!(body.get("tools").is_none());
        assert!(body.get("stream").is_none());
    }

    #[tokio::test]
    async fn request_shape_has_no_metadata() {
        let body = warmup_body_from_request().await;

        assert!(body.get("metadata").is_none());
    }

    #[test]
    fn request_body_matches_locked_json_byte_for_byte() {
        let expected = br#"{"max_tokens":1,"messages":[{"content":".","role":"user"}],"model":"claude-haiku-4-5-20251001"}"#;

        assert_eq!(
            warmup_body_bytes().expect("body serializes"),
            expected.to_vec()
        );
    }
}
