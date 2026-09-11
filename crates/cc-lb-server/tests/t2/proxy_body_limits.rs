use std::convert::Infallible;
use std::error::Error;
use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Method, Request, Response, StatusCode, header},
};
use bytes::Bytes;
use cc_lb_config::Config;
use cc_lb_engine::{Body as UpstreamBody, DispatchError, UpstreamDispatch};
use cc_lb_server::app::build_app_for_testing_with_dispatch;
use cc_lb_upstream::SignedRequest;
use futures_util::stream;
use http_body_util::{BodyExt, Full};
use tower::ServiceExt;

const MESSAGES_CAP_BYTES: usize = 256;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn t2__oversized_content_length_returns_413_without_polling_the_body() -> TestResult<()> {
    let app = test_app().await?;
    let body = Body::from_stream(stream::once(async {
        panic!("body must not be polled when content-length already exceeds the route cap");
        #[allow(unreachable_code)]
        Ok::<Bytes, Infallible>(Bytes::new())
    }));
    let request = Request::builder()
        .method(Method::POST)
        .uri("/v1/messages")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-api-key", "sk-ant-test")
        .header("anthropic-version", "2023-06-01")
        .header(header::CONTENT_LENGTH, (MESSAGES_CAP_BYTES + 1).to_string())
        .body(body)
        .expect("proxy request");

    let response = app.router.oneshot(request).await?;

    assert_payload_too_large(response).await;
    Ok(())
}

#[tokio::test]
async fn t2__streaming_body_over_cap_returns_413() -> TestResult<()> {
    let app = test_app().await?;
    let body = Body::from_stream(stream::iter([
        Ok::<_, Infallible>(Bytes::from(vec![b'x'; 128])),
        Ok::<_, Infallible>(Bytes::from(vec![b'x'; 129])),
    ]));
    let request = proxy_request(Method::POST, "/v1/messages", body);

    let response = app.router.oneshot(request).await?;

    assert_payload_too_large(response).await;
    Ok(())
}

#[tokio::test]
async fn t2__body_exactly_at_messages_cap_reaches_upstream_dispatch() -> TestResult<()> {
    let app = test_app().await?;
    let body = messages_body_with_len(MESSAGES_CAP_BYTES);

    let response = app
        .router
        .oneshot(proxy_request(
            Method::POST,
            "/v1/messages",
            Body::from(body),
        ))
        .await?;
    let status = response.status();
    let body = body_text(response).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains("msg_fake_"));
    Ok(())
}

#[tokio::test]
async fn t2__messages_and_files_paths_use_their_distinct_configured_caps() -> TestResult<()> {
    let app = test_app().await?;
    let body = messages_body_with_len(MESSAGES_CAP_BYTES + 1);

    let messages = app
        .router
        .clone()
        .oneshot(proxy_request(
            Method::POST,
            "/v1/messages",
            Body::from(body.clone()),
        ))
        .await?;
    let messages_status = messages.status();
    let messages_body = body_text(messages).await;
    let files = app
        .router
        .oneshot(proxy_request(Method::POST, "/v1/files", Body::from(body)))
        .await?;
    let files_status = files.status();
    let files_body = body_text(files).await;

    assert_eq!(
        messages_status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{messages_body}"
    );
    assert_eq!(files_status, StatusCode::OK, "{files_body}");
    assert!(files_body.contains("file_abc123"));
    Ok(())
}

async fn test_app() -> Result<cc_lb_server::app::App, cc_lb_server::app::BuildError> {
    let mut config = Config::default();
    config.body.messages_cap_bytes = MESSAGES_CAP_BYTES as u64;
    config.body.files_cap_bytes = 1_048_576;
    let dispatcher: Arc<dyn UpstreamDispatch> = Arc::new(BodyLimitDispatch);
    build_app_for_testing_with_dispatch(
        config,
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatcher),
    )
    .await
}

fn proxy_request(method: Method, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-api-key", "sk-ant-test")
        .header("anthropic-version", "2023-06-01")
        .body(body)
        .expect("proxy request")
}

async fn assert_payload_too_large(response: Response<Body>) {
    let status = response.status();
    let body = body_text(response).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert!(
        body.contains("body_too_large"),
        "expected standard body-too-large payload, got {body:?}"
    );
}

async fn body_text(response: Response<Body>) -> String {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf-8 response")
}

fn messages_body_with_len(len: usize) -> String {
    const PREFIX: &str =
        r#"{"model":"claude-sonnet-4-5-20250929","messages":[{"role":"user","content":""#;
    const SUFFIX: &str = r#""}],"max_tokens":16}"#;

    let padding_len = len
        .checked_sub(PREFIX.len() + SUFFIX.len())
        .expect("configured test cap must fit the fixed JSON envelope");
    let mut body = String::with_capacity(len);
    body.push_str(PREFIX);
    body.extend(std::iter::repeat_n('x', padding_len));
    body.push_str(SUFFIX);
    assert_eq!(body.len(), len);
    body
}

struct BodyLimitDispatch;

#[async_trait]
impl UpstreamDispatch for BodyLimitDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<Response<UpstreamBody>, DispatchError> {
        let (url, _method, _headers, _body) = request.into_parts();
        let body = match url.path() {
            "/v1/messages" => br#"{"id":"msg_fake_000000000000000000000000","type":"message","role":"assistant","content":[],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50}}"#.as_slice(),
            "/v1/files" => br#"{"id":"file_abc123","type":"file"}"#.as_slice(),
            path => panic!("unexpected dispatch path: {path}"),
        };
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(UpstreamBody::new(Full::new(Bytes::copy_from_slice(body))))
            .map_err(|error| DispatchError::RequestBuild {
                reason: error.to_string(),
            })
    }
}
