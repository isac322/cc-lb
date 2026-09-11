use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Method, Request, Response, StatusCode},
};
use bytes::Bytes;
use cc_lb_config::Config;
use cc_lb_engine::{Body as UpstreamBody, DispatchError, UpstreamDispatch};
use cc_lb_server::app::build_app_for_testing_with_dispatch;
use cc_lb_upstream::SignedRequest;
use http_body_util::{BodyExt, Full};
use tower::ServiceExt;

#[tokio::test]
async fn t2__proxy_router_routes_table_oneshot() {
    struct Case {
        method: Method,
        uri: &'static str,
        body: &'static str,
    }

    let cases = [
        Case {
            method: Method::POST,
            uri: "/v1/messages",
            body: r#"{"model":"claude-3-5-sonnet-20241022","messages":[],"max_tokens":1}"#,
        },
        Case {
            method: Method::POST,
            uri: "/v1/messages/count_tokens",
            body: r#"{"model":"claude-3-5-sonnet-20241022","messages":[]}"#,
        },
        Case {
            method: Method::GET,
            uri: "/v1/models",
            body: "{}",
        },
        Case {
            method: Method::GET,
            uri: "/v1/models/model_abc123",
            body: "{}",
        },
        Case {
            method: Method::POST,
            uri: "/v1/files",
            body: "{}",
        },
        Case {
            method: Method::GET,
            uri: "/v1/files",
            body: "{}",
        },
        Case {
            method: Method::GET,
            uri: "/v1/files/file_abc123",
            body: "{}",
        },
        Case {
            method: Method::DELETE,
            uri: "/v1/files/file_abc123",
            body: "{}",
        },
        Case {
            method: Method::GET,
            uri: "/v1/files/file_abc123/content",
            body: "{}",
        },
        Case {
            method: Method::POST,
            uri: "/api/v1/custom",
            body: "{}",
        },
        Case {
            method: Method::POST,
            uri: "/v1/custom",
            body: "{}",
        },
    ];
    let seen = Arc::new(Mutex::new(Vec::new()));
    let dispatcher: Arc<dyn UpstreamDispatch> = Arc::new(RouteFixtureDispatch {
        seen: Arc::clone(&seen),
    });
    let app = build_app_for_testing_with_dispatch(
        Config::default(),
        cc_lb_testkit::fixed_clock(1_700_000_000),
        Some(dispatcher),
    )
    .await
    .expect("build app");

    for case in &cases {
        let response = request(
            app.router.clone(),
            case.method.clone(),
            case.uri,
            Body::from(case.body),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "route {} {} reaches lifecycle dispatch",
            case.method,
            case.uri,
        );
        let _ = body_text(response).await;
    }

    assert_eq!(
        *seen.lock().expect("seen routes lock"),
        cases
            .iter()
            .map(|case| (case.method.clone(), case.uri.to_owned()))
            .collect::<Vec<_>>(),
    );
}

async fn request(router: axum::Router, method: Method, uri: &str, body: Body) -> Response<Body> {
    router
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .header("content-type", "application/json")
                .body(body)
                .expect("request"),
        )
        .await
        .expect("route response")
}

async fn body_text(response: Response<Body>) -> String {
    let body = response
        .into_body()
        .collect()
        .await
        .expect("response body")
        .to_bytes();
    String::from_utf8(body.to_vec()).expect("utf-8 response")
}

struct RouteFixtureDispatch {
    seen: Arc<Mutex<Vec<(Method, String)>>>,
}

#[async_trait]
impl UpstreamDispatch for RouteFixtureDispatch {
    async fn dispatch(
        &self,
        request: SignedRequest,
    ) -> Result<Response<UpstreamBody>, DispatchError> {
        let (url, method, _headers, _body) = request.into_parts();
        self.seen
            .lock()
            .expect("seen routes lock")
            .push((method.clone(), url.path().to_owned()));
        let body = match (method.as_str(), url.path()) {
            ("POST", "/v1/messages") => br#"{"id":"msg_fake_000000000000000000000000","type":"message","role":"assistant","content":[],"stop_reason":"end_turn","usage":{"input_tokens":100,"output_tokens":50}}"#.as_slice(),
            ("POST", "/v1/messages/count_tokens") => br#"{"input_tokens":1}"#.as_slice(),
            _ => br#"{}"#.as_slice(),
        };

        Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(UpstreamBody::new(Full::new(Bytes::copy_from_slice(body))))
            .map_err(|error| DispatchError::RequestBuild {
                reason: error.to_string(),
            })
    }
}
