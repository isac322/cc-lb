mod common;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::OriginalUri;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::any;
use cc_lb_config::{AuthStrategy, Config, UpstreamKind, UpstreamSpec};
use cc_lb_server::app::build_app_with_path;
use fake_anthropic::{AppConfig, app as fake_anthropic_app};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::ServiceExt;
use url::Url;

#[derive(Debug)]
struct UpstreamState {
    headers: Mutex<Option<HeaderMap>>,
}

impl UpstreamState {
    fn new() -> Self {
        Self {
            headers: Mutex::new(None),
        }
    }

    fn record(&self, headers: HeaderMap) {
        *self.headers.lock().expect("record headers") = Some(headers);
    }

    fn last_headers(&self) -> HeaderMap {
        self.headers
            .lock()
            .expect("last headers")
            .clone()
            .expect("recorded headers")
    }
}

#[tokio::test]
async fn forwards_selected_headers_to_fake_anthropic() {
    let (upstream_addr, _upstream) = spawn_fake_anthropic().await;
    let app = build_app_with_path(config_for_upstream(upstream_addr), None).expect("build app");

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .header("x-organization-uuid", "org-123")
                .header("X-Trusted-Device-Token", "device-456")
                .header("Connection", "close")
                .header("content-type", "application/json")
                .body(Body::from(Bytes::from_static(
                    br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
                )))
                .expect("request builds"),
        )
        .await
        .expect("proxy response");
    assert_eq!(response.status(), StatusCode::OK);

    let last_request = common::http_get(upstream_addr, "/__last_request")
        .await
        .expect("last request response");
    assert_eq!(last_request.status, 200);

    let body: Value = serde_json::from_str(&last_request.body).expect("last request json");
    assert_eq!(body["x_api_key"], "sk-ant-test");
    assert_eq!(body["headers"]["x-organization-uuid"], "org-123");
    assert_eq!(body["headers"]["x-trusted-device-token"], "device-456");
    assert_eq!(
        body["headers"].as_object().expect("headers object").len(),
        2
    );
}

#[tokio::test]
async fn strips_connection_before_upstream_forwarding() {
    let (upstream_addr, upstream_state, _upstream) = spawn_recording_upstream().await;
    let app = build_app_with_path(config_for_upstream(upstream_addr), None).expect("build app");

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .header("anthropic-version", "2023-06-01")
                .header("x-organization-uuid", "org-123")
                .header("X-Trusted-Device-Token", "device-456")
                .header("Connection", "close")
                .header("content-type", "application/json")
                .body(Body::from(Bytes::from_static(
                    br#"{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"hi"}],"max_tokens":10}"#,
                )))
                .expect("request builds"),
        )
        .await
        .expect("proxy response");
    assert_eq!(response.status(), StatusCode::OK);

    let headers = upstream_state.last_headers();
    assert!(!headers.contains_key("connection"));
    assert_eq!(
        headers
            .get("x-organization-uuid")
            .and_then(|value| value.to_str().ok()),
        Some("org-123")
    );
    assert_eq!(
        headers
            .get("x-trusted-device-token")
            .and_then(|value| value.to_str().ok()),
        Some("device-456")
    );
}

fn config_for_upstream(upstream_addr: SocketAddr) -> Config {
    let mut config = Config::default();
    config.upstreams.insert(
        "primary".to_owned(),
        UpstreamSpec {
            kind: UpstreamKind::Custom,
            base_url: Some(Url::parse(&format!("http://{upstream_addr}")).expect("upstream url")),
            region: None,
            project: None,
            auth_strategy: AuthStrategy::ApiKey,
            credentials_ref: None,
        },
    );
    config
}

async fn spawn_fake_anthropic() -> (SocketAddr, JoinHandle<Result<(), std::io::Error>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake upstream");
    let addr = listener.local_addr().expect("fake addr");
    let task = tokio::spawn(async move {
        axum::serve(listener, fake_anthropic_app(AppConfig::default())).await
    });
    (addr, task)
}

async fn spawn_recording_upstream() -> (
    SocketAddr,
    Arc<UpstreamState>,
    JoinHandle<Result<(), std::io::Error>>,
) {
    let state = Arc::new(UpstreamState::new());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    let upstream_state = state.clone();
    let app = Router::new().fallback(any(move |headers: HeaderMap, _uri: OriginalUri| {
        let upstream_state = upstream_state.clone();
        async move {
            upstream_state.record(headers);
            (StatusCode::OK, [("content-type", "application/json")], "{}").into_response()
        }
    }));
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    (addr, state, task)
}
