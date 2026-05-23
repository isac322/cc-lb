use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::OriginalUri;
use axum::http::{Method, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::any;
use cc_lb_config::{AuthStrategy, Config, UpstreamKind, UpstreamSpec};
use cc_lb_server::app::build_app_with_path;
use http_body_util::BodyExt;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::ServiceExt;
use url::Url;

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: Method,
    path: String,
}

#[derive(Debug)]
struct UpstreamState {
    requests: Mutex<Vec<RecordedRequest>>,
}

impl UpstreamState {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, method: Method, path: String) {
        self.requests
            .lock()
            .expect("record upstream request")
            .push(RecordedRequest { method, path });
    }

    fn last_request(&self) -> RecordedRequest {
        self.requests
            .lock()
            .expect("last upstream request")
            .last()
            .expect("recorded upstream request")
            .clone()
    }
}

#[tokio::test]
async fn api_wildcard_forwarding() {
    let (upstream_addr, upstream_state, _upstream) = spawn_recording_upstream().await;
    let config = config_for_upstream(upstream_addr);
    let app = build_app_with_path(config, None).expect("build app");

    let usage = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/oauth/usage")
                .header("x-api-key", "sk-ant-test")
                .body(Body::from(Bytes::from_static(b"{}")))
                .expect("usage request"),
        )
        .await
        .expect("usage response");
    assert_eq!(usage.status(), StatusCode::OK);
    let usage_body = usage
        .into_body()
        .collect()
        .await
        .expect("usage body")
        .to_bytes();
    assert!(String::from_utf8_lossy(&usage_body).contains("/api/oauth/usage"));
    assert_eq!(upstream_state.last_request().method, Method::POST);
    assert_eq!(upstream_state.last_request().path, "/api/oauth/usage");
    write_evidence(
        ".omo/evidence/task-2-api-oauth-usage.txt",
        "status=200 method=POST path=/api/oauth/usage",
    );

    let anon = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/event_logging/batch")
                .body(Body::from(Bytes::from_static(b"[]")))
                .expect("anon request"),
        )
        .await
        .expect("anon response");
    assert_eq!(anon.status(), StatusCode::OK);
    let anon_body = anon
        .into_body()
        .collect()
        .await
        .expect("anon body")
        .to_bytes();
    assert!(String::from_utf8_lossy(&anon_body).contains("/api/event_logging/batch"));
    assert_eq!(upstream_state.last_request().method, Method::POST);
    assert_eq!(
        upstream_state.last_request().path,
        "/api/event_logging/batch"
    );
    write_evidence(
        ".omo/evidence/task-2-event-logging-anon.txt",
        "status=200 method=POST path=/api/event_logging/batch",
    );

    let messages = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/messages")
                .header("x-api-key", "sk-ant-test")
                .body(Body::from(Bytes::from_static(b"{}")))
                .expect("messages request"),
        )
        .await
        .expect("messages response");
    assert_eq!(messages.status(), StatusCode::OK);
    let messages_body = messages
        .into_body()
        .collect()
        .await
        .expect("messages body")
        .to_bytes();
    assert!(String::from_utf8_lossy(&messages_body).contains("/v1/messages"));
    assert_eq!(upstream_state.last_request().method, Method::POST);
    assert_eq!(upstream_state.last_request().path, "/v1/messages");
    write_evidence(
        ".omo/evidence/task-2-regression-v1.txt",
        "status=200 method=POST path=/v1/messages",
    );

    app.drain_controller().trigger();
    let draining = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/oauth/usage")
                .header("x-api-key", "sk-ant-test")
                .body(Body::from(Bytes::from_static(b"{}")))
                .expect("drain request"),
        )
        .await
        .expect("drain response");
    assert_eq!(draining.status(), StatusCode::SERVICE_UNAVAILABLE);
    write_evidence(
        ".omo/evidence/task-2-drain-503.txt",
        "status=503 path=/api/oauth/usage",
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
    let app = Router::new()
        .fallback(any(move |method: Method, uri: OriginalUri| {
            let upstream_state = upstream_state.clone();
            async move {
                upstream_state.record(method, uri.0.path().to_owned());
                (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    format!(
                        r#"{{"method":"{}","path":"{}"}}"#,
                        upstream_state.last_request().method,
                        upstream_state.last_request().path
                    ),
                )
                    .into_response()
            }
        }))
        .with_state(state.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await });
    (addr, state, task)
}

fn write_evidence(path: &str, contents: &str) {
    std::fs::create_dir_all(".omo/evidence").expect("create evidence dir");
    std::fs::write(path, contents).expect("write evidence");
}
