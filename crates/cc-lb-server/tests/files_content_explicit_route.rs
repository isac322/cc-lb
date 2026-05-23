use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::OriginalUri;
use axum::http::{Method, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::any;
use axum::Router;
use cc_lb_config::{AuthStrategy, Config, UpstreamKind, UpstreamSpec};
use cc_lb_server::app::{
    build_app_with_path, PROXY_FILES_ROUTE_COLLECTION, PROXY_FILES_ROUTE_ITEM,
    PROXY_FILES_ROUTE_ITEM_CONTENT, PROXY_FILES_ROUTE_PATHS,
};
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
async fn files_content_route_is_registered_explicitly() {
    assert_eq!(
        PROXY_FILES_ROUTE_PATHS,
        &[
            PROXY_FILES_ROUTE_COLLECTION,
            PROXY_FILES_ROUTE_ITEM,
            PROXY_FILES_ROUTE_ITEM_CONTENT,
        ],
    );
    write_evidence(
        ".omo/evidence/task-3-route-registration.txt",
        "routes=/v1/files,/v1/files/{id},/v1/files/{id}/content",
    );
}

#[tokio::test]
async fn files_content_forwards_exact_path_and_method() {
    let (upstream_addr, upstream_state, _upstream) = spawn_recording_upstream().await;
    let config = config_for_upstream(upstream_addr);
    let app = build_app_with_path(config, None).expect("build app");

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/files/abc123/content")
                .header("x-api-key", "sk-ant-test")
                .body(Body::empty())
                .expect("content request"),
        )
        .await
        .expect("content response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("content body")
        .to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("/v1/files/abc123/content"));
    assert_eq!(upstream_state.last_request().method, Method::GET);
    assert_eq!(upstream_state.last_request().path, "/v1/files/abc123/content");
    write_evidence(
        ".omo/evidence/task-3-files-content.txt",
        "status=200 method=GET path=/v1/files/abc123/content",
    );
}

#[tokio::test]
async fn files_routes_remain_intact() {
    let (upstream_addr, upstream_state, _upstream) = spawn_recording_upstream().await;
    let config = config_for_upstream(upstream_addr);
    let app = build_app_with_path(config, None).expect("build app");

    let files = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/files")
                .header("x-api-key", "sk-ant-test")
                .body(Body::empty())
                .expect("files request"),
        )
        .await
        .expect("files response");
    assert_eq!(files.status(), StatusCode::OK);
    let files_body = files
        .into_body()
        .collect()
        .await
        .expect("files body")
        .to_bytes();
    assert!(String::from_utf8_lossy(&files_body).contains("/v1/files"));
    assert_eq!(upstream_state.last_request().method, Method::GET);
    assert_eq!(upstream_state.last_request().path, "/v1/files");

    let delete_file = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/v1/files/file_abc123")
                .header("x-api-key", "sk-ant-test")
                .body(Body::empty())
                .expect("delete request"),
        )
        .await
        .expect("delete response");
    assert_eq!(delete_file.status(), StatusCode::OK);
    let delete_body = delete_file
        .into_body()
        .collect()
        .await
        .expect("delete body")
        .to_bytes();
    assert!(String::from_utf8_lossy(&delete_body).contains("/v1/files/file_abc123"));
    assert_eq!(upstream_state.last_request().method, Method::DELETE);
    assert_eq!(upstream_state.last_request().path, "/v1/files/file_abc123");
    write_evidence(
        ".omo/evidence/task-3-regression-files.txt",
        "status=200 method=GET path=/v1/files; status=200 method=DELETE path=/v1/files/file_abc123",
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
    let app = Router::new().fallback(any(move |method: Method, uri: OriginalUri| {
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
