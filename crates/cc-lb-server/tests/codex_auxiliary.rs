//! Auxiliary requests through real session admission and SQLite owner bindings.
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::routing::any;
use axum::Router;
use tower::ServiceExt;

use crate::codex_control_support::{PROVIDER_TOKEN, build_oauth_session_app};

type Records = Arc<Mutex<Vec<(Uri, HeaderMap, Bytes)>>>;

async fn record(
    State(records): State<Records>,
    request: Request,
) -> (StatusCode, &'static str) {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, 1024 * 1024).await.expect("provider body");
    records.lock().expect("records").push((parts.uri, parts.headers, body));
    (StatusCode::OK, r#"{"result":"selected-owner-result"}"#)
}

struct Provider {
    url: url::Url,
    records: Records,
    task: tokio::task::JoinHandle<()>,
}

impl Provider {
    async fn start() -> Self {
        let records = Records::default();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind provider");
        let url = format!("http://{}/backend-api/codex/", listener.local_addr().unwrap())
            .parse().unwrap();
        let router = Router::new().fallback(any(record)).with_state(records.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve provider");
        });
        Self { url, records, task }
    }
}

impl Drop for Provider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn notes_preserve_wire_and_use_only_the_session_owner() {
    let selected = Provider::start().await;
    let alternate = Provider::start().await;
    let support = build_oauth_session_app(selected.url.clone(), alternate.url.clone(), vec![]).await;
    for operation in ["list_files_by_prefix", "read_file", "search_contents", "append_to_file", "write_file", "thread_hint"] {
        let body = r#"{"path":"qa.txt","content":"opaque notes","context":{"session_id":"client-context"}}"#;
        let request = Request::builder()
            .method("POST")
            .uri(format!("/backend-api/codex/alpha/notes/v2/{operation}?trace=a%2Fb"))
            .header(header::AUTHORIZATION, format!("Bearer {}", support.access))
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-openai-encrypted-tool-arguments", "true")
            .header("x-openai-tool-output-truncation-policy", "opaque-policy")
            .body(Body::from(body)).unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            support.app.router.clone().oneshot(request),
        ).await.expect("notes request completes").unwrap();
        let status = response.status();
        let response_body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(status, StatusCode::OK, "{operation}: {response_body:?}");
        assert_eq!(response_body.as_ref(), br#"{"result":"selected-owner-result"}"#);
        let records = selected.records.lock().unwrap();
        let (uri, headers, sent_body) = records.last().expect("selected owner receives request");
        assert_eq!(uri.path(), format!("/backend-api/codex/alpha/notes/v2/{operation}"));
        assert_eq!(uri.query(), Some("trace=a%2Fb"));
        assert_eq!(headers[header::AUTHORIZATION], format!("Bearer {PROVIDER_TOKEN}"));
        assert_eq!(headers["x-openai-encrypted-tool-arguments"], "true");
        assert_eq!(headers["x-openai-tool-output-truncation-policy"], "opaque-policy");
        assert_eq!(sent_body.as_ref(), body.as_bytes());
        assert!(alternate.records.lock().unwrap().is_empty(), "must not dispatch to alternate owner");
    }
    assert_eq!(selected.records.lock().unwrap().len(), 6);
}

#[tokio::test]
async fn notes_do_not_fail_over_when_the_session_owner_is_disabled() {
    let selected = Provider::start().await;
    let alternate = Provider::start().await;
    let support = build_oauth_session_app(selected.url.clone(), alternate.url.clone(), vec![]).await;
    sqlx::query("UPDATE upstream_spec_v1 SET enabled = 0 WHERE id = ?")
        .bind(support.selected_upstream_id.to_string())
        .execute(support.storage.pool())
        .await
        .expect("disable selected owner");
    for operation in ["list_files_by_prefix", "read_file", "search_contents", "append_to_file", "write_file", "thread_hint"] {
        let request = Request::builder()
            .method("POST")
            .uri(format!("/backend-api/codex/alpha/notes/v2/{operation}"))
            .header(header::AUTHORIZATION, format!("Bearer {}", support.access))
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"path":"qa.txt"}"#)).unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            support.app.router.clone().oneshot(request),
        ).await.expect("owner rejection completes").unwrap();
        let status = response.status();
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{operation}: {body:?}");
        assert!(selected.records.lock().unwrap().is_empty());
        assert!(alternate.records.lock().unwrap().is_empty(), "must not fail over to another owner");
    }
}
