use crate::admin_test_common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use cc_lb_admin::ports::{
    RoutePreviewError, RoutePreviewInput, RoutePreviewOutcome, RoutePreviewPort, RoutePreviewWinner,
};
use cc_lb_admin::{AdminPorts, AdminState, router};
use cc_lb_config::Config;
use cc_lb_domain::RoutingTrace;
use http_body_util::BodyExt;
use serde_json::json;
use tower::ServiceExt;
use uuid::Uuid;

struct PreviewPort;

impl RoutePreviewPort for PreviewPort {
    fn preview_route(
        &self,
        _input: RoutePreviewInput,
    ) -> Result<RoutePreviewOutcome, RoutePreviewError> {
        Ok(RoutePreviewOutcome {
            trace: RoutingTrace {
                stages: Vec::new(),
                terminal_decision: None,
            },
            winner: Some(RoutePreviewWinner {
                upstream_id: Uuid::from_u128(7),
                name: "preview-upstream".to_owned(),
            }),
        })
    }
}

fn test_state() -> AdminState {
    let config = Config::default();
    AdminState {
        storage: None,
        key_store: None,
        aead: Arc::new(cc_lb_aead::AeadService::from_master_key([0; 32])),
        limit_engine: admin_test_common::limit_engine(),
        lifecycle: None,
        dynamic_view: admin_test_common::dynamic_view_holder(&config),
        config: Arc::new(Config::default()),
        scheduler: None,
        admin_auth: crate::admin_test_common::static_token_auth("test-token"),
        lazy_refresher: None,
        runtime: None,
        data_dir: None,
        warmup_dialect_dispatcher: None,
        subscription_metadata_hook: None,
        start_time: std::time::Instant::now(),
        event_bus: None,
        storage_tail: cc_lb_admin::events::storage_tail_channel(),
        clock: Arc::new(cc_lb_clock::SystemClock),
    }
}

async fn send_preview_request(
    authorization: Option<&str>,
    body: Option<(&str, Vec<u8>)>,
) -> (StatusCode, serde_json::Value) {
    let app = router(test_state());
    let mut builder = Request::builder()
        .method("POST")
        .uri("/admin/v1/router/preview");
    if let Some(token) = authorization {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request_body = if let Some((content_type, payload)) = body {
        builder = builder.header(header::CONTENT_TYPE, content_type);
        Body::from(payload)
    } else {
        Body::empty()
    };
    let response = app
        .oneshot(builder.body(request_body).expect("request builds"))
        .await
        .expect("request completes");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let value = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
    };
    (status, value)
}

#[tokio::test]
async fn t2__route_preview_returns_admin_port_projection() {
    let mut state = test_state();
    state.lifecycle = Some(AdminPorts {
        route_preview: Some(Arc::new(PreviewPort)),
        ..AdminPorts::default()
    });
    let response = router(state)
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/v1/router/preview")
                .header(header::AUTHORIZATION, "Bearer test-token")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"principal_id":"preview-principal"}"#))
                .expect("request builds"),
        )
        .await
        .expect("request completes");
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let body: serde_json::Value = serde_json::from_slice(&bytes).expect("response is json");

    assert_eq!(body["trace"]["stages"], serde_json::json!([]));
    assert_eq!(
        body["winner"]["upstream_id"],
        Uuid::from_u128(7).to_string()
    );
    assert_eq!(body["winner"]["name"], "preview-upstream");
}

#[tokio::test]
async fn t2__missing_authorization_returns_401() {
    let (status, _) = send_preview_request(
        None,
        Some((
            "application/json",
            serde_json::to_vec(&json!({ "principal_id": "any" })).unwrap(),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn t2__wrong_admin_token_returns_401() {
    let (status, _) = send_preview_request(
        Some("wrong-token"),
        Some((
            "application/json",
            serde_json::to_vec(&json!({ "principal_id": "any" })).unwrap(),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn t2__lifecycle_unavailable_returns_500_with_structured_error() {
    let (status, body) = send_preview_request(
        Some("test-token"),
        Some((
            "application/json",
            serde_json::to_vec(&json!({ "principal_id": "example-principal" })).unwrap(),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"], "lifecycle_unavailable");
    assert!(body["detail"].is_string());
}

#[tokio::test]
async fn t2__malformed_json_body_returns_400() {
    let (status, _) = send_preview_request(
        Some("test-token"),
        Some(("application/json", b"{ not json".to_vec())),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
