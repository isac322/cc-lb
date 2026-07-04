use axum::body::Body;
use axum::http::{Request, StatusCode};
use cc_lb_admin::internal_partials::{InternalPartialsState, router};
use cc_lb_engine::{PartialRetentionCache, RequestEventUpdate};
use cc_lb_storage_api::RequestEventPartial;
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn internal_partial_fetch_returns_payload_when_token_valid() {
    let cache = PartialRetentionCache::new(std::time::Duration::from_secs(300), 10);
    let update = RequestEventUpdate::Partial(RequestEventPartial {
        event_id: "event-present".to_owned(),
        request_id: "req-present".to_owned(),
        ts: 1_700_000_000,
        ts_ms: 1_700_000_000_000,
        last_update_ms: 1_700_000_000_000,
        ..RequestEventPartial::default()
    });
    cache.insert(
        "event-present".to_owned(),
        serde_json::to_vec(&update).expect("update serializes"),
    );

    let response = router(InternalPartialsState {
        retention: cache,
        cluster_token: "cluster-token".to_owned(),
    })
    .oneshot(request("event-present", Some("cluster-token")))
    .await
    .expect("request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("body collects")
        .to_bytes();
    let parsed: RequestEventUpdate = serde_json::from_slice(&body).expect("body is update json");
    assert_eq!(parsed.event_id(), "event-present");
}

#[tokio::test]
async fn internal_partial_fetch_rejects_missing_or_wrong_token() {
    let app = router(InternalPartialsState {
        retention: PartialRetentionCache::new(std::time::Duration::from_secs(300), 10),
        cluster_token: "cluster-token".to_owned(),
    });

    let missing = app
        .clone()
        .oneshot(request("event-present", None))
        .await
        .expect("missing-token request succeeds");
    assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);

    let wrong = app
        .oneshot(request("event-present", Some("wrong-token")))
        .await
        .expect("wrong-token request succeeds");
    assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn internal_partial_fetch_returns_not_found_when_event_absent() {
    let response = router(InternalPartialsState {
        retention: PartialRetentionCache::new(std::time::Duration::from_secs(300), 10),
        cluster_token: "cluster-token".to_owned(),
    })
    .oneshot(request("missing-event", Some("cluster-token")))
    .await
    .expect("request succeeds");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

fn request(event_id: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("GET")
        .uri(format!("/internal/v1/partials/{event_id}"));
    if let Some(token) = token {
        builder = builder.header("X-Cluster-Token", token);
    }
    builder.body(Body::empty()).expect("request builds")
}
