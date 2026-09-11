use axum::http::StatusCode;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use serde_json::json;

use crate::admin_test_common::spawn_admin_server;

#[tokio::test]
async fn t2__events_detail_single_event_and_404() {
    let server = spawn_admin_server().await;
    let event = RequestEvent {
        ts: 1_700_000_001,
        ts_ms: Some(1_700_000_001_250),
        request_id: "req-event-detail".to_owned(),
        event_id: Some("event-detail-found".to_owned()),
        principal_id: Some("principal-detail".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 207,
        duration_ms: 37,
        ..RequestEvent::default()
    };
    server
        .storage
        .append_request_event(&event)
        .await
        .expect("event fixture is stored");

    let (found_status, _, found) = server
        .client
        .get("/admin/v1/events/detail/event-detail-found")
        .await;
    assert_eq!(found_status, StatusCode::OK);
    assert_eq!(
        found,
        serde_json::to_value(&event).expect("request event serializes")
    );

    let (missing_status, _, missing) = server
        .client
        .get("/admin/v1/events/detail/event-detail-missing")
        .await;
    assert_eq!(missing_status, StatusCode::NOT_FOUND);
    assert_eq!(missing, json!({ "error": "not_found" }));
}
