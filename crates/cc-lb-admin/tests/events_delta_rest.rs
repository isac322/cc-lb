mod config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use config_admin_common::{app, authed_json, temp_storage, test_state};

#[tokio::test]
async fn events_delta_returns_rows_next_cursor_and_exhaustion() {
    let (_dir, storage) = temp_storage().await;
    let first_cursor = storage
        .append_request_event(&request_event(1))
        .await
        .unwrap();
    let second_cursor = storage
        .append_request_event(&request_event(2))
        .await
        .unwrap();
    let third_cursor = storage
        .append_request_event(&request_event(3))
        .await
        .unwrap();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, first_page, _) = authed_json(
        app(state.clone()),
        "GET",
        &format!("/admin/v1/events/delta?since_cursor={first_cursor}&limit=1"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_page["events"].as_array().unwrap().len(), 1);
    assert_eq!(first_page["events"][0]["request_id"], "req-2");
    assert_eq!(first_page["next_cursor"], second_cursor);
    assert_eq!(first_page["exhausted"], false);

    let (status, _, second_page, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/events/delta?since_cursor={second_cursor}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second_page["events"].as_array().unwrap().len(), 1);
    assert_eq!(second_page["events"][0]["request_id"], "req-3");
    assert_eq!(second_page["next_cursor"], third_cursor);
    assert_eq!(second_page["exhausted"], true);
}

fn request_event(index: u64) -> RequestEvent {
    RequestEvent {
        ts: 1_800_000_000 + index,
        ts_ms: Some((1_800_000_000 + index) * 1_000),
        request_id: format!("req-{index}"),
        event_id: Some(format!("event-{index:06}")),
        principal_id: Some("principal-a".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        duration_ms: 10,
        ..Default::default()
    }
}
