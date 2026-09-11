use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use config_admin_common::{app, authed_json, test_state};

#[tokio::test]
async fn t3__events_delta_returns_rows_next_cursor_and_exhaustion() {
    let (_dir, storage) = crate::config_admin_common::sqlite_temp_storage().await;
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
    assert_eq!(first_page["events"][0]["json_parse_ms"], 0.125);
    assert_eq!(first_page["events"][0]["cache_structure_ms"], 0.25);
    assert_eq!(first_page["events"][0]["cache_token_key_ms"], 0.375);
    assert_eq!(first_page["events"][0]["cache_count_lookup_ms"], 0.5);
    assert_eq!(first_page["events"][0]["cache_tokenizer_queue_ms"], 0.625);
    assert_eq!(first_page["events"][0]["cache_serialize_ms"], 0.75);
    assert_eq!(first_page["events"][0]["cache_tokenize_ms"], 0.0);
    assert_eq!(first_page["events"][0]["prepare_signer_ms"], 1.25);
    assert_eq!(first_page["events"][0]["request_body_first_chunk_ms"], 0.25);
    assert!(first_page["events"][0]["request_body_receive_ms"].is_null());
    assert_eq!(first_page["events"][0]["request_body_wait_ms"], 0.0);
    assert_eq!(first_page["events"][0]["request_body_process_ms"], 0.5);
    assert_eq!(first_page["events"][0]["request_body_chunk_count"], 0);
    assert_eq!(first_page["events"][0]["response_body_wait_ms"], 0.75);
    assert_eq!(first_page["events"][0]["response_body_process_ms"], 0.0);
    assert_eq!(
        first_page["events"][0]["response_body_downstream_poll_gap_ms"],
        1.25
    );
    assert_eq!(first_page["events"][0]["retry_overhead_ms"], 1.5);
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
        json_parse_ms: Some(0.125),
        cache_structure_ms: Some(0.25),
        cache_token_key_ms: Some(0.375),
        cache_count_lookup_ms: Some(0.5),
        cache_tokenizer_queue_ms: Some(0.625),
        cache_serialize_ms: Some(0.75),
        cache_tokenize_ms: Some(0.0),
        prepare_signer_ms: Some(1.25),
        request_body_first_chunk_ms: Some(0.25),
        request_body_receive_ms: None,
        request_body_wait_ms: Some(0.0),
        request_body_process_ms: Some(0.5),
        request_body_chunk_count: Some(0),
        response_body_wait_ms: Some(0.75),
        response_body_process_ms: Some(0.0),
        response_body_downstream_poll_gap_ms: Some(1.25),
        retry_overhead_ms: Some(1.5),
        ..Default::default()
    }
}
