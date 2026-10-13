use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_domain::{InternalError, RoutingTrace};
use cc_lb_request_log::{
    REQUEST_DIAGNOSTIC_PAYLOAD_KEYS, RequestCacheBreakpoint, RequestCacheBreakpointSource,
    RequestDiagnostics,
};
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use config_admin_common::{app, authed_json, temp_storage, test_state};
use serde_json::Value;

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

#[tokio::test]
async fn stripped_rows_are_served_identically_without_the_five_diagnostic_keys() {
    let (_dir, storage) = temp_storage().await;
    let full = with_diagnostics(request_event(1));
    let mut stripped = with_diagnostics(request_event(2));
    let taken = RequestDiagnostics::take_from(&mut stripped);
    assert!(
        !taken.cache_breakpoints.is_empty(),
        "fixture carries diagnostics"
    );
    storage.append_request_event(&full).await.unwrap();
    storage.append_request_event(&stripped).await.unwrap();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, page, _) = authed_json(
        app(state.clone()),
        "GET",
        "/admin/v1/events/delta?since_cursor=0",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let events = page["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert_same_row_without_diagnostics(&events[0], &events[1]);

    let mut details = Vec::new();
    for event_id in ["event-000001", "event-000002"] {
        let (status, _, detail, _) = authed_json(
            app(state.clone()),
            "GET",
            &format!("/admin/v1/events/detail/{event_id}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        details.push(detail);
    }
    assert_same_row_without_diagnostics(&details[0], &details[1]);
}

/// `stripped` must equal `full` minus exactly the five diagnostic keys (ids aside).
fn assert_same_row_without_diagnostics(full: &Value, stripped: &Value) {
    let full = full.as_object().unwrap();
    let stripped = stripped.as_object().unwrap();
    for key in REQUEST_DIAGNOSTIC_PAYLOAD_KEYS {
        assert!(full.contains_key(key), "full row lost {key}: {full:?}");
        assert!(
            !stripped.contains_key(key),
            "stripped row still exposes {key}: {stripped:?}"
        );
    }
    let mut expected = full.clone();
    for key in REQUEST_DIAGNOSTIC_PAYLOAD_KEYS {
        expected.remove(key);
    }
    for (key, value) in stripped {
        if matches!(key.as_str(), "request_id" | "event_id" | "ts" | "ts_ms") {
            continue;
        }
        assert_eq!(expected.get(key), Some(value), "field {key} differs");
    }
    assert_eq!(
        expected.len(),
        stripped.len(),
        "same key set apart from the five"
    );
}

fn with_diagnostics(mut event: RequestEvent) -> RequestEvent {
    event.cache_control_message_indices = vec![0, 2];
    event.cache_breakpoints = vec![RequestCacheBreakpoint {
        block_index: 1,
        source: RequestCacheBreakpointSource::System,
        path: "system[0]".to_owned(),
        message_index: None,
        ttl: None,
        prefix_hash: "prefix".to_owned(),
        prefix_token_count: 64,
        lookback_prefixes: Vec::new(),
        token_estimate_source: None,
    }];
    event.routing_trace = Some(RoutingTrace::default());
    event.iterations = Some(serde_json::json!([{ "input_tokens": 1 }]));
    event.internal_errors = vec![InternalError {
        message: Some("token=[REDACTED] tail".to_owned()),
        ..InternalError::default()
    }];
    event
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
