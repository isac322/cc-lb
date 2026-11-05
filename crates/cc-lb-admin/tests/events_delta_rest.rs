use crate::config_admin_common;

use axum::http::StatusCode;
use cc_lb_config::Config;
use cc_lb_domain::{InternalError, RoutingTrace};
use cc_lb_request_log::{
    RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestDiagnostics, RequestEventKind,
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
async fn events_delta_filters_before_paging_and_traverses_each_match_once() {
    let (_dir, storage) = temp_storage().await;
    let mut cursors = Vec::new();
    for index in 1..=512 {
        let mut event = request_event(index);
        event.event_kind = Some(RequestEventKind::Messages);
        let source_kind = if index <= 510 { "request" } else { "renewal" };
        event.source_kind = Some(source_kind.to_owned());
        // Await the committed insert, rather than the asynchronous assembler.
        cursors.push(storage.append_request_event(&event).await.unwrap());
    }
    let state = test_state(Config::default(), Some(storage));
    let messages: Vec<_> = (1..=510).map(|index| format!("req-{index}")).collect();
    let renewals = vec!["req-511".to_owned(), "req-512".to_owned()];
    let all: Vec<_> = (1..=512).map(|index| format!("req-{index}")).collect();
    let empty = Vec::new();

    for (filters, expected, page_sizes) in [
        ("", &messages, vec![500, 10]),
        ("&event_kind=messages", &messages, vec![500, 10]),
        ("&source_kind=request", &messages, vec![500, 10]),
        ("&source_kind=all", &all, vec![500, 12]),
        ("&event_kind=renewal", &renewals, vec![2]),
        ("&source_kind=renewal", &renewals, vec![2]),
        (
            "&source_kind=renewal&event_kind=renewal",
            &renewals,
            vec![2],
        ),
        ("&source_kind=renewal&event_kind=messages", &empty, vec![0]),
    ] {
        let mut cursor = 0;
        let mut seen = Vec::new();
        for (page_index, expected_size) in page_sizes.iter().enumerate() {
            let page = delta_page(state.clone(), cursor, 500, filters).await;
            let events = page["events"].as_array().unwrap();
            assert_eq!(
                events.len(),
                *expected_size,
                "filters={filters}, page={page_index}"
            );
            seen.extend(
                events
                    .iter()
                    .map(|event| event["request_id"].as_str().unwrap().to_owned()),
            );
            let next_cursor = page["next_cursor"].as_u64().unwrap();
            assert!(next_cursor >= cursor, "cursor regressed: {page}");
            if page_index == 0 && *expected_size == 500 {
                assert_eq!(next_cursor, cursors[499]);
            }
            assert_eq!(
                page["exhausted"],
                page_index + 1 == page_sizes.len(),
                "filters={filters}, page={page_index}"
            );
            cursor = next_cursor;
        }
        assert_eq!(&seen, expected, "filters={filters}");
    }

    let first = delta_page(state.clone(), 0, 1, "&event_kind=renewal").await;
    assert_eq!(first["events"][0]["request_id"], "req-511");
    assert_eq!(first["next_cursor"], cursors[510]);
    assert_eq!(first["exhausted"], false);
    let second = delta_page(state.clone(), cursors[510], 1, "&event_kind=renewal").await;
    assert_eq!(second["events"].as_array().unwrap().len(), 1);
    assert_eq!(second["events"][0]["request_id"], "req-512");
    assert_eq!(second["next_cursor"], cursors[511]);
    assert_eq!(second["exhausted"], false);
    let end = delta_page(state, cursors[511], 1, "&event_kind=renewal").await;
    assert!(end["events"].as_array().unwrap().is_empty());
    assert_eq!(end["next_cursor"], cursors[511]);
    assert_eq!(end["exhausted"], true);
}

#[tokio::test]
async fn events_delta_empty_exhausted_page_advances_to_hi_without_regressing_future_cursor() {
    let (_dir, storage) = temp_storage().await;
    let mut cursor_hi = 0;
    for index in 1..=3 {
        cursor_hi = storage
            .append_request_event(&request_event(index))
            .await
            .unwrap();
    }
    let state = test_state(Config::default(), Some(storage));
    for since in [0, cursor_hi, cursor_hi + 100] {
        let page = delta_page(state.clone(), since, 500, "&principal_id=absent").await;
        assert!(page["events"].as_array().unwrap().is_empty());
        assert_eq!(page["exhausted"], true);
        assert_eq!(page["next_cursor"], since.max(cursor_hi));
    }
}

async fn delta_page(
    state: cc_lb_admin::AdminState,
    since: u64,
    limit: usize,
    filters: &str,
) -> Value {
    let (status, _, page, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/v1/events/delta?since_cursor={since}&limit={limit}{filters}"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    page
}

#[tokio::test]
async fn stored_rows_carry_no_diagnostics_and_live_finals_match() {
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
    for event in events {
        for key in [
            "cache_breakpoints",
            "cache_control_message_indices",
            "routing_trace",
            "iterations",
            "internal_errors",
        ] {
            assert!(
                event.get(key).is_none(),
                "stored row exposes diagnostic key {key}: {event}"
            );
        }
    }
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

/// Both storage reads omit diagnostics and preserve the same operational fields (ids aside).
fn assert_same_row_without_diagnostics(full: &Value, stripped: &Value) {
    let full = full.as_object().unwrap();
    let stripped = stripped.as_object().unwrap();
    for key in [
        "cache_breakpoints",
        "cache_control_message_indices",
        "routing_trace",
        "iterations",
        "internal_errors",
    ] {
        assert!(!full.contains_key(key));
        assert!(!stripped.contains_key(key));
    }
    for (key, value) in stripped {
        if matches!(key.as_str(), "request_id" | "event_id" | "ts" | "ts_ms") {
            continue;
        }
        assert_eq!(full.get(key), Some(value), "field {key} differs");
    }
    assert_eq!(full.len(), stripped.len());
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
