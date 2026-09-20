use crate::config_admin_common;

use std::sync::Arc;

use axum::http::StatusCode;
use cc_lb_clock::{ClockHandle, TestClock};
use cc_lb_config::Config;
use cc_lb_request_log::RequestEventKind;
use cc_lb_storage_api::{RequestEvent, RequestEventStore};
use config_admin_common::{
    app, authed_bytes, authed_json, temp_storage, temp_storage_with_clock, test_state,
    test_state_with_clock,
};
use uuid::Uuid;

const TEST_NOW_UNIX_SECS: u64 = 1_700_000_000;

#[tokio::test]
async fn events_recent_returns_empty_with_no_traffic() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) = authed_json(app(state), "GET", "/admin/events/recent", None).await;
    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 0);
    assert_eq!(body["count"], 0);
}

#[tokio::test]
async fn events_recent_accepts_limit_param() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));

    let (status, _, body, _) =
        authed_json(app(state), "GET", "/admin/events/recent?limit=10", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["limit"], 10);
}

#[tokio::test]
async fn events_recent_rejects_invalid_limit() {
    let (_dir, storage) = temp_storage().await;
    let state = test_state(Config::default(), Some(storage));
    let (status, _, _) =
        authed_bytes(app(state), "GET", "/admin/events/recent?limit=abc", None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn events_recent_filters_by_upstream_id() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    let other_upstream_id = Uuid::from_u128(2);

    storage
        .append_request_event(&request_event(
            clock.as_ref(),
            1,
            "req-target",
            upstream_id,
            "target-upstream",
        ))
        .await
        .unwrap();
    storage
        .append_request_event(&request_event(
            clock.as_ref(),
            2,
            "req-other",
            other_upstream_id,
            "other-upstream",
        ))
        .await
        .unwrap();

    let state = test_state_with_clock(Config::default(), Some(storage), clock);
    let (status, _, body, _) = authed_json(
        app(state),
        "GET",
        &format!("/admin/events/recent?upstream_id={upstream_id}"),
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    let events = body["events"].as_array().expect("events array");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["request_id"], "req-target");
    assert_eq!(events[0]["upstream_id"], upstream_id.to_string());
    assert_eq!(body["count"], 1);
}

#[tokio::test]
async fn events_recent_serializes_request_timings_without_losing_zero_or_fraction() {
    let (_dir, storage) = temp_storage().await;
    let event = RequestEvent {
        ts: TEST_NOW_UNIX_SECS,
        ts_ms: Some(TEST_NOW_UNIX_SECS * 1_000),
        request_id: "req-setup-timings".to_owned(),
        event_id: Some("event-setup-timings".to_owned()),
        status: 200,
        duration_ms: 10,
        json_parse_ms: Some(0.125),
        cache_structure_ms: Some(0.0),
        cache_token_key_ms: Some(0.25),
        cache_count_lookup_ms: Some(0.5),
        cache_tokenizer_queue_ms: Some(0.75),
        cache_serialize_ms: Some(1.0),
        cache_tokenize_ms: Some(0.0),
        prepare_signer_ms: Some(2.0),
        request_body_first_chunk_ms: Some(0.125),
        request_body_receive_ms: None,
        request_body_wait_ms: Some(0.0),
        request_body_process_ms: Some(0.25),
        request_body_chunk_count: Some(0),
        response_body_wait_ms: Some(0.5),
        response_body_process_ms: Some(0.0),
        response_body_downstream_poll_gap_ms: Some(0.75),
        retry_overhead_ms: Some(1.25),
        ..RequestEvent::default()
    };
    storage.append_request_event(&event).await.unwrap();
    let state = test_state(Config::default(), Some(storage));
    let admin = app(state);

    let (status, _, body, _) =
        authed_json(admin.clone(), "GET", "/admin/events/recent?limit=1", None).await;

    assert_eq!(status, StatusCode::OK);
    let row = &body["events"][0];
    assert_eq!(row["json_parse_ms"], 0.125);
    assert_eq!(row["cache_structure_ms"], 0.0);
    assert_eq!(row["cache_token_key_ms"], 0.25);
    assert_eq!(row["cache_count_lookup_ms"], 0.5);
    assert_eq!(row["cache_tokenizer_queue_ms"], 0.75);
    assert_eq!(row["cache_serialize_ms"], 1.0);
    assert_eq!(row["cache_tokenize_ms"], 0.0);
    assert_eq!(row["prepare_signer_ms"], 2.0);
    assert_eq!(row["request_body_first_chunk_ms"], 0.125);
    assert!(row["request_body_receive_ms"].is_null());
    assert_eq!(row["request_body_wait_ms"], 0.0);
    assert_eq!(row["request_body_process_ms"], 0.25);
    assert_eq!(row["request_body_chunk_count"], 0);
    assert_eq!(row["response_body_wait_ms"], 0.5);
    assert_eq!(row["response_body_process_ms"], 0.0);
    assert_eq!(row["response_body_downstream_poll_gap_ms"], 0.75);
    assert_eq!(row["retry_overhead_ms"], 1.25);

    let (status, _, detail, _) = authed_json(
        admin,
        "GET",
        "/admin/v1/events/detail/event-setup-timings",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["json_parse_ms"], 0.125);
    assert_eq!(detail["cache_structure_ms"], 0.0);
    assert_eq!(detail["cache_token_key_ms"], 0.25);
    assert_eq!(detail["cache_count_lookup_ms"], 0.5);
    assert_eq!(detail["cache_tokenizer_queue_ms"], 0.75);
    assert_eq!(detail["cache_serialize_ms"], 1.0);
    assert_eq!(detail["cache_tokenize_ms"], 0.0);
    assert_eq!(detail["prepare_signer_ms"], 2.0);
    assert_eq!(detail["request_body_first_chunk_ms"], 0.125);
    assert!(detail["request_body_receive_ms"].is_null());
    assert_eq!(detail["request_body_wait_ms"], 0.0);
    assert_eq!(detail["request_body_process_ms"], 0.25);
    assert_eq!(detail["request_body_chunk_count"], 0);
    assert_eq!(detail["response_body_wait_ms"], 0.5);
    assert_eq!(detail["response_body_process_ms"], 0.0);
    assert_eq!(detail["response_body_downstream_poll_gap_ms"], 0.75);
    assert_eq!(detail["retry_overhead_ms"], 1.25);
}

#[tokio::test]
async fn events_recent_uses_compound_cursor_for_same_timestamp_pages() {
    let (_dir, storage) = temp_storage().await;
    let ts_ms = 1_800_000_000_000;
    let upstream_id = Uuid::from_u128(1);
    storage
        .append_request_event(&request_event_with_cursor(
            ts_ms,
            "event-b",
            "req-newer",
            upstream_id,
        ))
        .await
        .unwrap();
    storage
        .append_request_event(&request_event_with_cursor(
            ts_ms,
            "event-a",
            "req-older",
            upstream_id,
        ))
        .await
        .unwrap();
    let state = test_state(Config::default(), Some(storage));

    let (status, _, first_page, _) = authed_json(
        app(state.clone()),
        "GET",
        "/admin/events/recent?limit=1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_page["events"][0]["request_id"], "req-newer");

    let (status, _, second_page, _) = authed_json(
        app(state),
        "GET",
        "/admin/events/recent?limit=1&until_ts_ms=1800000000000&until_event_id=event-b",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(second_page["events"].as_array().unwrap().len(), 1);
    assert_eq!(second_page["events"][0]["request_id"], "req-older");
}

#[tokio::test]
async fn events_recent_cursor_paginates_past_five_hundred_rows() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    for index in 0..675 {
        storage
            .append_request_event(&request_event(
                clock.as_ref(),
                index,
                &format!("req-deep-{index:03}"),
                upstream_id,
                "target-upstream",
            ))
            .await
            .unwrap();
    }
    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage),
        clock,
    ));

    let mut cursor: Option<(u64, String)> = None;
    let mut request_ids = std::collections::HashSet::new();
    let mut page_sizes = Vec::new();
    loop {
        let uri = match cursor.as_ref() {
            Some((ts_ms, event_id)) => format!(
                "/admin/events/recent?limit=200&source_kind=all&until_ts_ms={ts_ms}&until_event_id={event_id}"
            ),
            None => "/admin/events/recent?limit=200&source_kind=all".to_owned(),
        };
        let (status, _, body, _) = authed_json(admin_app.clone(), "GET", &uri, None).await;
        assert_eq!(status, StatusCode::OK);
        let events = body["events"].as_array().unwrap();
        page_sizes.push(events.len());
        for event in events {
            assert!(
                request_ids.insert(event["request_id"].as_str().unwrap().to_owned()),
                "duplicate request across cursor pages"
            );
        }
        if events.len() < 200 {
            break;
        }
        let oldest = events.last().unwrap();
        cursor = Some((
            oldest["ts_ms"].as_u64().unwrap(),
            oldest["event_id"].as_str().unwrap().to_owned(),
        ));
    }

    assert_eq!(page_sizes, [200, 200, 200, 75]);
    assert_eq!(request_ids.len(), 675);
}

#[tokio::test]
async fn events_recent_applies_each_filter_and_their_combination() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);
    let other_upstream_id = Uuid::from_u128(2);

    let mut events = Vec::new();
    for (index, request_id) in [
        "target",
        "wrong-principal",
        "wrong-session",
        "wrong-model",
        "wrong-upstream",
        "wrong-status",
        "wrong-source",
    ]
    .into_iter()
    .enumerate()
    {
        let mut event = request_event(
            clock.as_ref(),
            index as u64,
            request_id,
            upstream_id,
            "target-upstream",
        );
        event.principal_id = Some("principal-target".to_owned());
        event.thread_id = Some("thread-target".to_owned());
        event.model = Some("model-target".to_owned());
        event.status = 429;
        event.source_kind = Some("renewal".to_owned());
        match request_id {
            "wrong-principal" => event.principal_id = Some("principal-other".to_owned()),
            "wrong-session" => event.thread_id = Some("thread-other".to_owned()),
            "wrong-model" => event.model = Some("other-model".to_owned()),
            "wrong-upstream" => {
                event.upstream_id = Some(other_upstream_id);
                event.upstream_name = Some("other-upstream".to_owned());
            }
            "wrong-status" => event.status = 200,
            "wrong-source" => event.source_kind = Some("request".to_owned()),
            _ => {}
        }
        events.push(event);
    }
    for event in &events {
        storage.append_request_event(event).await.unwrap();
    }
    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage),
        clock,
    ));

    for (query, excluded_request_id) in [
        (
            "principal_id=principal-target&source_kind=all".to_owned(),
            "wrong-principal",
        ),
        (
            "thread_id=thread-target&source_kind=all".to_owned(),
            "wrong-session",
        ),
        (
            "model=model-target&source_kind=all".to_owned(),
            "wrong-model",
        ),
        ("model=model-tar&source_kind=all".to_owned(), "wrong-model"),
        (
            "model=MODEL-Target&source_kind=all".to_owned(),
            "wrong-model",
        ),
        (
            format!("upstream_id={upstream_id}&source_kind=all"),
            "wrong-upstream",
        ),
        (
            "status_class=4xx&source_kind=all".to_owned(),
            "wrong-status",
        ),
        ("source_kind=renewal".to_owned(), "wrong-source"),
    ] {
        let (status, _, body, _) = authed_json(
            admin_app.clone(),
            "GET",
            &format!("/admin/events/recent?limit=20&{query}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let request_ids = body["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["request_id"].as_str())
            .collect::<Vec<_>>();
        assert!(request_ids.contains(&"target"), "{query}");
        assert!(!request_ids.contains(&excluded_request_id), "{query}");
    }

    let (status, _, body, _) = authed_json(
        admin_app,
        "GET",
        &format!(
            "/admin/events/recent?limit=20&principal_id=principal-target&thread_id=thread-target&model=model-target&upstream_id={upstream_id}&status_class=4xx&source_kind=renewal"
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let request_ids = body["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["request_id"].as_str())
        .collect::<Vec<_>>();
    assert_eq!(request_ids, ["target"]);
}

#[tokio::test]
async fn events_recent_rejects_unknown_event_kind() {
    let (_dir, storage) = temp_storage().await;
    let admin_app = app(test_state(Config::default(), Some(storage)));

    for query in ["event_kind=bogus", "event_kind=", "event_kind=MESSAGES"] {
        let (status, _, body, _) = authed_json(
            admin_app.clone(),
            "GET",
            &format!("/admin/events/recent?{query}"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(body["error"], "invalid_event_kind", "{query}");
    }
}

#[tokio::test]
async fn events_recent_filters_by_event_kind() {
    let clock = test_clock();
    let (_dir, storage) = temp_storage_with_clock(clock.clone()).await;
    let upstream_id = Uuid::from_u128(1);

    let mut messages = request_event(clock.as_ref(), 0, "req-messages", upstream_id, "up");
    messages.event_kind = Some(RequestEventKind::Messages);
    let mut renewal = request_event(clock.as_ref(), 1, "req-renewal", upstream_id, "up");
    renewal.source_kind = Some("renewal".to_owned());
    let unclassified = request_event(clock.as_ref(), 2, "req-unclassified", upstream_id, "up");
    for event in [&messages, &renewal, &unclassified] {
        storage.append_request_event(event).await.unwrap();
    }
    let admin_app = app(test_state_with_clock(
        Config::default(),
        Some(storage),
        clock,
    ));

    let request_ids = |body: &serde_json::Value| -> Vec<String> {
        body["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|event| event["request_id"].as_str().map(str::to_owned))
            .collect()
    };

    for (query, expected) in [
        ("event_kind=messages", vec!["req-messages"]),
        ("event_kind=renewal", vec!["req-renewal"]),
        ("event_kind=unclassified", vec!["req-unclassified"]),
        ("event_kind=other", vec![]),
        // An explicit event_kind bypasses only the implicit renewal
        // exclusion; a supplied source_kind stays conjunctive.
        ("event_kind=messages&source_kind=renewal", vec![]),
        (
            "event_kind=renewal&source_kind=renewal",
            vec!["req-renewal"],
        ),
        // No event_kind preserves the legacy default: renewals excluded.
        ("", vec!["req-unclassified", "req-messages"]),
    ] {
        let uri = if query.is_empty() {
            "/admin/events/recent?limit=20".to_owned()
        } else {
            format!("/admin/events/recent?limit=20&{query}")
        };
        let (status, _, body, _) = authed_json(admin_app.clone(), "GET", &uri, None).await;
        assert_eq!(status, StatusCode::OK, "{query}");
        assert_eq!(request_ids(&body), expected, "{query}");
    }

    let (status, _, body, _) = authed_json(
        admin_app,
        "GET",
        "/admin/events/recent?limit=20&event_kind=messages",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["events"][0]["event_kind"], "messages");
}

#[tokio::test]
async fn events_recent_503_when_storage_missing() {
    let state = config_admin_common::test_state_without_storage();
    let (status, _, _) = authed_bytes(app(state), "GET", "/admin/events/recent", None).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

fn request_event(
    clock: &dyn cc_lb_clock::Clock,
    index: u64,
    request_id: &str,
    upstream_id: Uuid,
    upstream_name: &str,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some((current_unix_secs(clock).saturating_sub(60) + index) * 1000),
        request_id: request_id.to_owned(),
        principal_id: Some("principal-a".to_owned()),
        key_id: Some(format!("key-{index}")),
        upstream_id: Some(upstream_id),
        upstream_name: Some(upstream_name.to_owned()),
        event_id: Some(format!("event-{index:06}")),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(index),
        output_tokens: Some(0),
        duration_ms: 10,
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..Default::default()
    }
}

fn request_event_with_cursor(
    ts_ms: u64,
    event_id: &str,
    request_id: &str,
    upstream_id: Uuid,
) -> RequestEvent {
    RequestEvent {
        ts_ms: Some(ts_ms),
        request_id: request_id.to_owned(),
        event_id: Some(event_id.to_owned()),
        principal_id: Some("principal-a".to_owned()),
        upstream_id: Some(upstream_id),
        upstream_name: Some("target-upstream".to_owned()),
        model: Some("claude-sonnet-4-5".to_owned()),
        status: 200,
        input_tokens: Some(1),
        output_tokens: Some(0),
        duration_ms: 10,
        request_body_first_chunk_ms: None,
        request_body_receive_ms: None,
        request_body_wait_ms: None,
        request_body_process_ms: None,
        request_body_chunk_count: None,
        response_body_wait_ms: None,
        response_body_process_ms: None,
        response_body_downstream_poll_gap_ms: None,
        retry_overhead_ms: None,
        ..Default::default()
    }
}

fn test_clock() -> ClockHandle {
    Arc::new(TestClock::new_at_secs(TEST_NOW_UNIX_SECS))
}

fn current_unix_secs(clock: &dyn cc_lb_clock::Clock) -> u64 {
    cc_lb_clock::unix_secs(clock.now())
}
