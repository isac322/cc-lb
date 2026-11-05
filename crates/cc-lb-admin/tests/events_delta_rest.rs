use crate::config_admin_common;

use axum::{
    extract::Query,
    http::{StatusCode, Uri, header},
};
use cc_lb_config::Config;
use cc_lb_domain::{InternalError, RoutingTrace};
use cc_lb_request_log::{RequestCacheBreakpoint, RequestCacheBreakpointSource, RequestDiagnostics};
use cc_lb_storage_api::{AuditQueryScope, AuditStore, RequestEvent, RequestEventStore};
use config_admin_common::{app, authed_bytes, authed_json, temp_storage, test_state};
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
        let (status, _, detail, _) =
            authed_json(app(state.clone()), "GET", &event_detail_uri(event_id), None).await;
        assert_eq!(status, StatusCode::OK);
        details.push(detail);
    }
    assert_same_row_without_diagnostics(&details[0], &details[1]);
}

#[tokio::test]
async fn event_detail_query_transport_preserves_full_id_and_audit() {
    let (_dir, storage) = temp_storage().await;
    let event_id = "renewal:00000000-0000-0000-0000-000000000001/thread:thread%252F%41+&=?# 한글/agent:agent%2F:1";
    let mut event = with_diagnostics(request_event(1));
    event.event_id = Some(event_id.to_owned());
    event.principal_id = Some("00000000-0000-0000-0000-000000000001".to_owned());
    event.thread_id = Some("thread%252F%41+&=?# 한글".to_owned());
    event.claude_agent_id = Some("agent%2F".to_owned());
    event.source_ref_id = Some("turn%252F+&=?# 한글".to_owned());
    event.source_kind = Some("renewal".to_owned());
    event.status = 429;
    event.error_code = Some("upstream_4xx".to_owned());
    event.upstream_error_type = Some("rate_limit_error".to_owned());
    event.upstream_error_message = Some("renewal diagnostic".to_owned());
    storage.append_request_event(&event).await.unwrap();
    let admin = app(test_state(Config::default(), Some(storage.clone())));

    let (status, headers, bytes) =
        authed_bytes(admin.clone(), "GET", &event_detail_uri(event_id), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    let detail: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(detail["event_id"], event_id);
    assert_eq!(detail["request_id"], "req-1");
    assert_eq!(
        detail["principal_id"],
        event.principal_id.as_deref().unwrap()
    );
    assert_eq!(detail["thread_id"], event.thread_id.as_deref().unwrap());
    assert_eq!(detail["claude_agent_id"], "agent%2F");
    assert_eq!(detail["source_ref_id"], "turn%252F+&=?# 한글");
    assert_eq!(detail["source_kind"], "renewal");
    assert_eq!(detail["status"], 429);
    assert_eq!(detail["error_code"], "upstream_4xx");
    assert_eq!(detail["upstream_error_type"], "rate_limit_error");
    assert_eq!(detail["upstream_error_message"], "renewal diagnostic");
    assert_eq!(detail["json_parse_ms"], 0.125);
    assert_eq!(detail["cache_tokenize_ms"], 0.0);
    assert_eq!(detail["prepare_signer_ms"], 1.25);
    assert_eq!(detail["duration_ms"], 10);
    for key in [
        "cache_breakpoints",
        "cache_control_message_indices",
        "routing_trace",
        "iterations",
        "internal_errors",
    ] {
        assert!(detail.get(key).is_none(), "stored detail exposes {key}");
    }
    assert!(detail.get("events").is_none());
    let persisted = storage.get_request_event(event_id).await.unwrap().unwrap();
    assert_eq!(persisted.event_id.as_deref(), Some(event_id));
    assert_eq!(persisted.thread_id, event.thread_id);
    assert_eq!(persisted.source_ref_id, event.source_ref_id);

    let entries = storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 100, false)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    let entry = &entries[0];
    assert_eq!(
        entry.admin_action.as_deref(),
        Some("request_event_detail_read")
    );
    assert_eq!(
        entry.actor.as_deref(),
        Some("static-token/test-static-token")
    );
    assert_eq!(entry.actor_authority.as_deref(), Some("static-token"));
    assert_eq!(entry.actor_subject.as_deref(), Some("test-static-token"));
    assert_eq!(entry.actor_kind.as_deref(), Some("break_glass"));
    assert_eq!(entry.status, StatusCode::OK.as_u16());
    assert_eq!(entry.route, "/admin/v1/events/detail");
    assert_eq!(entry.principal_id, "");
    assert_eq!(entry.upstream, "admin");
    assert!(entry.api_key_id.is_none());
    assert_eq!(entry.payload.as_ref().unwrap()["event_id"], event_id);

    let (status, headers, bytes) =
        authed_bytes(admin, "GET", &event_detail_uri("unknown/renewal%2F"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    let missing: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(missing, serde_json::json!({ "error": "not_found" }));
    assert_eq!(
        storage
            .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 100, false)
            .await
            .unwrap(),
        entries,
        "an unknown ID must not append a detail-read audit"
    );
}

#[tokio::test]
async fn event_detail_query_transport_rejects_missing_or_duplicate_id() {
    let (_dir, storage) = temp_storage().await;
    let admin = app(test_state(Config::default(), Some(storage.clone())));

    for uri in [
        "/admin/v1/events/detail",
        "/admin/v1/events/detail?id=first&id=second",
    ] {
        let (status, headers, bytes) = authed_bytes(admin.clone(), "GET", uri, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "validation_failed");
        assert_eq!(body["field"], "id");
    }
    assert!(
        storage
            .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 100, false)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn event_detail_query_transport_preserves_malformed_percent_decoding() {
    let (_dir, storage) = temp_storage().await;
    let admin = app(test_state(Config::default(), Some(storage.clone())));

    for (index, suffix) in ["%", "%2", "%GG", "%FF"].into_iter().enumerate() {
        let uri = format!("/admin/v1/events/detail?id=malformed-{index}-{suffix}");
        let reference = Query::<DetailId>::try_from_uri(&uri.parse::<Uri>().unwrap())
            .expect("the installed Query<String> accepts malformed percent encoding");
        let mut event = request_event(index as u64 + 10);
        event.event_id = Some(reference.id.clone());
        storage.append_request_event(&event).await.unwrap();

        let (status, headers, bytes) = authed_bytes(admin.clone(), "GET", &uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        let detail: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(detail["event_id"], reference.id, "{uri}");
        assert_eq!(detail["request_id"], event.request_id, "{uri}");
    }
}

#[tokio::test]
async fn event_detail_query_transport_preserves_empty_extra_and_fallback_boundaries() {
    let (_dir, storage) = temp_storage().await;
    storage
        .append_request_event(&request_event(1))
        .await
        .unwrap();
    let admin = app(test_state(Config::default(), Some(storage.clone())));

    let (status, _, bytes) = authed_bytes(
        admin.clone(),
        "GET",
        "/admin/v1/events/detail?id=event-000001&limit=1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let detail: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(detail["event_id"], "event-000001");
    assert!(detail.get("events").is_none());

    let (status, headers, bytes) =
        authed_bytes(admin.clone(), "GET", "/admin/v1/events/detail?id=", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");
    let missing: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(missing, serde_json::json!({ "error": "not_found" }));

    for uri in [
        "/admin/v1/events/detail/event-000001",
        "/admin/v1/events/detail/renewal:principal/thread:thread-a:1",
        "/admin/v1/unknown-event-detail",
    ] {
        let (status, _, bytes) = authed_bytes(admin.clone(), "GET", uri, None).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert!(
            bytes.is_empty(),
            "unmatched route must retain its empty body: {uri}"
        );
    }
    let entries = storage
        .query_recent_audit(AuditQueryScope::All, 0, u64::MAX, 100, false)
        .await
        .unwrap();
    assert_eq!(
        entries.len(),
        1,
        "only the successful detail read is audited"
    );
}

#[derive(Debug, serde::Deserialize)]
struct DetailId {
    id: String,
}

fn event_detail_uri(event_id: &str) -> String {
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("id", event_id)
        .finish();
    format!("/admin/v1/events/detail?{query}")
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
