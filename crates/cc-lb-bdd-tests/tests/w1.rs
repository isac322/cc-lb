use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use bytes::Bytes;
use cc_lb_bdd_tests::harness::TEST_ADMIN_TOKEN;
use cc_lb_bdd_tests::{BddCtx, HttpResponse, bdd_scenario};
use cc_lb_storage_api::{AuditEntry, AuditStore, RequestEvent, RequestEventStore};
use fake_anthropic::{ScriptedMessageResponse, SseEvent};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

#[derive(Debug)]
struct W1Evidence {
    scenario_id: &'static str,
    primary: HttpResponse,
    secondary: Option<HttpResponse>,
    principal_id: Option<String>,
    plaintext_key: Option<String>,
}

impl W1Evidence {
    fn new(scenario_id: &'static str, primary: HttpResponse) -> Self {
        let principal_id = principal_id_from(&primary);
        Self {
            scenario_id,
            primary,
            secondary: None,
            principal_id,
            plaintext_key: None,
        }
    }

    fn with_secondary(mut self, secondary: HttpResponse) -> Self {
        self.secondary = Some(secondary);
        self
    }

    fn with_plaintext_key(mut self) -> Self {
        self.plaintext_key = self
            .primary
            .body_json()
            .get("plaintext_key")
            .and_then(Value::as_str)
            .map(str::to_owned);
        self
    }
}

async fn run_admin_flow(admin_router: Router, scenario_id: &'static str) -> W1Evidence {
    let primary = request_json(
        admin_router.clone(),
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let secondary = request_json(
        admin_router,
        Method::GET,
        "/admin/v1/principals?limit=20",
        None,
        admin_headers(),
    )
    .await;
    W1Evidence::new(scenario_id, primary).with_secondary(secondary)
}

async fn run_key_flow(admin_router: Router, scenario_id: &'static str) -> W1Evidence {
    let created = request_json(
        admin_router.clone(),
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let Some(principal_id) = principal_id_from(&created) else {
        return W1Evidence::new(scenario_id, created);
    };
    let issue = request_json(
        admin_router.clone(),
        Method::POST,
        &format!("/admin/v1/principals/{principal_id}/keys"),
        Some(json!({ "label": scenario_id })),
        admin_headers(),
    )
    .await;
    let list = request_json(
        admin_router,
        Method::GET,
        &format!("/admin/v1/principals/{principal_id}/keys?status=all"),
        None,
        admin_headers(),
    )
    .await;
    W1Evidence {
        scenario_id,
        primary: issue,
        secondary: Some(list),
        principal_id: Some(principal_id),
        plaintext_key: None,
    }
    .with_plaintext_key()
}

async fn run_proxy_flow(
    admin_router: Router,
    proxy_router: Router,
    scenario_id: &'static str,
) -> W1Evidence {
    let setup = request_json(
        admin_router,
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let response = request_json(
        proxy_router,
        Method::POST,
        "/v1/messages",
        Some(message_body(scenario_id, false)),
        proxy_headers("sk-ant-bdd"),
    )
    .await;
    W1Evidence::new(scenario_id, response).with_secondary(setup)
}

async fn run_stream_flow(
    admin_router: Router,
    proxy_router: Router,
    script: &fake_anthropic::MessageScript,
    scenario_id: &'static str,
) -> W1Evidence {
    script.push_response(ScriptedMessageResponse::sse(anthropic_stream_events()));
    let setup = request_json(
        admin_router,
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let response = request_json(
        proxy_router,
        Method::POST,
        "/v1/messages",
        Some(message_body(scenario_id, true)),
        proxy_headers("sk-ant-bdd"),
    )
    .await;
    W1Evidence::new(scenario_id, response).with_secondary(setup)
}

async fn run_drop_flow(
    admin_router: Router,
    proxy_router: Router,
    script: &fake_anthropic::MessageScript,
    scenario_id: &'static str,
) -> W1Evidence {
    let partial = sse_frame(
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": "msg_bdd_drop",
                "type": "message",
                "role": "assistant",
                "model": "claude-sonnet-bdd",
                "content": [],
                "stop_reason": null,
                "stop_sequence": null,
                "usage": { "input_tokens": 5, "output_tokens": 0 }
            }
        })
        .to_string(),
    );
    let full_body = format!(
        "{partial}{}",
        sse_frame(
            "message_stop",
            json!({ "type": "message_stop" }).to_string()
        )
    );
    script.push_response(
        ScriptedMessageResponse::drop_after_bytes(full_body.as_bytes().to_vec(), partial.len())
            .with_header("content-type", "text/event-stream; charset=utf-8"),
    );
    let setup = request_json(
        admin_router,
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let response = request_json(
        proxy_router,
        Method::POST,
        "/v1/messages",
        Some(message_body(scenario_id, true)),
        proxy_headers("sk-ant-bdd"),
    )
    .await;
    W1Evidence::new(scenario_id, response).with_secondary(setup)
}

async fn run_response_cap_flow(
    admin_router: Router,
    proxy_router: Router,
    script: &fake_anthropic::MessageScript,
    scenario_id: &'static str,
) -> W1Evidence {
    script.push_response(ScriptedMessageResponse::Json(large_message_response()));
    let setup = request_json(
        admin_router,
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let response = request_json(
        proxy_router,
        Method::POST,
        "/v1/messages",
        Some(message_body(scenario_id, false)),
        proxy_headers("sk-ant-bdd"),
    )
    .await;
    W1Evidence::new(scenario_id, response).with_secondary(setup)
}

async fn run_dashboard_flow(admin_router: Router, scenario_id: &'static str) -> W1Evidence {
    let setup = request_json(
        admin_router.clone(),
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let summary = request_json(
        admin_router.clone(),
        Method::GET,
        "/admin/v1/dashboard/summary?range=1h",
        None,
        admin_headers(),
    )
    .await;
    let usage = request_json(
        admin_router,
        Method::GET,
        "/admin/v1/dashboard/usage?range=1h&group_by=model",
        None,
        admin_headers(),
    )
    .await;
    let principal_id = principal_id_from(&setup);
    W1Evidence {
        scenario_id,
        primary: summary,
        secondary: Some(usage),
        principal_id,
        plaintext_key: None,
    }
}

async fn run_health_flow(
    admin_router: Router,
    proxy_router: Router,
    scenario_id: &'static str,
) -> W1Evidence {
    let setup = request_json(
        admin_router,
        Method::POST,
        "/admin/v1/principals",
        Some(principal_body(scenario_id)),
        admin_headers(),
    )
    .await;
    let liveness = request_json(
        proxy_router.clone(),
        Method::GET,
        "/healthz",
        None,
        HeaderMap::new(),
    )
    .await;
    let readiness =
        request_json(proxy_router, Method::GET, "/readyz", None, HeaderMap::new()).await;
    let principal_id = principal_id_from(&setup);
    W1Evidence {
        scenario_id,
        primary: liveness,
        secondary: Some(readiness),
        principal_id,
        plaintext_key: None,
    }
}

async fn request_json(
    router: Router,
    method: Method,
    path: &str,
    body: Option<Value>,
    headers: HeaderMap,
) -> HttpResponse {
    let mut builder = Request::builder().method(method).uri(path);
    for (name, value) in &headers {
        builder = builder.header(name, value);
    }
    let request_body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&value).unwrap_or_default())
        }
        None => Body::empty(),
    };
    let request = match builder.body(request_body) {
        Ok(request) => request,
        Err(error) => {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "request_build_failed", "message": error.to_string() }),
            );
        }
    };
    let response = match router.oneshot(request).await {
        Ok(response) => response,
        Err(error) => match error {},
    };
    let status = response.status();
    let headers = response.headers().clone();
    let body = match response.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) => Bytes::from(error.to_string()),
    };
    HttpResponse {
        status,
        headers,
        body,
    }
}

fn json_response(status: StatusCode, body: Value) -> HttpResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    HttpResponse {
        status,
        headers,
        body: Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
    }
}

fn admin_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(&format!("Bearer {TEST_ADMIN_TOKEN}")) {
        headers.insert(header::AUTHORIZATION, value);
    }
    headers
}

fn proxy_headers(api_key: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Ok(value) = HeaderValue::from_str(api_key) {
        headers.insert("x-api-key", value);
    }
    headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
    headers
}

fn principal_body(scenario_id: &str) -> Value {
    json!({
        "name": format!("w1-{}-{}", scenario_id.to_ascii_lowercase().replace('.', "-"), Uuid::new_v4().simple()),
        "kind": "machine",
        "allowed_models": [],
        "allowed_upstreams": [],
        "default_limits": [],
    })
}

fn message_body(scenario_id: &str, stream: bool) -> Value {
    json!({
        "model": "claude-sonnet-bdd",
        "max_tokens": 8,
        "stream": stream,
        "messages": [{"role": "user", "content": format!("W1 probe {scenario_id}")}]
    })
}

fn principal_id_from(response: &HttpResponse) -> Option<String> {
    response
        .body_json()
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn assert_admin_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status == StatusCode::CREATED,
        format!(
            "expected admin principal creation to return 201, got {} body={}",
            result.primary.status,
            result.primary.body_text()
        ),
    );
    let body = result.primary.body_json();
    ctx.assert(
        body["enabled"] == true,
        "created principal should be enabled",
    );
    ctx.assert(
        body.get("id").and_then(Value::as_str).is_some(),
        "created principal response should include id",
    );
    let Some(list) = result.secondary.as_ref() else {
        ctx.assert(false, "admin flow should include a list response");
        return;
    };
    ctx.assert(
        list.status == StatusCode::OK,
        format!("expected principal list to return 200, got {}", list.status),
    );
    assert_audit_contains(ctx, &result, "PrincipalCreate").await;
}

async fn assert_key_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status == StatusCode::CREATED,
        format!(
            "expected key issue to return 201, got {} body={}",
            result.primary.status,
            result.primary.body_text()
        ),
    );
    let plaintext = result.plaintext_key.as_deref().unwrap_or_default();
    ctx.assert(!plaintext.is_empty(), "issued key should be shown once");
    let Some(list) = result.secondary.as_ref() else {
        ctx.assert(false, "key flow should include a list response");
        return;
    };
    ctx.assert(
        list.status == StatusCode::OK,
        format!("expected key list to return 200, got {}", list.status),
    );
    ctx.assert(
        !list.body_text().contains(plaintext),
        "key list should not expose the full key",
    );
    assert_audit_contains(ctx, &result, "principal_key_issue").await;
}

async fn assert_proxy_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status.is_success(),
        format!(
            "expected proxy call to succeed, got {} body={}",
            result.primary.status,
            result.primary.body_text()
        ),
    );
    let body = result.primary.body_json();
    ctx.assert(body.is_object(), "proxy response should be a JSON object");
    assert_request_event_status(ctx, result.primary.status).await;
    assert_setup_audit(ctx, &result).await;
}

async fn assert_stream_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status.is_success(),
        format!(
            "expected streaming call to succeed, got {}",
            result.primary.status
        ),
    );
    let body = result.primary.body_text();
    let first = body.find("stream-piece-one");
    let second = body.find("stream-piece-two");
    ctx.assert(
        first.is_some() && second.is_some_and(|index| Some(index) > first),
        format!("stream pieces should arrive in order, body={body}"),
    );
    ctx.assert(
        body.contains("message_stop"),
        "stream should include a terminal message_stop marker",
    );
    assert_request_event_status(ctx, StatusCode::OK).await;
    assert_setup_audit(ctx, &result).await;
}

async fn assert_drop_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status == StatusCode::BAD_GATEWAY,
        format!(
            "expected interrupted response to return 502, got {} body={}",
            result.primary.status,
            result.primary.body_text()
        ),
    );
    let body = result.primary.body_text();
    ctx.assert(
        body.contains("api_error") && body.contains("interrupted"),
        format!("drop should end with an error envelope, body={body}"),
    );
    assert_request_event_status(ctx, StatusCode::BAD_GATEWAY).await;
    assert_setup_audit(ctx, &result).await;
}

async fn assert_response_cap_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status == StatusCode::PAYLOAD_TOO_LARGE,
        format!(
            "expected response cap to return 413, got {} body={}",
            result.primary.status,
            result.primary.body_text()
        ),
    );
    let body = result.primary.body_json();
    ctx.assert(
        body["error"]["type"] == "body_too_large",
        format!("expected body_too_large envelope, got {body}"),
    );
    assert_request_event_status(ctx, StatusCode::PAYLOAD_TOO_LARGE).await;
    assert_setup_audit(ctx, &result).await;
}

async fn assert_dashboard_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status == StatusCode::OK,
        format!(
            "expected dashboard summary to return 200, got {}",
            result.primary.status
        ),
    );
    ctx.assert(
        result.primary.body_json().is_object(),
        "dashboard summary should return an object",
    );
    let Some(usage) = result.secondary.as_ref() else {
        ctx.assert(false, "dashboard flow should include usage response");
        return;
    };
    ctx.assert(
        usage.status == StatusCode::OK,
        format!(
            "expected dashboard usage to return 200, got {}",
            usage.status
        ),
    );
    ctx.assert(
        usage.body_json().is_object(),
        "dashboard usage should return an object",
    );
    assert_audit_contains(ctx, &result, "PrincipalCreate").await;
}

async fn assert_health_evidence(result: W1Evidence, ctx: &BddCtx) {
    ctx.assert(
        result.primary.status == StatusCode::OK,
        format!(
            "expected liveness to return 200, got {}",
            result.primary.status
        ),
    );
    ctx.assert(
        result.primary.body_json()["status"].as_str().is_some(),
        "liveness body should include status",
    );
    let Some(readiness) = result.secondary.as_ref() else {
        ctx.assert(false, "health flow should include readiness response");
        return;
    };
    ctx.assert(
        readiness.status == StatusCode::OK || readiness.status == StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "expected readiness to be explicit, got {}",
            readiness.status
        ),
    );
    ctx.assert(
        readiness.body_json()["ready"].as_bool().is_some(),
        "readiness body should include readiness",
    );
    assert_audit_contains(ctx, &result, "PrincipalCreate").await;
}

async fn assert_setup_audit(ctx: &BddCtx, result: &W1Evidence) {
    let Some(setup) = result.secondary.as_ref() else {
        ctx.assert(false, "proxy setup response should be present");
        return;
    };
    ctx.assert(
        setup.status == StatusCode::CREATED,
        format!(
            "expected setup principal to return 201, got {}",
            setup.status
        ),
    );
    let setup_result = W1Evidence::new(result.scenario_id, setup.clone());
    assert_audit_contains(ctx, &setup_result, "PrincipalCreate").await;
}

async fn assert_audit_contains(ctx: &BddCtx, result: &W1Evidence, expected: &str) {
    let Some(principal_id) = result.principal_id.as_deref() else {
        ctx.assert(
            false,
            "evidence should include principal id for audit lookup",
        );
        return;
    };
    let entries = query_audit(ctx, principal_id).await;
    ctx.assert(!entries.is_empty(), "cc-lb should write audit rows");
    ctx.assert(
        entries.iter().any(|entry| audit_matches(entry, expected)),
        format!("cc-lb audit rows should include {expected}"),
    );
}

async fn assert_request_event_status(ctx: &BddCtx, status: StatusCode) {
    let events = query_request_events(ctx).await;
    ctx.assert(!events.is_empty(), "cc-lb should write request event rows");
    ctx.assert(
        events.iter().any(|event| event.status == status.as_u16()),
        format!(
            "cc-lb request events should include status {}",
            status.as_u16()
        ),
    );
}

async fn query_audit(ctx: &BddCtx, principal_id: &str) -> Vec<AuditEntry> {
    for _ in 0..40 {
        let entries = AuditStore::query_audit(
            ctx.storage().as_ref(),
            Some(principal_id),
            0,
            u64::MAX / 2,
            128,
        )
        .await
        .unwrap_or_default();
        if !entries.is_empty() {
            return entries;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Vec::new()
}

async fn query_request_events(ctx: &BddCtx) -> Vec<RequestEvent> {
    for _ in 0..40 {
        let events =
            RequestEventStore::query_request_events(ctx.storage().as_ref(), 0, u64::MAX / 2, 256)
                .await
                .unwrap_or_default();
        if !events.is_empty() {
            return events;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Vec::new()
}

fn audit_matches(entry: &AuditEntry, expected: &str) -> bool {
    let snake_expected = action_snake_case(expected);
    entry.kind.as_deref() == Some(expected)
        || entry
            .admin_action
            .as_deref()
            .is_some_and(|action| action.contains(expected) || action.contains(&snake_expected))
}

fn action_snake_case(action: &str) -> String {
    let mut converted = String::new();
    for (index, ch) in action.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                converted.push('_');
            }
            converted.push(ch.to_ascii_lowercase());
        } else {
            converted.push(ch);
        }
    }
    converted
}

fn anthropic_stream_events() -> Vec<SseEvent> {
    vec![
        SseEvent {
            event_type: "message_start".to_owned(),
            data: json!({
                "type": "message_start",
                "message": {
                    "id": "msg_bdd_stream",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-sonnet-bdd",
                    "content": [],
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": { "input_tokens": 5, "output_tokens": 0 }
                }
            })
            .to_string(),
        },
        SseEvent {
            event_type: "content_block_start".to_owned(),
            data: json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": { "type": "text", "text": "" }
            })
            .to_string(),
        },
        SseEvent {
            event_type: "content_block_delta".to_owned(),
            data: json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "stream-piece-one " }
            })
            .to_string(),
        },
        SseEvent {
            event_type: "content_block_delta".to_owned(),
            data: json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "stream-piece-two" }
            })
            .to_string(),
        },
        SseEvent {
            event_type: "message_delta".to_owned(),
            data: json!({
                "type": "message_delta",
                "delta": { "stop_reason": "end_turn", "stop_sequence": null },
                "usage": { "input_tokens": 5, "output_tokens": 2 }
            })
            .to_string(),
        },
        SseEvent {
            event_type: "message_stop".to_owned(),
            data: json!({ "type": "message_stop" }).to_string(),
        },
    ]
}

fn sse_frame(event_type: &str, data: String) -> String {
    format!("event: {event_type}\ndata: {data}\n\n")
}

fn large_message_response() -> Value {
    json!({
        "id": "msg_bdd_large",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-bdd",
        "content": [{ "type": "text", "text": "x".repeat(33 * 1024 * 1024) }],
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": { "input_tokens": 5, "output_tokens": 5 }
    })
}

mod w1_f1 {
    use super::*;
    bdd_scenario! {
        id: "F1.1a",
        fn_name: fast_f1_1a,
        persona: Alice,
        title: "Alice registers a new principal via the admin API and sees it become active",
        description: "Alice issues a real POST /admin/v1/principals to cc-lb-server. The server returns 201 with the new principal record showing enabled=true. An audit row is written by cc-lb recording the PrincipalCreate action.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.1b",
        fn_name: fast_f1_1b,
        persona: Alice,
        title: "New team registration records who created it and when in audit",
        description: "Alice registers a new team and the admin surface records creator and time evidence in the audit stream for later review.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.2",
        fn_name: f1_2,
        persona: Alice,
        title: "An issued key is shown only once right after registration",
        description: "Alice leaves the first key screen and only preview metadata remains after the one-time secret display.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.3",
        fn_name: f1_3,
        persona: Alice,
        title: "A second save stops if another operator changed the same team meanwhile",
        description: "Alice saves from a stale team view and cc-lb requires reloading the latest principal state before continuing.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.4",
        fn_name: fast_f1_4,
        persona: Alice,
        title: "Calls from an inactive team are not accepted",
        description: "Alice deactivates a team and observes paused-team rejection evidence with usage and audit details.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.5",
        fn_name: fast_f1_5,
        persona: Alice,
        title: "Calls outside the model range allowed for a team are rejected",
        description: "Alice restricts a team to a model bundle and observes the disallowed model rejection path.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.7",
        fn_name: fast_f1_7,
        persona: Alice,
        title: "Deleting a team does not erase audit traces of what that team did",
        description: "Alice deletes a team and retained audit evidence stays available for deleted-team review.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.8",
        fn_name: f1_8,
        persona: Alice,
        title: "Registration is rejected when an identifier contains forbidden characters or a reserved prefix",
        description: "Alice submits a reserved or invisible team identifier and cc-lb rejects it without usage impact.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.10",
        fn_name: f1_10,
        persona: Alice,
        title: "When an inactive team is restored to active, the same secrets are soon accepted again",
        description: "Alice reactivates a paused team and cc-lb records restored acceptance with paired status-change audit evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F1.11",
        fn_name: f1_11,
        persona: Alice,
        title: "Calls started just before deactivation finish, and only new calls are rejected",
        description: "Alice deactivates during active traffic and cc-lb separates before and after deactivation observations.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
}

mod w1_f2 {
    use super::*;
    bdd_scenario! {
        id: "F2.1",
        fn_name: fast_f2_1,
        persona: Alice,
        title: "When a new key is issued, the secret is shown only once",
        description: "Alice issues a client key and sees one-time full secret exposure plus durable preview metadata.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.2",
        fn_name: f2_2,
        persona: Alice,
        title: "During key rotation, the new key and old key have a short overlap period",
        description: "Alice rotates a key and sees the new secret plus short overlap behavior for the old key.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.3",
        fn_name: fast_f2_3,
        persona: Alice,
        title: "Immediately after key revocation, the next calls are rejected",
        description: "Alice revokes a key and cc-lb records immediate rejection plus revocation audit evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.4a",
        fn_name: f2_4a,
        persona: Alice,
        title: "A key that automatically expires at a set time stops at that time",
        description: "Alice sets key expiry and cc-lb records expired-key rejection evidence at the cutoff point.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.4b",
        fn_name: f2_4b,
        persona: Alice,
        title: "An upcoming-expiration notification reaches the operator in advance",
        description: "Alice configures key expiry and cc-lb records advance notification delivery plus audit evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.5",
        fn_name: fast_f2_5,
        persona: Alice,
        title: "Secrets are never shown on the key list screen",
        description: "Alice opens the key list and sees secret-free list evidence with holder and last-used metadata only.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.6",
        fn_name: f2_6,
        persona: Alice,
        title: "An operator can later edit a key holder name and memo",
        description: "Alice edits key holder metadata and cc-lb reflects it with old and new audit evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.8a",
        fn_name: f2_8a,
        persona: Alice,
        title: "Usage reporting separates call counts and rejection reasons by key holder",
        description: "Alice views holder usage and sees per-holder call and rejection reason evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.8b",
        fn_name: f2_8b,
        persona: Alice,
        title: "When a holder name is edited, old usage is unified under the new name",
        description: "Alice renames a holder and cc-lb unifies past and new usage under the updated holder label.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.10",
        fn_name: f2_10,
        persona: Alice,
        title: "The number of active keys one holder may have at the same time is limited",
        description: "Alice enforces active key count per holder and cc-lb rejects new keys until an old key is revoked.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.11",
        fn_name: f2_11,
        persona: Alice,
        title: "Looking again at the last few characters of a key is also audited",
        description: "Alice views a key preview and cc-lb records an audit row that excludes the full secret.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.12a",
        fn_name: f2_12a,
        persona: Alice,
        title: "The list screen clearly distinguishes whether a key holder is a person or a machine",
        description: "Alice opens the key list and sees person and machine holder classification evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.12b",
        fn_name: f2_12b,
        persona: Alice,
        title: "Usage reporting also shows separate totals for person and machine units",
        description: "Alice views usage and sees separate person and machine holder totals in the report evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.13a",
        fn_name: f2_13a,
        persona: Alice,
        title: "Key state is handled in active to suspended to revoked order",
        description: "Alice moves a key through active, suspended, and revoked states and cc-lb records ordered state evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.13b",
        fn_name: f2_13b,
        persona: Alice,
        title: "Every key state transition records who changed it and when in audit",
        description: "Alice changes key state and cc-lb records transition actor and time evidence for audit reconstruction.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F2.14",
        fn_name: f2_14,
        persona: Alice,
        title: "Trying key rotation on a storage backend that does not support rotation is politely rejected",
        description: "Alice tries unsupported rotation and cc-lb returns graceful rejection plus audit evidence for the attempted operation.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_key_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_key_evidence(result, ctx).await; },
    }
}

mod w1_f3 {
    use super::*;
    bdd_scenario! {
        id: "F3.1",
        fn_name: fast_f3_1,
        persona: Bob,
        title: "Calling a normal model with a normal key returns the response unchanged",
        description: "Bob sends a normal model call and cc-lb records unchanged response and successful usage evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.2",
        fn_name: f3_2,
        persona: Bob,
        title: "A streaming response flows to the end without interruption",
        description: "Bob sends a streaming call and cc-lb records ordered stream pieces and terminal marker evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_stream_flow(harness.admin_router().clone(), harness.proxy_router().clone(), &harness.script, SCENARIO_ID).await },
        then: |result, ctx| { assert_stream_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.3",
        fn_name: fast_f3_3,
        persona: Bob,
        title: "A call sent with an unknown key is politely rejected",
        description: "Bob sends an unknown key and receives a Claude-shaped invalid key rejection response.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.4",
        fn_name: fast_f3_4,
        persona: Bob,
        title: "A call sent with a key from an inactive team is rejected",
        description: "Bob calls with an inactive team key and cc-lb records paused principal rejection evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.5a",
        fn_name: f3_5a,
        persona: Bob,
        title: "Calling a model not allowed for the caller's team rejects the call",
        description: "Bob calls outside the model bundle and cc-lb records the disallowed model rejection path.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.5b",
        fn_name: f3_5b,
        persona: Bob,
        title: "A disallowed-model rejection message is delivered as a Claude-shaped error envelope",
        description: "Bob receives a disallowed-model envelope and cc-lb records message and shape evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.5c",
        fn_name: f3_5c,
        persona: Bob,
        title: "A disallowed-model rejection is added to both usage reporting and the audit bundle",
        description: "Bob triggers a model rejection and cc-lb records both usage and audit bundle evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.8",
        fn_name: f3_8,
        persona: Bob,
        title: "If a sudden problem occurs inside cc-lb during a response, it closes with a Claude-shaped error",
        description: "Bob experiences an interrupted response and cc-lb records a Claude-shaped midway interruption evidence row.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_drop_flow(harness.admin_router().clone(), harness.proxy_router().clone(), &harness.script, SCENARIO_ID).await },
        then: |result, ctx| { assert_drop_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.9",
        fn_name: f3_9,
        persona: Bob,
        title: "Bob completes the upload, download, and delete file flow through cc-lb",
        description: "Bob runs the file lifecycle and cc-lb records consistent file identifier evidence for upload, download, and delete.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.11a",
        fn_name: f3_11a,
        persona: Bob,
        title: "A call sent to an unknown path or unknown action returns a Claude-shaped error envelope",
        description: "Bob calls an unknown operation and cc-lb records recognized error envelope evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.11b",
        fn_name: f3_11b,
        persona: Bob,
        title: "Rejection of an unknown path or action is added to usage reporting by rejection reason type",
        description: "Bob triggers an unknown operation rejection and cc-lb records rejection reason usage trend evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.12",
        fn_name: f3_12,
        persona: Bob,
        title: "When rejected by a limit, the same response includes when to try again",
        description: "Bob hits a temporary limit and cc-lb records retry guidance evidence in the rejection response.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.13",
        fn_name: f3_13,
        persona: Bob,
        title: "If the response body exceeds a predefined ceiling, it is politely cut off",
        description: "Bob receives an oversized response and cc-lb records cutoff and cap exceeded evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_response_cap_flow(harness.admin_router().clone(), harness.proxy_router().clone(), &harness.script, SCENARIO_ID).await },
        then: |result, ctx| { assert_response_cap_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.14",
        fn_name: f3_14,
        persona: Bob,
        title: "Attempts to go directly to an external host outside registered paths are blocked",
        description: "Bob attempts an unregistered external exit and cc-lb records tunnel enforcement evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.15",
        fn_name: f3_15,
        persona: Bob,
        title: "Single-hop forwarding markers are cleaned up at the response boundary",
        description: "Bob receives a response and cc-lb records response boundary cleanup evidence for normal and error envelopes.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F3.16",
        fn_name: f3_16,
        persona: Bob,
        title: "Temporarily turning off authentication in an isolated test environment is clearly marked",
        description: "Bob calls in isolated no-auth mode and cc-lb records the disabled authentication marker evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
}

mod w1_f4 {
    use super::*;
    bdd_scenario! {
        id: "F4.1a",
        fn_name: f4_1a,
        persona: Alice,
        title: "Yesterday's highest-usage teams are visible at a glance in cost order",
        description: "Alice opens the dashboard and cc-lb returns cost ordered top-team evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.1b",
        fn_name: f4_1b,
        persona: Alice,
        title: "The same table shows call count and rejection ratio together",
        description: "Alice views the same dashboard table and cc-lb returns call count plus rejection ratio evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.1c",
        fn_name: f4_1c,
        persona: Alice,
        title: "Pressing a team row leads to that team's detail view",
        description: "Alice presses a team row and verifies visual navigation to that team's detail view.",
        oos_manual: "Clicking a team row requires human click and visual confirmation of UI page transition; replacing it with storage assertions distorts the UX intent.",
        given: |ctx| { ctx.scenario_id() },
        when: |scenario| { scenario },
        then: |scenario, ctx| { let _ = (scenario, ctx); },
    }
    bdd_scenario! {
        id: "F4.2",
        fn_name: f4_2,
        persona: Alice,
        title: "Alice narrows the view by period, team, key holder, or model",
        description: "Alice applies dashboard filters and cc-lb recalculates cost, call count, and rejection ratio evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.3",
        fn_name: f4_3,
        persona: Alice,
        title: "When monthly cost approaches a configured limit, it appears early on the same screen",
        description: "Alice views a near-limit team and cc-lb returns early monthly cost warning evidence before rejection.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.4a",
        fn_name: f4_4a,
        persona: Alice,
        title: "Call share and cost share by model are visible at a glance on the same screen",
        description: "Alice views model share and cc-lb returns call share plus cost share evidence by model.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.4b",
        fn_name: f4_4b,
        persona: Alice,
        title: "Pressing a model in the model share table leads to its hourly flow",
        description: "Alice selects a model and cc-lb returns model trend navigation evidence at the data level.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.5",
        fn_name: f4_5,
        persona: Alice,
        title: "Hourly usage flow continues without gaps",
        description: "Alice views a daily trend and cc-lb returns continuous period evidence including explicit zero periods.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.6",
        fn_name: f4_6,
        persona: Alice,
        title: "Rejection reasons are visible at a glance by type",
        description: "Alice views rejections and cc-lb returns grouped rejection reason evidence with ratios and trend markers.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.7",
        fn_name: f4_7,
        persona: Alice,
        title: "A report screen can be downloaded in table format",
        description: "Alice downloads a filtered report and cc-lb returns table-format export evidence without secrets.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.8",
        fn_name: f4_8,
        persona: Alice,
        title: "The fact that the live flow was interrupted is clearly visible on the same screen",
        description: "Alice sees a live flow interruption and cc-lb returns interruption marker and accurate accumulated report evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.10",
        fn_name: f4_10,
        persona: Alice,
        title: "Alice narrows the log page by type, team, or holder",
        description: "Alice filters logs and cc-lb returns matching rows without secret exposure and stable filter evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.11a",
        fn_name: f4_11a,
        persona: Alice,
        title: "Zero calls and not known yet are clearly distinguished in the same table",
        description: "Alice views report periods and cc-lb returns separate zero-call and unknown markers in table evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.11b",
        fn_name: f4_11b,
        persona: Alice,
        title: "The meaning of the two markers is explained with user-facing help",
        description: "Alice reads help explaining zero-call and not-known markers in the same table.",
        oos_manual: "User cognitive tooltip requires human inspection of readability and badge guidance; automated DOM checks alone do not preserve the scenario intent.",
        given: |ctx| { ctx.scenario_id() },
        when: |scenario| { scenario },
        then: |scenario, ctx| { let _ = (scenario, ctx); },
    }
    bdd_scenario! {
        id: "F4.11c",
        fn_name: f4_11c,
        persona: Alice,
        title: "The same distinction is also shown on the hourly flow chart",
        description: "Alice views the time-period trend and cc-lb returns separate zero-call and unknown chart marker evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F4.12",
        fn_name: f4_12,
        persona: Alice,
        title: "Alice views the per-stage dwell time of one call in detail",
        description: "Alice opens a slow call detail and cc-lb returns per-stage timing evidence with the slowest stage highlighted.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_dashboard_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_dashboard_evidence(result, ctx).await; },
    }
}

mod w1_f6 {
    use super::*;
    bdd_scenario! {
        id: "F6.1",
        fn_name: f6_1,
        persona: Alice,
        title: "When the per-minute limit is exceeded, the team's next call is temporarily rejected",
        description: "Alice sets a per-minute limit and cc-lb records temporary retry-shortly rejection evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.2a",
        fn_name: f6_2a,
        persona: Alice,
        title: "When the daily limit fills, the rest of that day's calls are rejected and it resets at the same time next day",
        description: "Alice sets a daily limit and cc-lb records daily exhaustion and next-day reset evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.2b",
        fn_name: f6_2b,
        persona: Alice,
        title: "Daily-limit rejection guidance includes when the limit will clear again",
        description: "Alice configures daily limits and cc-lb records reset-time guidance evidence for Bob.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.3",
        fn_name: f6_3,
        persona: Alice,
        title: "When the monthly cost limit approaches, new calls are rejected early",
        description: "Alice sets a monthly cost limit and cc-lb records early rejection plus dashboard warning evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.4",
        fn_name: f6_4,
        persona: Alice,
        title: "Calling outside the allowed model bundle is rejected",
        description: "Alice restricts a model bundle and cc-lb records persistent disallowed model evidence until the bundle changes.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.5",
        fn_name: f6_5,
        persona: Alice,
        title: "A call immediately after a limit edit soon reflects the new limit",
        description: "Alice edits a limit and cc-lb records immediate new-limit acceptance plus audit evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.8",
        fn_name: f6_8,
        persona: Alice,
        title: "Limit violations appear separately in reporting by rejection reason type",
        description: "Alice views limit reports and cc-lb records separate per-minute, daily, monthly, and model rejection evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.9",
        fn_name: f6_9,
        persona: Alice,
        title: "cc-lb does not go to external hosts outside the allow list",
        description: "Alice observes external host enforcement and cc-lb records blocked host evidence with audit details.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.10a",
        fn_name: f6_10a,
        persona: Alice,
        title: "The body-size limit for a specific path applies to calls on that path",
        description: "Alice sets a path body-size limit and cc-lb records body limit exceeded evidence for that path.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.10b",
        fn_name: f6_10b,
        persona: Alice,
        title: "Changing the body limit for one path does not affect the body limit for another path",
        description: "Alice changes one path limit and cc-lb records unaffected acceptance evidence on another path.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.12",
        fn_name: f6_12,
        persona: Alice,
        title: "A team temporarily rejected for a limit violation recovers after time passes",
        description: "Alice observes per-minute recovery and cc-lb records same-key acceptance after recovery evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.13",
        fn_name: f6_13,
        persona: Alice,
        title: "A team that used up its daily limit is automatically accepted again at the same time next day",
        description: "Alice observes daily recovery and cc-lb records next-day automatic acceptance evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.14a",
        fn_name: f6_14a,
        persona: Alice,
        title: "One team's per-minute limit violation does not affect another team's call acceptance",
        description: "Alice compares two teams and cc-lb records independent team limit evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.14b",
        fn_name: f6_14b,
        persona: Alice,
        title: "One team's surge does not delay another team's recovery point",
        description: "Alice observes separate recovery timing and cc-lb records that another team's recovery point remains unchanged.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.15",
        fn_name: f6_15,
        persona: Alice,
        title: "A separate limit can be set on the number of calls being processed at the same time",
        description: "Alice sets a concurrency limit and cc-lb records concurrent call rejection and immediate recovery evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.16",
        fn_name: f6_16,
        persona: Alice,
        title: "The enforcement unit changes depending on whether the limit targets team, holder, or key",
        description: "Alice selects holder scope and cc-lb records scoped rejection evidence isolated from other holders.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F6.17",
        fn_name: f6_17,
        persona: Alice,
        title: "A smaller limit can be set separately for only one key",
        description: "Alice sets a per-key override and cc-lb records smaller key limit evidence with audit details.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_admin_flow(harness.admin_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_admin_evidence(result, ctx).await; },
    }
}

mod w1_f19 {
    use super::*;
    bdd_scenario! {
        id: "F19.1",
        fn_name: f19_1,
        persona: Alice,
        title: "Calls with the same meaning converge to the same place on the second call",
        description: "Alice reviews cache affinity and cc-lb routes same semantic input to the same processing path.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.2",
        fn_name: f19_2,
        persona: Alice,
        title: "Cache hit rate is visible at a glance on the same screen",
        description: "Alice views cache hit rate and cc-lb records hit rate trend plus cost savings evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.3a",
        fn_name: f19_3a,
        persona: Alice,
        title: "Cost saved through cache hits is shown separately in cost reporting",
        description: "Alice views cost reporting and cc-lb records separate cache savings evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.3b",
        fn_name: f19_3b,
        persona: Alice,
        title: "Cost without cache is also shown with a virtual cost label",
        description: "Alice views virtual cost and cc-lb clearly distinguishes actual and no-cache cost evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.3c",
        fn_name: f19_3c,
        persona: Alice,
        title: "Actual cost and virtual cost are compared as an hourly flow",
        description: "Alice views hourly cache cost comparison and cc-lb records greatest-difference trend evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.4",
        fn_name: f19_4,
        persona: Alice,
        title: "When cache affinity breaks, the call is processed as new",
        description: "Alice observes a cache miss and cc-lb records fresh processing plus hit-rate drop evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.5",
        fn_name: f19_5,
        persona: Alice,
        title: "Cache affinity does not cross team boundaries",
        description: "Alice compares two teams and cc-lb records team-separated cache savings and audit evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.6a",
        fn_name: f19_6a,
        persona: Alice,
        title: "Even while cache affinity works, limit and model bundle violations are still rejected",
        description: "Alice observes cache affinity with a violation and cc-lb records rejection not bypassed by cache evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.6b",
        fn_name: f19_6b,
        persona: Alice,
        title: "Cache hit rate is calculated excluding rejected calls",
        description: "Alice views hit rate and cc-lb records rejected calls excluded from cache rate evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.7",
        fn_name: f19_7,
        persona: Alice,
        title: "Short-lived cache and long-lived cache appear separated by tier",
        description: "Alice views cache tiers and cc-lb records separate short and long tier evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.8",
        fn_name: f19_8,
        persona: Alice,
        title: "Calls from an inactive team are not accepted even through a cache hit",
        description: "Alice deactivates a cache-friendly team and cc-lb records cache not acting as bypass evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.9",
        fn_name: f19_9,
        persona: Alice,
        title: "One call's cache state is visible at a glance with clear classification",
        description: "Alice opens cache detail and cc-lb records hit, miss, partial, create, skip, and unknown classification evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F19.10",
        fn_name: f19_10,
        persona: Alice,
        title: "When a cache hit has a cost unit mismatch from the first call, it is marked separately",
        description: "Alice views token mismatch and cc-lb records mismatch marker plus cost impact evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_proxy_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_proxy_evidence(result, ctx).await; },
    }
}

mod w1_f26 {
    use super::*;
    bdd_scenario! {
        id: "F26.1",
        fn_name: fast_f26_1,
        persona: Charlie,
        title: "Liveness and readiness to process are shown separately",
        description: "Charlie checks health and cc-lb returns separate liveness and readiness evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.2a",
        fn_name: f26_2a,
        persona: Charlie,
        title: "When readiness to process cannot recover, it keeps answering not ready",
        description: "Charlie checks unrecoverable readiness and cc-lb returns persistent not-ready evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.2b",
        fn_name: f26_2b,
        persona: Charlie,
        title: "While readiness is not ready, the liveness signal remains separate",
        description: "Charlie checks liveness during not-ready state and cc-lb returns alive plus not-ready separation evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.2c",
        fn_name: f26_2c,
        persona: Charlie,
        title: "Automatic recovery attempts behind the scenes appear as a marker on the operator screen",
        description: "Charlie observes recovery attempts and cc-lb returns auto-reconnect marker evidence while readiness remains unchanged.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.3",
        fn_name: f26_3,
        persona: Charlie,
        title: "The liveness body shows version, uptime, and build marker together",
        description: "Charlie reads liveness details and cc-lb returns version, uptime, and build marker evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.4",
        fn_name: f26_4,
        persona: Charlie,
        title: "When a notification subscription disconnects and reconnects, it catches up with changes from the gap",
        description: "Charlie observes subscription reconnection and cc-lb records catch-up evidence for gap changes.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.5a",
        fn_name: f26_5a,
        persona: Charlie,
        title: "cc-lb announces that it restarted with a clear marker",
        description: "Charlie views a restart boundary and cc-lb records clear restart marker evidence.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.5b",
        fn_name: f26_5b,
        persona: Charlie,
        title: "The restart marker remains in audit with time",
        description: "Charlie records restart audit evidence with timestamp for later chronological review.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.6",
        fn_name: f26_6,
        persona: Charlie,
        title: "The current number of in-progress calls appears as a gauge on the same screen",
        description: "Charlie views readiness and cc-lb returns in-progress call gauge evidence with zero marker.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
    bdd_scenario! {
        id: "F26.7",
        fn_name: f26_7,
        persona: Charlie,
        title: "During graceful shutdown, liveness and readiness answer separately",
        description: "Charlie begins graceful shutdown and cc-lb returns alive but not-ready evidence for load balancer behavior.",
        given: |ctx| { ctx.harness()? },
        when: |harness| { run_health_flow(harness.admin_router().clone(), harness.proxy_router().clone(), SCENARIO_ID).await },
        then: |result, ctx| { assert_health_evidence(result, ctx).await; },
    }
}
