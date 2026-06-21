use std::time::Duration;

use anyhow::Result;
use axum::http::{Method, StatusCode};
use cc_lb_bdd_tests::{Bob, HttpResponse};
use fake_anthropic::SseEvent;
use serde_json::json;

use super::w1_common::{message_body, principal_name};

pub(crate) struct W1ProxyObservation {
    admin: HttpResponse,
    response: HttpResponse,
    before_requests: usize,
    after_requests: usize,
}

pub(crate) async fn w1_proxy_observe(bob: &Bob<'_>, marker: &str) -> Result<W1ProxyObservation> {
    let admin = bob
        .admin_request(
            Method::POST,
            "/admin/v1/principals",
            Some(json!({
                "name": principal_name(marker),
                "kind": "machine",
                "allowed_models": [],
                "allowed_upstreams": [],
                "default_limits": [],
            })),
        )
        .await;
    let before_requests = bob.recorded_message_count();
    let response = bob.bob_send_message(message_body(marker)).await;
    let mut after_requests = bob.recorded_message_count();
    for _ in 0..20 {
        if after_requests > before_requests {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        after_requests = bob.recorded_message_count();
    }
    Ok(W1ProxyObservation {
        admin,
        response,
        before_requests,
        after_requests,
    })
}

pub(crate) async fn w1_stream_observe(bob: &Bob<'_>, marker: &str) -> Result<W1ProxyObservation> {
    bob.push_sse_response(anthropic_stream_events())?;
    let admin = create_proxy_principal(bob, marker).await;
    let before_requests = bob.recorded_message_count();
    let response = bob.bob_send_message(stream_message_body(marker)).await;
    let after_requests = wait_for_recorded_request(bob, before_requests).await;
    Ok(W1ProxyObservation {
        admin,
        response,
        before_requests,
        after_requests,
    })
}

pub(crate) async fn w1_drop_observe(bob: &Bob<'_>, marker: &str) -> Result<W1ProxyObservation> {
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
    bob.push_drop_response(full_body.as_bytes().to_vec(), partial.len())?;
    let admin = create_proxy_principal(bob, marker).await;
    let before_requests = bob.recorded_message_count();
    let response = bob.bob_send_message(stream_message_body(marker)).await;
    let after_requests = wait_for_recorded_request(bob, before_requests).await;
    Ok(W1ProxyObservation {
        admin,
        response,
        before_requests,
        after_requests,
    })
}

pub(crate) async fn w1_response_cap_observe(
    bob: &Bob<'_>,
    marker: &str,
) -> Result<W1ProxyObservation> {
    bob.push_json_response(large_message_response())?;
    let admin = create_proxy_principal(bob, marker).await;
    let before_requests = bob.recorded_message_count();
    let response = bob.bob_send_message(message_body(marker)).await;
    let after_requests = wait_for_recorded_request(bob, before_requests).await;
    Ok(W1ProxyObservation {
        admin,
        response,
        before_requests,
        after_requests,
    })
}

pub(crate) async fn assert_proxy_result(result: W1ProxyObservation, ctx: &cc_lb_bdd_tests::BddCtx) {
    ctx.assert(
        result.admin.status == StatusCode::CREATED,
        format!(
            "expected proxy setup to create a principal, got {} body={}",
            result.admin.status,
            result.admin.body_text()
        ),
    );
    ctx.assert(
        result.response.status.is_success(),
        format!(
            "expected proxy call to succeed, got {} body={}",
            result.response.status,
            result.response.body_text()
        ),
    );
    ctx.assert(
        result.after_requests > result.before_requests,
        "fake upstream should record the proxied request",
    );
    let alice = ctx.alice().await;
    let events = alice.query_request_events().await;
    ctx.assert(!events.is_empty(), "cc-lb should record a request event");
    let matching_event = events
        .iter()
        .find(|event| event.status == result.response.status.as_u16());
    ctx.assert(
        matching_event.is_some(),
        "cc-lb should record the proxied response status in request events",
    );
    let event = matching_event.unwrap_or(&events[0]);
    ctx.assert(
        event.principal_id.is_some(),
        "request event should include the authenticated principal",
    );
    ctx.assert(
        event.key_id.is_some(),
        "request event should include the accepted key",
    );
    ctx.assert(
        event.model.as_deref() == Some("claude-sonnet-bdd"),
        "request event should include the requested model",
    );
}

pub(crate) async fn assert_stream_result(
    result: W1ProxyObservation,
    ctx: &cc_lb_bdd_tests::BddCtx,
) {
    assert_proxy_setup_and_upstream(&result, ctx);
    ctx.assert(
        result.response.status.is_success(),
        format!(
            "expected streaming call to succeed, got {}",
            result.response.status
        ),
    );
    let body = result.response.body_text();
    let first = body.find("stream-piece-one");
    let second = body.find("stream-piece-two");
    let stop = body.find("message_stop");
    ctx.assert(
        first.is_some() && second.is_some_and(|index| Some(index) > first),
        format!("stream pieces should arrive in order, body={body}"),
    );
    ctx.assert(
        stop.is_some(),
        "stream should include a terminal message_stop marker",
    );
    assert_event_status(ctx, StatusCode::OK).await;
}

pub(crate) async fn assert_drop_result(result: W1ProxyObservation, ctx: &cc_lb_bdd_tests::BddCtx) {
    assert_proxy_setup_and_upstream(&result, ctx);
    let body = result.response.body_text();
    ctx.assert(
        body.contains("api_error") && body.contains("the response was interrupted midway"),
        format!("drop should end with a Claude-shaped error envelope, body={body}"),
    );
    assert_event_status(ctx, StatusCode::BAD_GATEWAY).await;
}

pub(crate) async fn assert_response_cap_result(
    result: W1ProxyObservation,
    ctx: &cc_lb_bdd_tests::BddCtx,
) {
    assert_proxy_setup_and_upstream(&result, ctx);
    ctx.assert(
        result.response.status == StatusCode::PAYLOAD_TOO_LARGE,
        format!(
            "expected response cap cutoff 413, got {} body={}",
            result.response.status,
            result.response.body_text()
        ),
    );
    let body = result.response.body_json();
    ctx.assert(
        body["error"]["type"] == "body_too_large",
        format!("expected body_too_large envelope, got {body}"),
    );
    assert_event_status(ctx, StatusCode::PAYLOAD_TOO_LARGE).await;
}

async fn create_proxy_principal(bob: &Bob<'_>, marker: &str) -> HttpResponse {
    bob.admin_request(
        Method::POST,
        "/admin/v1/principals",
        Some(json!({
            "name": principal_name(marker),
            "kind": "machine",
            "allowed_models": [],
            "allowed_upstreams": [],
            "default_limits": [],
        })),
    )
    .await
}

async fn wait_for_recorded_request(bob: &Bob<'_>, before_requests: usize) -> usize {
    let mut after_requests = bob.recorded_message_count();
    for _ in 0..20 {
        if after_requests > before_requests {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
        after_requests = bob.recorded_message_count();
    }
    after_requests
}

fn assert_proxy_setup_and_upstream(result: &W1ProxyObservation, ctx: &cc_lb_bdd_tests::BddCtx) {
    ctx.assert(
        result.admin.status == StatusCode::CREATED,
        format!(
            "expected proxy setup to create a principal, got {} body={}",
            result.admin.status,
            result.admin.body_text()
        ),
    );
    ctx.assert(
        result.after_requests > result.before_requests,
        "fake upstream should record the proxied request",
    );
}

async fn assert_event_status(ctx: &cc_lb_bdd_tests::BddCtx, status: StatusCode) {
    let alice = ctx.alice().await;
    let events = alice.query_request_events().await;
    ctx.assert(
        events.iter().any(|event| event.status == status.as_u16()),
        format!(
            "cc-lb should record request event status {}",
            status.as_u16()
        ),
    );
}

fn stream_message_body(marker: &str) -> serde_json::Value {
    json!({
        "model": "claude-sonnet-bdd",
        "max_tokens": 8,
        "stream": true,
        "messages": [{"role": "user", "content": format!("W1 stream probe {marker}")}]
    })
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

fn large_message_response() -> serde_json::Value {
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
