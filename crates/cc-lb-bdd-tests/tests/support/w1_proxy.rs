use std::time::Duration;

use anyhow::Result;
use axum::http::{Method, StatusCode};
use cc_lb_bdd_tests::{Bob, HttpResponse};
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
