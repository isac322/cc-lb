mod common;

use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cc_lb_core::{Lifecycle, LifecycleConfig};
use cc_lb_plugin_api::{AuthnError, AuthnOutcome, ObserveEvent, RequestContext};
use http::header::CONTENT_TYPE;
use http::{HeaderValue, Request, StatusCode};
use serde_json::Value;
use url::Url;

use common::{collect_body, DispatchMode, MockDispatch, RecordingHook, TestRouter, TestState};

const SECRET_HEADER_TOKEN: &str = "sk-ant-oat01-task46-secret";
const SECRET_HEADER_VALUE: &str = "Bearer sk-ant-oat01-task46-secret";
const SECRET_BODY_TOKEN: &str = "sk-ant-oat01-task46-body-secret";
const SECRET_BEARER_TOKEN: &str = "task46-body-secret";
const SECRET_BODY_RAW: &[u8] =
    br#"{"api_key":"sk-ant-oat01-task46-body-secret","authorization":"Bearer task46-body-secret"}"#;

#[tokio::test]
async fn authn_runtime_error_maps_to_anthropic_error_without_secret_leak() {
    let state = TestState::default();
    let hook = Arc::new(RecordingHook::default());
    let lifecycle = Lifecycle::new(
        Arc::new(FailingRuntimeAuthn {
            reason: "plugin call failed: plugin ran out of fuel".to_owned(),
        }),
        Arc::new(TestRouter {
            base_url: Url::parse("http://upstream.local/").expect("test URL parses"),
        }),
        Arc::new(MockDispatch {
            state: state.clone(),
            mode: DispatchMode::StreamingOk,
        }),
        vec![hook.clone()],
        LifecycleConfig::default(),
    );

    let process_id_before = std::process::id();
    let response = lifecycle
        .handle(secret_request())
        .await
        .expect("lifecycle handles authn runtime failure");
    let (status, headers, body) = collect_body(response).await;
    let body_text = String::from_utf8_lossy(&body);
    let body_json: Value = serde_json::from_slice(&body).expect("Anthropic error body is JSON");

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body_json["type"], "error");
    assert_eq!(body_json["error"]["type"], "authentication_error");
    assert_eq!(body_json["error"]["message"], "authentication failed");
    assert!(headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json")));
    assert_eq!(state.upstream_calls.load(Ordering::Relaxed), 0);
    assert_eq!(std::process::id(), process_id_before);
    assert_no_task46_secret(&body_text);

    let events = hook.events.lock().expect("events lock");
    assert!(events.iter().any(|event| matches!(
        event,
        ObserveEvent::Error { code, source, .. }
            if code == "authentication_error" && source == "authn"
    )));
    assert!(events.iter().any(|event| matches!(
        event,
        ObserveEvent::RequestFinished { status, .. } if *status == StatusCode::UNAUTHORIZED
    )));
    assert_no_task46_secret(&format!("{events:?}"));

    println!(
        "task46_host_error_shape boundary=Lifecycle/AuthnError::Runtime status={} error_type={} redacted=true pid_unchanged=true upstream_calls={}",
        status.as_u16(),
        body_json["error"]["type"].as_str().unwrap_or("unknown"),
        state.upstream_calls.load(Ordering::Relaxed)
    );
}

struct FailingRuntimeAuthn {
    reason: String,
}

#[async_trait]
impl cc_lb_plugin_api::AuthnPlugin for FailingRuntimeAuthn {
    async fn authenticate(&self, _ctx: &RequestContext) -> Result<AuthnOutcome, AuthnError> {
        Err(AuthnError::Runtime {
            reason: self.reason.clone(),
        })
    }
}

fn secret_request() -> Request<Bytes> {
    Request::builder()
        .method("POST")
        .uri("/v1/messages?mode=task46")
        .header(
            "authorization",
            HeaderValue::from_static(SECRET_HEADER_VALUE),
        )
        .header("x-api-key", HeaderValue::from_static(SECRET_HEADER_TOKEN))
        .header("anthropic-version", "2023-06-01")
        .body(Bytes::from_static(SECRET_BODY_RAW))
        .expect("test request builds")
}

fn assert_no_task46_secret(output: &str) {
    for secret in [
        SECRET_HEADER_TOKEN,
        SECRET_HEADER_VALUE,
        SECRET_BODY_TOKEN,
        SECRET_BEARER_TOKEN,
    ] {
        assert!(
            !output.contains(secret),
            "output leaked task46 secret material"
        );
    }
}
