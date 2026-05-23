mod sse_relay_support;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use cc_lb_core::{ErrorNormalizer, SseBatchConfig, SseRelay, UpstreamKind};
use serde_json::Value;
use sse_relay_support::{RecordingHook, TestDialect, body_from_chunks, collect_response_body};

#[test]
fn bedrock_exception_json_becomes_anthropic_sse_error_frame() {
    let normalizer = ErrorNormalizer::new();
    let payload = Bytes::from_static(
        br#"{"__type":"ModelStreamErrorException","message":"forced fake model stream error"}"#,
    );

    let frame = normalizer.normalize_sse_error_frame(UpstreamKind::BedrockRuntime, &payload);
    let value = parse_sse_error_data(&frame);

    assert_eq!(value.get("type").and_then(Value::as_str), Some("error"));
    assert_eq!(
        value
            .get("error")
            .and_then(|error| error.get("type"))
            .and_then(Value::as_str),
        Some("api_error")
    );
    assert_eq!(
        value
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str),
        Some("forced fake model stream error")
    );
}

#[tokio::test]
async fn relay_converts_configured_bedrock_error_event() {
    let hook = Arc::new(RecordingHook::default());
    let relay = SseRelay {
        obs: hook,
        dialect: Arc::new(TestDialect),
        batch: SseBatchConfig {
            max_events: 32,
            max_age: Duration::from_secs(60),
        },
        quota: None,
        principal_id: "principal-sse".to_owned(),
        reservation: None,
        error_normalizer: Some(Arc::new(ErrorNormalizer::new())),
        upstream_kind: Some(UpstreamKind::BedrockRuntime),
    };
    let upstream = Bytes::from_static(
        b"event: error\ndata: {\"__type\":\"ValidationException\",\"message\":\"bad\"}\n\n",
    );

    let response = relay.into_response_from_body(body_from_chunks(
        vec![upstream.clone()],
        Duration::ZERO,
        None,
    ));
    let output = collect_response_body(response).await;
    let value = parse_sse_error_data(&output);

    assert_ne!(output, upstream);
    assert_eq!(
        value
            .get("error")
            .and_then(|error| error.get("type"))
            .and_then(Value::as_str),
        Some("invalid_request_error")
    );
    assert_eq!(
        value
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str),
        Some("bad")
    );
}

fn parse_sse_error_data(frame: &Bytes) -> Value {
    let text = std::str::from_utf8(frame).expect("SSE frame is UTF-8");
    assert!(text.starts_with("event: error\ndata: "));
    assert!(text.ends_with("\n\n"));
    let data = text
        .strip_prefix("event: error\ndata: ")
        .and_then(|value| value.strip_suffix("\n\n"))
        .expect("SSE error frame has one data line");
    serde_json::from_str(data).expect("SSE data is JSON")
}
