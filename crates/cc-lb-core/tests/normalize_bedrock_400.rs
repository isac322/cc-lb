use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use cc_lb_core::{ErrorNormalizer, UpstreamKind};
use cc_lb_dialect_bedrock::BedrockRuntimeDialect;
use http::StatusCode;
use serde_json::Value;

#[test]
fn bedrock_validation_error_normalizes_to_anthropic_shape() {
    let normalizer = ErrorNormalizer::new().with_dialect(
        UpstreamKind::BedrockRuntime,
        Arc::new(BedrockRuntimeDialect),
    );
    let body = Bytes::from_static(br#"{"__type":"ValidationException","message":"bad"}"#);

    let normalized = normalizer.normalize_http_error(
        UpstreamKind::BedrockRuntime,
        StatusCode::BAD_REQUEST,
        &body,
    );
    let value: Value = serde_json::from_slice(&normalized).expect("normalized JSON parses");

    assert_eq!(value.get("type").and_then(Value::as_str), Some("error"));
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

    write_evidence("task-24-bedrock-err-to-anthropic.json", &normalized);
}

fn write_evidence(file_name: &str, body: &Bytes) {
    let value: Value = serde_json::from_slice(body).expect("evidence JSON parses");
    let pretty = serde_json::to_vec_pretty(&value).expect("evidence JSON serializes");
    for dir in evidence_dirs() {
        fs::create_dir_all(&dir).expect("evidence directory is created");
        fs::write(dir.join(file_name), &pretty).expect("evidence is written");
    }
}

fn evidence_dirs() -> Vec<PathBuf> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    vec![
        PathBuf::from(std::env::var("OUT_DIR").unwrap_or_else(|_| ".omo/evidence".to_owned())),
        manifest_dir.join("../../.omo/evidence"),
    ]
}
