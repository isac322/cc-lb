//! Test-only adapter for the unmodified, pinned official Codex SSE parser.
use super::process_sse;
use crate::common::ResponseEvent;
use crate::error::ApiError;
use bytes::Bytes;
use codex_client::TransportError;
use futures::stream;
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc;

const SAFE_MESSAGE: &str = "upstream SSE response could not be safely processed";
const GENERIC_EOF: &str = "stream closed before response.completed";

async fn replay(body: Vec<u8>) -> Vec<String> {
    // Fragment the actual emitted bytes, including across UTF-8/SSE boundaries.
    let chunks: Vec<Result<Bytes, TransportError>> = body
        .chunks(7)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect();
    let (tx, mut rx) = mpsc::channel::<Result<ResponseEvent, ApiError>>(1600);
    let parser = tokio::spawn(process_sse(
        Box::pin(stream::iter(chunks)),
        tx,
        Duration::from_secs(5),
        None,
    ));
    let mut errors = Vec::new();
    while let Some(event) = rx.recv().await {
        match event {
            Err(ApiError::Retryable { message, .. } | ApiError::Stream(message)) => {
                errors.push(message);
            }
            Err(other) => panic!("unexpected Codex classification: {other:?}"),
            Ok(_) => {}
        }
    }
    parser.await.expect("official parser task panicked");
    errors
}

#[tokio::test]
async fn local_error_contract_replay() {
    let path = std::env::var("CC_LB_REPLAY_SSE").expect("set CC_LB_REPLAY_SSE to responses-fault.sse");
    let emitted = std::fs::read(path).expect("read emitted proxy SSE bytes");
    assert!(emitted.windows(b"response.output_text.delta".len()).any(|bytes| bytes == b"response.output_text.delta"));
    let actual = replay(emitted).await;
    let expected_mode = std::env::var("CC_LB_REPLAY_EXPECT").unwrap_or_else(|_| "new".into());
    match expected_mode.as_str() {
        "new" => assert_eq!(actual, vec![SAFE_MESSAGE.to_owned()]),
        "old" => assert_eq!(actual, vec![GENERIC_EOF.to_owned()]),
        other => panic!("invalid CC_LB_REPLAY_EXPECT: {other}"),
    }

    // Same normal event prefix in both negative controls. These are fixtures,
    // not a replacement implementation of any part of the Codex parser.
    let prefix = b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_negative_control\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n";
    let old_envelope = format!(
        "event: error\ndata: {}\n\n",
        json!({"error": {"code": "api_error", "type": "api_error", "param": null, "message": SAFE_MESSAGE}})
    );
    let type_only = format!(
        "event: error\ndata: {}\n\n",
        json!({"type": "error", "error": {"code": "api_error", "type": "api_error", "param": null, "message": SAFE_MESSAGE}})
    );
    let mut controls = Vec::new();
    for (name, suffix) in [("old-http-envelope", old_envelope), ("generic-type-error", type_only)] {
        let body = [prefix.as_slice(), suffix.as_bytes()].concat();
        let errors = replay(body).await;
        assert_eq!(errors, vec![GENERIC_EOF.to_owned()], "negative control: {name}");
        controls.push(json!({"name": name, "errors": errors}));
    }
    let report = json!({
        "codex_revision": "4e4767ffa763ec0d0a19e04cf8ff0163a987297b",
        "parser": "codex-api::sse::responses::process_sse",
        "expect": expected_mode,
        "emitted_stream_errors": actual,
        "negative_controls": controls,
    });
    if let Ok(path) = std::env::var("CC_LB_REPLAY_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).expect("serialize replay report"))
            .expect("write replay report");
    }
    println!("{report}");
}
