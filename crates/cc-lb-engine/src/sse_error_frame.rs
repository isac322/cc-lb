use crate::error_format::provider_error_body;
use crate::provider_capability::ProviderCapability;
use bytes::{Bytes, BytesMut};
use http::StatusCode;
use serde_json::json;
const RESPONSES_ID_MAX_BYTES: usize = 512;

#[derive(Clone, Debug, Default)]
pub(crate) struct ResponsesSseErrorContext {
    response_id: Option<String>,
    last_sequence_number: Option<u64>,
}

impl ResponsesSseErrorContext {
    pub(crate) fn observe(&mut self, value: Option<&serde_json::Value>) {
        let Some(value) = value else {
            return;
        };
        if self.response_id.is_none() {
            let candidate = value
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(serde_json::Value::as_str)
                .or_else(|| value.get("response_id").and_then(serde_json::Value::as_str));
            if let Some(candidate) = candidate
                && !candidate.is_empty()
                && candidate.len() <= RESPONSES_ID_MAX_BYTES
            {
                self.response_id = Some(candidate.to_owned());
            }
        }
        if let Some(sequence_number) = value
            .get("sequence_number")
            .and_then(serde_json::Value::as_u64)
        {
            self.last_sequence_number = Some(
                self.last_sequence_number
                    .unwrap_or_default()
                    .max(sequence_number),
            );
        }
    }

    fn next_sequence_number(&self) -> u64 {
        self.last_sequence_number
            .map_or(0, |sequence| sequence.saturating_add(1))
    }
}

pub(crate) fn make_responses_failed_frame(
    context: &ResponsesSseErrorContext,
    error_type: &str,
    message: &str,
) -> Bytes {
    let mut response = json!({
        "object": "response",
        "created_at": 0,
        "status": "failed",
        "error": {
            "code": error_type,
            "message": message,
        },
        "incomplete_details": null,
        "output": [],
        "usage": null,
    });
    if let Some(response_id) = context.response_id.as_deref() {
        response["id"] = json!(response_id);
    }
    let json = json!({
        "type": "response.failed",
        "sequence_number": context.next_sequence_number(),
        "response": response,
    });
    let mut frame = BytesMut::new();
    frame.extend_from_slice(b"event: response.failed\n");
    frame.extend_from_slice(b"data: ");
    frame.extend_from_slice(json.to_string().as_bytes());
    frame.extend_from_slice(b"\n\n");
    frame.freeze()
}

pub fn make_error_frame(error_type: &str, message: &str) -> Bytes {
    let json = json!({
        "type": "error",
        "error": {
            "type": error_type,
            "message": message,
        }
    });
    make_error_frame_from_json(&json)
}

pub(crate) fn make_provider_error_frame(
    capability: ProviderCapability,
    status: StatusCode,
    error_type: &str,
    message: &str,
    limit_kind: Option<&str>,
) -> Bytes {
    let Ok(json) = serde_json::from_slice(&provider_error_body(
        capability, status, error_type, message, limit_kind,
    )) else {
        return make_error_frame(error_type, message);
    };
    make_error_frame_from_json(&json)
}

pub fn make_error_frame_from_json(json: &serde_json::Value) -> Bytes {
    let mut frame = BytesMut::new();
    frame.extend_from_slice(b"event: error\n");
    frame.extend_from_slice(b"data: ");
    frame.extend_from_slice(json.to_string().as_bytes());
    frame.extend_from_slice(b"\n\n");
    frame.freeze()
}
