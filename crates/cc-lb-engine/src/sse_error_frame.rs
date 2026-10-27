use crate::error_format::provider_error_body;
use crate::provider_capability::ProviderCapability;
use bytes::{Bytes, BytesMut};
use http::StatusCode;
use serde_json::json;

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
