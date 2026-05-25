use aws_eventstream_codec::{DecodeError, DecodedMessage, decode_messages};
use bytes::Bytes;
use http::StatusCode;
use serde_json::Value;
use thiserror::Error;

use crate::error_map::bedrock_error_to_anthropic_json;

#[derive(Debug, Error)]
pub enum EventStreamConvertError {
    #[error("invalid AWS event-stream frame: {source}")]
    Decode {
        #[from]
        source: DecodeError,
    },
    #[error("event-stream payload is not valid UTF-8")]
    InvalidUtf8,
    #[error("event-stream payload is not valid JSON")]
    InvalidJson,
}

pub fn convert_eventstream_to_sse_bytes(input: &[u8]) -> Result<Bytes, EventStreamConvertError> {
    record_missing_streaming_usage("bedrock");
    let messages = decode_messages(input)?;
    let mut output = Vec::new();

    for message in messages {
        if is_exception(&message) {
            append_exception_sse(&mut output, &message)?;
            break;
        }

        append_event_sse(&mut output, &message)?;
    }

    Ok(Bytes::from(output))
}

fn is_exception(message: &DecodedMessage) -> bool {
    message.header_str(":message-type") == Some("exception")
        || message.header_str(":exception-type").is_some()
}

fn append_event_sse(
    output: &mut Vec<u8>,
    message: &DecodedMessage,
) -> Result<(), EventStreamConvertError> {
    let payload_text = payload_text(message)?;
    let payload_json = serde_json::from_str::<Value>(payload_text)
        .map_err(|_| EventStreamConvertError::InvalidJson)?;
    let event_name = payload_json
        .get("type")
        .and_then(Value::as_str)
        .or_else(|| message.header_str(":event-type"))
        .unwrap_or("unknown_eventstream_message");

    append_sse(output, event_name, payload_text.as_bytes());
    Ok(())
}

fn append_exception_sse(
    output: &mut Vec<u8>,
    message: &DecodedMessage,
) -> Result<(), EventStreamConvertError> {
    let payload_text = payload_text(message)?;
    let payload_json = serde_json::from_str::<Value>(payload_text)
        .map_err(|_| EventStreamConvertError::InvalidJson)?;
    let exception_type = message
        .header_str(":exception-type")
        .or_else(|| message.header_str(":event-type"));
    let message_text = payload_json
        .get("message")
        .or_else(|| payload_json.get("originalMessage"))
        .and_then(Value::as_str);
    let error = bedrock_error_to_anthropic_json(
        StatusCode::INTERNAL_SERVER_ERROR,
        exception_type,
        message_text,
    );
    append_sse(output, "error", &error);
    Ok(())
}

fn payload_text(message: &DecodedMessage) -> Result<&str, EventStreamConvertError> {
    std::str::from_utf8(&message.payload).map_err(|_| EventStreamConvertError::InvalidUtf8)
}

fn append_sse(output: &mut Vec<u8>, event_name: &str, payload: &[u8]) {
    output.extend_from_slice(b"event: ");
    output.extend_from_slice(event_name.as_bytes());
    output.extend_from_slice(b"\ndata: ");
    output.extend_from_slice(payload);
    output.extend_from_slice(b"\n\n");
}

// TODO(v2): extract usage from converse-stream events.
fn record_missing_streaming_usage(dialect: &str) {
    tracing::warn!(
        dialect,
        "bedrock streaming usage extraction not implemented; cost limits may undercount"
    );
}
