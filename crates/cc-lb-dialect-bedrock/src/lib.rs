//! Bedrock Runtime and Bedrock Mantle dialects.

#![forbid(unsafe_code)]

pub mod error_map;
pub mod eventstream_converter;
pub mod mantle;
pub mod runtime;

pub use error_map::{bedrock_error_to_anthropic_json, map_bedrock_error_type};
pub use eventstream_converter::{convert_eventstream_to_sse_bytes, EventStreamConvertError};
pub use mantle::BedrockMantleDialect;
pub use runtime::{BedrockBodyTransform, BedrockRuntimeDialect};

pub(crate) fn is_text_event_stream(value: &str) -> bool {
    value
        .split(',')
        .any(|part| part.trim().eq_ignore_ascii_case("text/event-stream"))
}
