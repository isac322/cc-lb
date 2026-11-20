//! Public OpenAI `/v1` compatibility for ChatGPT-subscription (`openai_oauth`)
//! upstreams (issue #1355).
//!
//! The Codex backend at `https://chatgpt.com/backend-api/codex` accepts a
//! narrower request contract than the public API. The functions here translate
//! public-shape requests and responses. They are pure: they never perform I/O,
//! never store request data, and leave Codex-shaped requests untouched. The
//! lifecycle decides when to call them, based on the selected upstream kind.
//!
//! Submodules, one per operation family:
//! - [`request`]: `POST /v1/responses` request normalization and policy.
//! - [`response`]: SSE aggregation, error-body and SSE-event normalization.
//! - [`chat`]: Chat Completions to and from Responses.
//! - [`models`]: Codex model catalog to the public model list.
//! - [`compact`]: `responses/compact` emulation.
//! - [`images`]: image edit multipart conversion and image stream wrapping.
//! - [`realtime`]: realtime call request conversion and public-origin URLs.
//! - [`transcription`]: dictation-endpoint transcription conversion.

pub mod chat;
pub mod compact;
pub mod images;
pub mod models;
pub mod realtime;
pub mod request;
pub mod response;
pub mod transcription;

/// A request the compatibility layer refuses. The lifecycle renders it as
/// HTTP 400 with the public error envelope
/// `{"error":{"message","type":"invalid_request_error","param","code"}}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompatReject {
    /// The offending request field, as a JSON path such as `temperature`,
    /// `tools[0].type` or `include[1]`.
    pub param: String,
    /// Machine-readable code, for example `unsupported_parameter` or
    /// `unsupported_value`.
    pub code: &'static str,
    /// Human-readable message naming the parameter and the reason.
    pub message: String,
}

impl CompatReject {
    /// `unsupported_parameter` for a field the subscription backend rejects.
    pub fn unsupported_parameter(param: impl Into<String>) -> Self {
        let param = param.into();
        let message = format!(
            "Unsupported parameter: '{param}' is not supported by ChatGPT-subscription upstreams."
        );
        Self {
            param,
            code: "unsupported_parameter",
            message,
        }
    }

    /// `unsupported_value` for a field whose value the backend rejects.
    pub fn unsupported_value(param: impl Into<String>, value: &str) -> Self {
        let param = param.into();
        let message = format!(
            "Unsupported value: '{value}' for '{param}' is not supported by ChatGPT-subscription upstreams."
        );
        Self {
            param,
            code: "unsupported_value",
            message,
        }
    }
}
