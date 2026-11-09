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

#[derive(Default)]
pub(crate) struct TransformedSseOutput {
    pub(crate) emitted_error: bool,
    pub(crate) completes_response: bool,
}

impl ResponsesSseErrorContext {
    pub(crate) fn observe(&mut self, value: Option<&serde_json::Value>) {
        let Some(value) = value else {
            return;
        };
        self.observe_value(value, false);
    }

    /// Observe replacement metadata and terminal semantics in one parsing pass.
    pub(crate) fn observe_transformed_output(
        &mut self,
        bytes: &[u8],
        capability: ProviderCapability,
    ) -> TransformedSseOutput {
        let mut output = TransformedSseOutput::default();
        let mut offset = 0;
        while let Some(end) = crate::usage_parser::find_sse_event_end(&bytes[offset..]) {
            let end = offset + end;
            let parsed = crate::usage_parser::parse_sse_event(&bytes[offset..end]);
            if !output.completes_response {
                output.completes_response =
                    crate::lifecycle::sse_event_completes_response(capability, &parsed, false);
            }
            if let Some(value) = parsed.value().ok().flatten() {
                let is_responses = matches!(
                    capability,
                    ProviderCapability::OpenaiResponses
                        | ProviderCapability::OpenaiResponsesCompact
                );
                output.emitted_error = output.emitted_error
                    || if is_responses {
                        // Responses clients dispatch on data.type, not the SSE
                        // event name or unrelated error-shaped payload fields.
                        let native_error = |error: &serde_json::Value| {
                            error
                                .get("code")
                                .and_then(serde_json::Value::as_str)
                                .is_some()
                                && error
                                    .get("message")
                                    .and_then(serde_json::Value::as_str)
                                    .is_some()
                        };
                        match value.get("type").and_then(serde_json::Value::as_str) {
                            Some("response.failed") => value
                                .get("response")
                                .and_then(|response| response.get("error"))
                                .is_some_and(native_error),
                            Some("error") => {
                                value
                                    .get("message")
                                    .and_then(serde_json::Value::as_str)
                                    .is_some()
                                    && value
                                        .get("code")
                                        .is_none_or(|code| code.is_null() || code.is_string())
                            }
                            _ => false,
                        }
                    } else {
                        parsed.event_name() == Some(b"error")
                            || value.get("type").and_then(serde_json::Value::as_str)
                                == Some("error")
                            || value.get("error").is_some_and(|error| !error.is_null())
                    };
                if is_responses {
                    self.observe_value(value, true);
                }
            }
            offset = end;
        }
        output
    }

    fn next_sequence_number(&self) -> u64 {
        self.last_sequence_number
            .map_or(0, |sequence| sequence.saturating_add(1))
    }

    fn observe_value(&mut self, value: &serde_json::Value, replace_identity: bool) {
        if self.response_id.is_none() || replace_identity {
            let candidate = value
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(serde_json::Value::as_str)
                .or_else(|| value.get("response_id").and_then(serde_json::Value::as_str));
            if let Some(candidate) = candidate
                && !candidate.is_empty()
                && candidate.len() <= RESPONSES_ID_MAX_BYTES
                && self.response_id.as_deref() != Some(candidate)
            {
                if let Some(response_id) = self.response_id.as_mut() {
                    response_id.clear();
                    response_id.push_str(candidate);
                } else {
                    self.response_id = Some(candidate.to_owned());
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_failure_preserves_bounded_identity_and_maximum_sequence() {
        let assert_failure = |context: &ResponsesSseErrorContext,
                              response_id: Option<&str>,
                              sequence_number: u64| {
            let frame = make_responses_failed_frame(context, "test_error", "test failure");
            let parsed = crate::usage_parser::parse_sse_event(&frame);
            let value = parsed.value().unwrap().unwrap();
            assert_eq!(value["type"], "response.failed");
            assert_eq!(
                value["response"]
                    .get("id")
                    .and_then(serde_json::Value::as_str),
                response_id,
            );
            assert_eq!(value["sequence_number"].as_u64(), Some(sequence_number));
        };
        let mut context = ResponsesSseErrorContext::default();
        let original = json!({"response": {"id": "resp-original"}, "sequence_number": 7});
        context.observe(Some(&original));
        context.observe(Some(&original));
        context.observe(Some(&json!({
            "response": {"id": "resp-ignored"},
            "sequence_number": 9,
        })));
        assert_failure(&context, Some("resp-original"), 10);

        context.observe_transformed_output(
            b"data: {\"response_id\":\"resp-original\",\"sequence_number\":6}\n\n",
            ProviderCapability::OpenaiResponses,
        );
        assert_failure(&context, Some("resp-original"), 10);

        context.observe_transformed_output(
            b"data: {\"response_id\":\"resp-new\",\"sequence_number\":12}\n\n",
            ProviderCapability::OpenaiResponses,
        );
        assert_failure(&context, Some("resp-new"), 13);

        let oversized = json!({
            "response_id": "x".repeat(RESPONSES_ID_MAX_BYTES + 1),
            "sequence_number": 10,
        });
        context.observe_transformed_output(
            format!("data: {oversized}\n\n").as_bytes(),
            ProviderCapability::OpenaiResponses,
        );
        assert_failure(&context, Some("resp-new"), 13);

        let mut oversized_context = ResponsesSseErrorContext::default();
        oversized_context.observe(Some(&oversized));
        assert_failure(&oversized_context, None, 11);
    }

    #[test]
    fn transformed_responses_error_terminal_requires_native_payload_schema() {
        for capability in [
            ProviderCapability::OpenaiResponses,
            ProviderCapability::OpenaiResponsesCompact,
        ] {
            for bytes in [
                b"event: response.failed\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"text\"}\n\n".as_slice(),
                b"event: response.failed\ndata: {\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"test\"}}}\n\n".as_slice(),
                b"event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n".as_slice(),
                b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}\n\n".as_slice(),
                b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"test\"}}}\n\n".as_slice(),
                b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":42,\"message\":\"wrong code type\"}}}\n\n".as_slice(),
                b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"test\",\"message\":42}}}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"response.output_text.delta\",\"error\":{\"code\":\"test\"}}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\",\"error\":{\"code\":\"test\",\"message\":\"nested\"}}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\"}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\",\"code\":\"test\"}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\",\"code\":42,\"message\":\"wrong code type\"}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\",\"code\":\"test\",\"message\":42}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"response.output_text.delta\",\"code\":\"test\",\"message\":\"not terminal\"}\n\n".as_slice(),
            ] {
                let mut context = ResponsesSseErrorContext::default();
                assert!(!context.observe_transformed_output(bytes, capability).emitted_error);
            }
            for bytes in [
                b"event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"test\",\"message\":\"native failure\"}}}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\",\"code\":\"arbitrary_provider_code\",\"message\":\"native error\"}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\",\"code\":null,\"message\":\"native error without code\",\"param\":null,\"sequence_number\":9}\n\n".as_slice(),
                b"event: error\ndata: {\"type\":\"error\",\"message\":\"native error with omitted code\",\"param\":null,\"sequence_number\":9}\n\n".as_slice(),
            ] {
                let mut context = ResponsesSseErrorContext::default();
                assert!(context.observe_transformed_output(bytes, capability).emitted_error);
            }
        }
    }
}
