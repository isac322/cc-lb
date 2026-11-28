//! Response-side normalization for public OpenAI Responses clients.
//!
//! The Codex backend emits Responses-shaped SSE events, with one provider-only
//! `response.metadata` event and (for some failures) a provider error envelope.
//! This module contains the small, request-scoped transformations needed by
//! the public `/v1` surface.

use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

/// Builds the final public Response JSON from a backend SSE stream for a client
/// that sent `stream:false`.
pub struct SseAggregator {
    echo_metadata: Option<Value>,
    output_items: BTreeMap<u64, Value>,
    terminal: Option<AggregateTerminal>,
}

enum AggregateTerminal {
    Response(Value),
    Error { status: u16, body: Vec<u8> },
}

impl SseAggregator {
    /// Starts an aggregator, optionally echoing the request's metadata into
    /// the final response.
    pub fn new(echo_metadata: Option<Value>) -> Self {
        Self {
            echo_metadata,
            output_items: BTreeMap::new(),
            terminal: None,
        }
    }

    /// Returns whether a terminal response or error frame has been accepted.
    pub fn has_terminal(&self) -> bool {
        self.terminal.is_some()
    }

    /// Feeds one parsed SSE event JSON (the `data:` payload).
    pub fn push(&mut self, event: &Value) {
        if self.terminal.is_some() {
            return;
        }

        let Some(event_type) = event_type(event) else {
            return;
        };

        // This event contains provider moderation metadata and is not part of
        // the public Responses SSE protocol.
        if drop_sse_event(event_type) {
            return;
        }

        match event_type {
            "error" => {
                self.terminal = Some(AggregateTerminal::Error {
                    status: event_error_status(event),
                    body: event_error_body(event),
                });
            }
            "response.failed" => {
                self.terminal = Some(AggregateTerminal::Error {
                    status: event_error_status(event),
                    body: event_error_body(event),
                });
            }
            "response.completed" | "response.incomplete" => {
                self.terminal = Some(AggregateTerminal::Response(
                    event.get("response").cloned().unwrap_or_else(|| json!({})),
                ));
            }
            "response.output_item.done" => {
                self.remember_output_item(event);
            }
            _ => {}
        }
    }

    fn remember_output_item(&mut self, event: &Value) {
        let Some(output_index) = event.get("output_index").and_then(Value::as_u64) else {
            return;
        };
        let Some(item) = event.get("item") else {
            return;
        };
        self.output_items.insert(output_index, item.clone());
    }

    /// Returns the terminal aggregate once the stream has ended.
    pub fn finish(self) -> AggregateOutcome {
        let SseAggregator {
            echo_metadata,
            output_items,
            terminal,
        } = self;

        let Some(terminal) = terminal else {
            return AggregateOutcome::Truncated;
        };

        match terminal {
            AggregateTerminal::Error { status, body } => AggregateOutcome::Error { status, body },
            AggregateTerminal::Response(mut response) => {
                if let Some(response_object) = response.as_object_mut() {
                    let output_is_empty = response_object
                        .get("output")
                        .and_then(Value::as_array)
                        .is_none_or(|output| output.is_empty());
                    if output_is_empty && !output_items.is_empty() {
                        response_object.insert(
                            "output".to_owned(),
                            Value::Array(output_items.into_values().collect()),
                        );
                    }
                    if let Some(metadata) = echo_metadata.as_ref() {
                        response_object.insert("metadata".to_owned(), metadata.clone());
                    }
                }
                AggregateOutcome::Response(response)
            }
        }
    }
}

/// The terminal outcome of an aggregated backend SSE stream.
#[derive(Debug, PartialEq)]
pub enum AggregateOutcome {
    /// 200 with the final Response object (status completed or incomplete).
    /// Output items collected from `response.output_item.done` fill `output`
    /// when the terminal event's `response.output` is empty or missing, in
    /// output-index order.
    Response(Value),
    /// An `error` event or `response.failed`, with a public error envelope.
    Error { status: u16, body: Vec<u8> },
    /// The stream ended without a terminal event.
    Truncated,
}

/// If `body` is a backend `{"detail": ...}` error, returns the public error
/// envelope. `None` means the body is already public-shaped or is not JSON.
pub fn public_error_from_detail(status: u16, body: &[u8]) -> Option<Vec<u8>> {
    let value: Value = sonic_rs::from_slice(body).ok()?;
    let object = value.as_object()?;
    if object.get("error").is_some_and(Value::is_object) {
        return None;
    }
    let detail = object.get("detail")?;
    let message = match detail {
        Value::String(message) => message.clone(),
        other => serde_json::to_string(other).ok()?,
    };
    let envelope = public_error_value(message, public_error_type(status), None, None);
    serde_json::to_vec(&envelope).ok()
}

/// Returns true for an SSE event type that must not reach a public-shape
/// client.
pub fn drop_sse_event(event_type: &str) -> bool {
    event_type == "response.metadata"
}

/// Classification used by the truncation retry gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EarlyEvent {
    Pending,
    Output,
    ContextOverflow,
    OtherTerminal,
}

/// Classifies an early SSE event for the truncation retry.
pub fn classify_early_event(event: &Value) -> EarlyEvent {
    let Some(event_type) = event_type(event) else {
        return EarlyEvent::Pending;
    };

    if matches!(event_type, "error" | "response.failed") {
        return if event_error_code(event).as_deref() == Some("context_length_exceeded") {
            EarlyEvent::ContextOverflow
        } else {
            EarlyEvent::OtherTerminal
        };
    }

    if matches!(event_type, "response.completed" | "response.incomplete") {
        return EarlyEvent::OtherTerminal;
    }

    if event_type == "response.output_item.added"
        || event_type == "response.output_item.done"
        || event_type.ends_with(".delta")
        || event_type.starts_with("response.output_text.")
    {
        return EarlyEvent::Output;
    }

    EarlyEvent::Pending
}

/// Inserts client metadata into a response object, either nested in an SSE
/// lifecycle event or at the root of an aggregated Response object.
pub fn echo_metadata_into(event_or_response: &mut Value, metadata: &Value) {
    let Some(object) = event_or_response.as_object_mut() else {
        return;
    };

    if let Some(response) = object.get_mut("response").and_then(Value::as_object_mut) {
        response.insert("metadata".to_owned(), metadata.clone());
        return;
    }

    // Aggregated Response objects have no event `type` and carry a response
    // object/status. Do not add metadata to provider-only events such as
    // output deltas.
    if !object.contains_key("type")
        && (object.get("object").and_then(Value::as_str) == Some("response")
            || object.contains_key("status"))
    {
        object.insert("metadata".to_owned(), metadata.clone());
    }
}

fn event_type(event: &Value) -> Option<&str> {
    event.get("type").and_then(Value::as_str)
}

fn event_error_code(event: &Value) -> Option<String> {
    error_field(event, "code")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn event_error_status(event: &Value) -> u16 {
    let code = event_error_code(event)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let error_type = error_field(event, "type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();

    if code == "context_length_exceeded" || error_type == "invalid_request_error" {
        return 400;
    }
    if error_type == "rate_limit_error"
        || code.contains("rate_limit")
        || code.contains("usage_limit")
        || code.contains("quota")
    {
        return 429;
    }
    500
}

fn event_error_body(event: &Value) -> Vec<u8> {
    let code = event_error_code(event);
    let message = error_field(event, "message")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| code.clone())
        .unwrap_or_else(|| "The upstream request failed.".to_owned());
    let param = error_field(event, "param")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let status = event_error_status(event);
    let error_type = if status == 400 {
        "invalid_request_error"
    } else if status == 429 {
        "rate_limit_error"
    } else {
        "server_error"
    };
    serde_json::to_vec(&public_error_value(message, error_type, param, code)).unwrap_or_default()
}

fn error_field<'a>(event: &'a Value, field: &str) -> Option<&'a Value> {
    event
        .get("error")
        .and_then(|error| error.get(field))
        .or_else(|| {
            event
                .get("response")
                .and_then(|response| response.get("error"))
                .and_then(|error| error.get(field))
        })
        .or_else(|| event.get(field))
}

fn public_error_type(status: u16) -> &'static str {
    match status {
        401 => "authentication_error",
        403 => "permission_error",
        429 => "rate_limit_error",
        500..=599 => "server_error",
        _ => "invalid_request_error",
    }
}

fn public_error_value(
    message: String,
    error_type: &str,
    param: Option<String>,
    code: Option<String>,
) -> Value {
    let mut error = Map::new();
    error.insert("message".to_owned(), Value::String(message));
    error.insert("type".to_owned(), Value::String(error_type.to_owned()));
    error.insert("param".to_owned(), param.map_or(Value::Null, Value::String));
    error.insert("code".to_owned(), code.map_or(Value::Null, Value::String));
    let mut envelope = Map::new();
    envelope.insert("error".to_owned(), Value::Object(error));
    Value::Object(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(value: Value) -> Value {
        value
    }

    #[test]
    fn nonstream_aggregates_backend_sse_and_rebuilds_output() {
        let mut aggregate = SseAggregator::new(Some(json!({"request": "metadata"})));
        aggregate.push(&event(json!({
            "type": "response.created",
            "response": {"id": "resp_1", "status": "in_progress", "output": []}
        })));
        aggregate.push(&event(
            json!({"type": "response.in_progress", "response": {"id": "resp_1"}}),
        ));
        aggregate.push(&event(
            json!({"type": "response.metadata", "metadata": {"provider": true}}),
        ));
        aggregate.push(&event(json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {"type": "message", "id": "msg_1"}
        })));
        aggregate.push(&event(json!({
            "type": "response.content_part.added",
            "output_index": 0,
            "content_index": 0,
            "part": {"type": "output_text", "text": ""}
        })));
        aggregate.push(&event(json!({
            "type": "response.output_text.delta",
            "output_index": 0,
            "delta": "hello"
        })));
        aggregate.push(&event(json!({
            "type": "response.output_text.done",
            "output_index": 0,
            "text": "hello"
        })));
        aggregate.push(&event(json!({
            "type": "response.output_item.done",
            "output_index": 0,
            "item": {
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "hello"}]
            }
        })));
        aggregate.push(&event(json!({
            "type": "response.completed",
            "response": {"id": "resp_1", "status": "completed", "output": [], "usage": {"input_tokens": 4}}
        })));

        let AggregateOutcome::Response(response) = aggregate.finish() else {
            panic!("expected response");
        };
        assert_eq!(response["status"], "completed");
        assert_eq!(response["output"][0]["id"], "msg_1");
        assert_eq!(response["usage"]["input_tokens"], 4);
        assert_eq!(response["metadata"], json!({"request": "metadata"}));
    }

    #[test]
    fn incomplete_response_is_returned_and_missing_output_is_rebuilt() {
        let mut aggregate = SseAggregator::new(None);
        aggregate.push(&event(json!({
            "type": "response.output_item.done",
            "output_index": 1,
            "item": {"type": "reasoning", "id": "r_1"}
        })));
        aggregate.push(&event(json!({
            "type": "response.incomplete",
            "response": {"status": "incomplete", "incomplete_details": {"reason": "max_output_tokens"}}
        })));

        let AggregateOutcome::Response(response) = aggregate.finish() else {
            panic!("expected response");
        };
        assert_eq!(response["status"], "incomplete");
        assert_eq!(response["output"][0]["type"], "reasoning");
    }

    #[test]
    fn context_overflow_error_is_public_400() {
        let mut aggregate = SseAggregator::new(None);
        aggregate.push(&event(json!({
            "type": "error",
            "code": "context_length_exceeded",
            "message": "Too many tokens",
            "param": "input"
        })));
        let AggregateOutcome::Error { status, body } = aggregate.finish() else {
            panic!("expected error");
        };
        assert_eq!(status, 400);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"error": {"message": "Too many tokens", "type": "invalid_request_error", "param": "input", "code": "context_length_exceeded"}})
        );
    }

    #[test]
    fn usage_limit_error_is_public_429() {
        let mut aggregate = SseAggregator::new(None);
        aggregate.push(&event(json!({
            "type": "response.failed",
            "response": {"error": {"code": "usage_limit_reached", "message": "Usage limit reached"}}
        })));
        let AggregateOutcome::Error { status, body } = aggregate.finish() else {
            panic!("expected error");
        };
        assert_eq!(status, 429);
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["error"]["type"],
            "rate_limit_error"
        );
    }

    #[test]
    fn detail_errors_convert_and_public_errors_passthrough() {
        let body = br#"{"detail":"Bad input"}"#;
        assert_eq!(
            serde_json::from_slice::<Value>(&public_error_from_detail(400, body).unwrap()).unwrap(),
            json!({"error": {"message": "Bad input", "type": "invalid_request_error", "param": null, "code": null}})
        );

        let object_detail = br#"{"detail":{"field":"input","reason":"bad"}}"#;
        let converted = public_error_from_detail(403, object_detail).unwrap();
        let converted = serde_json::from_slice::<Value>(&converted).unwrap();
        assert_eq!(converted["error"]["type"], "permission_error");
        assert_eq!(
            converted["error"]["message"],
            r#"{"field":"input","reason":"bad"}"#
        );

        assert!(
            public_error_from_detail(400, br#"{"error":{"message":"already public"}}"#).is_none()
        );
        assert!(public_error_from_detail(400, br#"not json"#).is_none());
    }

    #[test]
    fn early_events_are_classified() {
        assert!(matches!(
            classify_early_event(&json!({"type": "response.created"})),
            EarlyEvent::Pending
        ));
        assert!(matches!(
            classify_early_event(&json!({"type": "response.metadata"})),
            EarlyEvent::Pending
        ));
        assert!(matches!(
            classify_early_event(&json!({"type": "response.output_item.added"})),
            EarlyEvent::Output
        ));
        assert!(matches!(
            classify_early_event(&json!({"type": "response.output_text.delta"})),
            EarlyEvent::Output
        ));
        assert!(matches!(
            classify_early_event(&json!({"type": "error", "code": "context_length_exceeded"})),
            EarlyEvent::ContextOverflow
        ));
        assert!(matches!(
            classify_early_event(&json!({"type": "response.failed", "code": "other"})),
            EarlyEvent::OtherTerminal
        ));
    }

    #[test]
    fn metadata_echo_targets_nested_and_aggregated_responses() {
        let metadata = json!({"trace": "client"});
        let mut event = json!({"type": "response.in_progress", "response": {"id": "r"}});
        echo_metadata_into(&mut event, &metadata);
        assert_eq!(event["response"]["metadata"], metadata);

        let mut response = json!({"object": "response", "status": "completed"});
        echo_metadata_into(&mut response, &metadata);
        assert_eq!(response["metadata"], metadata);
    }

    #[test]
    fn truncated_stream_is_not_a_partial_response() {
        let mut aggregate = SseAggregator::new(None);
        aggregate.push(&event(
            json!({"type": "response.created", "response": {"id": "r"}}),
        ));
        assert!(matches!(aggregate.finish(), AggregateOutcome::Truncated));
    }
}
