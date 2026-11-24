use serde_json::{Map, Value, json};

/// Translate the public compact request into a Codex Responses request.
pub fn compact_request_to_responses(body: &Value) -> Result<Value, super::CompatReject> {
    if body
        .get("previous_response_id")
        .is_some_and(|v| !v.is_null())
    {
        return Err(super::CompatReject::unsupported_parameter(
            "previous_response_id",
        ));
    }
    let mut out = Map::new();
    for key in ["model", "instructions", "tools", "reasoning"] {
        if let Some(value) = body.get(key) {
            out.insert(key.to_owned(), value.clone());
        }
    }
    if let Some(input) = body.get("input") {
        let input = if let Some(text) = input.as_str() {
            json!([{"type":"message","role":"user","content":[{"type":"input_text","text":text}]}])
        } else {
            input.clone()
        };
        let mut items = input.as_array().cloned().unwrap_or_default();
        items.push(json!({"type":"compaction_trigger"}));
        out.insert("input".into(), Value::Array(items));
    } else {
        out.insert("input".into(), json!([{"type":"compaction_trigger"}]));
    }
    out.insert("store".into(), Value::Bool(false));
    out.insert("stream".into(), Value::Bool(true));
    Ok(Value::Object(out))
}

/// Convert an aggregated Responses result into the public compact resource.
pub fn compaction_resource(aggregated: &Value) -> Value {
    let output = aggregated
        .get("output")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|item| {
                    matches!(item.get("type").and_then(Value::as_str), Some("compaction"))
                        || (item.get("type").and_then(Value::as_str) == Some("message")
                            && item.get("role").and_then(Value::as_str) == Some("user"))
                })
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut result = Map::new();
    result.insert(
        "id".into(),
        aggregated.get("id").cloned().unwrap_or(Value::Null),
    );
    result.insert("object".into(), Value::String("response.compaction".into()));
    result.insert("output".into(), Value::Array(output));
    result.insert(
        "created_at".into(),
        aggregated
            .get("created_at")
            .cloned()
            .unwrap_or(Value::from(0)),
    );
    result.insert(
        "usage".into(),
        aggregated
            .get("usage")
            .cloned()
            .unwrap_or_else(|| json!({})),
    );
    if let Some(image) = aggregated.get("image") {
        result.insert("image".into(), image.clone());
    }
    Value::Object(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn string_input_is_wrapped_and_triggered() {
        let v = compact_request_to_responses(&json!({"model":"m","input":"hi"})).unwrap();
        assert_eq!(
            v["input"].as_array().unwrap().last().unwrap()["type"],
            "compaction_trigger"
        );
        assert_eq!(v["input"][0]["role"], "user");
        assert_eq!(v["store"], false);
        assert_eq!(v["stream"], true);
    }
    #[test]
    fn previous_response_is_rejected() {
        let e = compact_request_to_responses(&json!({"model":"m","previous_response_id":"r"}))
            .unwrap_err();
        assert_eq!(e.param, "previous_response_id");
    }
    #[test]
    fn resource_keeps_user_messages_and_compaction() {
        let v = compaction_resource(
            &json!({"id":"r","created_at":3,"usage":{"total_tokens":2},"output":[{"type":"message","role":"assistant"},{"type":"message","role":"user"},{"type":"compaction","id":"c","encrypted_content":"e"}]}),
        );
        assert_eq!(v["object"], "response.compaction");
        assert_eq!(v["output"].as_array().unwrap().len(), 2);
    }
}
