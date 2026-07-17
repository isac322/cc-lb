use std::path::Path;

use serde::Serialize;
use serde_json::Value;

pub fn write_redacted_json(path: &Path, evidence: &impl Serialize) -> Result<(), String> {
    let value =
        serde_json::to_value(evidence).map_err(|error| format!("serialize evidence: {error}"))?;
    write_redacted_value(path, value)
}

pub fn write_redacted_value(path: &Path, value: Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("create evidence directory: {error}"))?;
    }
    let serialized = serialize_redacted_value(value)?;
    std::fs::write(path, serialized).map_err(|error| format!("write evidence: {error}"))
}

pub fn serialize_redacted_value(mut value: Value) -> Result<String, String> {
    redact_value(&mut value);
    serde_json::to_string_pretty(&value)
        .map_err(|error| format!("serialize redacted evidence: {error}"))
}

fn redact_value(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(redact_value),
        Value::Object(values) => {
            for (name, nested) in values {
                redact_value(nested);
                if sensitive_name(name) && nested.is_string() {
                    *nested = Value::String("[REDACTED]".to_owned());
                }
            }
        }
        Value::String(text) if contains_secret(text) => {
            *text = "[REDACTED]".to_owned();
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.contains("key")
        || name.contains("token")
        || name.contains("secret")
        || matches!(name.as_str(), "authorization" | "password")
}

fn contains_secret(value: &str) -> bool {
    value.contains("sk-") || value.contains("Bearer ")
}
