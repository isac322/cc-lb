use extism_pdk::*;
use serde_json::{json, Value};

thread_local! {
    static COUNTER: std::cell::RefCell<usize> = std::cell::RefCell::new(0);
}

#[plugin_fn]
pub fn route(Json(input): Json<Value>) -> FnResult<Json<Value>> {
    let candidates = match input.get("candidates").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => {
            return Ok(Json(json!({
                "upstream_id": Value::Null,
                "dialect": "self_plugin",
                "upstream": "AnthropicDirect"
            })));
        }
    };

    if candidates.is_empty() {
        return Ok(Json(json!({
            "upstream_id": Value::Null,
            "dialect": "self_plugin",
            "upstream": "AnthropicDirect"
        })));
    }

    let idx = COUNTER.with(|c| {
        let mut counter = c.borrow_mut();
        let index = *counter % candidates.len();
        *counter = counter.wrapping_add(1);
        index
    });

    let selected_upstream_id = candidates
        .get(idx)
        .and_then(|candidate| candidate.get("upstream_id"))
        .cloned()
        .unwrap_or(Value::Null);

    Ok(Json(json!({
        "upstream_id": selected_upstream_id,
        "dialect": "self_plugin",
        "upstream": "AnthropicDirect"
    })))
}
