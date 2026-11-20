use serde_json::{Value, json};

/// Client-version value cc-lb adds when a public client omits it.
pub const CODEX_CATALOG_CLIENT_VERSION: &str = "0.171.0";

/// True when the request query already carries client_version (Codex): passthrough unchanged.
pub fn is_codex_catalog_query(query: Option<&str>) -> bool {
    query.unwrap_or("").split('&').any(|part| {
        part.split_once('=')
            .map_or(part == "client_version", |(key, _)| key == "client_version")
    })
}

/// Query string to send upstream for a public client (preserve other params, append client_version).
pub fn catalog_query_for_public(query: Option<&str>) -> String {
    let query = query.unwrap_or("");
    if is_codex_catalog_query(Some(query)) {
        query.to_owned()
    } else if query.is_empty() {
        format!("client_version={CODEX_CATALOG_CLIENT_VERSION}")
    } else {
        format!("{query}&client_version={CODEX_CATALOG_CLIENT_VERSION}")
    }
}

/// Convert a Codex catalog response to the public model-list shape.
pub fn public_model_list(catalog: &Value) -> Option<Value> {
    let models = catalog.get("models")?.as_array()?;
    let mut data = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for model in models {
        let Some(slug) = model.get("slug").and_then(Value::as_str) else {
            continue;
        };
        if seen.insert(slug) {
            data.push(json!({"id": slug, "object": "model", "created": 0, "owned_by": "openai"}));
        }
    }
    Some(json!({"object": "list", "data": data}))
}

/// Convert one catalog entry to the public model shape.
pub fn public_model(catalog: &Value, id: &str) -> Option<Value> {
    let models = catalog.get("models")?.as_array()?;
    models
        .iter()
        .find(|model| model.get("slug").and_then(Value::as_str) == Some(id))
        .map(|_| json!({"id": id, "object": "model", "created": 0, "owned_by": "openai"}))
}

pub fn model_not_found_body(id: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"error": {"message": format!("The model '{id}' does not exist"), "type": "invalid_request_error", "param": "model", "code": "model_not_found"}})).expect("model error is serializable")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn query_detection_and_building() {
        assert!(is_codex_catalog_query(Some("foo=1&client_version=9")));
        assert!(is_codex_catalog_query(Some("client_version")));
        assert_eq!(
            catalog_query_for_public(Some("foo=1")),
            "foo=1&client_version=0.171.0"
        );
        assert_eq!(catalog_query_for_public(None), "client_version=0.171.0");
    }
    #[test]
    fn list_dedupes_and_skips_missing() {
        let c = json!({"models":[{"slug":"a"},{"x":1},{"slug":"a"},{"slug":"b"}]});
        assert_eq!(
            public_model_list(&c).unwrap()["data"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
    #[test]
    fn not_found_body_is_public_envelope() {
        let v: Value = serde_json::from_slice(&model_not_found_body("x")).unwrap();
        assert_eq!(v["error"]["code"], "model_not_found");
    }
}
