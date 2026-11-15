//! Schema-only FetchAvailableModels projection. Transport, credentials and
//! resolved-target selection remain the authenticated executor's responsibility.
use serde_json::{Value, json};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum CatalogError {
    InvalidModels,
}

pub(crate) fn project_fetch_available_models(raw: &Value) -> Result<Value, CatalogError> {
    let models = raw
        .get("models")
        .and_then(Value::as_object)
        .ok_or(CatalogError::InvalidModels)?;
    let mut native = Vec::with_capacity(models.len());
    for (id, details) in models {
        let id = id.strip_prefix("models/").unwrap_or(id);
        if id.is_empty() {
            continue;
        }
        if !details.is_object() {
            return Err(CatalogError::InvalidModels);
        }
        let mut model = json!({"name": format!("models/{id}")});
        for field in ["displayName", "description"] {
            if let Some(value) = details.get(field).and_then(Value::as_str) {
                model[field] = value.into();
            }
        }
        native.push(model);
    }
    Ok(json!({"models": native}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_descriptor_map_lists_and_unknown_fields_off_wire() {
        let raw = json!({
            "models": {"model": {"displayName": "Model", "description": "Description",
                "supportsImages": true, "maxTokens": 123, "maxOutputTokens": 45,
                "vertexModelId": "vertex", "modelExperimentIds": ["experiment"],
                "futureSupport": {"nested": true}}},
            "defaultAgentModelId": "model", "agentModelSorts": [{"futureSort": 1}],
            "commandModelIds": ["model"], "tabModelIds": [],
            "imageGenerationModelIds": [], "mqueryModelIds": [], "webSearchModelIds": [],
            "commitMessageModelIds": [], "audioTranscriptionModelIds": [],
            "deprecatedModelIds": {"old": {"newModelId": "model"}}, "tieredModelIds": {"flash": ["model"]},
            "experimentIds": [123], "sharedModelExperiments": {"shared": {}},
            "groupedModels": {"group": {}}, "futureField": [1, {"x": true}]
        });
        let native = project_fetch_available_models(&raw).unwrap();
        assert_eq!(
            native,
            json!({"models": [{
                "name": "models/model", "displayName": "Model", "description": "Description"
            }]})
        );
    }

    #[test]
    fn separates_list_model_configs_and_array_models() {
        for raw in [
            json!({"modelConfigs": []}),
            json!({"models": []}),
            json!({"models": {"bad": []}}),
        ] {
            assert!(matches!(
                project_fetch_available_models(&raw),
                Err(CatalogError::InvalidModels)
            ));
        }
    }

    #[test]
    fn map_keys_supply_names_without_vertex_aliases_or_fake_fields() {
        let native = project_fetch_available_models(&json!({"models": {
            "": {}, "models/": {}, "models/model": {
                "vertexModelId": "other", "displayName": 1, "description": null,
                "supportedGenerationMethods": ["generateContent"], "inputTokenLimit": 123
            }
        }}))
        .unwrap();
        assert_eq!(native, json!({"models": [{"name": "models/model"}]}));
    }
}
