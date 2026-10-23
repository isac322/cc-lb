use http::Method;
use serde_json::Value;

/// Provider operations that may be routed across credentials and upstreams.
/// Provider-owned server state inside these operations is preserved and pinned
/// through upstream affinity; account-bound operations have no capability here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderCapability {
    AnthropicMessages,
    OpenaiResponses,
    GeminiGenerateContent,
}

/// Classifies a downstream operation against the consistency-safe routing
pub(crate) fn classify(method: &Method, path: &str) -> Option<ProviderCapability> {
    if method != Method::POST {
        return None;
    }
    match path {
        "/v1/messages" | "/v1/messages/count_tokens" => Some(ProviderCapability::AnthropicMessages),
        "/v1/responses" => Some(ProviderCapability::OpenaiResponses),
        path if is_gemini_generation_path(path) => Some(ProviderCapability::GeminiGenerateContent),
        _ => None,
    }
}

/// Returns whether a classified provider generation request body may be
/// forwarded. Provider-owned state (`previous_response_id`, `conversation`,
/// `store: true`, `cachedContent`, `thoughtSignature`, Anthropic
/// files/containers) is always forwarded unchanged, including malformed or
/// locally unhashable shapes: the provider, not cc-lb, classifies malformed
/// input. Affinity extraction may yield no key for such references, but the
/// supported operation is never refused here.
pub(crate) fn body_is_consistency_safe(_capability: ProviderCapability, _body: &Value) -> bool {
    true
}

fn is_gemini_generation_path(path: &str) -> bool {
    let model_path = path.strip_prefix("/v1beta/models/").unwrap_or_default();
    let model = model_path
        .strip_suffix(":generateContent")
        .or_else(|| model_path.strip_suffix(":streamGenerateContent"));
    model.is_some_and(|model| !model.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{ProviderCapability, body_is_consistency_safe, classify};
    use http::Method;
    use serde_json::json;

    #[test]
    fn allows_consistency_safe_operations() {
        assert_eq!(
            classify(&Method::POST, "/v1/messages"),
            Some(ProviderCapability::AnthropicMessages)
        );
        assert_eq!(
            classify(&Method::POST, "/v1/messages/count_tokens"),
            Some(ProviderCapability::AnthropicMessages)
        );
        assert_eq!(
            classify(&Method::POST, "/v1/responses"),
            Some(ProviderCapability::OpenaiResponses)
        );
        assert_eq!(
            classify(
                &Method::POST,
                "/v1beta/models/gemini-2.5-pro:generateContent"
            ),
            Some(ProviderCapability::GeminiGenerateContent)
        );
        assert_eq!(
            classify(
                &Method::POST,
                "/v1beta/models/gemini-2.5-pro:streamGenerateContent"
            ),
            Some(ProviderCapability::GeminiGenerateContent)
        );
    }

    #[test]
    fn rejects_account_bound_and_unknown_operations() {
        for (method, path) in [
            (Method::GET, "/v1/models"),
            (Method::GET, "/v1/models/gemini-2.5-pro"),
            (Method::GET, "/v1/files"),
            (Method::POST, "/v1/files"),
            (Method::DELETE, "/v1/files/file_abc123"),
            (Method::GET, "/v1/responses"),
            (Method::POST, "/v1/unknown"),
            (Method::POST, "/v1beta/models/gemini-2.5-pro:unknown"),
            (Method::POST, "/v1beta/models/:generateContent"),
        ] {
            assert_eq!(classify(&method, path), None, "{method} {path}");
        }
    }

    #[test]
    fn forwards_openai_state_including_malformed_references() {
        for body in [
            json!({"previous_response_id": "resp_123"}),
            json!({"conversation": "conv_123"}),
            json!({"conversation": {"id": "conv_123"}}),
            json!({"store": true}),
            json!({"previous_response_id": null, "conversation": null, "store": false}),
            json!({"previous_response_id": 7}),
            json!({"previous_response_id": ""}),
            json!({"conversation": {"name": "conv"}}),
            json!({"conversation": ["conv_123"]}),
        ] {
            assert!(
                body_is_consistency_safe(ProviderCapability::OpenaiResponses, &body),
                "{body}"
            );
        }
    }

    #[test]
    fn forwards_gemini_and_anthropic_state_without_recursive_rejection() {
        for (capability, body) in [
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"cachedContent": "cachedContents/abc"}),
            ),
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"contents": [{"parts": [{"thoughtSignature": "sig", "metadata": {"cachedContent": 1}}]}]}),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"messages": [{"content": [{"type": "document", "source": {"type": "file", "file_id": "file_123"}}]}]}),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"container": "container_123"}),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"container": {"id": "container_123", "skills": []}}),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"container": {"skills": [{"type": "anthropic", "skill_id": "pptx"}]}}),
            ),
        ] {
            assert!(body_is_consistency_safe(capability, &body), "{body}");
        }
        for (capability, body) in [
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"cachedContent": {"name": "cachedContents/abc"}}),
            ),
            (
                ProviderCapability::GeminiGenerateContent,
                json!({"cachedContent": ""}),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"container": {"id": 1}}),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!({"container": 7}),
            ),
            (
                ProviderCapability::AnthropicMessages,
                json!("not an object"),
            ),
        ] {
            assert!(
                body_is_consistency_safe(capability, &body),
                "malformed provider state must be forwarded: {body}"
            );
        }
    }

    #[test]
    fn allows_transcripts_tools_and_provider_safe_generation_options() {
        assert!(body_is_consistency_safe(
            ProviderCapability::AnthropicMessages,
            &json!({"messages": [{"role": "user", "content": "full transcript"}], "tools": [{"name": "lookup"}]})
        ));
        assert!(body_is_consistency_safe(
            ProviderCapability::OpenaiResponses,
            &json!({
                "input": [{"type": "function_call", "name": "lookup"}],
                "prompt_cache_key": "cache-key",
                "store": false,
                "previous_response_id": null
            })
        ));
        assert!(body_is_consistency_safe(
            ProviderCapability::GeminiGenerateContent,
            &json!({"contents": [{"parts": [{"text": "hello"}]}], "generationConfig": {"temperature": 0.2}, "systemInstruction": {"parts": [{"text": "be concise"}]}})
        ));
    }
}
