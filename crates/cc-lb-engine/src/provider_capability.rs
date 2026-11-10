use cc_lb_domain::Provider;
use http::Method;
use serde_json::Value;

/// Provider operations that may be routed across credentials and upstreams.
/// Provider-owned server state inside these operations is preserved and pinned
/// through upstream affinity; account-bound operations have no capability here.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderCapability {
    AnthropicMessages,
    AnthropicCountTokens,
    OpenaiResponses,
    GeminiGenerateContent,
    GeminiCountTokens,
    GeminiCachedContentsCreate,
    AnthropicModels,
    AnthropicComplete,
    OpenaiResponsesInputTokens,
    OpenaiResponsesCompact,
    OpenaiChatCompletions,
    OpenaiCompletions,
    OpenaiEmbeddings,
    OpenaiModerations,
    OpenaiAudio,
    OpenaiImages,
    OpenaiModels,
    OpenaiCodexAuxiliary,
    GeminiEmbedContent,
    GeminiPredict,
    GeminiModels,
    GeminiOpenaiChatCompletions,
    GeminiOpenaiEmbeddings,
    GeminiOpenaiImages,
    GeminiOpenaiModels,
}

/// Wire-level usage formats, independent of the credential used to dispatch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UsageShape {
    AnthropicMessages,
    OpenaiResponses,
    OpenaiChatCompletions,
    OpenaiCompletions,
    OpenaiEmbeddings,
    OpenaiAudio,
    OpenaiImages,
    GeminiGenerateContent,
    GeminiEmbed,
    None,
}

impl ProviderCapability {
    pub(crate) const fn provider(self) -> Provider {
        match self {
            Self::AnthropicMessages
            | Self::AnthropicCountTokens
            | Self::AnthropicModels
            | Self::AnthropicComplete => Provider::Anthropic,
            Self::OpenaiResponses
            | Self::OpenaiResponsesInputTokens
            | Self::OpenaiResponsesCompact
            | Self::OpenaiChatCompletions
            | Self::OpenaiCompletions
            | Self::OpenaiEmbeddings
            | Self::OpenaiModerations
            | Self::OpenaiAudio
            | Self::OpenaiImages
            | Self::OpenaiModels
            | Self::OpenaiCodexAuxiliary => Provider::Openai,
            Self::GeminiGenerateContent
            | Self::GeminiCountTokens
            | Self::GeminiCachedContentsCreate
            | Self::GeminiEmbedContent
            | Self::GeminiPredict
            | Self::GeminiModels
            | Self::GeminiOpenaiChatCompletions
            | Self::GeminiOpenaiEmbeddings
            | Self::GeminiOpenaiImages
            | Self::GeminiOpenaiModels => Provider::Gemini,
        }
    }

    pub(crate) const fn usage_shape(self) -> UsageShape {
        match self {
            Self::AnthropicMessages => UsageShape::AnthropicMessages,
            Self::OpenaiResponses | Self::OpenaiResponsesCompact => UsageShape::OpenaiResponses,
            Self::OpenaiChatCompletions | Self::GeminiOpenaiChatCompletions => {
                UsageShape::OpenaiChatCompletions
            }
            Self::OpenaiCompletions => UsageShape::OpenaiCompletions,
            Self::OpenaiEmbeddings | Self::GeminiOpenaiEmbeddings => UsageShape::OpenaiEmbeddings,
            Self::OpenaiAudio => UsageShape::OpenaiAudio,
            Self::OpenaiImages => UsageShape::OpenaiImages,
            Self::GeminiGenerateContent => UsageShape::GeminiGenerateContent,
            Self::GeminiEmbedContent => UsageShape::GeminiEmbed,
            Self::AnthropicCountTokens
            | Self::AnthropicModels
            | Self::AnthropicComplete
            | Self::OpenaiResponsesInputTokens
            | Self::OpenaiModerations
            | Self::OpenaiModels
            | Self::OpenaiCodexAuxiliary
            | Self::GeminiCountTokens
            | Self::GeminiCachedContentsCreate
            | Self::GeminiPredict
            | Self::GeminiModels
            | Self::GeminiOpenaiImages
            | Self::GeminiOpenaiModels => UsageShape::None,
        }
    }
}

/// New operations require an explicit matching provider scope. Shared legacy
/// routes retain their original operation set.
pub(crate) fn classify(
    method: &Method,
    path: &str,
    scope: Option<Provider>,
) -> Option<ProviderCapability> {
    use ProviderCapability as C;
    let legacy = if method == Method::POST {
        match path {
            "/v1/responses" => Some(C::OpenaiResponses),
            "/v1/messages" => Some(C::AnthropicMessages),
            "/v1/messages/count_tokens" => Some(C::AnthropicCountTokens),
            path if legacy_gemini_action(path, "countTokens") => Some(C::GeminiCountTokens),
            path if legacy_gemini_action(path, "generateContent")
                || legacy_gemini_action(path, "streamGenerateContent") =>
            {
                Some(C::GeminiGenerateContent)
            }
            _ => None,
        }
    } else {
        None
    };
    if let Some(capability) = legacy {
        return scope
            .is_none_or(|provider| provider == capability.provider())
            .then_some(capability);
    }
    match scope? {
        Provider::Anthropic => match (method, path) {
            (&Method::GET, path) if model_catalog(path, "/v1/models", false) => {
                Some(C::AnthropicModels)
            }
            (&Method::POST, "/v1/complete") => Some(C::AnthropicComplete),
            _ => None,
        },
        Provider::Openai => match (method, path) {
            (&Method::POST, "/v1/responses/input_tokens") => Some(C::OpenaiResponsesInputTokens),
            (&Method::POST, "/v1/responses/compact") => Some(C::OpenaiResponsesCompact),
            (&Method::POST, "/v1/chat/completions") => Some(C::OpenaiChatCompletions),
            (&Method::POST, "/v1/completions") => Some(C::OpenaiCompletions),
            (&Method::POST, "/v1/embeddings") => Some(C::OpenaiEmbeddings),
            (&Method::POST, "/v1/moderations") => Some(C::OpenaiModerations),
            (
                &Method::POST,
                "/v1/audio/speech" | "/v1/audio/transcriptions" | "/v1/audio/translations",
            ) => Some(C::OpenaiAudio),
            (
                &Method::POST,
                "/v1/images/generations" | "/v1/images/edits" | "/v1/images/variations",
            ) => Some(C::OpenaiImages),
            (&Method::GET, path) if model_catalog(path, "/v1/models", false) => {
                Some(C::OpenaiModels)
            }
            (&Method::POST, "/v1/alpha/search" | "/v1/memories/trace_summarize") => {
                Some(C::OpenaiCodexAuxiliary)
            }
            _ => None,
        },
        Provider::Gemini => match (method, path) {
            (&Method::POST, "/v1beta/cachedContents") => Some(C::GeminiCachedContentsCreate),
            (&Method::POST, path)
                if gemini_action(path, "embedContent")
                    || gemini_action(path, "batchEmbedContents") =>
            {
                Some(C::GeminiEmbedContent)
            }
            (&Method::POST, path) if gemini_action(path, "predict") => Some(C::GeminiPredict),
            (&Method::GET, path)
                if model_catalog(path, "/v1beta/models", true)
                    || model_catalog(path, "/v1/models", true) =>
            {
                Some(C::GeminiModels)
            }
            (&Method::POST, "/v1beta/openai/chat/completions") => {
                Some(C::GeminiOpenaiChatCompletions)
            }
            (&Method::POST, "/v1beta/openai/embeddings") => Some(C::GeminiOpenaiEmbeddings),
            (&Method::POST, "/v1beta/openai/images/generations") => Some(C::GeminiOpenaiImages),
            (&Method::GET, path) if model_catalog(path, "/v1beta/openai/models", true) => {
                Some(C::GeminiOpenaiModels)
            }
            _ => None,
        },
    }
}

fn model_catalog(path: &str, root: &str, reject_colon: bool) -> bool {
    path == root
        || path
            .strip_prefix(root)
            .and_then(|suffix| suffix.strip_prefix('/'))
            .is_some_and(|id| {
                !id.is_empty() && !id.contains('/') && (!reject_colon || !id.contains(':'))
            })
}

/// Returns whether a classified provider generation request body may be
/// forwarded. Provider-owned state (`previous_response_id`, `conversation`,
/// `store: true`, `cachedContent`, `thoughtSignature`, Anthropic
/// files/containers) is always forwarded unchanged, including malformed or
/// locally unhashable shapes: the provider, not cc-lb, classifies malformed
/// input. Affinity extraction may yield no key for such references, but the
/// supported operation is never refused here.
pub(crate) fn body_is_consistency_safe(capability: ProviderCapability, body: &Value) -> bool {
    capability != ProviderCapability::GeminiCachedContentsCreate
        || body
            .get("model")
            .and_then(Value::as_str)
            .is_some_and(gemini_cached_contents_model_supported)
}
/// Remove exactly one Gemini resource-model prefix from an internal identity.
///
/// The wire body and URL remain unchanged; this helper is only for provider
/// identity values used by routing, limits, pricing, and observability.
pub(crate) fn canonical_gemini_model_id(model: &str) -> &str {
    if model.starts_with("tunedModels/") || model.starts_with("models/tunedModels/") {
        return model;
    }
    model.strip_prefix("models/").unwrap_or(model)
}

#[cfg(test)]
mod canonical_model_tests {
    use super::canonical_gemini_model_id;

    #[test]
    fn strips_exactly_one_models_prefix() {
        assert_eq!(
            canonical_gemini_model_id("models/gemini-2.5-flash"),
            "gemini-2.5-flash"
        );
        assert_eq!(
            canonical_gemini_model_id("models/models/gemini-2.5-flash"),
            "models/gemini-2.5-flash"
        );
    }

    #[test]
    fn preserves_tuned_model_identities() {
        for model in ["tunedModels/custom", "models/tunedModels/custom"] {
            assert_eq!(canonical_gemini_model_id(model), model);
        }
    }

    #[test]
    fn tuned_marker_only_matches_as_leading_resource_segment() {
        assert_eq!(
            canonical_gemini_model_id("models/gemini/tunedModels/custom"),
            "gemini/tunedModels/custom"
        );
        assert_eq!(
            canonical_gemini_model_id("models/tunedModels/custom/versions/1"),
            "models/tunedModels/custom/versions/1"
        );
    }

    #[test]
    fn preserves_non_resource_gemini_identities() {
        for model in [
            "gemini-2.5-flash",
            "tunedModels/custom",
            "publishers/google/models/gemini-2.5-flash",
        ] {
            assert_eq!(canonical_gemini_model_id(model), model);
        }
    }
}

fn gemini_cached_contents_model_supported(model: &str) -> bool {
    let Some(model) = model.strip_prefix("models/") else {
        return false;
    };
    if let Some(suffix) = model.strip_prefix("gemini-2.5-") {
        return !suffix.is_empty() && !suffix.contains('/');
    }
    model == "gemini-3"
        || model.strip_prefix("gemini-3").is_some_and(|suffix| {
            suffix.starts_with(['.', '-']) && suffix.len() > 1 && !suffix.contains('/')
        })
}

fn gemini_action(path: &str, action: &str) -> bool {
    path.strip_prefix("/v1beta/models/")
        .and_then(|model_path| model_path.rsplit_once(':'))
        .is_some_and(|(model, suffix)| {
            !model.is_empty() && !model.contains(['/', ':']) && suffix == action
        })
}

fn legacy_gemini_action(path: &str, action: &str) -> bool {
    path.strip_prefix("/v1beta/models/")
        .and_then(|model_path| model_path.rsplit_once(':'))
        .is_some_and(|(model, suffix)| !model.is_empty() && suffix == action)
}

#[cfg(test)]
mod tests {
    use super::{ProviderCapability, body_is_consistency_safe, classify};
    use http::Method;
    use serde_json::json;

    #[test]
    fn legacy_operations_require_matching_scope_when_present() {
        use cc_lb_domain::Provider;
        for (path, capability) in [
            ("/v1/messages", ProviderCapability::AnthropicMessages),
            (
                "/v1/messages/count_tokens",
                ProviderCapability::AnthropicCountTokens,
            ),
            ("/v1/responses", ProviderCapability::OpenaiResponses),
            (
                "/v1beta/models/gemini-2.5-pro:generateContent",
                ProviderCapability::GeminiGenerateContent,
            ),
            (
                "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
                ProviderCapability::GeminiGenerateContent,
            ),
            (
                "/v1beta/models/gemini-2.5-pro:countTokens",
                ProviderCapability::GeminiCountTokens,
            ),
        ] {
            assert_eq!(
                classify(&Method::POST, path, None),
                Some(capability),
                "{path}"
            );
            for provider in [Provider::Anthropic, Provider::Openai, Provider::Gemini] {
                assert_eq!(
                    classify(&Method::POST, path, Some(provider)),
                    (provider == capability.provider()).then_some(capability),
                    "{provider:?} {path}"
                );
            }
            assert_eq!(classify(&Method::GET, path, None), None, "{path}");
        }
    }

    #[test]
    fn new_operations_are_scoped_and_method_specific() {
        use ProviderCapability as C;
        use cc_lb_domain::Provider;
        for (method, path, capability) in [
            (Method::GET, "/v1/models", C::AnthropicModels),
            (Method::GET, "/v1/models/claude", C::AnthropicModels),
            (Method::POST, "/v1/complete", C::AnthropicComplete),
            (
                Method::POST,
                "/v1/responses/input_tokens",
                C::OpenaiResponsesInputTokens,
            ),
            (
                Method::POST,
                "/v1/responses/compact",
                C::OpenaiResponsesCompact,
            ),
            (
                Method::POST,
                "/v1/chat/completions",
                C::OpenaiChatCompletions,
            ),
            (Method::POST, "/v1/completions", C::OpenaiCompletions),
            (Method::POST, "/v1/embeddings", C::OpenaiEmbeddings),
            (Method::POST, "/v1/moderations", C::OpenaiModerations),
            (Method::POST, "/v1/audio/speech", C::OpenaiAudio),
            (Method::POST, "/v1/audio/transcriptions", C::OpenaiAudio),
            (Method::POST, "/v1/audio/translations", C::OpenaiAudio),
            (Method::POST, "/v1/images/generations", C::OpenaiImages),
            (Method::POST, "/v1/images/edits", C::OpenaiImages),
            (Method::POST, "/v1/images/variations", C::OpenaiImages),
            (Method::GET, "/v1/models", C::OpenaiModels),
            (Method::GET, "/v1/models/gpt", C::OpenaiModels),
            (Method::POST, "/v1/alpha/search", C::OpenaiCodexAuxiliary),
            (
                Method::POST,
                "/v1/memories/trace_summarize",
                C::OpenaiCodexAuxiliary,
            ),
            (
                Method::POST,
                "/v1beta/cachedContents",
                C::GeminiCachedContentsCreate,
            ),
            (
                Method::POST,
                "/v1beta/models/gemini:embedContent",
                C::GeminiEmbedContent,
            ),
            (
                Method::POST,
                "/v1beta/models/gemini:batchEmbedContents",
                C::GeminiEmbedContent,
            ),
            (
                Method::POST,
                "/v1beta/models/imagen:predict",
                C::GeminiPredict,
            ),
            (Method::GET, "/v1beta/models", C::GeminiModels),
            (Method::GET, "/v1beta/models/gemini", C::GeminiModels),
            (Method::GET, "/v1/models", C::GeminiModels),
            (Method::GET, "/v1/models/gemini", C::GeminiModels),
            (
                Method::POST,
                "/v1beta/openai/chat/completions",
                C::GeminiOpenaiChatCompletions,
            ),
            (
                Method::POST,
                "/v1beta/openai/embeddings",
                C::GeminiOpenaiEmbeddings,
            ),
            (
                Method::POST,
                "/v1beta/openai/images/generations",
                C::GeminiOpenaiImages,
            ),
            (Method::GET, "/v1beta/openai/models", C::GeminiOpenaiModels),
            (
                Method::GET,
                "/v1beta/openai/models/gemini",
                C::GeminiOpenaiModels,
            ),
        ] {
            assert_eq!(
                classify(&method, path, None),
                None,
                "unscoped {method} {path}"
            );
            assert_eq!(
                classify(&method, path, Some(capability.provider())),
                Some(capability),
                "{method} {path}"
            );
            for provider in [Provider::Anthropic, Provider::Openai, Provider::Gemini] {
                if provider != capability.provider()
                    && !(method == Method::GET && path.starts_with("/v1/models"))
                {
                    assert_eq!(
                        classify(&method, path, Some(provider)),
                        None,
                        "{provider:?} {path}"
                    );
                }
            }
            assert_eq!(
                classify(&Method::DELETE, path, Some(capability.provider())),
                None,
                "DELETE {path}"
            );
        }
    }

    #[test]
    fn model_catalog_colon_rules_follow_provider_syntax() {
        use ProviderCapability as C;
        use cc_lb_domain::Provider;

        assert_eq!(
            classify(
                &Method::GET,
                "/v1/models/ft:gpt-4o-mini:org::abc",
                Some(Provider::Openai),
            ),
            Some(C::OpenaiModels)
        );
        assert_eq!(
            classify(
                &Method::GET,
                "/v1/models/claude-3-7-sonnet:custom",
                Some(Provider::Anthropic),
            ),
            Some(C::AnthropicModels)
        );
        for path in [
            "/v1beta/models/gemini-2.5-pro:custom",
            "/v1/models/gemini-2.5-pro:custom",
            "/v1beta/openai/models/gemini-2.5-pro:custom",
        ] {
            assert_eq!(
                classify(&Method::GET, path, Some(Provider::Gemini)),
                None,
                "{path}"
            );
        }
    }

    #[test]
    fn rejects_account_bound_unknown_and_malformed_operations() {
        use cc_lb_domain::Provider;
        for (provider, method, path) in [
            (Provider::Anthropic, Method::POST, "/v1/files"),
            (Provider::Anthropic, Method::POST, "/v1/messages/batches"),
            (Provider::Anthropic, Method::GET, "/v1/skills"),
            (Provider::Anthropic, Method::GET, "/v1/agents"),
            (Provider::Anthropic, Method::GET, "/v1/sessions"),
            (Provider::Openai, Method::POST, "/v1/files"),
            (Provider::Openai, Method::POST, "/v1/uploads"),
            (Provider::Openai, Method::POST, "/v1/batches"),
            (Provider::Openai, Method::POST, "/v1/vector_stores"),
            (Provider::Openai, Method::POST, "/v1/conversations"),
            (Provider::Openai, Method::POST, "/v1/containers"),
            (Provider::Openai, Method::GET, "/v1/chat/completions/chat_1"),
            (
                Provider::Openai,
                Method::POST,
                "/v1/chat/completions/chat_1",
            ),
            (
                Provider::Openai,
                Method::DELETE,
                "/v1/chat/completions/chat_1",
            ),
            (Provider::Openai, Method::GET, "/v1/responses/resp_1"),
            (Provider::Openai, Method::DELETE, "/v1/responses/resp_1"),
            (
                Provider::Openai,
                Method::POST,
                "/v1/responses/resp_1/cancel",
            ),
            (
                Provider::Openai,
                Method::GET,
                "/v1/responses/resp_1/input_items",
            ),
            (Provider::Openai, Method::POST, "/v1/realtime/calls"),
            (
                Provider::Openai,
                Method::POST,
                "/v1/realtime/client_secrets",
            ),
            (Provider::Openai, Method::POST, "/v1/videos"),
            (Provider::Openai, Method::GET, "/v1/audio/voices"),
            (Provider::Gemini, Method::POST, "/upload/v1beta/files"),
            (Provider::Gemini, Method::GET, "/v1beta/files"),
            (Provider::Gemini, Method::GET, "/download/v1beta/files/file"),
            (Provider::Gemini, Method::POST, "/v1beta/batches"),
            (Provider::Gemini, Method::GET, "/v1beta/operations/id"),
            (
                Provider::Gemini,
                Method::POST,
                "/v1beta/models/veo:predictLongRunning",
            ),
            (Provider::Gemini, Method::POST, "/v1beta/fileSearchStores"),
            (Provider::Gemini, Method::POST, "/v1beta/interactions"),
            (Provider::Gemini, Method::POST, "/v1beta/agents"),
            (Provider::Gemini, Method::POST, "/v1beta/auth_tokens"),
            (Provider::Gemini, Method::POST, "/v1beta/openai/files"),
            (Provider::Gemini, Method::POST, "/v1beta/openai/batches"),
            (Provider::Gemini, Method::POST, "/v1internal:loadCodeAssist"),
            (Provider::Gemini, Method::POST, "/v1internal:onboardUser"),
            (
                Provider::Gemini,
                Method::POST,
                "/v1internal:retrieveUserQuotaSummary",
            ),
            (
                Provider::Gemini,
                Method::POST,
                "/v1internal:listExperiments",
            ),
            (
                Provider::Gemini,
                Method::POST,
                "/v1beta/models/:generateContent",
            ),
            (Provider::Gemini, Method::POST, "/v1beta/models/a/b:predict"),
            (
                Provider::Gemini,
                Method::POST,
                "/v1beta/models/gemini:unknown",
            ),
            (Provider::Openai, Method::GET, "/v1/models/gpt/nested"),
        ] {
            assert_eq!(
                classify(&method, path, Some(provider)),
                None,
                "{method} {path}"
            );
            assert_eq!(
                classify(&method, path, None),
                None,
                "unscoped {method} {path}"
            );
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
    #[test]
    fn cached_contents_accepts_only_supported_gemini_model_families() {
        for model in [
            "models/gemini-2.5-flash",
            "models/gemini-2.5-pro",
            "models/gemini-3",
            "models/gemini-3.1-pro",
            "models/gemini-3-flash",
        ] {
            assert!(body_is_consistency_safe(
                ProviderCapability::GeminiCachedContentsCreate,
                &json!({"model": model}),
            ));
        }
        for model in [
            "gemini-2.5-flash",
            "models/gemini-4-pro",
            "models/claude-3",
            "models/gemini-2.5-flash/nested",
            "models/gemini-3x",
        ] {
            assert!(!body_is_consistency_safe(
                ProviderCapability::GeminiCachedContentsCreate,
                &json!({"model": model}),
            ));
        }
    }
}
