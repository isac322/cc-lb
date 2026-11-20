//! Public `/v1` compatibility policy for ChatGPT-subscription (`openai_oauth`)
//! upstreams.
//!
//! The Codex backend rejects or ignores parts of the public OpenAI request
//! contract. Each knob below chooses how cc-lb handles one such difference.
//! The server configuration supplies [`OpenaiOauthCompatPolicy`]; each
//! principal may carry an [`OpenaiOauthCompatOverride`] whose set fields win.
//! Requests that Codex sends in the backend shape never trigger any knob.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A parameter the backend does not accept: drop it, or reject the request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DropOrReject {
    /// Remove the parameter and serve the request.
    #[default]
    Drop,
    /// Answer 400 `invalid_request_error` naming the parameter.
    Reject,
}

/// A value the backend does not accept: send the nearest supported value, or
/// reject the request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CoerceOrReject {
    /// Replace the value with the nearest supported one.
    #[default]
    Coerce,
    /// Answer 400 `invalid_request_error` naming the parameter.
    Reject,
}

/// A feature cc-lb can emulate on top of the backend, or reject.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmulateOrReject {
    /// Emulate the feature.
    #[default]
    Emulate,
    /// Answer 400 `invalid_request_error` naming the parameter.
    Reject,
}

/// A feature cc-lb can emulate, ignore, or reject.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EmulateDropOrReject {
    /// Emulate the feature.
    #[default]
    Emulate,
    /// Remove the parameter and serve the request without the feature.
    Drop,
    /// Answer 400 `invalid_request_error` naming the parameter.
    Reject,
}

/// Handling of request `metadata`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MetadataHandling {
    /// Remove it upstream and copy the client's value into the response object.
    #[default]
    DropEcho,
    /// Remove it upstream; the response carries no client metadata.
    Drop,
    /// Answer 400 `invalid_request_error` naming the parameter.
    Reject,
}

/// Handling of hosted tools that only an API-key account can run
/// (`file_search`, `code_interpreter`) when no `openai_api_key` upstream can
/// serve the request.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HostedToolHandling {
    /// Remove the tool, and release a `tool_choice` that forces it.
    #[default]
    DropTool,
    /// Answer 400 `invalid_request_error` naming the tool.
    Reject,
}

/// Whether an optional subscription-backed operation is served.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnabledOrDisabled {
    /// The operation is not served by subscription upstreams.
    #[default]
    Disabled,
    /// The operation is served by subscription upstreams.
    Enabled,
}

/// Effective compatibility policy. Every field is concrete.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct OpenaiOauthCompatPolicy {
    /// `store: true`: coerce to `false`, or reject.
    pub store_true: CoerceOrReject,
    /// `temperature`, `top_p`.
    pub sampling: DropOrReject,
    /// `top_logprobs` and `include: ["message.output_text.logprobs"]`.
    pub logprobs: DropOrReject,
    /// `truncation: "auto"`: emulate by trimming the oldest items and
    /// retrying on `context_length_exceeded`, drop, or reject.
    pub truncation_auto: EmulateDropOrReject,
    /// `prompt_cache_retention: "in_memory"` (the backend retains up to 24h).
    pub prompt_cache_in_memory: DropOrReject,
    /// `service_tier` `auto` or `flex`: coerce to `default`, or reject.
    pub service_tier: CoerceOrReject,
    /// `web_search_preview` tool: coerce to `web_search`, or reject.
    pub web_search_preview: CoerceOrReject,
    /// `metadata`, `user`, `safety_identifier`.
    pub metadata: MetadataHandling,
    /// `max_tool_calls`.
    pub max_tool_calls: DropOrReject,
    /// `background: true` when no API-key upstream can serve it.
    pub background: DropOrReject,
    /// `file_search` / `code_interpreter` when no API-key upstream can serve it.
    pub hosted_tools: HostedToolHandling,
    /// `POST /v1/responses/compact`.
    pub compact: EmulateOrReject,
    /// Chat Completions `n > 1`: fan out `n` requests, or reject.
    pub chat_n: EmulateOrReject,
    /// Chat Completions `stop`.
    pub chat_stop: EmulateDropOrReject,
    /// Chat Completions `seed`, `frequency_penalty`, `presence_penalty`.
    pub chat_seed_penalties: DropOrReject,
    /// `POST /v1/audio/transcriptions` through the subscription dictation
    /// endpoint. The upstream retains submitted audio for 30 days.
    pub transcription: EnabledOrDisabled,
}

/// Per-principal override. Unset fields inherit the server policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct OpenaiOauthCompatOverride {
    /// See [`OpenaiOauthCompatPolicy::store_true`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_true: Option<CoerceOrReject>,
    /// See [`OpenaiOauthCompatPolicy::sampling`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling: Option<DropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::logprobs`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<DropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::truncation_auto`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_auto: Option<EmulateDropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::prompt_cache_in_memory`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_in_memory: Option<DropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::service_tier`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<CoerceOrReject>,
    /// See [`OpenaiOauthCompatPolicy::web_search_preview`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub web_search_preview: Option<CoerceOrReject>,
    /// See [`OpenaiOauthCompatPolicy::metadata`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataHandling>,
    /// See [`OpenaiOauthCompatPolicy::max_tool_calls`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tool_calls: Option<DropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::background`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub background: Option<DropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::hosted_tools`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hosted_tools: Option<HostedToolHandling>,
    /// See [`OpenaiOauthCompatPolicy::compact`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compact: Option<EmulateOrReject>,
    /// See [`OpenaiOauthCompatPolicy::chat_n`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_n: Option<EmulateOrReject>,
    /// See [`OpenaiOauthCompatPolicy::chat_stop`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_stop: Option<EmulateDropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::chat_seed_penalties`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_seed_penalties: Option<DropOrReject>,
    /// See [`OpenaiOauthCompatPolicy::transcription`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<EnabledOrDisabled>,
}

impl OpenaiOauthCompatOverride {
    /// The effective policy: each set field wins over `server`.
    pub fn resolve(&self, server: &OpenaiOauthCompatPolicy) -> OpenaiOauthCompatPolicy {
        OpenaiOauthCompatPolicy {
            store_true: self.store_true.unwrap_or(server.store_true),
            sampling: self.sampling.unwrap_or(server.sampling),
            logprobs: self.logprobs.unwrap_or(server.logprobs),
            truncation_auto: self.truncation_auto.unwrap_or(server.truncation_auto),
            prompt_cache_in_memory: self
                .prompt_cache_in_memory
                .unwrap_or(server.prompt_cache_in_memory),
            service_tier: self.service_tier.unwrap_or(server.service_tier),
            web_search_preview: self.web_search_preview.unwrap_or(server.web_search_preview),
            metadata: self.metadata.unwrap_or(server.metadata),
            max_tool_calls: self.max_tool_calls.unwrap_or(server.max_tool_calls),
            background: self.background.unwrap_or(server.background),
            hosted_tools: self.hosted_tools.unwrap_or(server.hosted_tools),
            compact: self.compact.unwrap_or(server.compact),
            chat_n: self.chat_n.unwrap_or(server.chat_n),
            chat_stop: self.chat_stop.unwrap_or(server.chat_stop),
            chat_seed_penalties: self
                .chat_seed_penalties
                .unwrap_or(server.chat_seed_penalties),
            transcription: self.transcription.unwrap_or(server.transcription),
        }
    }

    /// True when no field is set.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_decided_behavior() {
        let policy = OpenaiOauthCompatPolicy::default();
        assert_eq!(policy.store_true, CoerceOrReject::Coerce);
        assert_eq!(policy.sampling, DropOrReject::Drop);
        assert_eq!(policy.truncation_auto, EmulateDropOrReject::Emulate);
        assert_eq!(policy.metadata, MetadataHandling::DropEcho);
        assert_eq!(policy.hosted_tools, HostedToolHandling::DropTool);
        assert_eq!(policy.compact, EmulateOrReject::Emulate);
        assert_eq!(policy.chat_n, EmulateOrReject::Emulate);
        assert_eq!(policy.chat_stop, EmulateDropOrReject::Emulate);
        assert_eq!(policy.transcription, EnabledOrDisabled::Disabled);
    }

    #[test]
    fn set_override_fields_win_and_unset_fields_inherit() {
        let server = OpenaiOauthCompatPolicy {
            sampling: DropOrReject::Reject,
            ..OpenaiOauthCompatPolicy::default()
        };
        let principal = OpenaiOauthCompatOverride {
            store_true: Some(CoerceOrReject::Reject),
            transcription: Some(EnabledOrDisabled::Enabled),
            ..OpenaiOauthCompatOverride::default()
        };
        let effective = principal.resolve(&server);
        assert_eq!(effective.store_true, CoerceOrReject::Reject);
        assert_eq!(effective.transcription, EnabledOrDisabled::Enabled);
        assert_eq!(effective.sampling, DropOrReject::Reject);
        assert_eq!(effective.compact, EmulateOrReject::Emulate);
    }

    #[test]
    fn override_round_trips_without_unset_fields() {
        let principal = OpenaiOauthCompatOverride {
            chat_n: Some(EmulateOrReject::Reject),
            ..OpenaiOauthCompatOverride::default()
        };
        let json = serde_json::to_string(&principal).expect("serializes");
        assert_eq!(json, r#"{"chat_n":"reject"}"#);
        let back: OpenaiOauthCompatOverride = serde_json::from_str(&json).expect("parses");
        assert_eq!(back, principal);
        assert!(serde_json::from_str::<OpenaiOauthCompatOverride>(r#"{"chat_n":"drop"}"#).is_err());
        assert!(serde_json::from_str::<OpenaiOauthCompatOverride>(r#"{"bogus":"drop"}"#).is_err());
    }
}
