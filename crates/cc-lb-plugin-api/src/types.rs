//! Shared public data types for plugin boundaries.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[cfg(test)]
use uuid::Uuid;

#[doc(hidden)]
pub use cc_lb_domain::{
    BreakpointOrigin, CacheAffinityCandidate, CacheAffinityTrace, CacheBreakpoint,
    CacheBreakpointSource, CacheLookbackPrefix, CachePricingSummary, CacheScore, CandidateUrgency,
    CredentialStrategy, GLOBAL_PRINCIPAL, InternalError, InternalErrorKind, InternalErrorStage,
    MAX_ERROR_MESSAGE_LEN, MAX_ROUTING_TRACE_STAGES, MAX_STAGE_NAME_LEN, Principal, PrincipalKind,
    RateLimitKind, RateLimitObservation, RoutingTrace, StageDecision, SubscriptionPreferenceTrace,
    SubscriptionQuotaCandidateSnapshot, SubscriptionQuotaDataState, SubscriptionTier,
    TerminalDecision, TerminalStrategy, TtlClass, Upstream, UpstreamCandidate, UpstreamKind,
    WarmCacheEntry, WrhKeySource,
};

/// Composite key identifying a plugin slot per principal × plugin name.
///
/// Used as the trait-level identity for [`crate::FilterPlugin`] via
/// `FilterPlugin::slot_key` and as the runtime-side cache lookup key
/// for the wasmtime per-worker `WorkerInstance` map.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct SlotKey {
    /// Principal id this slot is bound to, or [`GLOBAL_PRINCIPAL`] for
    /// proxy-wide globals.
    pub principal: String,
    /// Stable plugin name.
    pub plugin: String,
}

impl SlotKey {
    /// Build a per-principal slot key.
    pub fn new(principal: impl Into<String>, plugin: impl Into<String>) -> Self {
        Self {
            principal: principal.into(),
            plugin: plugin.into(),
        }
    }

    /// Build a proxy-wide global slot key.
    pub fn global(plugin: impl Into<String>) -> Self {
        Self::new(GLOBAL_PRINCIPAL, plugin)
    }
}

/// Per-principal quota window and model allow-list.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrincipalQuotas {
    /// Maximum request count allowed within `window`.
    pub requests_per_window: u64,
    /// Maximum input token count allowed within `window`.
    pub input_tokens_per_window: u64,
    /// Maximum output token count allowed within `window`.
    pub output_tokens_per_window: u64,
    /// Quota window duration.
    pub window: Duration,
    /// Glob-style model names allowed for this principal.
    pub allowed_models: Vec<String>,
}

/// Plugin manifest passed to runtime adapters when instantiating plugins.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Plugin name from configuration.
    pub name: String,
    /// Filesystem path or runtime-specific locator for the plugin artifact.
    pub artifact: String,
    /// Preferred plugin wire envelope version. Omitted manifests default to v1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wire_version: Option<u8>,
    /// Runtime configuration provided to the plugin.
    pub config: serde_json::Value,
    /// Runtime-specific metadata not interpreted by the core API contract.
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
    /// Pure-mode dispatch: every hook call builds a fresh wasm `Store`
    /// (no thread_local cache, no version-compare). Default `true` —
    /// stateless plugins (the common case) benefit from full isolation
    /// per call. Opt out only for plugins that genuinely need to keep
    /// mutable state across calls in the same worker.
    #[serde(default = "default_pure")]
    pub pure: bool,
}

/// `serde` default for [`PluginManifest::pure`]. Omitted manifests are
/// treated as pure to match the cc-lb-server-side default expectation.
pub fn default_pure() -> bool {
    true
}

/// Reason for a passthrough routing decision.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PassthroughCause {
    /// Upstream is healthy and available.
    HealthyUpstream,
    /// No alternative upstream available.
    NoAlternative,
    /// Plugin returned passthrough decision.
    PluginDecision,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_and_manifest_serde_round_trip() {
        let upstreams = vec![Upstream::AnthropicDirect { base_url: None }];

        for upstream in upstreams {
            let json = serde_json::to_string(&upstream).unwrap();
            let decoded: Upstream = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, upstream);
        }

        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "name": "authn",
            "artifact": "plugin.wasm",
            "config": {"enabled": true}
        }))
        .unwrap();
        assert_eq!(manifest.name, "authn");
        assert_eq!(manifest.wire_version, None);
        assert!(manifest.metadata.is_empty());

        let manifest: PluginManifest = serde_json::from_value(serde_json::json!({
            "name": "cache-aware",
            "artifact": "plugin.wasm",
            "wire_version": 2,
            "config": {}
        }))
        .unwrap();
        assert_eq!(manifest.wire_version, Some(2));
    }

    #[test]
    fn public_enums_cover_all_current_variants() {
        let principal_kinds = [
            PrincipalKind::ApiKey,
            PrincipalKind::OAuthSubject,
            PrincipalKind::InternalKey,
            PrincipalKind::WorkloadIdentity,
            PrincipalKind::SubscriptionBearer,
        ];
        assert_eq!(principal_kinds.len(), 5);

        let strategies = [
            CredentialStrategy::ApiKey,
            CredentialStrategy::OAuth,
            CredentialStrategy::InternalForwarded,
        ];
        assert_eq!(strategies.len(), 3);
    }

    #[test]
    fn cache_types_roundtrip() {
        let ttl_class = TtlClass::Ephemeral1h;
        let json = serde_json::to_string(&ttl_class).unwrap();
        let decoded: TtlClass = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, ttl_class);

        let origin = BreakpointOrigin::AutoCacheInferred;
        let json = serde_json::to_string(&origin).unwrap();
        let decoded: BreakpointOrigin = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, origin);

        let source = CacheBreakpointSource::System;
        let json = serde_json::to_string(&source).unwrap();
        let decoded: CacheBreakpointSource = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, source);

        let breakpoint = CacheBreakpoint {
            block_index: 0,
            source: CacheBreakpointSource::Message,
            path: "messages.0.content.1".to_owned(),
            message_index: Some(0),
            prefix_hash: "abc123".to_owned(),
            prefix_token_count: 100,
            requested_ttl: TtlClass::Ephemeral5m,
            origin: BreakpointOrigin::Explicit,
            lookback_prefixes: vec![CacheLookbackPrefix {
                prefix_hash: "abc123".to_owned(),
                content_block_index: 0,
                lookback_distance: 0,
            }],
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        };
        let json = serde_json::to_string(&breakpoint).unwrap();
        let decoded: CacheBreakpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, breakpoint);

        let warm_entry = WarmCacheEntry {
            prefix_hash: "def456".to_owned(),
            expires_at_unix_secs: 1700000000,
            ttl_class: TtlClass::Ephemeral1h,
            last_observed_at_unix_secs: 1699999000,
            content_block_index: 0,
            estimated_prefix_tokens: 0,
            token_estimate_source: "local_tiktoken_v1".to_owned(),
            hash_schema_version: 4,
        };
        let json = serde_json::to_string(&warm_entry).unwrap();
        let decoded: WarmCacheEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, warm_entry);

        let cache_score = CacheScore {
            predicted_cache_read_tokens: 50,
            predicted_cache_creation_tokens_5m: 100,
            predicted_cache_creation_tokens_1h: 200,
            predicted_uncached_input_tokens: 25,
            predicted_expires_at_unix_secs: Some(1700000000),
            matched_breakpoint_index: Some(0),
            confidence: 0.95,
            ambiguity_reason: None,
            matched_v3_cache_key: Some("v3-cache-key".to_owned()),
            breakpoint_content_block_index: Some(1),
            matched_content_block_index: Some(1),
            lookback_distance: Some(0),
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        };
        let json = serde_json::to_string(&cache_score).unwrap();
        let decoded: CacheScore = serde_json::from_str(&json).unwrap();
        assert_eq!(
            decoded.predicted_cache_read_tokens,
            cache_score.predicted_cache_read_tokens
        );
        assert_eq!(
            decoded.predicted_cache_creation_tokens_5m,
            cache_score.predicted_cache_creation_tokens_5m
        );
        assert_eq!(
            decoded.predicted_cache_creation_tokens_1h,
            cache_score.predicted_cache_creation_tokens_1h
        );
        assert_eq!(
            decoded.predicted_uncached_input_tokens,
            cache_score.predicted_uncached_input_tokens
        );
        assert_eq!(
            decoded.predicted_expires_at_unix_secs,
            cache_score.predicted_expires_at_unix_secs
        );
        assert_eq!(
            decoded.matched_breakpoint_index,
            cache_score.matched_breakpoint_index
        );
        assert!((decoded.confidence - cache_score.confidence).abs() < 0.0001);
        assert_eq!(decoded.ambiguity_reason, cache_score.ambiguity_reason);
    }

    #[test]
    fn upstream_candidate_cache_score_roundtrip() {
        let candidate_no_cache = UpstreamCandidate {
            upstream_id: Uuid::new_v4(),
            name: "test-upstream".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: vec![],
            subscription_quotas: vec![],
            observed_at_unix_secs: 1700000000,
            cache_score: None,
            base_url: None,
            plan_capacity_ratio: None,
            organization_type: None,
            rate_limit_tier: None,
            seat_tier: None,
        };
        let json = serde_json::to_string(&candidate_no_cache).unwrap();
        let decoded: UpstreamCandidate = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.upstream_id, candidate_no_cache.upstream_id);
        assert_eq!(decoded.name, candidate_no_cache.name);
        assert_eq!(decoded.cache_score, None);

        let cache_score = CacheScore {
            predicted_cache_read_tokens: 50,
            predicted_cache_creation_tokens_5m: 100,
            predicted_cache_creation_tokens_1h: 200,
            predicted_uncached_input_tokens: 25,
            predicted_expires_at_unix_secs: Some(1700000000),
            matched_breakpoint_index: Some(0),
            confidence: 0.95,
            ambiguity_reason: None,
            matched_v3_cache_key: Some("v3-cache-key".to_owned()),
            breakpoint_content_block_index: Some(1),
            matched_content_block_index: Some(1),
            lookback_distance: Some(0),
            token_estimate_source: Some("local_tiktoken_v1".to_owned()),
        };
        let candidate_with_cache = UpstreamCandidate {
            upstream_id: Uuid::new_v4(),
            name: "test-upstream-cached".to_owned(),
            kind: UpstreamKind::AnthropicApiKey,
            observed_rate_limits: vec![],
            subscription_quotas: vec![],
            observed_at_unix_secs: 1700000000,
            cache_score: Some(cache_score),
            base_url: None,
            plan_capacity_ratio: None,
            organization_type: None,
            rate_limit_tier: None,
            seat_tier: None,
        };
        let json = serde_json::to_string(&candidate_with_cache).unwrap();
        let decoded: UpstreamCandidate = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.upstream_id, candidate_with_cache.upstream_id);
        assert_eq!(decoded.name, candidate_with_cache.name);
        assert!(decoded.cache_score.is_some());
        assert_eq!(decoded.cache_score.unwrap().predicted_cache_read_tokens, 50);
    }

    #[test]
    fn upstream_candidate_deserialize_without_cache_score_field() {
        let json = r#"{
            "upstream_id": "00000000-0000-0000-0000-000000000001",
            "name": "legacy-upstream",
            "kind": "anthropic_api_key",
            "observed_rate_limits": [],
            "subscription_quotas": [],
            "observed_at_unix_secs": 1700000000
        }"#;
        let candidate: UpstreamCandidate = serde_json::from_str(json).unwrap();
        assert!(candidate.cache_score.is_none());
        assert_eq!(candidate.name, "legacy-upstream");
    }

    #[test]
    fn wrh_key_source_default_is_request_id() {
        assert_eq!(WrhKeySource::default(), WrhKeySource::RequestId);
    }

    #[test]
    fn wrh_key_source_serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&WrhKeySource::CacheHash).unwrap(),
            "\"cache_hash\""
        );
        assert_eq!(
            serde_json::to_string(&WrhKeySource::RequestId).unwrap(),
            "\"request_id\""
        );
        let decoded: WrhKeySource = serde_json::from_str("\"thread_id\"").unwrap();
        assert_eq!(decoded, WrhKeySource::RequestId);
    }

    #[test]
    fn subscription_preference_trace_deserializes_current_payload() {
        let payload = r#"{"chosen_tier":"known_base","candidates":[],"wrh_key_source":"request_id","previous_tier":null,"rendezvous_salt_version":"v9","cache_cost_basis_version":"v1","formula_winner_upstream_id":null,"kept_upstream_id":null,"incumbent_upstream_id":null,"estimated_switch_cache_loss_micros":null,"cache_loss_status":null,"switch_gate_reason":"no_previous_owner"}"#;
        let decoded: SubscriptionPreferenceTrace = serde_json::from_str(payload).unwrap();
        assert_eq!(decoded.chosen_tier, SubscriptionTier::KnownBase);
        assert!(decoded.candidates.is_empty());
        assert_eq!(decoded.wrh_key_source, WrhKeySource::RequestId);
        assert!(decoded.previous_tier.is_none());
        assert_eq!(decoded.rendezvous_salt_version.as_deref(), Some("v9"));
        assert_eq!(decoded.cache_cost_basis_version.as_deref(), Some("v1"));
    }

    #[test]
    fn subscription_preference_trace_deserializes_v10_fixture_old_fields() {
        let payload = include_str!("../tests/fixtures/subscription_preference_trace_v10.json");
        let decoded: SubscriptionPreferenceTrace = serde_json::from_str(payload).unwrap();
        let candidate = decoded.candidates.first().unwrap();

        assert_eq!(decoded.rendezvous_salt_version.as_deref(), Some("v10"));
        assert_eq!(candidate.tier, SubscriptionTier::KnownBase);
        assert_eq!(candidate.urgency, 0.75);
        assert_eq!(candidate.quota_urgency, 0.25);
        assert_eq!(candidate.effective_weight, 0.75);
    }

    #[test]
    fn subscription_preference_trace_deserializes_v10_defaults() {
        let payload = include_str!("../tests/fixtures/subscription_preference_trace_v10.json");
        let decoded: SubscriptionPreferenceTrace = serde_json::from_str(payload).unwrap();
        let candidate = decoded.candidates.first().unwrap();

        assert_eq!(candidate.quota_urgency_5h, None);
        assert_eq!(candidate.quota_urgency_7d, None);
        assert_eq!(candidate.quota_urgency_combined, None);
        assert_eq!(candidate.quota_weight_factor, 1.0);
        assert!(!candidate.quota_uniform_fallback);
        println!(
            "Rust defaults: quota_urgency_5h={:?}, quota_urgency_7d={:?}, quota_urgency_combined={:?}, quota_weight_factor={}, quota_uniform_fallback={}",
            candidate.quota_urgency_5h,
            candidate.quota_urgency_7d,
            candidate.quota_urgency_combined,
            candidate.quota_weight_factor,
            candidate.quota_uniform_fallback
        );
    }

    #[test]
    fn stage_decision_deserializes_legacy_payload_without_cache_affinity() {
        let legacy = r#"{"stage_name":"cache_affinity"}"#;
        let decoded: StageDecision = serde_json::from_str(legacy).unwrap();
        assert_eq!(decoded.stage_name, "cache_affinity");
        assert!(decoded.cache_affinity.is_none());
        assert!(decoded.subscription_preference.is_none());
    }
}
