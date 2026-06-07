//! v2 is a fork of v1 to allow additive cache-related fields. v1 is FROZEN.

extern crate alloc;

use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeaderWire {
    pub name: String,
    pub value_base64: String,
}

impl HeaderWire {
    pub fn dry_run_sample() -> Self {
        Self {
            name: String::from("x-cc-lb-dry-run"),
            value_base64: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Principal {
    pub id: String,
    pub kind: String,
    pub claims: serde_json::Map<String, serde_json::Value>,
}

impl Principal {
    pub fn dry_run_sample() -> Self {
        Self {
            id: String::from("dry-run-principal"),
            kind: String::from("api_key"),
            claims: serde_json::Map::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RequestWire {
    pub request_id: String,
    pub headers: Vec<HeaderWire>,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub body_base64: String,
}

impl RequestWire {
    pub fn dry_run_sample() -> Self {
        Self {
            request_id: String::from("dry-run-request"),
            headers: Vec::new(),
            method: String::from("POST"),
            path: String::from("/v1/messages"),
            query: None,
            body_base64: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RateLimitObservationWire {
    pub kind: String,
    pub window: String,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub reset: Option<String>,
}

impl RateLimitObservationWire {
    pub fn dry_run_sample() -> Self {
        Self {
            kind: String::from("requests"),
            window: String::from("dry-run"),
            limit: None,
            remaining: None,
            reset: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub struct SubscriptionQuotaCandidateSnapshotWire {
    pub window: String,
    pub source: String,
    pub data_state: String,
    pub utilization: Option<f64>,
    pub status: Option<String>,
    pub resets_at_unix_secs: Option<u64>,
    pub surpassed_threshold: Option<bool>,
    pub representative_claim: Option<String>,
    pub disabled_reason: Option<String>,
    pub observed_at_unix_millis: Option<u64>,
    pub age_secs: Option<u64>,
}

impl SubscriptionQuotaCandidateSnapshotWire {
    pub fn dry_run_sample() -> Self {
        Self {
            window: String::from("5h"),
            source: String::from("missing"),
            data_state: String::from("missing"),
            utilization: None,
            status: None,
            resets_at_unix_secs: None,
            surpassed_threshold: None,
            representative_claim: None,
            disabled_reason: None,
            observed_at_unix_millis: None,
            age_secs: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CandidateWire {
    pub upstream_id: String,
    pub name: String,
    pub kind: String,
    pub observed_rate_limits: Vec<RateLimitObservationWire>,
    pub subscription_quotas: Vec<SubscriptionQuotaCandidateSnapshotWire>,
    pub observed_at_unix_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_score: Option<CacheScoreWire>,
}

impl CandidateWire {
    pub fn dry_run_sample() -> Self {
        Self {
            upstream_id: String::from("00000000-0000-0000-0000-000000000000"),
            name: String::from("dry-run-upstream"),
            kind: String::from("anthropic_api_key"),
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 0,
            cache_score: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[serde(deny_unknown_fields)]
pub enum UpstreamWire {
    AnthropicDirect,
    CustomAnthropicSpec { base_url: String },
}

impl UpstreamWire {
    pub fn dry_run_sample() -> Self {
        Self::AnthropicDirect
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[serde(deny_unknown_fields)]
pub enum DialectBinding {
    #[serde(rename = "self")]
    SelfReferenced,
}

impl DialectBinding {
    pub fn dry_run_sample() -> Self {
        Self::SelfReferenced
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ShapedRequestWire {
    pub url: String,
    pub method: String,
    pub headers: Vec<HeaderWire>,
    pub body_base64: String,
}

impl ShapedRequestWire {
    pub fn dry_run_sample() -> Self {
        Self {
            url: String::from("https://api.anthropic.com/v1/messages"),
            method: String::from("POST"),
            headers: Vec::new(),
            body_base64: String::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum UpstreamErrorCategory {
    Unauthorized,
    Retryable,
    Failed,
}

impl UpstreamErrorCategory {
    pub fn dry_run_sample() -> Self {
        Self::Unauthorized
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UpstreamErrorWire {
    pub status: u16,
    pub body_base64: Option<String>,
    pub category: UpstreamErrorCategory,
}

impl UpstreamErrorWire {
    pub fn dry_run_sample() -> Self {
        Self {
            status: 401,
            body_base64: None,
            category: UpstreamErrorCategory::dry_run_sample(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[serde(deny_unknown_fields)]
pub enum ObserveEventWire {
    RequestStarted {
        request_id: String,
        downstream_user_agent: Option<String>,
    },
    AuthnComplete {
        principal_id: String,
        principal_kind: String,
    },
    UpstreamChosen {
        upstream: UpstreamWire,
    },
    Chunk {
        batch_index: u64,
        event_count: usize,
        total_bytes: usize,
    },
    RequestFinished {
        status: u16,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cache_creation_input_tokens: Option<u64>,
        cache_read_input_tokens: Option<u64>,
        duration_ms: u64,
    },
    Error {
        code: String,
        message: String,
        source: String,
    },
}

impl ObserveEventWire {
    pub fn dry_run_sample() -> Self {
        Self::RequestStarted {
            request_id: String::from("dry-run-request"),
            downstream_user_agent: None,
        }
    }
}

// Cache-related wire types (v2 only)

#[derive(Clone, Debug, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum TtlClassWire {
    #[default]
    Ephemeral5m,
    Ephemeral1h,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum BreakpointOriginWire {
    Explicit,
    AutoCacheInferred,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum CacheBreakpointSourceWire {
    Tools,
    System,
    Message,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheBreakpointWire {
    pub block_index: u32,
    pub source: CacheBreakpointSourceWire,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u32>,
    pub prefix_hash: String,
    pub prefix_token_count: u64,
    pub requested_ttl: TtlClassWire,
    pub origin: BreakpointOriginWire,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WarmCacheEntryWire {
    pub prefix_hash: String,
    pub expires_at_unix_secs: u64,
    pub ttl_class: TtlClassWire,
    pub last_observed_at_unix_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheScoreWire {
    pub predicted_cache_read_tokens: u32,
    pub predicted_cache_creation_tokens_5m: u32,
    pub predicted_cache_creation_tokens_1h: u32,
    pub predicted_uncached_input_tokens: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_expires_at_unix_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_breakpoint_index: Option<u32>,
    pub confidence: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ambiguity_reason: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_score_wire_roundtrip() {
        let score = CacheScoreWire {
            predicted_cache_read_tokens: 1000,
            predicted_cache_creation_tokens_5m: 200,
            predicted_cache_creation_tokens_1h: 0,
            predicted_uncached_input_tokens: 50,
            predicted_expires_at_unix_secs: Some(1700000000),
            matched_breakpoint_index: Some(0),
            confidence: 0.85,
            ambiguity_reason: None,
        };
        let json = serde_json::to_string(&score).unwrap();
        let parsed: CacheScoreWire = serde_json::from_str(&json).unwrap();
        assert_eq!(score, parsed);
    }

    #[test]
    fn candidate_wire_with_cache_score_roundtrip() {
        let candidate = CandidateWire {
            upstream_id: String::from("00000000-0000-0000-0000-000000000000"),
            name: String::from("test-upstream"),
            kind: String::from("anthropic_api_key"),
            observed_rate_limits: Vec::new(),
            subscription_quotas: Vec::new(),
            observed_at_unix_secs: 1_717_171_717,
            cache_score: Some(CacheScoreWire {
                predicted_cache_read_tokens: 500,
                predicted_cache_creation_tokens_5m: 100,
                predicted_cache_creation_tokens_1h: 50,
                predicted_uncached_input_tokens: 25,
                predicted_expires_at_unix_secs: None,
                matched_breakpoint_index: Some(1),
                confidence: 0.95,
                ambiguity_reason: None,
            }),
        };
        let json = serde_json::to_string(&candidate).unwrap();
        let parsed: CandidateWire = serde_json::from_str(&json).unwrap();
        assert_eq!(candidate, parsed);
    }

    #[test]
    fn candidate_wire_without_cache_score_deserializes() {
        let json = r#"{
            "upstream_id": "11111111-1111-1111-1111-111111111111",
            "name": "legacy-upstream",
            "kind": "anthropic_api_key",
            "observed_rate_limits": [],
            "subscription_quotas": [],
            "observed_at_unix_secs": 1000000000
        }"#;
        let parsed: CandidateWire = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.cache_score, None);
    }

    #[test]
    fn cache_breakpoint_wire_roundtrip() {
        let bp = CacheBreakpointWire {
            block_index: 0,
            source: CacheBreakpointSourceWire::Tools,
            path: String::from("/v1/messages/create"),
            message_index: Some(5),
            prefix_hash: String::from("abc123def456"),
            prefix_token_count: 2048,
            requested_ttl: TtlClassWire::Ephemeral5m,
            origin: BreakpointOriginWire::Explicit,
        };
        let json = serde_json::to_string(&bp).unwrap();
        let parsed: CacheBreakpointWire = serde_json::from_str(&json).unwrap();
        assert_eq!(bp, parsed);
    }
}
