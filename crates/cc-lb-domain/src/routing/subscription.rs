use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::SubscriptionTier;

/// Per-candidate weighted-rendezvous-hash urgency score and tier.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateUrgency {
    /// Upstream identifier this urgency was computed for.
    pub upstream_id: Uuid,
    /// Tier the candidate was assessed into.
    pub tier: SubscriptionTier,
    /// WRH selection weight actually used.
    pub urgency: f64,
    /// Pre-boost quota urgency component.
    pub quota_urgency: f64,
    /// Five-hour use-it-or-lose-it pressure.
    #[serde(default)]
    pub quota_urgency_5h: Option<f64>,
    /// Shared seven-day pressure for normal v11 routing, or effective weekly
    /// `(U_7d^6 + U_fable^6)^(1/6)` for exact Fable under v11-fable.
    #[serde(default)]
    pub quota_urgency_7d: Option<f64>,
    /// Combined use-it-or-lose-it pressure.
    #[serde(default)]
    pub quota_urgency_combined: Option<f64>,
    /// Tier-specific quota factor.
    #[serde(default = "default_quota_weight_factor")]
    pub quota_weight_factor: f64,
    /// Whether the tier used uniform quota weighting.
    #[serde(default)]
    pub quota_uniform_fallback: bool,
    /// Predicted cache-read input tokens.
    pub predicted_cache_read_tokens: u32,
    /// Predicted 5-minute cache-creation tokens.
    pub predicted_cache_creation_tokens_5m: u32,
    /// Predicted 1-hour cache-creation tokens.
    pub predicted_cache_creation_tokens_1h: u32,
    /// Predicted uncached input tokens.
    pub predicted_uncached_input_tokens: u32,
    /// Net priced cache value ratio.
    pub cache_ratio: f64,
    /// Cache-value weight multiplier.
    pub cache_weight_multiplier: f64,
    /// Same-tier warning multiplier.
    pub warning_multiplier: f64,
    /// Cache-read savings ratio.
    pub cache_savings_ratio: f64,
    /// Estimated input-side cost in micros USD.
    pub estimated_input_cost_micros: u64,
    /// Final WRH weight.
    pub effective_weight: f64,
    /// Net priced cache value in micro-USD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_value_micros: Option<i64>,
    /// Matched proxy-local v3 cache key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_v3_cache_key: Option<String>,
    /// Matched content-block index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_content_block_index: Option<u32>,
    /// Requested breakpoint content-block index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breakpoint_content_block_index: Option<u32>,
    /// Distance from breakpoint to matched block.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback_distance: Option<u32>,
    /// Source of the token estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
}

const fn default_quota_weight_factor() -> f64 {
    1.0
}

fn option_f64_eq(left: Option<f64>, right: Option<f64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left.total_cmp(&right).is_eq(),
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

impl PartialEq for CandidateUrgency {
    fn eq(&self, other: &Self) -> bool {
        self.upstream_id == other.upstream_id
            && self.tier == other.tier
            && self.urgency.total_cmp(&other.urgency).is_eq()
            && self.quota_urgency.total_cmp(&other.quota_urgency).is_eq()
            && option_f64_eq(self.quota_urgency_5h, other.quota_urgency_5h)
            && option_f64_eq(self.quota_urgency_7d, other.quota_urgency_7d)
            && option_f64_eq(self.quota_urgency_combined, other.quota_urgency_combined)
            && self
                .quota_weight_factor
                .total_cmp(&other.quota_weight_factor)
                .is_eq()
            && self.quota_uniform_fallback == other.quota_uniform_fallback
            && self.predicted_cache_read_tokens == other.predicted_cache_read_tokens
            && self.predicted_cache_creation_tokens_5m == other.predicted_cache_creation_tokens_5m
            && self.predicted_cache_creation_tokens_1h == other.predicted_cache_creation_tokens_1h
            && self.predicted_uncached_input_tokens == other.predicted_uncached_input_tokens
            && self.cache_ratio.total_cmp(&other.cache_ratio).is_eq()
            && self
                .cache_weight_multiplier
                .total_cmp(&other.cache_weight_multiplier)
                .is_eq()
            && self
                .warning_multiplier
                .total_cmp(&other.warning_multiplier)
                .is_eq()
            && self
                .cache_savings_ratio
                .total_cmp(&other.cache_savings_ratio)
                .is_eq()
            && self.estimated_input_cost_micros == other.estimated_input_cost_micros
            && self
                .effective_weight
                .total_cmp(&other.effective_weight)
                .is_eq()
            && self.cache_value_micros == other.cache_value_micros
            && self.matched_v3_cache_key == other.matched_v3_cache_key
            && self.matched_content_block_index == other.matched_content_block_index
            && self.breakpoint_content_block_index == other.breakpoint_content_block_index
            && self.lookback_distance == other.lookback_distance
            && self.token_estimate_source == other.token_estimate_source
    }
}

impl Eq for CandidateUrgency {}

/// Selection rule used for the subscription-preference winner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WrhKeySource {
    /// Filter used the matched proxy-local v3 cache key.
    CacheHash,
    /// Filter fell back to the request identifier.
    #[default]
    #[serde(alias = "thread_id")]
    RequestId,
    /// Deterministic cost-first-v1 ranking without a routing key.
    CostFirst,
}

/// Structured trace emitted by the subscription-preference filter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriptionPreferenceTrace {
    /// Tier the winning candidate was selected from.
    pub chosen_tier: SubscriptionTier,
    /// Candidates that participated in tier assessment.
    pub candidates: Vec<CandidateUrgency>,
    /// Legacy selection-key field, retaining the winner-selection rule for trace compatibility.
    pub wrh_key_source: WrhKeySource,
    /// Previous tier for the same thread, when known.
    pub previous_tier: Option<SubscriptionTier>,
    /// Version of the WRH salt and algorithm.
    pub rendezvous_salt_version: Option<String>,
    /// Version of the deterministic winner-selection formula.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formula_version: Option<String>,
    /// Version label for cache-cost fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_cost_basis_version: Option<String>,
    /// Raw formula winner before retention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formula_winner_upstream_id: Option<Uuid>,
    /// Upstream retained after switch gating.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept_upstream_id: Option<Uuid>,
    /// Prior same-tier owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incumbent_upstream_id: Option<Uuid>,
    /// Estimated incremental switch cache cost.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_switch_cache_loss_micros: Option<u64>,
    /// Pricing and cache availability for the estimate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_loss_status: Option<String>,
    /// Machine-readable switch-gate outcome.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub switch_gate_reason: Option<String>,
    /// Bucket-level v3 cache-affinity key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_v3_cache_affinity_key: Option<String>,
    /// Analysis-only predicted cache reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_would_have_predicted_read_tokens: Option<u32>,
    /// Analysis-only predicted upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_would_have_picked_upstream_id: Option<Uuid>,
}
