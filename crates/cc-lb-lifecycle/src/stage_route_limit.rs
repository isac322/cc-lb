use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Stage payloads: Route
// ---------------------------------------------------------------------------

/// Route selected by the router.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouteInfo {
    pub upstream_id: Uuid,
    pub upstream_name: String,
    pub model: Option<String>,
    /// Pricing bucket for the chosen upstream. Values used by the pricing
    /// subscriber to select the correct cost model: `"anthropic_key"` or
    /// `"anthropic_oauth"`. `None` means the pricing default (Anthropic key).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_trace: Option<cc_lb_domain::RoutingTrace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_read_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_v3_cache_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breakpoint_content_block_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_content_block_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback_distance: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_creation_tokens_5m: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicted_cache_creation_tokens_1h: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_value_micros: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub formula_winner_upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kept_upstream_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_5h: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_7d: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_urgency_combined: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quota_warning_multiplier: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_would_have_predicted_read_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_would_have_picked_upstream_id: Option<Uuid>,
}

/// Reason routing failed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RouteFailure {
    /// Router pipeline could not be instantiated for this principal.
    RouterPipelineUnavailable,
    /// The filter chain returned zero candidates after evaluation.
    RouteNoUpstreamAfterFilter,
    /// No upstream is configured for the requested route.
    RouteNotConfigured,
}

// ---------------------------------------------------------------------------
// Stage payloads: LimitDecision
// ---------------------------------------------------------------------------

/// Whether the limit engine reserved capacity or rejected the request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "outcome", rename_all = "snake_case")]
#[non_exhaustive]
pub enum LimitDecisionKind {
    Reserved {
        reservation_id: String,
        amount: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_reserve_ms: Option<u64>,
    },
    Rejected {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        subject: Option<LimitSubject>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_summary: Option<LimitRequestSummary>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        route_summary: Option<RouteSummary>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit_violation: Option<String>,
    },
}

/// Identity carried by `LimitDecisionKind::Rejected` for downstream audit
/// subscribers to reconstruct the legacy `AuditEntry`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LimitSubject {
    pub principal_id: String,
    pub key_id: String,
}

/// Request shape summary carried by `LimitDecisionKind::Rejected`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LimitRequestSummary {
    pub model: String,
    pub path: String,
    pub method: String,
}

/// Route summary carried by `LimitDecisionKind::Rejected`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RouteSummary {
    pub upstream_name: String,
}
