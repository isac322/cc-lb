use cc_lb_domain::{Principal, SubscriptionPreferenceTrace, UpstreamCandidate};
use uuid::Uuid;

use crate::{PerCandidateReason, RouteDecision, RouteError, RoutingContext};

/// Filter plugin output containing upstream selection results and per-candidate reasons.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterOutput {
    /// Upstream IDs that passed the filter.
    pub kept_upstream_ids: Vec<Uuid>,
    /// Human-readable reason for the filtering decision.
    pub reason: String,
    /// Per-candidate filtering reasons.
    pub per_candidate_reasons: Vec<PerCandidateReason>,
    /// Optional structured subscription-preference trace payload.
    pub subscription_preference: Option<SubscriptionPreferenceTrace>,
}

/// Filter plugin errors returned by [`FilterPlugin`].
#[derive(Debug)]
pub enum FilterError {
    /// Runtime error during filtering.
    Runtime {
        /// Redacted runtime error reason.
        reason: String,
    },
    /// Trap error from a crashed or invalid plugin.
    Trap {
        /// Redacted trap error reason.
        reason: String,
    },
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime { reason } => write!(f, "filter runtime error: {reason}"),
            Self::Trap { reason } => write!(f, "filter trap error: {reason}"),
        }
    }
}

impl std::error::Error for FilterError {}

/// Filter plugin boundary for upstream candidate filtering and decision-making.
pub trait FilterPlugin: Send + Sync {
    /// Filters upstream candidates based on request and principal.
    fn filter(
        &self,
        ctx: &RoutingContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<FilterOutput, FilterError>;

    /// Returns the stable plugin identifier.
    fn plugin_id(&self) -> Uuid;

    /// Returns the human-readable plugin name.
    fn plugin_name(&self) -> &str;
}

/// Router plugin boundary.
pub trait RouterPlugin: Send + Sync {
    /// Selects the upstream and dialect for an authenticated request.
    fn route(
        &self,
        ctx: &RoutingContext,
        principal: &Principal,
        candidates: &[UpstreamCandidate],
    ) -> Result<RouteDecision, RouteError>;
}
