use std::sync::Arc;

use cc_lb_domain::Upstream;
use cc_lb_upstream::UpstreamDialect;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Router output selecting both an upstream and its dialect boundary object.
pub struct RouteDecision {
    /// Stable upstream identifier selected by the router, when provided by the plugin.
    pub upstream_id: Option<Uuid>,
    /// Upstream selected for the request.
    pub upstream: Upstream,
    /// Dialect plugin that shapes the request for the selected upstream.
    pub dialect: Arc<dyn UpstreamDialect>,
}

/// Per-candidate evaluation reason.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerCandidateReason {
    /// Candidate hit rate limit.
    RateLimited,
    /// Candidate has insufficient quota.
    InsufficientQuota,
    /// Candidate is unhealthy.
    Unhealthy,
    /// Candidate rejected by plugin.
    RejectedByPlugin,
}
