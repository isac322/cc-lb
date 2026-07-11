/// Anthropic plan metadata used to derive per-upstream capacity.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanInfo {
    /// Anthropic organization type.
    pub organization_type: Option<String>,
    /// Anthropic rate-limit tier.
    pub rate_limit_tier: Option<String>,
    /// Anthropic seat tier.
    pub seat_tier: Option<String>,
    /// Capacity ratio relative to Claude Pro.
    pub capacity_ratio: f64,
}
