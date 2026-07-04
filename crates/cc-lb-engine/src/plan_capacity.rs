//! Plan-capacity ratio lookup shared between admin dashboard aggregation and
//! the subscription-preference router filter.
//!
//! The ratio is expressed relative to Claude Pro (base = 1.0). Anthropic does
//! not publish absolute token limits; the values below are inferred from the
//! documented plan tier names (`default_claude_max_5x`, `default_claude_max_20x`,
//! team seat tiers) and match the ratios historically used by the admin pool
//! quota overview at `crates/cc-lb-admin/src/subscription_quotas.rs`.

/// Pro-plan baseline. All other ratios are expressed relative to this value.
pub const PRO_CAPACITY_RATIO: f64 = 1.0;

/// Per-upstream plan metadata routed through `DynamicView` so router filters
/// can read Anthropic subscription tier information without touching storage
/// on the request hot path.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlanInfo {
    pub organization_type: Option<String>,
    pub rate_limit_tier: Option<String>,
    pub seat_tier: Option<String>,
    pub capacity_ratio: f64,
}

/// Return the Pro-relative capacity ratio for an upstream given its
/// organization type, rate-limit tier, and seat tier.
///
/// Inputs are the raw strings surfaced by Anthropic's organization metadata
/// (`organization_type`, `rate_limit_tier`, `seat_tier`). Missing or unknown
/// values fall back to `PRO_CAPACITY_RATIO`.
pub fn plan_capacity_ratio(
    organization_type: Option<&str>,
    rate_limit_tier: Option<&str>,
    seat_tier: Option<&str>,
) -> f64 {
    let organization_type = organization_type.unwrap_or_default().to_ascii_lowercase();
    let rate_limit_tier = rate_limit_tier.unwrap_or_default().to_ascii_lowercase();
    let seat_tier = seat_tier.unwrap_or_default().to_ascii_lowercase();
    if organization_type == "claude_team" {
        return match seat_tier.as_str() {
            "team_standard" => 1.25,
            "team_tier_1" | "team_premium" => 6.25,
            _ if rate_limit_tier == "default_raven" => 1.25,
            _ if rate_limit_tier.contains("5x") => 6.25,
            _ => PRO_CAPACITY_RATIO,
        };
    }
    if rate_limit_tier.contains("20x") || rate_limit_tier.contains("x20") {
        20.0
    } else if rate_limit_tier.contains("5x") || rate_limit_tier.contains("x5") {
        5.0
    } else {
        PRO_CAPACITY_RATIO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_capacity_ratio_classifies_team_standard_and_premium() {
        assert_eq!(
            plan_capacity_ratio(
                Some("claude_team"),
                Some("default_raven"),
                Some("team_standard")
            ),
            1.25
        );
        assert_eq!(
            plan_capacity_ratio(
                Some("claude_team"),
                Some("default_claude_max_5x"),
                Some("team_tier_1")
            ),
            6.25
        );
        assert_eq!(
            plan_capacity_ratio(
                Some("claude_team"),
                Some("default_claude_max_5x"),
                Some("team_premium")
            ),
            6.25
        );
        assert_eq!(
            plan_capacity_ratio(Some("claude_max"), Some("default_claude_max_20x"), None),
            20.0
        );
    }

    #[test]
    fn plan_capacity_ratio_defaults_to_pro_for_unknown() {
        assert_eq!(plan_capacity_ratio(None, None, None), PRO_CAPACITY_RATIO);
        assert_eq!(
            plan_capacity_ratio(Some("claude_pro"), Some("default_pro"), None),
            PRO_CAPACITY_RATIO
        );
        assert_eq!(
            plan_capacity_ratio(Some("claude_max"), Some("default_claude_max_5x"), None),
            5.0
        );
    }
}
