//! Plan-capacity classification shared between admin aggregation and routing.
//!
//! Capacity ratios are expressed relative to Claude Pro (base = 1.0).

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub use cc_lb_domain::PlanInfo;

/// Pro-plan baseline. All other ratios are expressed relative to this value.
pub const PRO_CAPACITY_RATIO: f64 = 1.0;

/// Canonical plan tier used by the persisted ratio catalog and overrides.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierKey {
    Pro,
    TeamStandard,
    Max5x,
    TeamPremium,
    Max20x,
}

impl TierKey {
    pub const ALL: [TierKey; 5] = [
        TierKey::Pro,
        TierKey::TeamStandard,
        TierKey::Max5x,
        TierKey::TeamPremium,
        TierKey::Max20x,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            TierKey::Pro => "pro",
            TierKey::TeamStandard => "team_standard",
            TierKey::Max5x => "max_5x",
            TierKey::TeamPremium => "team_premium",
            TierKey::Max20x => "max_20x",
        }
    }

    pub const fn seed_pro_relative_ratio(self) -> f64 {
        match self {
            TierKey::Pro => 1.0,
            TierKey::TeamStandard => 1.25,
            TierKey::Max5x => 5.0,
            TierKey::TeamPremium => 6.25,
            TierKey::Max20x => 20.0,
        }
    }
}

impl fmt::Display for TierKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseTierKeyError(pub String);

impl fmt::Display for ParseTierKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown plan tier_key: {:?}", self.0)
    }
}

impl std::error::Error for ParseTierKeyError {}

impl FromStr for TierKey {
    type Err = ParseTierKeyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pro" => Ok(TierKey::Pro),
            "team_standard" => Ok(TierKey::TeamStandard),
            "max_5x" => Ok(TierKey::Max5x),
            "team_premium" => Ok(TierKey::TeamPremium),
            "max_20x" => Ok(TierKey::Max20x),
            other => Err(ParseTierKeyError(other.to_owned())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanTierClassification {
    Known(TierKey),
    Unknown,
}

impl PlanTierClassification {
    pub const fn tier_key(self) -> Option<TierKey> {
        match self {
            Self::Known(tier) => Some(tier),
            Self::Unknown => None,
        }
    }
}

pub fn classify_plan_tier(
    organization_type: Option<&str>,
    rate_limit_tier: Option<&str>,
    seat_tier: Option<&str>,
) -> PlanTierClassification {
    use PlanTierClassification::{Known, Unknown};

    let organization_type = organization_type.unwrap_or_default().to_ascii_lowercase();
    let rate_limit_tier = rate_limit_tier.unwrap_or_default().to_ascii_lowercase();
    let seat_tier = seat_tier.unwrap_or_default().to_ascii_lowercase();

    if organization_type == "claude_team" {
        if seat_tier == "team_standard" {
            return Known(TierKey::TeamStandard);
        }
        if seat_tier == "team_tier_1" || seat_tier == "team_premium" {
            return Known(TierKey::TeamPremium);
        }
        if rate_limit_tier == "default_raven" {
            return Known(TierKey::TeamStandard);
        }
        if rate_limit_tier.contains("5x") {
            return Known(TierKey::TeamPremium);
        }
        if seat_tier.is_empty()
            && (rate_limit_tier.is_empty()
                || rate_limit_tier == "default_pro"
                || rate_limit_tier.contains("pro"))
        {
            return Known(TierKey::Pro);
        }
        return Unknown;
    }

    if rate_limit_tier.contains("20x") || rate_limit_tier.contains("x20") {
        return Known(TierKey::Max20x);
    }
    if rate_limit_tier.contains("5x") || rate_limit_tier.contains("x5") {
        return Known(TierKey::Max5x);
    }
    if rate_limit_tier.is_empty() && organization_type.is_empty() && seat_tier.is_empty() {
        return Known(TierKey::Pro);
    }
    if rate_limit_tier == "default_pro"
        || rate_limit_tier.contains("pro")
        || organization_type == "claude_pro"
    {
        return Known(TierKey::Pro);
    }
    Unknown
}

pub fn plan_capacity_ratio(
    organization_type: Option<&str>,
    rate_limit_tier: Option<&str>,
    seat_tier: Option<&str>,
) -> f64 {
    match classify_plan_tier(organization_type, rate_limit_tier, seat_tier) {
        PlanTierClassification::Known(tier) => tier.seed_pro_relative_ratio(),
        PlanTierClassification::Unknown => PRO_CAPACITY_RATIO,
    }
}
