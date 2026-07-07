//! Plan-capacity classification shared between admin dashboard aggregation and
//! the subscription-preference router filter.
//!
//! The capacity ratio is expressed relative to Claude Pro (base = 1.0).
//! Anthropic does not publish absolute token limits; the seed values below are
//! inferred from the documented plan tier names (`default_claude_max_5x`,
//! `default_claude_max_20x`, team seat tiers) and match the ratios historically
//! used by the admin pool quota overview at
//! `crates/cc-lb-admin/src/subscription_quotas.rs`.
//!
//! These ratios are inferred guesses that change over time. They are therefore
//! managed in the database (`plan_tier_ratio_history_v1`) and historized; the
//! constants here are only the migration seed and a compile-time fallback. See
//! `docs/adr/0005-pool-quota-history-recompute-derivability.md`.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Pro-plan baseline. All other ratios are expressed relative to this value.
pub const PRO_CAPACITY_RATIO: f64 = 1.0;

pub use cc_lb_control::dynamic_view::PlanInfo;

/// Canonical plan tier. This is the typed key used by the DB ratio catalog
/// (`plan_tier_ratio_history_v1.tier_key`) and the tier-mapping override table.
///
/// The string form (`as_str` / `FromStr`) is the on-the-wire and on-disk
/// representation; it MUST stay in sync with the `CHECK (tier_key IN (...))`
/// constraints in the SQLite/Postgres migrations and the seeded catalog rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TierKey {
    /// Claude Pro (and unrecognized-but-baseline accounts). Ratio 1.0.
    Pro,
    /// Claude Team, standard seat. Ratio 1.25.
    TeamStandard,
    /// Claude Max 5x. Ratio 5.0.
    Max5x,
    /// Claude Team, premium/tier-1 seat (or team on a 5x rate). Ratio 6.25.
    TeamPremium,
    /// Claude Max 20x. Ratio 20.0.
    Max20x,
}

impl TierKey {
    /// All canonical tiers, in a stable order. Used to seed the ratio catalog
    /// and to assert conformance parity.
    pub const ALL: [TierKey; 5] = [
        TierKey::Pro,
        TierKey::TeamStandard,
        TierKey::Max5x,
        TierKey::TeamPremium,
        TierKey::Max20x,
    ];

    /// The canonical lowercase string form, matching the DB `tier_key` values.
    pub const fn as_str(self) -> &'static str {
        match self {
            TierKey::Pro => "pro",
            TierKey::TeamStandard => "team_standard",
            TierKey::Max5x => "max_5x",
            TierKey::TeamPremium => "team_premium",
            TierKey::Max20x => "max_20x",
        }
    }

    /// The seed / fallback Pro-relative capacity ratio for this tier. The
    /// authoritative runtime value comes from the DB catalog; this is only the
    /// migration seed and a compile-time fallback.
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

/// Error returned when parsing an unknown `tier_key` string.
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pro" => Ok(TierKey::Pro),
            "team_standard" => Ok(TierKey::TeamStandard),
            "max_5x" => Ok(TierKey::Max5x),
            "team_premium" => Ok(TierKey::TeamPremium),
            "max_20x" => Ok(TierKey::Max20x),
            other => Err(ParseTierKeyError(other.to_owned())),
        }
    }
}

/// The result of classifying an upstream's plan/organization metadata.
///
/// Unlike the legacy `plan_capacity_ratio`, an unrecognized metadata triple is
/// NOT silently collapsed to Pro: it becomes [`PlanTierClassification::Unknown`]
/// so it can be surfaced (log / metric / admin) and a human can add the tier
/// deliberately. For routing, an `Unknown` still falls back to the Pro ratio so
/// behavior is unchanged, but it stays visible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlanTierClassification {
    Known(TierKey),
    Unknown,
}

impl PlanTierClassification {
    /// The resolved tier, if the metadata was recognized.
    pub const fn tier_key(self) -> Option<TierKey> {
        match self {
            PlanTierClassification::Known(tier) => Some(tier),
            PlanTierClassification::Unknown => None,
        }
    }

    /// Whether the metadata was not recognized.
    pub const fn is_unknown(self) -> bool {
        matches!(self, PlanTierClassification::Unknown)
    }
}

/// Classify an upstream's organization/plan metadata into a canonical tier.
///
/// Inputs are the raw strings surfaced by Anthropic's organization metadata
/// (`organization_type`, `rate_limit_tier`, `seat_tier`). Matching preserves the
/// historical behavior of `plan_capacity_ratio` for every recognized case, but a
/// non-empty, unrecognized string yields [`PlanTierClassification::Unknown`]
/// instead of a silent Pro classification.
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
        // Team accounts: seat tier is the primary signal, rate tier secondary.
        if seat_tier == "team_standard" {
            return Known(TierKey::TeamStandard);
        }
        if seat_tier == "team_tier_1" || seat_tier == "team_premium" {
            return Known(TierKey::TeamPremium);
        }
        // Rate-tier fallbacks MUST precede the Unknown branch: legacy behavior
        // classified an unknown-but-non-empty seat by its rate (default_raven ->
        // 1.25, *5x* -> 6.25). Reordering re-introduces a ratio regression.
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
        // Seat and rate both unrecognized: legacy returned Pro (1.0); Unknown
        // also routes at 1.0 (no regression) but stays visible.
        return Unknown;
    }

    // Non-team accounts: the rate-limit tier drives the multiplier.
    if rate_limit_tier.contains("20x") || rate_limit_tier.contains("x20") {
        return Known(TierKey::Max20x);
    }
    if rate_limit_tier.contains("5x") || rate_limit_tier.contains("x5") {
        return Known(TierKey::Max5x);
    }
    // Recognized Pro / no-metadata baselines.
    if rate_limit_tier.is_empty() && organization_type.is_empty() && seat_tier.is_empty() {
        return Known(TierKey::Pro);
    }
    if rate_limit_tier == "default_pro"
        || rate_limit_tier.contains("pro")
        || organization_type == "claude_pro"
    {
        return Known(TierKey::Pro);
    }
    // Non-empty, unrecognized metadata -> surface instead of silently Pro.
    Unknown
}

/// Return the Pro-relative capacity ratio for an upstream given its
/// organization type, rate-limit tier, and seat tier, using the built-in seed
/// ratios.
///
/// This is the compile-time fallback used before the DB ratio catalog is
/// consulted (and by the admin aggregate). An unrecognized triple falls back to
/// [`PRO_CAPACITY_RATIO`]. The authoritative runtime ratio comes from
/// `plan_tier_ratio_history_v1`; see the module docs.
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

    #[test]
    fn classify_matches_known_tiers() {
        assert_eq!(
            classify_plan_tier(
                Some("claude_team"),
                Some("default_raven"),
                Some("team_standard")
            ),
            PlanTierClassification::Known(TierKey::TeamStandard)
        );
        assert_eq!(
            classify_plan_tier(Some("claude_team"), Some("default_raven"), None),
            PlanTierClassification::Known(TierKey::TeamStandard)
        );
        assert_eq!(
            classify_plan_tier(
                Some("claude_team"),
                Some("default_claude_max_5x"),
                Some("team_premium")
            ),
            PlanTierClassification::Known(TierKey::TeamPremium)
        );
        assert_eq!(
            classify_plan_tier(Some("claude_team"), Some("default_claude_max_5x"), None),
            PlanTierClassification::Known(TierKey::TeamPremium)
        );
        assert_eq!(
            classify_plan_tier(Some("claude_max"), Some("default_claude_max_20x"), None),
            PlanTierClassification::Known(TierKey::Max20x)
        );
        assert_eq!(
            classify_plan_tier(Some("claude_max"), Some("default_claude_max_5x"), None),
            PlanTierClassification::Known(TierKey::Max5x)
        );
        assert_eq!(
            classify_plan_tier(None, None, None),
            PlanTierClassification::Known(TierKey::Pro)
        );
        assert_eq!(
            classify_plan_tier(Some("claude_pro"), Some("default_pro"), None),
            PlanTierClassification::Known(TierKey::Pro)
        );
    }

    #[test]
    fn classify_surfaces_unknown_instead_of_silent_pro() {
        assert_eq!(
            classify_plan_tier(Some("claude_max"), Some("default_claude_max_10x"), None),
            PlanTierClassification::Unknown
        );
        assert_eq!(
            classify_plan_tier(
                Some("claude_team"),
                Some("default_enterprise"),
                Some("team_ultra")
            ),
            PlanTierClassification::Unknown
        );
        assert_eq!(
            classify_plan_tier(Some("claude_enterprise"), Some("default_enterprise"), None),
            PlanTierClassification::Unknown
        );
        assert_eq!(
            plan_capacity_ratio(Some("claude_max"), Some("default_claude_max_10x"), None),
            PRO_CAPACITY_RATIO
        );
    }

    #[test]
    fn classify_team_unknown_seat_falls_back_to_rate_ratio() {
        assert_eq!(
            classify_plan_tier(
                Some("claude_team"),
                Some("default_raven"),
                Some("team_ultra")
            ),
            PlanTierClassification::Known(TierKey::TeamStandard)
        );
        assert_eq!(
            plan_capacity_ratio(
                Some("claude_team"),
                Some("default_raven"),
                Some("team_ultra")
            ),
            1.25
        );
        assert_eq!(
            classify_plan_tier(
                Some("claude_team"),
                Some("default_claude_max_5x"),
                Some("team_ultra")
            ),
            PlanTierClassification::Known(TierKey::TeamPremium)
        );
        assert_eq!(
            plan_capacity_ratio(
                Some("claude_team"),
                Some("default_claude_max_5x"),
                Some("team_ultra")
            ),
            6.25
        );
    }

    #[test]
    fn tier_key_string_roundtrip_and_seed_ratios() {
        for tier in TierKey::ALL {
            assert_eq!(tier.as_str().parse::<TierKey>(), Ok(tier));
        }
        assert_eq!(TierKey::Pro.seed_pro_relative_ratio(), 1.0);
        assert_eq!(TierKey::TeamStandard.seed_pro_relative_ratio(), 1.25);
        assert_eq!(TierKey::Max5x.seed_pro_relative_ratio(), 5.0);
        assert_eq!(TierKey::TeamPremium.seed_pro_relative_ratio(), 6.25);
        assert_eq!(TierKey::Max20x.seed_pro_relative_ratio(), 20.0);
        assert!("nope".parse::<TierKey>().is_err());
    }

    fn legacy_plan_capacity_ratio(
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

    #[test]
    fn ratio_has_zero_regression_vs_legacy_across_input_space() {
        let orgs = [
            None,
            Some(""),
            Some("claude_team"),
            Some("CLAUDE_TEAM"),
            Some("claude_pro"),
            Some("claude_max"),
            Some("claude_enterprise"),
            Some("individual"),
            Some("xyz"),
        ];
        let rates = [
            None,
            Some(""),
            Some("default_raven"),
            Some("default_pro"),
            Some("default_claude_max_5x"),
            Some("default_claude_max_20x"),
            Some("DEFAULT_CLAUDE_MAX_5X"),
            Some("5x"),
            Some("20x"),
            Some("x5"),
            Some("x20"),
            Some("x5x"),
            Some("20x5x"),
            Some("pro"),
            Some("prox5"),
            Some("enterprise"),
            Some("raven"),
            Some("zzz"),
        ];
        let seats = [
            None,
            Some(""),
            Some("team_standard"),
            Some("team_tier_1"),
            Some("team_premium"),
            Some("TEAM_PREMIUM"),
            Some("team_ultra"),
            Some("team_"),
            Some("zzz"),
        ];
        let mut checked = 0u32;
        for &ot in &orgs {
            for &rlt in &rates {
                for &st in &seats {
                    let got = plan_capacity_ratio(ot, rlt, st);
                    let want = legacy_plan_capacity_ratio(ot, rlt, st);
                    assert_eq!(
                        got, want,
                        "ratio regression for ({ot:?}, {rlt:?}, {st:?}): got {got}, legacy {want}"
                    );
                    checked += 1;
                }
            }
        }
        assert_eq!(checked, 9 * 18 * 9);
    }
}
