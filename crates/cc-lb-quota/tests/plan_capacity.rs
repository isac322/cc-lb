use cc_lb_quota::plan_capacity::{
    PRO_CAPACITY_RATIO, PlanTierClassification, TierKey, classify_plan_tier, plan_capacity_ratio,
};

#[test]
fn classifies_known_tiers_and_retains_pro_fallbacks() {
    assert_eq!(
        classify_plan_tier(
            Some("claude_team"),
            Some("default_raven"),
            Some("team_standard")
        ),
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
        classify_plan_tier(Some("claude_max"), Some("default_claude_max_20x"), None),
        PlanTierClassification::Known(TierKey::Max20x)
    );
    assert_eq!(
        classify_plan_tier(None, None, None),
        PlanTierClassification::Known(TierKey::Pro)
    );
    assert_eq!(
        plan_capacity_ratio(Some("claude_max"), Some("default_claude_max_5x"), None),
        5.0
    );
}

#[test]
fn surfaces_unknown_metadata_without_changing_capacity_fallback() {
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
        plan_capacity_ratio(Some("claude_max"), Some("default_claude_max_10x"), None),
        PRO_CAPACITY_RATIO
    );
}

#[test]
fn team_rate_fallback_precedes_unknown_seat_classification() {
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
            Some("default_claude_max_5x"),
            Some("team_ultra")
        ),
        6.25
    );
}

#[test]
fn tier_strings_and_seed_ratios_are_canonical() {
    let expected = [
        (TierKey::Pro, 1.0),
        (TierKey::TeamStandard, 1.25),
        (TierKey::Max5x, 5.0),
        (TierKey::TeamPremium, 6.25),
        (TierKey::Max20x, 20.0),
    ];
    for (tier, ratio) in expected {
        assert_eq!(tier.as_str().parse::<TierKey>(), Ok(tier));
        assert_eq!(tier.seed_pro_relative_ratio(), ratio);
    }
    assert!("nope".parse::<TierKey>().is_err());
}

#[test]
fn capacity_ratio_matches_legacy_algorithm_across_regression_inputs() {
    let organization_types = [
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
    let rate_limit_tiers = [
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
    let seat_tiers = [
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

    let mut checked = 0;
    for organization_type in organization_types {
        for rate_limit_tier in rate_limit_tiers {
            for seat_tier in seat_tiers {
                assert_eq!(
                    plan_capacity_ratio(organization_type, rate_limit_tier, seat_tier),
                    legacy_plan_capacity_ratio(organization_type, rate_limit_tier, seat_tier),
                    "ratio regression for ({organization_type:?}, {rate_limit_tier:?}, {seat_tier:?})"
                );
                checked += 1;
            }
        }
    }
    assert_eq!(checked, 9 * 18 * 9);
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
