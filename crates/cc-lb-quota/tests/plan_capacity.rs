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
