use super::*;

fn parse(body: &str) -> Vec<SubscriptionQuotaSample> {
    let usage: UsageBody = serde_json::from_str(body).expect("usage body parses");
    records_from_usage(Uuid::nil(), usage, 1_700_000_000_000)
}

#[test]
fn rfc3339_resets_at_is_accepted() {
    let records = parse(r#"{"five_hour":{"utilization":42,"resets_at":"2026-06-21T20:30:00Z"}}"#);
    let five_hour = records
        .iter()
        .find(|r| r.window == SubscriptionQuotaWindow::FiveHour)
        .expect("five hour record");
    assert_eq!(five_hour.resets_at_unix_secs, Some(1_782_073_800));
}

#[test]
fn numeric_resets_at_is_accepted() {
    let records = parse(r#"{"seven_day":{"utilization":12,"resets_at":1800000000}}"#);
    let seven_day = records
        .iter()
        .find(|r| r.window == SubscriptionQuotaWindow::SevenDay)
        .expect("seven day record");
    assert_eq!(seven_day.resets_at_unix_secs, Some(1_800_000_000));
}

#[test]
fn float_resets_at_is_accepted() {
    let records = parse(r#"{"five_hour":{"utilization":1,"resets_at":1800000000.5}}"#);
    let five_hour = records
        .iter()
        .find(|r| r.window == SubscriptionQuotaWindow::FiveHour)
        .expect("five hour record");
    assert_eq!(five_hour.resets_at_unix_secs, Some(1_800_000_000));
}

#[test]
fn string_resets_at_does_not_poison_other_windows() {
    let records = parse(
        r#"{"five_hour":{"utilization":10,"resets_at":"2026-06-21T20:30:00Z"},"seven_day":{"utilization":3,"resets_at":1800000000}}"#,
    );
    assert_eq!(records.len(), 2);
}

#[test]
fn full_production_body_produces_all_four_records() {
    let body = r#"{"five_hour":{"utilization":60.0,"resets_at":"2026-06-21T08:10:00.885300+00:00"},"seven_day":{"utilization":21.0,"resets_at":"2026-06-25T19:00:00.885325+00:00"},"seven_day_oauth_apps":null,"seven_day_opus":null,"seven_day_sonnet":{"utilization":0.0,"resets_at":"2026-06-25T18:59:59.885338+00:00"},"extra_usage":{"is_enabled":false,"monthly_limit":null,"used_credits":null}}"#;
    let records = parse(body);
    let windows: Vec<_> = records.iter().map(|r| r.window).collect();
    assert_eq!(
        records.len(),
        4,
        "expected 4 records, got windows={:?}",
        windows
    );
}

#[test]
fn extra_usage_is_enabled_field_is_parsed() {
    let records =
        parse(r#"{"extra_usage":{"is_enabled":true,"monthly_limit":30000,"used_credits":15000}}"#);
    let overage = records
        .iter()
        .find(|r| r.window == SubscriptionQuotaWindow::Overage)
        .expect("overage record");
    assert_eq!(overage.extra_usage_enabled, Some(true));
    assert_eq!(overage.extra_usage_monthly_limit, Some(30000.0));
    assert_eq!(overage.extra_usage_used_credits, Some(15000.0));
}

#[test]
fn active_weekly_scoped_fable_limit_emits_normalized_window() {
    let records = parse(
        r#"{"limits":[{"kind":"weekly_scoped","percent":28,"resets_at":"2026-07-14T00:00:00Z","scope":{"model":{"display_name":"Fable","id":null},"surface":null},"is_active":true}]}"#,
    );

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].window, SubscriptionQuotaWindow::SevenDayFable);
    assert_eq!(records[0].utilization, Some(0.28));
    assert_eq!(records[0].resets_at_unix_secs, Some(1_783_987_200));
}

#[test]
fn active_fable_limit_percent_is_clamped_after_normalization() {
    let records = parse(
        r#"{"limits":[{"kind":"weekly_scoped","percent":128,"resets_at":1800000000,"scope":{"model":{"display_name":"Fable"}},"is_active":true}]}"#,
    );

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].utilization, Some(1.0));
}

#[test]
fn inactive_and_unrelated_scoped_limits_are_ignored() {
    let records = parse(
        r#"{"limits":[{"kind":"weekly_scoped","percent":28,"resets_at":1800000000,"scope":{"model":{"display_name":"Fable"}},"is_active":false},{"kind":"monthly_scoped","percent":28,"resets_at":1800000000,"scope":{"model":{"display_name":"Fable"}},"is_active":true},{"kind":"weekly_scoped","percent":28,"resets_at":1800000000,"scope":{"model":{"display_name":"Sonnet"}},"is_active":true},{"kind":"weekly_scoped","percent":28,"resets_at":1800000000,"scope":{"model":{"display_name":"Fable"}}}]}"#,
    );

    assert!(records.is_empty());
}
