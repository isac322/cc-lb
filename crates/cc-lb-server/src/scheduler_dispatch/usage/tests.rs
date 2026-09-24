use super::*;

fn parse(body: &str) -> Vec<SubscriptionQuotaSample> {
    let usage: UsageBody = serde_json::from_str(body).expect("usage body parses");
    records_from_usage(Uuid::nil(), usage, 1_700_000_000_000)
}
fn record(
    records: &[SubscriptionQuotaSample],
    window: SubscriptionQuotaWindow,
) -> &SubscriptionQuotaSample {
    records
        .iter()
        .find(|record| record.window == window)
        .expect("quota window record")
}

fn sample_windows(records: &[SubscriptionQuotaSample]) -> Vec<SubscriptionQuotaWindow> {
    records
        .iter()
        .filter(|record| record.sample_kind == SubscriptionQuotaSampleKind::Sample)
        .map(|record| record.window)
        .collect()
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
    assert_eq!(records.len(), 6);
    assert_eq!(sample_windows(&records).len(), 2);
}

#[test]
fn full_production_body_marks_omitted_windows_absent() {
    let body = r#"{"five_hour":{"utilization":60.0,"resets_at":"2026-06-21T08:10:00.885300+00:00"},"seven_day":{"utilization":21.0,"resets_at":"2026-06-25T19:00:00.885325+00:00"},"seven_day_oauth_apps":null,"seven_day_opus":null,"seven_day_sonnet":{"utilization":0.0,"resets_at":"2026-06-25T18:59:59.885338+00:00"},"extra_usage":{"is_enabled":false,"monthly_limit":null,"used_credits":null}}"#;
    let records = parse(body);
    assert_eq!(records.len(), 6);
    assert_eq!(sample_windows(&records).len(), 4);
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::SevenDayOpus).sample_kind,
        SubscriptionQuotaSampleKind::Absent
    );
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::SevenDayFable).sample_kind,
        SubscriptionQuotaSampleKind::Absent
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

    let fable = record(&records, SubscriptionQuotaWindow::SevenDayFable);
    assert_eq!(fable.sample_kind, SubscriptionQuotaSampleKind::Sample);
    assert_eq!(fable.utilization, Some(0.28));
    assert_eq!(fable.resets_at_unix_secs, Some(1_783_987_200));
}

#[test]
fn active_fable_limit_percent_is_clamped_after_normalization() {
    let records = parse(
        r#"{"limits":[{"kind":"weekly_scoped","percent":128,"resets_at":1800000000,"scope":{"model":{"display_name":"Fable"}},"is_active":true}]}"#,
    );

    let fable = record(&records, SubscriptionQuotaWindow::SevenDayFable);
    assert_eq!(fable.sample_kind, SubscriptionQuotaSampleKind::Sample);
    assert_eq!(fable.utilization, Some(1.0));
}

#[test]
fn inactive_weekly_scoped_fable_limit_is_still_recorded() {
    let records = parse(
        r#"{"limits":[{"kind":"weekly_scoped","percent":16,"resets_at":"2026-07-15T15:00:00Z","scope":{"model":{"display_name":"Fable","id":null},"surface":null},"is_active":false}]}"#,
    );

    let fable = record(&records, SubscriptionQuotaWindow::SevenDayFable);
    assert_eq!(fable.sample_kind, SubscriptionQuotaSampleKind::Sample);
    assert_eq!(fable.utilization, Some(0.16));
}

#[test]
fn weekly_scoped_fable_limit_without_is_active_is_recorded() {
    let records = parse(
        r#"{"limits":[{"kind":"weekly_scoped","percent":14,"resets_at":1800000000,"scope":{"model":{"display_name":"Fable"}}}]}"#,
    );

    let fable = record(&records, SubscriptionQuotaWindow::SevenDayFable);
    assert_eq!(fable.sample_kind, SubscriptionQuotaSampleKind::Sample);
    assert_eq!(fable.utilization, Some(0.14));
}

#[test]
fn non_weekly_or_non_fable_scoped_limits_are_ignored() {
    let records = parse(
        r#"{"limits":[{"kind":"monthly_scoped","percent":28,"resets_at":1800000000,"scope":{"model":{"display_name":"Fable"}},"is_active":true},{"kind":"weekly_scoped","percent":28,"resets_at":1800000000,"scope":{"model":{"display_name":"Sonnet"}},"is_active":true}]}"#,
    );

    assert!(sample_windows(&records).is_empty());
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::SevenDayFable).sample_kind,
        SubscriptionQuotaSampleKind::Absent
    );
}

#[test]
fn realistic_body_records_inactive_fable_beside_top_level_windows() {
    let body = r#"{"five_hour":{"utilization":11.0,"resets_at":"2026-07-11T15:20:00Z"},"seven_day":{"utilization":55.0,"resets_at":"2026-07-15T15:00:00Z"},"seven_day_sonnet":null,"seven_day_opus":null,"limits":[{"kind":"session","group":"session","percent":11,"scope":null,"is_active":false},{"kind":"weekly_all","group":"weekly","percent":55,"scope":null,"is_active":true},{"kind":"weekly_scoped","group":"weekly","percent":16,"resets_at":"2026-07-15T15:00:00Z","scope":{"model":{"id":null,"display_name":"Fable"},"surface":null},"is_active":false}]}"#;
    let records = parse(body);
    let fable = records
        .iter()
        .find(|r| r.window == SubscriptionQuotaWindow::SevenDayFable)
        .expect("fable window recorded from inactive weekly_scoped entry");
    assert_eq!(fable.utilization, Some(0.16));
    assert!(
        records
            .iter()
            .any(|r| r.window == SubscriptionQuotaWindow::FiveHour),
        "top-level five_hour still recorded"
    );
    assert!(
        records
            .iter()
            .any(|r| r.window == SubscriptionQuotaWindow::SevenDay),
        "top-level seven_day still recorded"
    );
}

#[test]
fn missing_seven_day_body_marks_only_omitted_windows_absent() {
    let records = parse(
        r#"{"five_hour":{"utilization":11.0,"resets_at":"2026-07-27T12:00:00Z"},"limits":[{"kind":"weekly_scoped","percent":16,"resets_at":"2026-08-03T12:00:00Z","scope":{"model":{"display_name":"Fable"}}}]}"#,
    );

    assert_eq!(
        sample_windows(&records),
        vec![
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDayFable,
        ]
    );
    let seven_day = record(&records, SubscriptionQuotaWindow::SevenDay);
    assert_eq!(seven_day.sample_kind, SubscriptionQuotaSampleKind::Absent);
    assert_eq!(seven_day.observed_at_unix_millis, 1_700_000_000_000);
    assert!(seven_day.utilization.is_none());
    assert!(seven_day.status.is_none());
    assert!(seven_day.resets_at_unix_secs.is_none());
}

fn observe(body: &str) -> (Option<CedarEmberStatus>, SubscriptionQuotaCache) {
    let cache = SubscriptionQuotaCache::new();
    let (sink, _rx) = SubscriptionQuotaSink::new();
    let cedar_ember = observe_usage_body(
        Uuid::nil(),
        body.as_bytes(),
        1_700_000_000_000,
        &sink,
        &cache,
    )
    .expect("usage body observed");
    (cedar_ember, cache)
}

fn five_hour_utilization(cache: &SubscriptionQuotaCache) -> Option<f64> {
    cache
        .snapshot_for_upstream(Uuid::nil(), 1_700_000_000_000, 60)
        .into_iter()
        .find(|snapshot| snapshot.window == "5h")
        .and_then(|snapshot| snapshot.utilization)
}

#[test]
fn cedar_ember_block_is_returned_alongside_quota() {
    // One response yields both states: the quota windows land in the cache
    // and the coupon is handed back for the caller to persist.
    let (cedar_ember, cache) = observe(
        r#"{"five_hour":{"utilization":95.0,"resets_at":"2026-09-24T20:00:00Z"},"cedar_ember":{"eligible":true,"at_limit":true,"grants":[{"id":"grant_01","resets_left":1}],"next_grant_id":"grant_01"}}"#,
    );
    // The provider reports percent; the quota pipeline stores a fraction.
    assert_eq!(five_hour_utilization(&cache), Some(0.95));
    let status = cedar_ember.expect("cedar_ember parsed");
    assert_eq!(status.next_grant_id.as_deref(), Some("grant_01"));
}

#[test]
fn malformed_cedar_ember_does_not_block_quota() {
    let (cedar_ember, cache) = observe(
        r#"{"five_hour":{"utilization":95.0,"resets_at":"2026-09-24T20:00:00Z"},"cedar_ember":{"eligible":true,"grants":"not-an-array"}}"#,
    );
    assert_eq!(five_hour_utilization(&cache), Some(0.95));
    // Malformed reads as "no usable coupon"; the caller persists that as a
    // cleared snapshot, so a prior coupon cannot linger as usable.
    assert!(cedar_ember.is_none());
}

#[test]
fn absent_or_null_cedar_ember_block_yields_none() {
    for body in [
        r#"{"five_hour":{"utilization":10.0,"resets_at":"2026-09-24T20:00:00Z"}}"#,
        r#"{"five_hour":{"utilization":10.0,"resets_at":"2026-09-24T20:00:00Z"},"cedar_ember":null}"#,
    ] {
        let (cedar_ember, cache) = observe(body);
        assert_eq!(five_hour_utilization(&cache), Some(0.1));
        assert!(cedar_ember.is_none());
    }
}
