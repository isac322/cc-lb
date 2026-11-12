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

fn assert_utilization(
    records: &[SubscriptionQuotaSample],
    window: SubscriptionQuotaWindow,
    expected: f64,
) {
    let utilization = record(records, window).utilization;
    assert!(
        utilization.is_some_and(|actual| (actual - expected).abs() < 1e-12),
        "{window:?}: expected {expected}, got {utilization:?}"
    );
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

#[test]
fn codex_api_response_maps_team_windows_by_duration() {
    let value = serde_json::json!({
        "rate_limit": {
            "primary_window": {
                "used_percent": 42.5,
                "limit_window_seconds": 18000,
                "reset_at": 1_800_000_300
            },
            "secondary_window": {
                "used_percent": 66.0,
                "limit_window_seconds": 604800,
                "reset_at": 1_800_600_000
            }
        }
    });
    let records = records_from_openai_api(Uuid::nil(), &value, 1_700_000_000_000);
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .all(|record| record.sample_kind == SubscriptionQuotaSampleKind::Sample)
    );
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::FiveHour).utilization,
        Some(0.425)
    );
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::SevenDay).utilization,
        Some(0.66)
    );
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::FiveHour).resets_at_unix_secs,
        Some(1_800_000_300)
    );
}

#[test]
fn codex_api_response_maps_pro_primary_to_seven_day_and_marks_five_hour_absent() {
    let value = serde_json::json!({
        "rate_limit": {
            "primary_window": {
                "used_percent": 12.5,
                "limit_window_seconds": 604800,
                "reset_at": 1_800_000_300
            },
            "secondary_window": null
        }
    });
    let records = records_from_openai_api(Uuid::nil(), &value, 1_700_000_000_000);
    assert_eq!(records.len(), 2);
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::SevenDay).sample_kind,
        SubscriptionQuotaSampleKind::Sample
    );
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::SevenDay).utilization,
        Some(0.125)
    );
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::FiveHour).sample_kind,
        SubscriptionQuotaSampleKind::Absent
    );
}

#[test]
fn codex_api_response_maps_available_credits_without_changing_base_windows() {
    let value = serde_json::json!({
        "rate_limit": {
            "primary_window": {
                "used_percent": 100.0,
                "limit_window_seconds": 604800,
                "reset_at": 1_800_600_000
            },
            "secondary_window": null
        },
        "credits": {
            "has_credits": true,
            "unlimited": false,
            "overage_limit_reached": false,
            "balance": "62500.0000000000",
            "approx_local_messages": [10, 20],
            "approx_cloud_messages": null
        }
    });

    let records = records_from_openai_api(Uuid::nil(), &value, 1_700_000_000_000);

    assert_eq!(
        record(&records, SubscriptionQuotaWindow::SevenDay).utilization,
        Some(1.0)
    );
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::FiveHour).sample_kind,
        SubscriptionQuotaSampleKind::Absent
    );
    let overage = record(&records, SubscriptionQuotaWindow::Overage);
    assert_eq!(overage.fallback_available, Some(true));
    let credits = overage.credits.as_ref().expect("credit state");
    assert_eq!(credits.has_credits, Some(true));
    assert_eq!(credits.unlimited, Some(false));
    assert_eq!(credits.limit_reached, Some(false));
    assert_eq!(credits.balance.as_deref(), Some("62500.0000000000"));
    assert_eq!(credits.approx_local_messages, Some([10, 20]));
    assert!(credits.approx_cloud_messages.is_none());
}

#[test]
fn codex_usage_and_header_credit_observations_drive_routing_and_keep_poll_restrictions() {
    use cc_lb_domain::{
        Principal, PrincipalKind, SubscriptionQuotaDataState, SubscriptionTier, UpstreamCandidate,
    };
    use cc_lb_engine::builtin_filters::subscription_preference::SubscriptionPreferenceFilter;
    use cc_lb_routing::{FilterPlugin, RoutingContext};

    const OBSERVED: u64 = 1_700_000_000_000;
    let cache = SubscriptionQuotaCache::new();
    let (sink, _receiver) = SubscriptionQuotaSink::new();
    let upstream_id = Uuid::from_u128(1);
    let key_id = Uuid::from_u128(2);
    let context = RoutingContext {
        request_id: "codex-credit-routing".to_owned(),
        thread_id: None,
        requested_service_tier: None,
        downstream_headers: http::HeaderMap::new(),
        method: http::Method::POST,
        path: "/openai/v1/responses".to_owned(),
        query: None,
        body_bytes: bytes::Bytes::new(),
        canonical_model_id: "gpt-5".to_owned(),
        cache_pricing: Default::default(),
    };
    let principal = Principal {
        id: "credit-test".to_owned(),
        kind: PrincipalKind::InternalKey,
    };
    let candidate = |kind, id, snapshots| UpstreamCandidate {
        upstream_id: id,
        name: "credit-test".to_owned(),
        kind,
        subscription_quotas: snapshots,
        observed_at_unix_secs: OBSERVED / 1_000,
        cache_score: None,
        base_url: None,
        plan_capacity_ratio: None,
        organization_type: None,
        rate_limit_tier: None,
        seat_tier: None,
    };
    let evaluate = |now| {
        SubscriptionPreferenceFilter::new()
            .filter(
                &context,
                &principal,
                &[
                    candidate(
                        cc_lb_domain::UpstreamKind::OpenaiOauth,
                        upstream_id,
                        cache.snapshot_for_upstream(upstream_id, now, 60),
                    ),
                    candidate(cc_lb_domain::UpstreamKind::OpenaiApiKey, key_id, vec![]),
                ],
            )
            .expect("quota filter")
    };
    let usage_body = |blocked| {
        serde_json::json!({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 100,
                    "limit_window_seconds": 604800,
                    "reset_at": 1_700_600_000u64
                },
                "secondary_window": null
            },
            "credits": {
                "has_credits": true,
                "unlimited": false,
                "overage_limit_reached": blocked,
                "balance": "62500.0000000000"
            }
        })
    };
    let observe = |blocked, at| {
        observe_provider_usage_body(
            ProviderUsageContext {
                upstream_id,
                kind: UpstreamKind::OpenaiOauth,
                profile: GeminiOAuthProfile::Antigravity,
                project_id: None,
            },
            &serde_json::to_vec(&usage_body(blocked)).expect("usage JSON"),
            at,
            &sink,
            &cache,
        )
        .expect("observe usage");
    };
    observe(true, OBSERVED);
    let headers = [
        ("x-codex-credits-has-credits", "True"),
        ("x-codex-credits-unlimited", "False"),
        ("x-codex-credits-balance", "62500.0000000000"),
    ]
    .into_iter()
    .map(|(name, value)| {
        (
            http::header::HeaderName::from_static(name),
            http::HeaderValue::from_static(value),
        )
    })
    .collect();
    for sample in cc_lb_quota::build_subscription_quota_samples(&headers, upstream_id, OBSERVED + 1)
    {
        cache.upsert_observation(upstream_id, &sample);
    }
    let snapshots = cache.snapshot_for_upstream(upstream_id, OBSERVED + 1, 60);
    let poll = snapshots
        .iter()
        .find(|snap| snap.window == "overage")
        .unwrap();
    let header = snapshots
        .iter()
        .find(|snap| snap.window == "unified")
        .unwrap();
    assert_eq!(poll.source.as_deref(), Some("api"));
    assert_eq!(header.source.as_deref(), Some("header"));
    assert_eq!(poll.credits.as_ref().unwrap().limit_reached, Some(true));
    assert_eq!(header.credits.as_ref().unwrap().limit_reached, None);
    assert_eq!(evaluate(OBSERVED + 1).kept_upstream_ids, vec![key_id]);

    // Missing credit information must not clear the usage-poll restriction.
    for sample in records_from_openai_api(
        upstream_id,
        &serde_json::json!({"rate_limit": usage_body(false)["rate_limit"]}),
        OBSERVED + 2,
    ) {
        cache.upsert_observation(upstream_id, &sample);
    }
    assert_eq!(evaluate(OBSERVED + 2).kept_upstream_ids, vec![key_id]);

    observe(false, OBSERVED + 3);
    let output = evaluate(OBSERVED + 3);
    assert_eq!(output.kept_upstream_ids, vec![upstream_id]);
    assert_eq!(
        output.subscription_preference.unwrap().chosen_tier,
        SubscriptionTier::Overage
    );
    let stale = cache.snapshot_for_upstream(upstream_id, OBSERVED + 61_000, 60);
    assert_eq!(
        stale
            .iter()
            .find(|snap| snap.window == "overage")
            .unwrap()
            .state,
        SubscriptionQuotaDataState::Stale
    );
    assert_eq!(evaluate(OBSERVED + 61_000).kept_upstream_ids, vec![key_id]);
}

#[test]
fn codex_credit_precedence_keeps_unlimited_and_limit_reached_distinct() {
    let unlimited = serde_json::json!({
        "credits": {
            "has_credits": false,
            "unlimited": true,
            "overage_limit_reached": false,
            "balance": null
        }
    });
    let unlimited_records = records_from_openai_api(Uuid::nil(), &unlimited, 1_700_000_000_000);
    let unlimited_record = record(&unlimited_records, SubscriptionQuotaWindow::Overage);
    assert_eq!(unlimited_record.fallback_available, Some(true));
    assert_eq!(
        unlimited_record
            .credits
            .as_ref()
            .and_then(|credits| credits.unlimited),
        Some(true)
    );

    let blocked = serde_json::json!({
        "credits": {
            "has_credits": true,
            "unlimited": true,
            "overage_limit_reached": true,
            "balance": "62500.0000000000"
        }
    });
    let blocked_records = records_from_openai_api(Uuid::nil(), &blocked, 1_700_000_000_000);
    let blocked_record = record(&blocked_records, SubscriptionQuotaWindow::Overage);
    assert_eq!(blocked_record.fallback_available, Some(false));
    assert_eq!(
        blocked_record
            .credits
            .as_ref()
            .and_then(|credits| credits.limit_reached),
        Some(true)
    );

    let balance_only = serde_json::json!({
        "credits": {
            "balance": "62500.0000000000"
        }
    });
    let balance_only_records =
        records_from_openai_api(Uuid::nil(), &balance_only, 1_700_000_000_000);
    let balance_only_record = record(&balance_only_records, SubscriptionQuotaWindow::Overage);
    assert_eq!(balance_only_record.fallback_available, None);
}

#[test]
fn codex_usage_retains_unavailable_credits_and_optional_estimates() {
    for balance in [serde_json::json!("0.0000000000"), serde_json::Value::Null] {
        let records = records_from_openai_api(
            Uuid::nil(),
            &serde_json::json!({
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 40,
                        "limit_window_seconds": 18000,
                        "reset_at": 1_700_018_000
                    },
                    "secondary_window": {
                        "used_percent": 60,
                        "limit_window_seconds": 604800,
                        "reset_at": 1_700_600_000
                    }
                },
                "credits": {
                    "has_credits": false,
                    "unlimited": false,
                    "overage_limit_reached": false,
                    "balance": balance,
                    "approx_local_messages": null,
                    "approx_cloud_messages": [2, 5]
                }
            }),
            1_700_000_000_000,
        );
        assert_utilization(&records, SubscriptionQuotaWindow::FiveHour, 0.4);
        assert_utilization(&records, SubscriptionQuotaWindow::SevenDay, 0.6);
        let overage = record(&records, SubscriptionQuotaWindow::Overage);
        assert_eq!(overage.fallback_available, Some(false));
        assert_eq!(overage.utilization, None);
        assert_eq!(overage.extra_usage_monthly_limit, None);
        let credits = overage.credits.as_ref().expect("unavailable credit state");
        assert_eq!(credits.balance.as_deref(), balance.as_str());
        assert_eq!(credits.approx_local_messages, None);
        assert_eq!(credits.approx_cloud_messages, Some([2, 5]));
    }
}

#[test]
fn malformed_openai_usage_body_emits_no_observations() {
    let cache = SubscriptionQuotaCache::new();
    let (sink, mut receiver) = SubscriptionQuotaSink::new();
    let result = observe_provider_usage_body(
        ProviderUsageContext {
            upstream_id: Uuid::nil(),
            kind: UpstreamKind::OpenaiOauth,
            profile: GeminiOAuthProfile::Antigravity,
            project_id: None,
        },
        b"{not-json",
        1_700_000_000_000,
        &sink,
        &cache,
    );
    assert!(result.is_err());
    assert!(receiver.try_recv().is_err());
}

#[test]
fn codex_api_response_drops_unknown_window_duration() {
    let value = serde_json::json!({
        "rate_limit": {
            "primary_window": {
                "used_percent": 12.5,
                "limit_window_seconds": 3600,
                "reset_at": 1_800_000_300
            }
        }
    });
    let records = records_from_openai_api(Uuid::nil(), &value, 1_700_000_000_000);
    assert!(records.is_empty());
}

#[test]
fn gemini_summary_response_maps_both_groups_and_windows() {
    let value = serde_json::json!({
        "groups": [
            {
                "displayName": "Gemini models",
                "buckets": [
                    {
                        "bucketId": "gemini-5h",
                        "window": "5h",
                        "remainingFraction": 0.75,
                        "resetTime": "2026-11-01T13:00:00Z"
                    },
                    {
                        "bucketId": "gemini-weekly",
                        "window": "weekly",
                        "remainingFraction": 0.5,
                        "resetTime": "2026-11-07T12:00:00Z"
                    }
                ]
            },
            {
                "displayName": "Claude and GPT-OSS models",
                "buckets": [
                    {
                        "bucketId": "3p-5h",
                        "window": "5h",
                        "remainingFraction": 0.9,
                        "resetTime": "2026-11-01T14:00:00Z"
                    },
                    {
                        "bucketId": "3p-weekly",
                        "window": "weekly",
                        "remainingFraction": 0.4,
                        "resetTime": "2026-11-07T15:00:00Z"
                    }
                ]
            }
        ]
    });
    let value = serde_json::json!({ "summary": value });
    let records = records_from_gemini_api(Uuid::nil(), None, &value, 1_793_527_200_000);
    assert_utilization(&records, SubscriptionQuotaWindow::GeminiFiveHour, 0.25);
    assert_utilization(&records, SubscriptionQuotaWindow::GeminiSevenDay, 0.5);
    assert_utilization(&records, SubscriptionQuotaWindow::ThirdPartyFiveHour, 0.1);
    assert_utilization(&records, SubscriptionQuotaWindow::ThirdPartySevenDay, 0.6);
}

#[test]
fn gemini_summary_disabled_bucket_is_absent_not_exhausted() {
    let value = serde_json::json!({
        "groups": [{
            "displayName": "Claude and GPT-OSS models",
            "buckets": [{
                "bucketId": "3p-5h",
                "window": "5h",
                "disabled": true,
                "remainingFraction": 0.0,
                "resetTime": "2026-11-01T13:00:00Z"
            }, {
                "bucketId": "3p-weekly",
                "window": "weekly",
                "remainingFraction": 0.14,
                "resetTime": "2026-11-07T15:00:00Z"
            }]
        }]
    });
    let value = serde_json::json!({ "summary": value });
    let records = records_from_gemini_api(Uuid::nil(), None, &value, 1_793_527_200_000);
    let five_hour = record(&records, SubscriptionQuotaWindow::ThirdPartyFiveHour);
    assert_eq!(five_hour.sample_kind, SubscriptionQuotaSampleKind::Absent);
    assert!(five_hour.utilization.is_none());
    assert_utilization(&records, SubscriptionQuotaWindow::ThirdPartySevenDay, 0.86);
}

#[test]
fn gemini_summary_only_disabled_group_falls_back_to_legacy_quota() {
    let value = serde_json::json!({
        "summary": {
            "groups": [{
                "displayName": "Gemini models",
                "buckets": [{
                    "bucketId": "gemini-5h",
                    "window": "5h",
                    "disabled": true,
                    "remainingFraction": 0.0,
                    "resetTime": "2026-11-01T13:00:00Z"
                }]
            }]
        },
        "fallback": {
            "gemini": {
                "buckets": [{
                    "modelId": "gemini-2.5-pro",
                    "remainingFraction": 0.7,
                    "resetTime": "2026-11-01T13:00:00Z"
                }]
            }
        }
    });
    let records = records_from_gemini_api(Uuid::nil(), None, &value, 1_793_527_200_000);
    assert_utilization(&records, SubscriptionQuotaWindow::GeminiFiveHour, 0.3);
}

#[test]
fn missing_gemini_summary_group_falls_back_and_classifies_by_reset_distance() {
    let value = serde_json::json!({
        "summary": {
            "groups": [{
                "displayName": "Claude and GPT-OSS models",
                "buckets": [{
                    "bucketId": "3p-weekly",
                    "window": "weekly",
                    "remainingFraction": 0.14,
                    "resetTime": "2026-11-07T15:00:00Z"
                }]
            }]
        },
        "fallback": {
            "gemini": {
                "buckets": [{
                    "modelId": "gemini-2.5-pro",
                    "remainingFraction": 0.7,
                    "resetTime": "2026-11-01T13:00:00Z"
                }]
            }
        }
    });
    let records = records_from_gemini_api(Uuid::nil(), None, &value, 1_793_527_200_000);
    assert_utilization(&records, SubscriptionQuotaWindow::GeminiFiveHour, 0.3);
    assert_eq!(
        record(&records, SubscriptionQuotaWindow::GeminiSevenDay).sample_kind,
        SubscriptionQuotaSampleKind::Absent
    );
    assert_utilization(&records, SubscriptionQuotaWindow::ThirdPartySevenDay, 0.86);
}

#[test]
fn antigravity_summary_response_maps_shared_windows_and_resets()
-> Result<(), Box<dyn std::error::Error>> {
    let summary = serde_json::json!({
        "groups": [
            {
                "buckets": [
                    {
                        "bucketId": "gemini-weekly",
                        "displayName": "Weekly Limit Remaining",
                        "window": "weekly",
                        "resetTime": "2026-11-07T10:43:23Z",
                        "remainingFraction": 0.60002846
                    },
                    {
                        "bucketId": "gemini-5h",
                        "displayName": "Five Hour Limit Remaining",
                        "window": "5h",
                        "resetTime": "2026-11-03T04:51:56Z",
                        "remainingFraction": 0.9899967
                    }
                ],
                "displayName": "Gemini Models",
                "description": "Models within this group: Gemini Flash, Gemini Pro"
            },
            {
                "buckets": [
                    {
                        "bucketId": "3p-weekly",
                        "displayName": "Weekly Limit Remaining",
                        "window": "weekly",
                        "resetTime": "2026-11-06T03:58:24Z",
                        "remainingFraction": 0.9889606
                    },
                    {
                        "bucketId": "3p-5h",
                        "displayName": "Five Hour Limit Remaining",
                        "window": "5h",
                        "resetTime": "2026-11-03T05:35:57Z",
                        "remainingFraction": 1
                    }
                ],
                "displayName": "Claude and GPT models",
                "description": "Models within this group: Claude Opus, Claude Sonnet, GPT-OSS"
            }
        ]
    });
    let body = serde_json::to_vec(&serde_json::json!({ "summary": summary }))?;
    let observed_at_unix_millis = 1_793_664_000_000;
    let cache = SubscriptionQuotaCache::new();
    let (sink, _rx) = SubscriptionQuotaSink::new();
    observe_provider_usage_body(
        ProviderUsageContext {
            upstream_id: Uuid::nil(),
            kind: UpstreamKind::GeminiOauth,
            profile: GeminiOAuthProfile::Antigravity,
            project_id: Some("test-project"),
        },
        &body,
        observed_at_unix_millis,
        &sink,
        &cache,
    )?;
    let snapshots = cache.snapshot_for_upstream(Uuid::nil(), observed_at_unix_millis, 60);
    assert_eq!(
        snapshots
            .iter()
            .filter(|snapshot| snapshot.observed_at_unix_millis.is_some())
            .count(),
        4
    );
    for (window, remaining, reset_time) in [
        (
            SubscriptionQuotaWindow::GeminiFiveHour,
            0.9899967,
            "2026-11-03T04:51:56Z",
        ),
        (
            SubscriptionQuotaWindow::GeminiSevenDay,
            0.60002846,
            "2026-11-07T10:43:23Z",
        ),
        (
            SubscriptionQuotaWindow::ThirdPartyFiveHour,
            1.0,
            "2026-11-03T05:35:57Z",
        ),
        (
            SubscriptionQuotaWindow::ThirdPartySevenDay,
            0.9889606,
            "2026-11-06T03:58:24Z",
        ),
    ] {
        let snapshot = snapshots
            .iter()
            .find(|snapshot| snapshot.window == window.as_str())
            .ok_or("missing quota window")?;
        let expected_utilization = 1.0 - remaining;
        assert!(
            snapshot
                .utilization
                .is_some_and(|actual| (actual - expected_utilization).abs() < 1e-12),
            "{window:?}: expected {expected_utilization}, got {:?}",
            snapshot.utilization
        );
        let expected_reset =
            u64::try_from(chrono::DateTime::parse_from_rfc3339(reset_time)?.timestamp())?;
        assert_eq!(snapshot.resets_at_unix_secs, Some(expected_reset));
    }
    Ok(())
}

#[test]
fn openai_refresh_success_requires_a_published_native_main_window() {
    for (duration, with_credits, with_additional, expected_main) in [
        (2_592_000, false, false, false),
        (2_592_000, true, false, false),
        (2_592_000, true, true, false),
        (18_000, false, false, true),
        (604_800, false, false, true),
    ] {
        let mut body = serde_json::json!({"rate_limit":{"primary_window":{
            "limit_window_seconds":duration,"used_percent":100,"reset_at":1_700_500_000
        }}});
        if with_credits {
            body["credits"] = serde_json::json!({"has_credits":true,"balance":"1"});
        }
        if with_additional {
            body["additional_rate_limits"] = serde_json::json!([{"rate_limit":{"primary_window":{
                "limit_window_seconds":604800,"used_percent":100,"reset_at":1_700_500_000
            }}}]);
        }
        let (_, main_published) = parse_provider_usage_body(
            ProviderUsageContext {
                upstream_id: Uuid::nil(),
                kind: UpstreamKind::OpenaiOauth,
                profile: GeminiOAuthProfile::Antigravity,
                project_id: None,
            },
            &serde_json::to_vec(&body).unwrap(),
            1_700_000_000_000,
        )
        .unwrap();
        assert_eq!(main_published, expected_main, "{body}");
    }
}
