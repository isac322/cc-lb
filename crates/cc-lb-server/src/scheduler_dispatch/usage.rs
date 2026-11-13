use cc_lb_control::anthropic_metadata::CedarEmberStatus;
use cc_lb_control::gemini_period_unknown_quota::{
    GeminiGroupPollUpdate, GeminiPeriodUnknownGroupRecord, GeminiPeriodUnknownMeter,
    GeminiQuotaGroupKind, GeminiRawMeterOrigin, normalize_gemini_period_unknown_meters,
};
use cc_lb_domain::SubscriptionQuotaCredits;
#[cfg(test)]
use cc_lb_engine::SubscriptionQuotaSink;
use cc_lb_oauth_protocol::GeminiOAuthProfile;
use cc_lb_scheduler::error::{Result as SchedulerResult, SchedulerError};
use cc_lb_storage_api::{
    SubscriptionQuotaSample, SubscriptionQuotaSampleKind, SubscriptionQuotaSource,
    SubscriptionQuotaStatus, SubscriptionQuotaWindow, upstream::UpstreamKind,
};
use serde::Deserialize;
use uuid::Uuid;

#[cfg(test)]
use crate::subscription_quota_cache::SubscriptionQuotaCache;

#[derive(Debug, Deserialize)]
struct UsageBody {
    five_hour: Option<UsageWindow>,
    seven_day: Option<UsageWindow>,
    seven_day_sonnet: Option<UsageWindow>,
    seven_day_opus: Option<UsageWindow>,
    limits: Option<Vec<UsageLimit>>,
    extra_usage: Option<ExtraUsage>,
    /// Kept as a raw value so a malformed `cedar_ember` block can never fail
    /// the quota parse; it is validated separately before caching.
    cedar_ember: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct UsageWindow {
    utilization: Option<f64>,
    resets_at: Option<ResetsAt>,
}

#[derive(Debug, Deserialize)]
struct UsageLimit {
    kind: String,
    percent: Option<f64>,
    resets_at: Option<ResetsAt>,
    scope: Option<UsageLimitScope>,
}

#[derive(Debug, Deserialize)]
struct UsageLimitScope {
    model: Option<UsageLimitModel>,
}

#[derive(Debug, Deserialize)]
struct UsageLimitModel {
    display_name: Option<String>,
}

// Anthropic's /api/oauth/usage emits `resets_at` as a Unix-seconds number or an RFC3339 string; accept both.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ResetsAt {
    Number(f64),
    String(String),
}

fn resets_at_to_unix_secs(value: ResetsAt) -> Option<u64> {
    match value {
        ResetsAt::Number(value) if value.is_finite() && value >= 0.0 => Some(value as u64),
        ResetsAt::Number(_) => None,
        ResetsAt::String(value) => chrono::DateTime::parse_from_rfc3339(value.trim())
            .ok()
            .and_then(|dt| u64::try_from(dt.timestamp()).ok()),
    }
}

#[derive(Debug, Deserialize)]
struct ExtraUsage {
    #[serde(rename = "is_enabled")]
    enabled: Option<bool>,
    monthly_limit: Option<f64>,
    used_credits: Option<f64>,
}

pub(super) fn parse_usage_body(
    upstream_id: Uuid,
    body: &[u8],
    observed_at_unix_millis: u64,
) -> SchedulerResult<(Vec<SubscriptionQuotaSample>, Option<CedarEmberStatus>)> {
    let mut usage: UsageBody = sonic_rs::from_slice(body)
        .map_err(|error| SchedulerError::Job(format!("oauth usage JSON parse failed: {error}")))?;
    // The provider omits the block entirely for unenrolled accounts; an
    // explicit null is the same answer. A present-but-malformed block also
    // yields None — the caller persists it as a cleared snapshot rather than
    // leaving a possibly-consumed coupon looking usable.
    let cedar_ember = match usage.cedar_ember.take() {
        Some(raw) if !raw.is_null() => match serde_json::from_value::<CedarEmberStatus>(raw) {
            Ok(status) => Some(status),
            Err(error) => {
                tracing::warn!(%upstream_id, %error, "cedar_ember block failed schema validation");
                None
            }
        },
        _ => None,
    };
    Ok((
        records_from_usage(upstream_id, usage, observed_at_unix_millis),
        cedar_ember,
    ))
}

fn records_from_usage(
    upstream_id: Uuid,
    usage: UsageBody,
    observed_at_unix_millis: u64,
) -> Vec<SubscriptionQuotaSample> {
    let UsageBody {
        five_hour,
        seven_day,
        seven_day_sonnet,
        seven_day_opus,
        limits,
        extra_usage,
        cedar_ember: _,
    } = usage;
    let mut records = Vec::new();
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::FiveHour,
        five_hour,
        observed_at_unix_millis,
    );
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::SevenDay,
        seven_day,
        observed_at_unix_millis,
    );
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::SevenDaySonnet,
        seven_day_sonnet,
        observed_at_unix_millis,
    );
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::SevenDayOpus,
        seven_day_opus,
        observed_at_unix_millis,
    );
    // Anthropic marks a per-model weekly meter (e.g. Fable) `is_active=false` unless that
    // model is the single currently-binding limit, which for a per-model meter is almost
    // never the case. The entry still carries real utilization, so record it regardless of
    // `is_active`; filtering on it drops live Fable usage entirely.
    let fable = limits.into_iter().flatten().find(|limit| {
        limit.kind == "weekly_scoped"
            && limit
                .scope
                .as_ref()
                .and_then(|scope| scope.model.as_ref())
                .and_then(|model| model.display_name.as_deref())
                == Some("Fable")
    });
    push_window(
        &mut records,
        upstream_id,
        SubscriptionQuotaWindow::SevenDayFable,
        fable.map(|limit| UsageWindow {
            utilization: limit.percent,
            resets_at: limit.resets_at,
        }),
        observed_at_unix_millis,
    );
    match extra_usage {
        Some(extra_usage) => {
            let utilization = match (extra_usage.used_credits, extra_usage.monthly_limit) {
                (Some(used), Some(limit)) if limit > 0.0 => Some((used / limit).clamp(0.0, 1.0)),
                _ => None,
            };
            records.push(base_record(
                upstream_id,
                SubscriptionQuotaWindow::Overage,
                SubscriptionQuotaSampleKind::Sample,
                observed_at_unix_millis,
                utilization,
                None,
                extra_usage.enabled,
                extra_usage.monthly_limit,
                extra_usage.used_credits,
            ));
        }
        None => records.push(absent_record(
            upstream_id,
            SubscriptionQuotaWindow::Overage,
            observed_at_unix_millis,
        )),
    }
    records
}

fn push_window(
    records: &mut Vec<SubscriptionQuotaSample>,
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    usage: Option<UsageWindow>,
    observed_at_unix_millis: u64,
) {
    match usage {
        Some(usage) => records.push(base_record(
            upstream_id,
            window,
            SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis,
            usage
                .utilization
                .map(|value| (value / 100.0).clamp(0.0, 1.0)),
            usage.resets_at.and_then(resets_at_to_unix_secs),
            None,
            None,
            None,
        )),
        None => records.push(absent_record(upstream_id, window, observed_at_unix_millis)),
    }
}

fn absent_record(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    observed_at_unix_millis: u64,
) -> SubscriptionQuotaSample {
    base_record(
        upstream_id,
        window,
        SubscriptionQuotaSampleKind::Absent,
        observed_at_unix_millis,
        None,
        None,
        None,
        None,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn base_record(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    sample_kind: SubscriptionQuotaSampleKind,
    observed_at_unix_millis: u64,
    utilization: Option<f64>,
    resets_at_unix_secs: Option<u64>,
    extra_usage_enabled: Option<bool>,
    extra_usage_monthly_limit: Option<f64>,
    extra_usage_used_credits: Option<f64>,
) -> SubscriptionQuotaSample {
    SubscriptionQuotaSample {
        upstream_id,
        window,
        source: SubscriptionQuotaSource::Api,
        sample_kind,
        observed_at_unix_millis,
        sample_id: Uuid::new_v4(),
        utilization,
        status: utilization.map(status_from_utilization),
        resets_at_unix_secs,
        surpassed_threshold: None,
        representative_claim: None,
        fallback_percentage: None,
        fallback_available: None,
        overage_in_use: None,
        overage_period_monthly_utilization: None,
        upgrade_paths: None,
        disabled_reason: None,
        extra_usage_enabled,
        extra_usage_monthly_limit,
        extra_usage_used_credits,
        credits: None,
        ingested_at_unix_millis: observed_at_unix_millis,
    }
}

fn status_from_utilization(utilization: f64) -> SubscriptionQuotaStatus {
    if utilization >= 1.0 {
        SubscriptionQuotaStatus::Rejected
    } else if utilization >= 0.8 {
        SubscriptionQuotaStatus::AllowedWarning
    } else {
        SubscriptionQuotaStatus::Allowed
    }
}

/// Provider identity and OAuth profile used to interpret a provider quota body.
pub(super) struct ProviderUsageContext<'a> {
    pub(super) upstream_id: Uuid,
    pub(super) kind: UpstreamKind,
    pub(super) profile: GeminiOAuthProfile,
    pub(super) project_id: Option<&'a str>,
}

#[derive(Debug, Default)]
pub(super) struct GeminiRawUpdates {
    pub(super) gemini: GeminiGroupPollUpdate,
    pub(super) third_party: GeminiGroupPollUpdate,
}

/// Parse provider-native OAuth quota responses into the shared subscription
/// quota sample shape. This runs only in the scheduler worker.
pub(super) fn parse_provider_usage_body(
    context: ProviderUsageContext<'_>,
    body: &[u8],
    observed_at_unix_millis: u64,
) -> SchedulerResult<(Vec<SubscriptionQuotaSample>, bool, GeminiRawUpdates)> {
    let ProviderUsageContext {
        upstream_id,
        kind,
        profile,
        project_id,
    } = context;
    let value: serde_json::Value = sonic_rs::from_slice(body).map_err(|error| {
        SchedulerError::Job(format!("provider quota JSON parse failed: {error}"))
    })?;
    let (records, raw_updates) = match kind {
        UpstreamKind::OpenaiOauth => (
            records_from_openai_api(upstream_id, &value, observed_at_unix_millis),
            GeminiRawUpdates::default(),
        ),
        UpstreamKind::GeminiOauth => match profile {
            GeminiOAuthProfile::GeminiCli | GeminiOAuthProfile::Antigravity => {
                records_from_gemini_api(upstream_id, project_id, &value, observed_at_unix_millis)
            }
        },
        _ => (Vec::new(), GeminiRawUpdates::default()),
    };
    let affected_main_published = kind != UpstreamKind::OpenaiOauth
        || ["primary_window", "secondary_window"].iter().any(|key| {
            value
                .get("rate_limit")
                .and_then(|rate_limit| rate_limit.get(*key))
                .is_some_and(|window| {
                    window
                        .get("limit_window_seconds")
                        .and_then(serde_json::Value::as_u64)
                        .and_then(codex_window_from_duration_secs)
                        .is_some()
                        && window
                            .get("used_percent")
                            .and_then(serde_json::Value::as_f64)
                            .is_some()
                })
        });
    Ok((records, affected_main_published, raw_updates))
}

#[cfg(test)]
fn observe_usage_body(
    upstream_id: Uuid,
    body: &[u8],
    observed_at_unix_millis: u64,
    sink: &SubscriptionQuotaSink,
    cache: &SubscriptionQuotaCache,
) -> SchedulerResult<Option<CedarEmberStatus>> {
    let (records, cedar_ember) = parse_usage_body(upstream_id, body, observed_at_unix_millis)?;
    for record in records {
        cache.upsert_observation(upstream_id, &record);
        sink.enqueue(record)
            .map_err(|error| SchedulerError::Job(error.to_string()))?;
    }
    Ok(cedar_ember)
}

#[cfg(test)]
fn observe_provider_usage_body(
    context: ProviderUsageContext<'_>,
    body: &[u8],
    observed_at_unix_millis: u64,
    sink: &SubscriptionQuotaSink,
    cache: &SubscriptionQuotaCache,
) -> SchedulerResult<()> {
    for record in parse_provider_usage_body(context, body, observed_at_unix_millis)?.0 {
        cache.upsert_observation(record.upstream_id, &record);
        sink.enqueue(record)
            .map_err(|error| SchedulerError::Job(error.to_string()))?;
    }
    Ok(())
}

// Codex RateLimitStatusPayload documents `rate_limit.primary_window` and
// `rate_limit.secondary_window` with used_percent, limit_window_seconds, reset_at.
// Resolve slots by duration: a Pro account's primary slot may be the weekly limit.
// Source: https://github.com/openai/codex/blob/main/codex-rs/backend-client/src/client.rs
fn records_from_openai_api(
    upstream_id: Uuid,
    value: &serde_json::Value,
    observed_at_unix_millis: u64,
) -> Vec<SubscriptionQuotaSample> {
    let mut records = Vec::new();
    collect_openai_windows(value, &mut records, upstream_id, observed_at_unix_millis);
    let has_base_window = !records.is_empty();
    if let Some(credits) = codex_credits_from_value(value) {
        let mut record = base_record(
            upstream_id,
            SubscriptionQuotaWindow::Overage,
            SubscriptionQuotaSampleKind::Sample,
            observed_at_unix_millis,
            None,
            None,
            None,
            None,
            None,
        );
        record.fallback_available = credits.fallback_available();
        record.credits = Some(credits);
        records.push(record);
    }
    // A recognized window establishes a usable snapshot, not an error or an
    // unsupported body. Omitted companion windows are explicitly absent.
    if has_base_window {
        for window in [
            SubscriptionQuotaWindow::FiveHour,
            SubscriptionQuotaWindow::SevenDay,
        ] {
            if !records.iter().any(|record| record.window == window) {
                records.push(absent_record(upstream_id, window, observed_at_unix_millis));
            }
        }
    }
    records
}

fn collect_openai_windows(
    value: &serde_json::Value,
    records: &mut Vec<SubscriptionQuotaSample>,
    upstream_id: Uuid,
    observed_at_unix_millis: u64,
) {
    let Some(object) = value.as_object() else {
        if let Some(items) = value.as_array() {
            for item in items {
                collect_openai_windows(item, records, upstream_id, observed_at_unix_millis);
            }
        }
        return;
    };
    for (name, child) in object {
        let lower = name.to_ascii_lowercase();
        let Some(child_object) = child.as_object() else {
            collect_openai_windows(child, records, upstream_id, observed_at_unix_millis);
            continue;
        };
        let Some(window) = child_object
            .get("limit_window_seconds")
            .and_then(serde_json::Value::as_u64)
            .and_then(codex_window_from_duration_secs)
        else {
            collect_openai_windows(child, records, upstream_id, observed_at_unix_millis);
            continue;
        };
        if (lower.contains("primary") || lower.contains("secondary"))
            && let Some(used) = child_object
                .get("used_percent")
                .and_then(serde_json::Value::as_f64)
        {
            let utilization = Some((used / 100.0).clamp(0.0, 1.0));
            let reset = child_object
                .get("reset_at")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| u64::try_from(value).ok());
            let mut record = base_record(
                upstream_id,
                window,
                SubscriptionQuotaSampleKind::Sample,
                observed_at_unix_millis,
                utilization,
                reset,
                None,
                None,
                None,
            );
            record.representative_claim = Some(format!("Codex {name}"));
            records.push(record);
        }
        collect_openai_windows(child, records, upstream_id, observed_at_unix_millis);
    }
}

fn codex_credits_from_value(value: &serde_json::Value) -> Option<SubscriptionQuotaCredits> {
    let object = value.get("credits")?.as_object()?;
    let credits = SubscriptionQuotaCredits {
        has_credits: object
            .get("has_credits")
            .and_then(serde_json::Value::as_bool),
        unlimited: object.get("unlimited").and_then(serde_json::Value::as_bool),
        limit_reached: object
            .get("overage_limit_reached")
            .and_then(serde_json::Value::as_bool),
        balance: object
            .get("balance")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        approx_local_messages: parse_message_estimate(object.get("approx_local_messages")),
        approx_cloud_messages: parse_message_estimate(object.get("approx_cloud_messages")),
    };
    (credits.has_credits.is_some()
        || credits.unlimited.is_some()
        || credits.limit_reached.is_some()
        || credits.balance.is_some()
        || credits.approx_local_messages.is_some()
        || credits.approx_cloud_messages.is_some())
    .then_some(credits)
}

fn parse_message_estimate(value: Option<&serde_json::Value>) -> Option<[u64; 2]> {
    let values = value?.as_array()?;
    let [lower, upper] = values.as_slice() else {
        return None;
    };
    Some([lower.as_u64()?, upper.as_u64()?])
}

fn codex_window_from_duration_secs(seconds: u64) -> Option<SubscriptionQuotaWindow> {
    match seconds {
        18_000 => Some(SubscriptionQuotaWindow::FiveHour),
        604_800 => Some(SubscriptionQuotaWindow::SevenDay),
        _ => None,
    }
}

fn records_from_gemini_api(
    upstream_id: Uuid,
    _project_id: Option<&str>,
    value: &serde_json::Value,
    observed_at_unix_millis: u64,
) -> (Vec<SubscriptionQuotaSample>, GeminiRawUpdates) {
    let summary = value.get("summary").unwrap_or(&serde_json::Value::Null);
    let summary_groups = summary.get("groups").and_then(serde_json::Value::as_array);
    let mut records = Vec::new();
    let mut updates = GeminiRawUpdates::default();
    for (group, name, windows, raw_group) in [
        (
            GeminiQuotaGroup::Gemini,
            "gemini",
            [
                SubscriptionQuotaWindow::GeminiFiveHour,
                SubscriptionQuotaWindow::GeminiSevenDay,
            ],
            GeminiQuotaGroupKind::Gemini,
        ),
        (
            GeminiQuotaGroup::ThirdParty,
            "third_party",
            [
                SubscriptionQuotaWindow::ThirdPartyFiveHour,
                SubscriptionQuotaWindow::ThirdPartySevenDay,
            ],
            GeminiQuotaGroupKind::ThirdParty,
        ),
    ] {
        let mut meters = Vec::new();
        let mut present = false;
        let mut summary_bucket_count = 0;
        let mut labeled_usable = false;
        if let Some(groups) = summary_groups {
            for entry in groups {
                let display_name = entry
                    .get("displayName")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                for bucket in entry
                    .get("buckets")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                {
                    if classify_gemini_summary_bucket(&display_name, bucket) != group {
                        continue;
                    }
                    summary_bucket_count += 1;
                    let window = gemini_summary_window(group, bucket);
                    if bucket.get("disabled").and_then(serde_json::Value::as_bool) == Some(true) {
                        if let Some(window) = window {
                            replace_gemini_record(
                                &mut records,
                                absent_record(upstream_id, window, observed_at_unix_millis),
                            );
                        }
                        continue;
                    }
                    if !gemini_summary_bucket_usable(group, bucket) {
                        continue;
                    }
                    present = true;
                    if let Some(window) = window {
                        if let Some(sample) = gemini_sample_from_bucket(
                            upstream_id,
                            window,
                            bucket,
                            observed_at_unix_millis,
                        ) {
                            labeled_usable = true;
                            replace_gemini_record(&mut records, sample);
                        }
                    } else if let Some(meter) = gemini_raw_meter(
                        raw_group,
                        bucket,
                        GeminiRawMeterOrigin::RetrieveUserQuotaSummary,
                    ) {
                        meters.push(meter);
                    }
                }
            }
        }
        let fallback = if value.get("summary").is_some() {
            value
                .get("fallback")
                .and_then(|fallback| fallback.get(name))
                .filter(|quota| quota.is_object())
        } else {
            value.is_object().then_some(value)
        };
        let mut fallback_succeeded = false;
        let mut fallback_labeled = false;
        let summary_windows = windows.map(|window| {
            records
                .iter()
                .any(|record| record.window == window)
                .then_some(window)
        });
        let mut explicit_windows = summary_windows;
        if !present && let Some(quota) = fallback {
            fallback_succeeded = true;
            for bucket in quota
                .get("buckets")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                let model = bucket
                    .get("modelId")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                let model_group = if model.starts_with("gemini-") {
                    GeminiQuotaGroup::Gemini
                } else if model.starts_with("claude-") || model.starts_with("gpt-") {
                    GeminiQuotaGroup::ThirdParty
                } else {
                    GeminiQuotaGroup::Unknown
                };
                if model_group != group
                    || bucket.get("disabled").and_then(serde_json::Value::as_bool) == Some(true)
                {
                    continue;
                }
                if let Some(window) = gemini_summary_window(group, bucket)
                    && let Some(sample) = gemini_sample_from_bucket(
                        upstream_id,
                        window,
                        bucket,
                        observed_at_unix_millis,
                    )
                {
                    fallback_labeled = true;
                    // Explicit summary evidence (including real Absent) wins.
                    if !summary_windows.contains(&Some(window)) {
                        replace_gemini_record(&mut records, sample);
                        let index = windows
                            .iter()
                            .position(|candidate| *candidate == window)
                            .expect("fallback window belongs to its group");
                        explicit_windows[index] = Some(window);
                    }
                } else if let Some(meter) =
                    gemini_raw_meter(raw_group, bucket, GeminiRawMeterOrigin::RetrieveUserQuota)
                {
                    meters.push(meter);
                }
            }
        }
        let meters = normalize_gemini_period_unknown_meters(raw_group, meters);
        if !meters.is_empty() {
            records.retain(|record| {
                !windows.contains(&record.window) || explicit_windows.contains(&Some(record.window))
            });
        }
        if meters.is_empty()
            && (labeled_usable
                || fallback_labeled
                || (summary_groups.is_some() && summary_bucket_count == 0 && fallback_succeeded))
        {
            for window in windows {
                if !records.iter().any(|record| record.window == window) {
                    records.push(absent_record(upstream_id, window, observed_at_unix_millis));
                }
            }
        }
        let update = if present || fallback_succeeded {
            GeminiGroupPollUpdate::Replace(GeminiPeriodUnknownGroupRecord {
                observed_at_unix_millis,
                meters,
            })
        } else {
            GeminiGroupPollUpdate::Unchanged
        };
        match group {
            GeminiQuotaGroup::Gemini => updates.gemini = update,
            GeminiQuotaGroup::ThirdParty => updates.third_party = update,
            GeminiQuotaGroup::Unknown => unreachable!(),
        }
    }
    (records, updates)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GeminiQuotaGroup {
    Gemini,
    ThirdParty,
    Unknown,
}

pub(super) fn classify_gemini_summary_bucket(
    display_name: &str,
    bucket: &serde_json::Value,
) -> GeminiQuotaGroup {
    let bucket_id = bucket
        .get("bucketId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    if bucket_id.starts_with("3p-") {
        GeminiQuotaGroup::ThirdParty
    } else if bucket_id.starts_with("gemini-") || display_name.contains("gemini") {
        GeminiQuotaGroup::Gemini
    } else if display_name.contains("claude") || display_name.contains("gpt") {
        GeminiQuotaGroup::ThirdParty
    } else {
        GeminiQuotaGroup::Unknown
    }
}

fn gemini_summary_window(
    group: GeminiQuotaGroup,
    bucket: &serde_json::Value,
) -> Option<SubscriptionQuotaWindow> {
    let bucket_id = bucket
        .get("bucketId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let window = bucket
        .get("window")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let five_hour = bucket_id.ends_with("-5h") || window == "5h";
    let weekly = bucket_id.ends_with("-weekly") || window == "weekly";
    match (group, five_hour, weekly) {
        (GeminiQuotaGroup::Gemini, true, false) => Some(SubscriptionQuotaWindow::GeminiFiveHour),
        (GeminiQuotaGroup::Gemini, false, true) => Some(SubscriptionQuotaWindow::GeminiSevenDay),
        (GeminiQuotaGroup::ThirdParty, true, false) => {
            Some(SubscriptionQuotaWindow::ThirdPartyFiveHour)
        }
        (GeminiQuotaGroup::ThirdParty, false, true) => {
            Some(SubscriptionQuotaWindow::ThirdPartySevenDay)
        }
        _ => None,
    }
}

pub(super) fn gemini_summary_bucket_usable(
    group: GeminiQuotaGroup,
    bucket: &serde_json::Value,
) -> bool {
    bucket.get("disabled").and_then(serde_json::Value::as_bool) != Some(true)
        && bucket
            .get("remainingFraction")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|value| value.is_finite())
        && (gemini_summary_window(group, bucket).is_none() || gemini_bucket_reset(bucket).is_some())
}

fn gemini_bucket_reset(bucket: &serde_json::Value) -> Option<u64> {
    bucket
        .get("resetTime")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .and_then(|value| u64::try_from(value.timestamp()).ok())
}

fn gemini_raw_meter(
    group: GeminiQuotaGroupKind,
    bucket: &serde_json::Value,
    origin: GeminiRawMeterOrigin,
) -> Option<GeminiPeriodUnknownMeter> {
    Some(GeminiPeriodUnknownMeter {
        group,
        model_id: bucket
            .get("modelId")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        bucket_id: bucket
            .get("bucketId")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        remaining_fraction: bucket
            .get("remainingFraction")
            .and_then(serde_json::Value::as_f64)
            .filter(|value| value.is_finite())?
            .clamp(0.0, 1.0),
        reset_time_unix_secs: gemini_bucket_reset(bucket),
        remaining_amount: bucket
            .get("remainingAmount")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        origin,
    })
}

fn gemini_sample_from_bucket(
    upstream_id: Uuid,
    window: SubscriptionQuotaWindow,
    bucket: &serde_json::Value,
    observed_at_unix_millis: u64,
) -> Option<SubscriptionQuotaSample> {
    let reset = bucket
        .get("resetTime")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .and_then(|value| u64::try_from(value.timestamp()).ok())?;
    let remaining = bucket
        .get("remainingFraction")
        .and_then(serde_json::Value::as_f64)
        .filter(|value| value.is_finite())?
        .clamp(0.0, 1.0);
    let claim = bucket
        .get("remainingAmount")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let mut record = base_record(
        upstream_id,
        window,
        SubscriptionQuotaSampleKind::Sample,
        observed_at_unix_millis,
        Some((1.0 - remaining).clamp(0.0, 1.0)),
        Some(reset),
        None,
        None,
        None,
    );
    record.representative_claim = claim;
    Some(record)
}

fn replace_gemini_record(
    records: &mut Vec<SubscriptionQuotaSample>,
    sample: SubscriptionQuotaSample,
) {
    if let Some(record) = records
        .iter_mut()
        .find(|record| record.window == sample.window)
    {
        let replace = record.sample_kind != SubscriptionQuotaSampleKind::Sample
            || sample.utilization > record.utilization;
        if replace {
            *record = sample;
        }
    } else {
        records.push(sample);
    }
}

#[cfg(test)]
mod tests;
