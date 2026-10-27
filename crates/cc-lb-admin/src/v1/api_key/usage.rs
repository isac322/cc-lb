//! `GET /admin/v1/upstreams/{id}/usage?range=&step=`: one API-key upstream's
//! usage card (plan §1.5, §1.6; S3b).
//!
//! Storage-only: the handler makes zero outbound calls. Provider-reported
//! buckets (`source: real`) are used when the poller has a usable report for
//! the current key; otherwise the series is cc-lb's own request log
//! (`source: estimated`). The `fix` code is derived here, at read time, from
//! the stored details row.

use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
};
use cc_lb_control::anthropic_api_key::{
    classify_billing_stop,
    wire::{
        KeyBillingStop, KeyUsageEnvelope, KeyUsageMonth, KeyUsageSource, KeyUsageTotals,
        Remediation,
    },
};
use cc_lb_storage_api::{
    ApiKeyClass, ApiKeyOrgCapability, ApiUsageAttribution, RemediationCode, Storage,
    UpstreamApiKeyDetailsRecord, UpstreamApiUsageBucketQuery, UpstreamApiUsageBucketRecord,
    UpstreamKind, UpstreamRecord, UsageRollupResolution,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::recheck::load_api_key_or_provider_upstream;
use crate::AdminState;
use crate::dashboard::{
    DashboardBuildError, DashboardRange, DashboardUsageResponse, UsageBucket, UsageGroupBy,
    UsageProjection, UsageSeries, auto_step, build_dashboard_usage_projected_checked,
    build_window_for_step, empty_bucket, parse_range, parse_step, validate_step_for_range,
};
use crate::v1::upstreams::{UpstreamError, storage, upstream_base_url};

const HOUR_SECS: u64 = 3_600;
const DAY_SECS: u64 = 86_400;
const HOUR_BUCKET_WIDTH: u32 = 3_600;
const DAY_BUCKET_WIDTH: u32 = 86_400;
/// Series key for usage Anthropic reports without a model (stored as `""`).
const OTHER_MODEL_KEY: &str = "Other";

#[derive(Debug, Deserialize)]
pub(crate) struct KeyUsageQuery {
    #[serde(default)]
    range: Option<String>,
    #[serde(default)]
    step: Option<String>,
}

#[derive(Debug, Serialize)]
struct UpstreamKeyUsageResponse {
    #[serde(flatten)]
    usage: DashboardUsageResponse,
    #[serde(flatten)]
    envelope: KeyUsageEnvelope,
}

pub(crate) async fn get_upstream_usage(
    State(state): State<AdminState>,
    Path(id): Path<String>,
    Query(query): Query<KeyUsageQuery>,
) -> Result<Response, UpstreamError> {
    let range = query
        .range
        .as_deref()
        .and_then(|value| parse_range(value).ok())
        .ok_or_else(|| bad_request("invalid_range", "range must be one of 15m, 1h, 6h, 24h, 7d"))?;
    let step = match query.step.as_deref() {
        Some(value) => parse_step(value)
            .map_err(|error| bad_request(error.as_str(), "step must be minute or hour"))?,
        None => auto_step(range),
    };
    let storage = storage(&state)?;
    let record = load_api_key_or_provider_upstream(storage, &id).await?;
    let details = storage.get_api_key_details(record.id).await?;
    let provenance = if matches!(
        record.kind,
        UpstreamKind::OpenaiApiKey | UpstreamKind::GeminiApiKey
    ) {
        Provenance::estimated(None)
    } else {
        let on_org_origin = state
            .anthropic_org_api
            .applies_to(&upstream_base_url(&record)?);
        derive_provenance(on_org_origin, &record, details.as_ref())
    };

    let now_unix_secs = cc_lb_clock::unix_secs(state.clock.now());
    let (month_start_unix_secs, month_end_unix_secs) = utc_month_bounds(now_unix_secs);

    let (usage, month_totals) = match &provenance.subject {
        Some(subject) => {
            let usage = reported_series(storage, record.id, subject, range, now_unix_secs).await?;
            let month = reported_month(
                storage,
                record.id,
                subject,
                month_start_unix_secs,
                month_end_unix_secs,
            )
            .await?;
            (usage, month)
        }
        None => {
            validate_step_for_range(range, step)
                .map_err(|error| bad_request(error.as_str(), "step is too fine for range"))?;
            let usage = build_dashboard_usage_projected_checked(
                storage,
                range,
                step,
                UsageGroupBy::Model,
                Some(record.id),
                now_unix_secs,
                UsageProjection::Full,
            )
            .await
            .map_err(|error| match error {
                DashboardBuildError::Query(error) => {
                    bad_request(error.as_str(), "invalid usage query")
                }
                DashboardBuildError::Storage(error) => UpstreamError::from(error),
            })?;
            let retention_start = match state.config.request_event_retention_days {
                0 => 0,
                days => now_unix_secs.saturating_sub(days.saturating_mul(DAY_SECS)),
            };
            let month = estimated_month(
                storage,
                record.id,
                month_start_unix_secs.max(retention_start),
                month_end_unix_secs,
            )
            .await?;
            (usage, month)
        }
    };

    let billing_stop = storage
        .latest_upstream_request_outcome(record.id, month_start_unix_secs.saturating_mul(1_000))
        .await?
        .and_then(|outcome| {
            classify_billing_stop(
                outcome.status,
                outcome.upstream_error_type.as_deref(),
                outcome.upstream_error_message.as_deref(),
            )
            .map(|kind| KeyBillingStop {
                kind,
                observed_at_unix_secs: outcome.observed_at_unix_ms / 1_000,
            })
        });

    let cost_usd_micros = usage
        .series
        .iter()
        .flat_map(|series| &series.buckets)
        .fold(0_u64, |sum, bucket| {
            sum.saturating_add(bucket.virtual_cost_micros)
        });
    let envelope = KeyUsageEnvelope {
        source: if provenance.subject.is_some() {
            KeyUsageSource::Real
        } else {
            KeyUsageSource::Estimated
        },
        attribution: provenance
            .subject
            .as_ref()
            .map_or(ApiUsageAttribution::ApiKey, |subject| subject.attribution),
        attribution_workspace_id: provenance
            .subject
            .as_ref()
            .filter(|subject| subject.attribution == ApiUsageAttribution::Workspace)
            .map(|subject| subject.subject_id.clone()),
        stale: provenance.stale,
        last_reported_unix_secs: details
            .as_ref()
            .and_then(|details| details.usage_last_success_at_unix_secs),
        totals: KeyUsageTotals {
            cost_usd_micros,
            cost_is_estimate: true,
        },
        fix: provenance.fix.map(|code| Remediation { code }),
        month: KeyUsageMonth {
            start_unix_secs: month_totals.start_unix_secs,
            end_unix_secs: month_end_unix_secs,
            tokens: month_totals.tokens,
            cost_usd_micros: month_totals.cost_usd_micros,
            billing_stop,
        },
    };
    Ok(Json(UpstreamKeyUsageResponse { usage, envelope }).into_response())
}

fn bad_request(error: &'static str, detail: &str) -> UpstreamError {
    UpstreamError::BadRequest {
        error,
        detail: detail.to_owned(),
    }
}

/// Whose provider-reported buckets the response reads.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReportSubject {
    attribution: ApiUsageAttribution,
    subject_id: String,
}

/// Plan §1.6: `subject` is `Some` for `source: real`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Provenance {
    subject: Option<ReportSubject>,
    stale: bool,
    fix: Option<RemediationCode>,
}

impl Provenance {
    const fn estimated(fix: Option<RemediationCode>) -> Self {
        Self {
            subject: None,
            stale: false,
            fix,
        }
    }
}

/// Plan §1.6 table, first match wins.
fn derive_provenance(
    on_org_origin: bool,
    record: &UpstreamRecord,
    details: Option<&UpstreamApiKeyDetailsRecord>,
) -> Provenance {
    if !on_org_origin {
        return Provenance::estimated(Some(RemediationCode::NonAnthropicBaseUrl));
    }
    let Some(details) = details.filter(|details| details.verified) else {
        return Provenance::estimated(Some(RemediationCode::UsageNeverChecked));
    };
    if details.key_class == ApiKeyClass::WorkspaceBound {
        return Provenance::estimated(Some(RemediationCode::KeyWorkspaceScopedUsageUnavailable));
    }
    let succeeded = details.usage_last_success_at_unix_secs.is_some();
    match details.usage_report_state {
        Some(ApiKeyOrgCapability::Denied) if succeeded => reported(
            record,
            details,
            true,
            Some(RemediationCode::UsagePollFailed),
        ),
        Some(ApiKeyOrgCapability::Denied) => {
            Provenance::estimated(Some(RemediationCode::RoleInsufficient))
        }
        Some(ApiKeyOrgCapability::Ok) if succeeded => {
            reported(record, details, details.usage_poll_failures >= 2, None)
        }
        Some(
            ApiKeyOrgCapability::Ok
            | ApiKeyOrgCapability::Error
            | ApiKeyOrgCapability::NotApplicable,
        )
        | None => Provenance::estimated(None),
    }
}

/// Reported figures for the current key, else its workspace. A row with no
/// subject at all cannot be read back and falls back to the estimate.
fn reported(
    record: &UpstreamRecord,
    details: &UpstreamApiKeyDetailsRecord,
    stale: bool,
    fix: Option<RemediationCode>,
) -> Provenance {
    let subject = match details.api_key_id.as_ref() {
        Some(api_key_id) => Some(ReportSubject {
            attribution: ApiUsageAttribution::ApiKey,
            subject_id: api_key_id.clone(),
        }),
        None => record
            .workspace_id
            .as_ref()
            .or(details.capabilities.workspace.id.as_ref())
            .map(|workspace_id| ReportSubject {
                attribution: ApiUsageAttribution::Workspace,
                subject_id: workspace_id.clone(),
            }),
    };
    match subject {
        Some(subject) => Provenance {
            subject: Some(subject),
            stale,
            fix,
        },
        None => Provenance::estimated(fix),
    }
}

fn bucket_query(
    upstream_id: Uuid,
    subject: &ReportSubject,
    bucket_width_secs: u32,
    start_unix_secs: u64,
    end_unix_secs: u64,
) -> UpstreamApiUsageBucketQuery {
    UpstreamApiUsageBucketQuery {
        upstream_id,
        attribution: subject.attribution,
        subject_id: subject.subject_id.clone(),
        bucket_width_secs,
        start_unix_secs,
        end_unix_secs,
    }
}

/// Reported `1h` buckets over the range, one series per model. Reports are
/// hourly, so the step is always `hour` and the window covers at least one
/// hour. Buckets are sparse; when Anthropic last answered is the stored
/// poll timestamp (`last_reported_unix_secs`), not the newest bucket.
async fn reported_series(
    storage: &dyn Storage,
    upstream_id: Uuid,
    subject: &ReportSubject,
    range: DashboardRange,
    now_unix_secs: u64,
) -> Result<DashboardUsageResponse, UpstreamError> {
    let step = UsageRollupResolution::Hour;
    let (range_start, window_end_unix_secs) = build_window_for_step(range, step, now_unix_secs);
    let window_start_unix_secs = range_start.min(window_end_unix_secs.saturating_sub(HOUR_SECS));
    let buckets = storage
        .query_upstream_api_usage_buckets(&bucket_query(
            upstream_id,
            subject,
            HOUR_BUCKET_WIDTH,
            window_start_unix_secs,
            window_end_unix_secs,
        ))
        .await?;
    let observed = !buckets.is_empty();
    let mut by_model: BTreeMap<String, Vec<UsageBucket>> = BTreeMap::new();
    for bucket in &buckets {
        let key = if bucket.model.is_empty() {
            OTHER_MODEL_KEY
        } else {
            bucket.model.as_str()
        };
        by_model
            .entry(key.to_owned())
            .or_default()
            .push(usage_bucket(bucket));
    }
    Ok(DashboardUsageResponse {
        range: range.as_str(),
        step: step.as_str(),
        group_by: UsageGroupBy::Model.as_str(),
        window_start_unix_secs,
        window_end_unix_secs,
        series: by_model
            .into_iter()
            .map(|(key, buckets)| UsageSeries {
                key,
                upstream_name: None,
                buckets,
            })
            .collect(),
        truncated_series_count: None,
        observed,
    })
}

fn usage_bucket(bucket: &UpstreamApiUsageBucketRecord) -> UsageBucket {
    let mut usage = empty_bucket(bucket.bucket_start_unix_secs);
    usage.input_tokens = bucket.uncached_input_tokens;
    usage.output_tokens = bucket.output_tokens;
    usage.cache_creation_input_tokens = bucket
        .cache_creation_5m_input_tokens
        .saturating_add(bucket.cache_creation_1h_input_tokens);
    usage.cache_read_input_tokens = bucket.cache_read_input_tokens;
    usage.virtual_cost_micros = bucket.list_cost_micros;
    usage
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MonthTotals {
    start_unix_secs: u64,
    tokens: u64,
    cost_usd_micros: u64,
}

/// Reported month: the subject's `1d` buckets from the 1st.
async fn reported_month(
    storage: &dyn Storage,
    upstream_id: Uuid,
    subject: &ReportSubject,
    month_start_unix_secs: u64,
    month_end_unix_secs: u64,
) -> Result<MonthTotals, UpstreamError> {
    let buckets = storage
        .query_upstream_api_usage_buckets(&bucket_query(
            upstream_id,
            subject,
            DAY_BUCKET_WIDTH,
            month_start_unix_secs,
            month_end_unix_secs,
        ))
        .await?;
    let mut totals = MonthTotals {
        start_unix_secs: month_start_unix_secs,
        tokens: 0,
        cost_usd_micros: 0,
    };
    for bucket in &buckets {
        totals.tokens = [
            bucket.uncached_input_tokens,
            bucket.cache_creation_5m_input_tokens,
            bucket.cache_creation_1h_input_tokens,
            bucket.cache_read_input_tokens,
            bucket.output_tokens,
        ]
        .into_iter()
        .fold(totals.tokens, u64::saturating_add);
        totals.cost_usd_micros = totals
            .cost_usd_micros
            .saturating_add(bucket.list_cost_micros);
    }
    Ok(totals)
}

/// Estimated month: cc-lb's hour rollups for this upstream from `start`
/// (the 1st, or the request-log retention cut when that is later).
async fn estimated_month(
    storage: &dyn Storage,
    upstream_id: Uuid,
    start_unix_secs: u64,
    month_end_unix_secs: u64,
) -> Result<MonthTotals, UpstreamError> {
    let rollups = storage
        .query_usage_rollups_in_range(
            UsageRollupResolution::Hour,
            start_unix_secs,
            month_end_unix_secs,
        )
        .await?;
    let mut totals = MonthTotals {
        start_unix_secs,
        tokens: 0,
        cost_usd_micros: 0,
    };
    for rollup in rollups
        .iter()
        .filter(|rollup| rollup.upstream_id == upstream_id)
    {
        totals.tokens = [
            rollup.input_tokens,
            rollup.output_tokens,
            rollup.cache_creation_input_tokens,
            rollup.cache_read_input_tokens,
        ]
        .into_iter()
        .fold(totals.tokens, u64::saturating_add);
        totals.cost_usd_micros = totals
            .cost_usd_micros
            .saturating_add(rollup.virtual_cost_micros);
    }
    Ok(totals)
}

/// `[1st 00:00 UTC, next 1st 00:00 UTC)` of the month containing `now`.
fn utc_month_bounds(now_unix_secs: u64) -> (u64, u64) {
    let days = i64::try_from(now_unix_secs / DAY_SECS).unwrap_or(i64::MAX);
    let (year, month) = civil_year_month(days);
    let (next_year, next_month) = if month == 12 {
        (year.saturating_add(1), 1)
    } else {
        (year, month + 1)
    };
    let to_secs = |days: i64| u64::try_from(days).unwrap_or(0).saturating_mul(DAY_SECS);
    (
        to_secs(days_from_civil(year, month, 1)),
        to_secs(days_from_civil(next_year, next_month, 1)),
    )
}

/// Year and month (1-12) of a day count since 1970-01-01 (proleptic
/// Gregorian; Howard Hinnant's `civil_from_days`).
fn civil_year_month(days_since_epoch: i64) -> (i64, i64) {
    let z = days_since_epoch.saturating_add(719_468);
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Hinnant's
/// `days_from_civil`).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use cc_lb_storage_api::{
        ApiKeyCapabilities, ApiKeyWorkspace, ApiKeyWorkspaceMode, UpstreamKind,
    };

    use super::*;

    const OCT_15_2026_NOON: u64 = 1_792_065_600;
    const OCT_1_2026: u64 = 1_790_812_800;
    const NOV_1_2026: u64 = 1_793_491_200;
    const DEC_1_2026: u64 = 1_796_083_200;
    const JAN_1_2027: u64 = 1_798_761_600;

    fn record(workspace_id: Option<&str>) -> UpstreamRecord {
        UpstreamRecord {
            kind: UpstreamKind::AnthropicApiKey,
            workspace_id: workspace_id.map(str::to_owned),
            ..UpstreamRecord::default()
        }
    }

    fn details(
        key_class: ApiKeyClass,
        state: Option<ApiKeyOrgCapability>,
        last_success: Option<u64>,
        failures: u32,
        api_key_id: Option<&str>,
    ) -> UpstreamApiKeyDetailsRecord {
        UpstreamApiKeyDetailsRecord {
            upstream_id: Uuid::nil(),
            verified: true,
            key_class,
            key_hint: "sk-ant-api03-...AAAA".to_owned(),
            api_key_id: api_key_id.map(str::to_owned),
            needs_workspace: key_class == ApiKeyClass::OrgScoped,
            capabilities: ApiKeyCapabilities {
                workspace: ApiKeyWorkspace {
                    mode: ApiKeyWorkspaceMode::Selected,
                    id: Some("wrkspc_P".to_owned()),
                    name: None,
                },
                ..ApiKeyCapabilities::not_checked()
            },
            remediation: Vec::new(),
            usage_report_state: state,
            last_error: None,
            checked_at_unix_secs: Some(1),
            last_attempt_at_unix_secs: Some(1),
            access_revision: 1,
            usage_poll_failures: failures,
            usage_next_poll_after_unix_secs: None,
            usage_last_success_at_unix_secs: last_success,
            usage_last_attempt_at_unix_secs: last_success,
            usage_poll_claim_token: None,
            usage_poll_claim_lease_until_unix_secs: None,
            created_at_unix_secs: 1,
            updated_at_unix_secs: 1,
        }
    }

    fn api_key_subject(id: &str) -> Option<ReportSubject> {
        Some(ReportSubject {
            attribution: ApiUsageAttribution::ApiKey,
            subject_id: id.to_owned(),
        })
    }

    #[test]
    fn custom_host_is_always_estimated() {
        let org_ok = details(
            ApiKeyClass::OrgScoped,
            Some(ApiKeyOrgCapability::Ok),
            Some(5),
            0,
            Some("apikey_1"),
        );
        assert_eq!(
            derive_provenance(false, &record(None), Some(&org_ok)),
            Provenance::estimated(Some(RemediationCode::NonAnthropicBaseUrl))
        );
    }

    #[test]
    fn unchecked_and_unverified_rows_read_never_checked() {
        let mut unverified = details(
            ApiKeyClass::OrgScoped,
            Some(ApiKeyOrgCapability::Ok),
            Some(5),
            0,
            Some("apikey_1"),
        );
        unverified.verified = false;
        for row in [None, Some(&unverified)] {
            assert_eq!(
                derive_provenance(true, &record(None), row),
                Provenance::estimated(Some(RemediationCode::UsageNeverChecked))
            );
        }
    }

    #[test]
    fn bound_keys_have_no_reports() {
        let bound = details(ApiKeyClass::WorkspaceBound, None, None, 0, None);
        assert_eq!(
            derive_provenance(true, &record(None), Some(&bound)),
            Provenance::estimated(Some(RemediationCode::KeyWorkspaceScopedUsageUnavailable))
        );
    }

    #[test]
    fn denied_is_stopped_after_a_success_and_a_role_fix_before() {
        let after = details(
            ApiKeyClass::OrgScoped,
            Some(ApiKeyOrgCapability::Denied),
            Some(5),
            0,
            Some("apikey_1"),
        );
        assert_eq!(
            derive_provenance(true, &record(None), Some(&after)),
            Provenance {
                subject: api_key_subject("apikey_1"),
                stale: true,
                fix: Some(RemediationCode::UsagePollFailed),
            }
        );
        let never = details(
            ApiKeyClass::OrgScoped,
            Some(ApiKeyOrgCapability::Denied),
            None,
            0,
            Some("apikey_1"),
        );
        assert_eq!(
            derive_provenance(true, &record(None), Some(&never)),
            Provenance::estimated(Some(RemediationCode::RoleInsufficient))
        );
    }

    #[test]
    fn error_not_applicable_unpolled_and_first_poll_pending_have_no_fix() {
        for (state, last_success) in [
            (Some(ApiKeyOrgCapability::Error), Some(5)),
            (Some(ApiKeyOrgCapability::NotApplicable), Some(5)),
            (None, Some(5)),
            (Some(ApiKeyOrgCapability::Ok), None),
        ] {
            let row = details(
                ApiKeyClass::OrgScoped,
                state,
                last_success,
                0,
                Some("apikey_1"),
            );
            assert_eq!(
                derive_provenance(true, &record(None), Some(&row)),
                Provenance::estimated(None),
                "{state:?}"
            );
        }
    }

    #[test]
    fn ok_reports_are_real_and_stale_after_two_failures() {
        let fresh = details(
            ApiKeyClass::OrgScoped,
            Some(ApiKeyOrgCapability::Ok),
            Some(5),
            1,
            Some("apikey_1"),
        );
        assert_eq!(
            derive_provenance(true, &record(Some("wrkspc_P")), Some(&fresh)),
            Provenance {
                subject: api_key_subject("apikey_1"),
                stale: false,
                fix: None,
            }
        );
        let stale = details(
            ApiKeyClass::OrgScoped,
            Some(ApiKeyOrgCapability::Ok),
            Some(5),
            2,
            Some("apikey_1"),
        );
        assert!(derive_provenance(true, &record(None), Some(&stale)).stale);
    }

    #[test]
    fn unresolved_key_ids_attribute_to_the_workspace() {
        let unresolved = details(
            ApiKeyClass::OrgScoped,
            Some(ApiKeyOrgCapability::Ok),
            Some(5),
            0,
            None,
        );
        assert_eq!(
            derive_provenance(true, &record(Some("wrkspc_S")), Some(&unresolved)).subject,
            Some(ReportSubject {
                attribution: ApiUsageAttribution::Workspace,
                subject_id: "wrkspc_S".to_owned(),
            })
        );
        // Without a spec workspace the captured workspace id is the subject.
        assert_eq!(
            derive_provenance(true, &record(None), Some(&unresolved)).subject,
            Some(ReportSubject {
                attribution: ApiUsageAttribution::Workspace,
                subject_id: "wrkspc_P".to_owned(),
            })
        );
    }

    // USG-09 / USG-10: UTC month blocks.
    #[test]
    fn month_bounds_follow_the_utc_calendar() {
        assert_eq!(utc_month_bounds(OCT_15_2026_NOON), (OCT_1_2026, NOV_1_2026));
        assert_eq!(utc_month_bounds(OCT_1_2026), (OCT_1_2026, NOV_1_2026));
        assert_eq!(utc_month_bounds(NOV_1_2026 - 1), (OCT_1_2026, NOV_1_2026));
        assert_eq!(utc_month_bounds(JAN_1_2027 - 1), (DEC_1_2026, JAN_1_2027));
        assert_eq!(utc_month_bounds(JAN_1_2027), (JAN_1_2027, 1_801_440_000));
        // Leap February.
        assert_eq!(
            utc_month_bounds(1_709_208_000),
            (1_706_745_600, 1_709_251_200)
        );
    }
}
