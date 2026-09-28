use std::collections::BTreeMap;

use cc_lb_storage_api::{
    RequestEventPrincipalCostBucket, RequestEventPrincipalCostQuery, Storage, StorageError,
    UsageRollup, UsageRollupResolution,
};
use serde::Serialize;
use uuid::Uuid;

const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const MAX_BUCKETS_PER_SERIES: u64 = 1_440;
const MAX_GROUPED_SERIES: usize = 20;
const MAX_PRINCIPAL_COST_SERIES: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardRange {
    FifteenMinutes,
    OneHour,
    SixHours,
    TwentyFourHours,
    SevenDays,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseRangeError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DashboardQueryError {
    InvalidRange,
    InvalidStep,
    InvalidGroupBy,
    StepTooFineForRange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageGroupBy {
    None,
    Model,
    Principal,
    Upstream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UsageProjection {
    Full,
    Totals,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParseUsageProjectionError;

#[derive(Debug, Clone, Serialize)]
pub struct DashboardSummaryResponse {
    pub range: &'static str,
    pub step: &'static str,
    pub window_start_unix_secs: u64,
    pub window_end_unix_secs: u64,
    pub totals: SummaryTotals,
    pub sparkline: Sparkline,
    pub observed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SummaryTotals {
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub virtual_cost_micros: u64,
    pub avg_latency_ms: f64,
    pub avg_proxy_setup_ms: f64,
    pub avg_shape_ms: f64,
    pub avg_sign_ms: f64,
    pub avg_upstream_ttfb_ms: f64,
    pub avg_upstream_body_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sparkline {
    pub buckets: Vec<UsageBucket>,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageBucket {
    pub bucket_start_unix_secs: u64,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    pub error_count: u64,
    pub virtual_cost_micros: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_input_micros: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_output_micros: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_cache_creation_5m_micros: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_cache_creation_1h_micros: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_cache_read_micros: Option<u64>,
    pub latency_ms_sum: u64,
    pub latency_count: u64,
    pub latency_ms_min: Option<u64>,
    pub latency_ms_max: Option<u64>,
    pub proxy_setup_ms_sum: u64,
    pub proxy_setup_ms_count: u64,
    pub shape_ms_sum: u64,
    pub shape_ms_count: u64,
    pub sign_ms_sum: u64,
    pub sign_ms_count: u64,
    pub upstream_ttfb_ms_sum: u64,
    pub upstream_ttfb_ms_count: u64,
    pub upstream_body_ms_sum: u64,
    pub upstream_body_ms_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DashboardUsageResponse {
    pub range: &'static str,
    pub step: &'static str,
    pub group_by: &'static str,
    pub window_start_unix_secs: u64,
    pub window_end_unix_secs: u64,
    pub series: Vec<UsageSeries>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncated_series_count: Option<u64>,
    pub observed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageSeries {
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_name: Option<String>,
    pub buckets: Vec<UsageBucket>,
}

#[derive(Debug)]
struct GroupAccumulator {
    total_request_count: u64,
    upstream_name: Option<String>,
    buckets: Vec<UsageBucket>,
}

#[derive(Debug)]
struct TotalGroupAccumulator {
    total_request_count: u64,
    upstream_name: Option<String>,
    buckets: BTreeMap<u64, UsageBucket>,
}

pub fn parse_range(value: &str) -> Result<DashboardRange, ParseRangeError> {
    match value {
        "15m" => Ok(DashboardRange::FifteenMinutes),
        "1h" => Ok(DashboardRange::OneHour),
        "6h" => Ok(DashboardRange::SixHours),
        "24h" => Ok(DashboardRange::TwentyFourHours),
        "7d" => Ok(DashboardRange::SevenDays),
        _ => Err(ParseRangeError),
    }
}

pub fn auto_step(range: DashboardRange) -> UsageRollupResolution {
    match range {
        DashboardRange::FifteenMinutes | DashboardRange::OneHour | DashboardRange::SixHours => {
            UsageRollupResolution::Minute
        }
        DashboardRange::TwentyFourHours | DashboardRange::SevenDays => UsageRollupResolution::Hour,
    }
}

pub fn build_window(range: DashboardRange, now_unix_secs: u64) -> (u64, u64) {
    build_window_for_step(range, auto_step(range), now_unix_secs)
}

pub fn parse_step(value: &str) -> Result<UsageRollupResolution, DashboardQueryError> {
    match value {
        "minute" => Ok(UsageRollupResolution::Minute),
        "hour" => Ok(UsageRollupResolution::Hour),
        _ => Err(DashboardQueryError::InvalidStep),
    }
}

pub fn parse_group_by(value: &str) -> Result<UsageGroupBy, DashboardQueryError> {
    match value {
        "none" => Ok(UsageGroupBy::None),
        "model" => Ok(UsageGroupBy::Model),
        "principal" => Ok(UsageGroupBy::Principal),
        "upstream" => Ok(UsageGroupBy::Upstream),
        _ => Err(DashboardQueryError::InvalidGroupBy),
    }
}

pub(crate) fn parse_usage_projection(
    value: Option<&str>,
) -> Result<UsageProjection, ParseUsageProjectionError> {
    match value {
        None | Some("full") => Ok(UsageProjection::Full),
        Some("totals") => Ok(UsageProjection::Totals),
        Some(_) => Err(ParseUsageProjectionError),
    }
}

pub fn validate_step_for_range(
    range: DashboardRange,
    step: UsageRollupResolution,
) -> Result<(), DashboardQueryError> {
    let width = step_width_secs(step);
    let range_secs = range.as_secs();
    let bucket_count = range_secs / width;
    if bucket_count == 0
        || !range_secs.is_multiple_of(width)
        || bucket_count > MAX_BUCKETS_PER_SERIES
    {
        return Err(DashboardQueryError::StepTooFineForRange);
    }
    Ok(())
}

pub async fn build_dashboard_summary(
    storage: &dyn Storage,
    range: DashboardRange,
    now_unix_secs: u64,
) -> Result<DashboardSummaryResponse, StorageError> {
    let step = auto_step(range);
    let (window_start_unix_secs, window_end_unix_secs) = build_window(range, now_unix_secs);
    let mut buckets = zero_filled_buckets(window_start_unix_secs, window_end_unix_secs, step);

    let rollups = storage
        .query_usage_rollups_in_range(step, window_start_unix_secs, window_end_unix_secs)
        .await?;
    let observed = !rollups.is_empty();
    for rollup in &rollups {
        add_rollup_to_buckets(&mut buckets, rollup, window_start_unix_secs, step);
    }
    let excluded_errors = storage
        .query_overview_excluded_error_buckets_in_range(
            step,
            window_start_unix_secs,
            window_end_unix_secs,
        )
        .await?;
    for excluded in excluded_errors {
        let Some(offset) = excluded.bucket_start.checked_sub(window_start_unix_secs) else {
            continue;
        };
        let index = (offset / step_width_secs(step)) as usize;
        let Some(bucket) = buckets.get_mut(index) else {
            continue;
        };
        if bucket.bucket_start_unix_secs == excluded.bucket_start {
            bucket.error_count = bucket.error_count.saturating_sub(excluded.error_count);
        }
    }

    let totals = summary_totals(&buckets);
    Ok(DashboardSummaryResponse {
        range: range.as_str(),
        step: step.as_str(),
        window_start_unix_secs,
        window_end_unix_secs,
        totals,
        sparkline: Sparkline { buckets },
        observed,
    })
}

async fn build_dashboard_usage_with_projection(
    storage: &dyn Storage,
    range: DashboardRange,
    step: UsageRollupResolution,
    group_by: UsageGroupBy,
    upstream_id: Option<Uuid>,
    now_unix_secs: u64,
    projection: UsageProjection,
) -> Result<DashboardUsageResponse, StorageError> {
    let (window_start_unix_secs, window_end_unix_secs) =
        build_window_for_step(range, step, now_unix_secs);
    let rollups = storage
        .query_usage_rollups_in_range(step, window_start_unix_secs, window_end_unix_secs)
        .await?;
    let rollups = rollups
        .into_iter()
        .filter(|rollup| upstream_id.is_none_or(|id| rollup.upstream_id == id))
        .collect::<Vec<_>>();
    let observed = !rollups.is_empty();
    let (mut series, truncated_series_count) = match projection {
        UsageProjection::Full => build_usage_series(
            group_by,
            &rollups,
            window_start_unix_secs,
            window_end_unix_secs,
            step,
        ),
        UsageProjection::Totals => {
            // Start from sparse rollup buckets instead of a dense window. Cost-only
            // buckets for selected principals are seeded before enrichment below;
            // this preserves both live-tail guardrails and recorded zero components.
            build_usage_totals_series(group_by, &rollups, window_start_unix_secs)
        }
    };
    if group_by == UsageGroupBy::Principal && !series.is_empty() {
        let principal_keys = selected_principal_cost_keys(&series);
        let bucket_width_secs = step_width_secs(step);
        let costs = storage
            .request_event_principal_costs(&RequestEventPrincipalCostQuery {
                since_unix_secs: window_start_unix_secs,
                until_unix_secs: window_end_unix_secs,
                bucket_width_secs,
                upstream_id,
                principal_keys,
            })
            .await?;
        if projection == UsageProjection::Totals {
            seed_principal_cost_buckets(&mut series, &costs);
        }
        enrich_principal_costs(&mut series, costs);
    }
    if projection == UsageProjection::Totals {
        collapse_usage_series(&mut series, window_start_unix_secs);
    }

    Ok(DashboardUsageResponse {
        range: range.as_str(),
        step: step.as_str(),
        group_by: group_by.as_str(),
        window_start_unix_secs,
        window_end_unix_secs,
        series,
        truncated_series_count,
        observed,
    })
}

pub(crate) async fn build_dashboard_usage_projected_checked(
    storage: &dyn Storage,
    range: DashboardRange,
    step: UsageRollupResolution,
    group_by: UsageGroupBy,
    upstream_id: Option<Uuid>,
    now_unix_secs: u64,
    projection: UsageProjection,
) -> Result<DashboardUsageResponse, DashboardBuildError> {
    validate_step_for_range(range, step)?;
    build_dashboard_usage_with_projection(
        storage,
        range,
        step,
        group_by,
        upstream_id,
        now_unix_secs,
        projection,
    )
    .await
    .map_err(Into::into)
}

#[derive(Debug)]
pub enum DashboardBuildError {
    Query(DashboardQueryError),
    Storage(StorageError),
}

impl From<DashboardQueryError> for DashboardBuildError {
    fn from(error: DashboardQueryError) -> Self {
        Self::Query(error)
    }
}

impl From<StorageError> for DashboardBuildError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl DashboardQueryError {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRange => "invalid_range",
            Self::InvalidStep => "invalid_step",
            Self::InvalidGroupBy => "invalid_group_by",
            Self::StepTooFineForRange => "step_too_fine_for_range",
        }
    }
}

impl DashboardRange {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FifteenMinutes => "15m",
            Self::OneHour => "1h",
            Self::SixHours => "6h",
            Self::TwentyFourHours => "24h",
            Self::SevenDays => "7d",
        }
    }

    fn as_secs(self) -> u64 {
        match self {
            Self::FifteenMinutes => 15 * MINUTE_SECS,
            Self::OneHour => HOUR_SECS,
            Self::SixHours => 6 * HOUR_SECS,
            Self::TwentyFourHours => 24 * HOUR_SECS,
            Self::SevenDays => 7 * 24 * HOUR_SECS,
        }
    }
}

impl UsageGroupBy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Model => "model",
            Self::Principal => "principal",
            Self::Upstream => "upstream",
        }
    }
}

pub(crate) fn build_window_for_step(
    range: DashboardRange,
    step: UsageRollupResolution,
    now_unix_secs: u64,
) -> (u64, u64) {
    let width = step_width_secs(step);
    let bucket_offset = now_unix_secs % width;
    // Minute/hour rollups are written atomically by one scheduler run; rounding up
    // includes their current partial buckets without adding one at an exact boundary.
    let window_end_unix_secs = if bucket_offset == 0 {
        now_unix_secs
    } else {
        now_unix_secs.saturating_add(width - bucket_offset)
    };
    let window_start_unix_secs = window_end_unix_secs.saturating_sub(range.as_secs());
    (window_start_unix_secs, window_end_unix_secs)
}

fn step_width_secs(step: UsageRollupResolution) -> u64 {
    match step {
        UsageRollupResolution::Minute => MINUTE_SECS,
        UsageRollupResolution::Hour => HOUR_SECS,
    }
}

fn zero_filled_buckets(
    window_start_unix_secs: u64,
    window_end_unix_secs: u64,
    step: UsageRollupResolution,
) -> Vec<UsageBucket> {
    let width = step_width_secs(step);
    let mut buckets = Vec::new();
    let mut bucket_start_unix_secs = window_start_unix_secs;
    while bucket_start_unix_secs < window_end_unix_secs {
        buckets.push(empty_bucket(bucket_start_unix_secs));
        bucket_start_unix_secs += width;
    }
    buckets
}

pub(crate) fn empty_bucket(bucket_start_unix_secs: u64) -> UsageBucket {
    UsageBucket {
        bucket_start_unix_secs,
        request_count: 0,
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        error_count: 0,
        virtual_cost_micros: 0,
        cost_input_micros: None,
        cost_output_micros: None,
        cost_cache_creation_5m_micros: None,
        cost_cache_creation_1h_micros: None,
        cost_cache_read_micros: None,
        latency_ms_sum: 0,
        latency_count: 0,
        latency_ms_min: None,
        latency_ms_max: None,
        proxy_setup_ms_sum: 0,
        proxy_setup_ms_count: 0,
        shape_ms_sum: 0,
        shape_ms_count: 0,
        sign_ms_sum: 0,
        sign_ms_count: 0,
        upstream_ttfb_ms_sum: 0,
        upstream_ttfb_ms_count: 0,
        upstream_body_ms_sum: 0,
        upstream_body_ms_count: 0,
    }
}

fn add_rollup_to_buckets(
    buckets: &mut [UsageBucket],
    rollup: &UsageRollup,
    window_start_unix_secs: u64,
    step: UsageRollupResolution,
) {
    let width = step_width_secs(step);
    let Some(offset) = rollup.bucket_start.checked_sub(window_start_unix_secs) else {
        return;
    };
    let index = (offset / width) as usize;
    let Some(bucket) = buckets.get_mut(index) else {
        return;
    };
    if bucket.bucket_start_unix_secs != rollup.bucket_start {
        return;
    }

    add_rollup_to_bucket(bucket, rollup);
}

fn add_rollup_to_bucket(bucket: &mut UsageBucket, rollup: &UsageRollup) {
    bucket.request_count += rollup.request_count;
    bucket.input_tokens += rollup.input_tokens;
    bucket.output_tokens += rollup.output_tokens;
    bucket.cache_creation_input_tokens += rollup.cache_creation_input_tokens;
    bucket.cache_read_input_tokens += rollup.cache_read_input_tokens;
    bucket.error_count += rollup.error_count;
    bucket.virtual_cost_micros += rollup.virtual_cost_micros;
    bucket.latency_ms_sum += rollup.latency_ms_sum;
    bucket.latency_count += rollup.latency_count;
    bucket.latency_ms_min = min_option(bucket.latency_ms_min, rollup.latency_ms_min);
    bucket.latency_ms_max = max_option(bucket.latency_ms_max, rollup.latency_ms_max);
    bucket.proxy_setup_ms_sum += rollup.proxy_setup_ms_sum;
    bucket.proxy_setup_ms_count += rollup.proxy_setup_ms_count;
    bucket.shape_ms_sum += rollup.shape_ms_sum;
    bucket.shape_ms_count += rollup.shape_ms_count;
    bucket.sign_ms_sum += rollup.sign_ms_sum;
    bucket.sign_ms_count += rollup.sign_ms_count;
    bucket.upstream_ttfb_ms_sum += rollup.upstream_ttfb_ms_sum;
    bucket.upstream_ttfb_ms_count += rollup.upstream_ttfb_ms_count;
    bucket.upstream_body_ms_sum += rollup.upstream_body_ms_sum;
    bucket.upstream_body_ms_count += rollup.upstream_body_ms_count;
}

fn min_option(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn max_option(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

fn summary_totals(buckets: &[UsageBucket]) -> SummaryTotals {
    let mut request_count = 0;
    let mut input_tokens = 0;
    let mut output_tokens = 0;
    let mut cache_creation_input_tokens = 0;
    let mut cache_read_input_tokens = 0;
    let mut error_count = 0;
    let mut virtual_cost_micros = 0;
    let mut latency_ms_sum = 0;
    let mut latency_count = 0;
    let mut proxy_setup_ms_sum = 0;
    let mut proxy_setup_ms_count = 0;
    let mut shape_ms_sum = 0;
    let mut shape_ms_count = 0;
    let mut sign_ms_sum = 0;
    let mut sign_ms_count = 0;
    let mut upstream_ttfb_ms_sum = 0;
    let mut upstream_ttfb_ms_count = 0;
    let mut upstream_body_ms_sum = 0;
    let mut upstream_body_ms_count = 0;

    for bucket in buckets {
        request_count += bucket.request_count;
        input_tokens += bucket.input_tokens;
        output_tokens += bucket.output_tokens;
        cache_creation_input_tokens += bucket.cache_creation_input_tokens;
        cache_read_input_tokens += bucket.cache_read_input_tokens;
        error_count += bucket.error_count;
        virtual_cost_micros += bucket.virtual_cost_micros;
        latency_ms_sum += bucket.latency_ms_sum;
        latency_count += bucket.latency_count;
        proxy_setup_ms_sum += bucket.proxy_setup_ms_sum;
        proxy_setup_ms_count += bucket.proxy_setup_ms_count;
        shape_ms_sum += bucket.shape_ms_sum;
        shape_ms_count += bucket.shape_ms_count;
        sign_ms_sum += bucket.sign_ms_sum;
        sign_ms_count += bucket.sign_ms_count;
        upstream_ttfb_ms_sum += bucket.upstream_ttfb_ms_sum;
        upstream_ttfb_ms_count += bucket.upstream_ttfb_ms_count;
        upstream_body_ms_sum += bucket.upstream_body_ms_sum;
        upstream_body_ms_count += bucket.upstream_body_ms_count;
    }

    SummaryTotals {
        request_count,
        input_tokens,
        output_tokens,
        cache_creation_input_tokens,
        cache_read_input_tokens,
        error_count,
        error_rate: if request_count == 0 {
            0.0
        } else {
            error_count as f64 / request_count as f64
        },
        virtual_cost_micros,
        avg_latency_ms: safe_avg(latency_ms_sum, latency_count),
        avg_proxy_setup_ms: safe_avg(proxy_setup_ms_sum, proxy_setup_ms_count),
        avg_shape_ms: safe_avg(shape_ms_sum, shape_ms_count),
        avg_sign_ms: safe_avg(sign_ms_sum, sign_ms_count),
        avg_upstream_ttfb_ms: safe_avg(upstream_ttfb_ms_sum, upstream_ttfb_ms_count),
        avg_upstream_body_ms: safe_avg(upstream_body_ms_sum, upstream_body_ms_count),
    }
}

fn safe_avg(sum: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum as f64 / count as f64
    }
}

pub(crate) fn build_usage_series(
    group_by: UsageGroupBy,
    rollups: &[UsageRollup],
    window_start_unix_secs: u64,
    window_end_unix_secs: u64,
    step: UsageRollupResolution,
) -> (Vec<UsageSeries>, Option<u64>) {
    if group_by == UsageGroupBy::None {
        let mut buckets = zero_filled_buckets(window_start_unix_secs, window_end_unix_secs, step);
        for rollup in rollups {
            add_rollup_to_buckets(&mut buckets, rollup, window_start_unix_secs, step);
        }
        return (
            vec![UsageSeries {
                key: "all".to_owned(),
                upstream_name: None,
                buckets,
            }],
            None,
        );
    }

    let mut groups: BTreeMap<String, GroupAccumulator> = BTreeMap::new();
    for rollup in rollups {
        let (key, upstream_name) = group_key(group_by, rollup);
        let entry = groups.entry(key).or_insert_with(|| GroupAccumulator {
            total_request_count: 0,
            upstream_name,
            buckets: zero_filled_buckets(window_start_unix_secs, window_end_unix_secs, step),
        });
        entry.total_request_count += rollup.request_count;
        add_rollup_to_buckets(&mut entry.buckets, rollup, window_start_unix_secs, step);
    }

    let mut ranked = groups.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_key, left), (right_key, right)| {
        right
            .total_request_count
            .cmp(&left.total_request_count)
            .then_with(|| left_key.cmp(right_key))
    });

    // Dashboard charts are capped at the top 20 dimension series by request count.
    let truncated_count = ranked.len().saturating_sub(MAX_GROUPED_SERIES);
    let series = ranked
        .into_iter()
        .take(MAX_GROUPED_SERIES)
        .map(|(key, group)| UsageSeries {
            key,
            upstream_name: group.upstream_name,
            buckets: group.buckets,
        })
        .collect();

    let truncated_series_count = if truncated_count == 0 {
        None
    } else {
        Some(truncated_count as u64)
    };
    (series, truncated_series_count)
}

fn build_usage_totals_series(
    group_by: UsageGroupBy,
    rollups: &[UsageRollup],
    window_start_unix_secs: u64,
) -> (Vec<UsageSeries>, Option<u64>) {
    if group_by == UsageGroupBy::None {
        let mut bucket = empty_bucket(window_start_unix_secs);
        for rollup in rollups {
            add_rollup_to_bucket(&mut bucket, rollup);
        }
        return (
            vec![UsageSeries {
                key: "all".to_owned(),
                upstream_name: None,
                buckets: vec![bucket],
            }],
            None,
        );
    }

    let mut groups = BTreeMap::<String, TotalGroupAccumulator>::new();
    for rollup in rollups {
        let (key, upstream_name) = group_key(group_by, rollup);
        let entry = groups.entry(key).or_insert_with(|| TotalGroupAccumulator {
            total_request_count: 0,
            upstream_name,
            buckets: BTreeMap::new(),
        });
        entry.total_request_count += rollup.request_count;
        let bucket = entry
            .buckets
            .entry(rollup.bucket_start)
            .or_insert_with(|| empty_bucket(rollup.bucket_start));
        add_rollup_to_bucket(bucket, rollup);
    }

    let mut ranked = groups.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_key, left), (right_key, right)| {
        right
            .total_request_count
            .cmp(&left.total_request_count)
            .then_with(|| left_key.cmp(right_key))
    });

    let truncated_count = ranked.len().saturating_sub(MAX_GROUPED_SERIES);
    let series = ranked
        .into_iter()
        .take(MAX_GROUPED_SERIES)
        .map(|(key, group)| UsageSeries {
            key,
            upstream_name: group.upstream_name,
            buckets: group.buckets.into_values().collect(),
        })
        .collect();
    let truncated_series_count = if truncated_count == 0 {
        None
    } else {
        Some(truncated_count as u64)
    };
    (series, truncated_series_count)
}

fn collapse_usage_series(series: &mut [UsageSeries], window_start_unix_secs: u64) {
    for item in series {
        let mut total = empty_bucket(window_start_unix_secs);
        for bucket in &item.buckets {
            add_usage_bucket_to_bucket(&mut total, bucket);
        }
        item.buckets.clear();
        item.buckets.push(total);
    }
}

fn add_usage_bucket_to_bucket(total: &mut UsageBucket, bucket: &UsageBucket) {
    total.request_count += bucket.request_count;
    total.input_tokens += bucket.input_tokens;
    total.output_tokens += bucket.output_tokens;
    total.cache_creation_input_tokens += bucket.cache_creation_input_tokens;
    total.cache_read_input_tokens += bucket.cache_read_input_tokens;
    total.error_count += bucket.error_count;
    total.virtual_cost_micros += bucket.virtual_cost_micros;
    total.cost_input_micros = sum_optional(total.cost_input_micros, bucket.cost_input_micros);
    total.cost_output_micros = sum_optional(total.cost_output_micros, bucket.cost_output_micros);
    total.cost_cache_creation_5m_micros = sum_optional(
        total.cost_cache_creation_5m_micros,
        bucket.cost_cache_creation_5m_micros,
    );
    total.cost_cache_creation_1h_micros = sum_optional(
        total.cost_cache_creation_1h_micros,
        bucket.cost_cache_creation_1h_micros,
    );
    total.cost_cache_read_micros =
        sum_optional(total.cost_cache_read_micros, bucket.cost_cache_read_micros);
    total.latency_ms_sum += bucket.latency_ms_sum;
    total.latency_count += bucket.latency_count;
    total.latency_ms_min = min_option(total.latency_ms_min, bucket.latency_ms_min);
    total.latency_ms_max = max_option(total.latency_ms_max, bucket.latency_ms_max);
    total.proxy_setup_ms_sum += bucket.proxy_setup_ms_sum;
    total.proxy_setup_ms_count += bucket.proxy_setup_ms_count;
    total.shape_ms_sum += bucket.shape_ms_sum;
    total.shape_ms_count += bucket.shape_ms_count;
    total.sign_ms_sum += bucket.sign_ms_sum;
    total.sign_ms_count += bucket.sign_ms_count;
    total.upstream_ttfb_ms_sum += bucket.upstream_ttfb_ms_sum;
    total.upstream_ttfb_ms_count += bucket.upstream_ttfb_ms_count;
    total.upstream_body_ms_sum += bucket.upstream_body_ms_sum;
    total.upstream_body_ms_count += bucket.upstream_body_ms_count;
}

fn sum_optional(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current + next),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn selected_principal_cost_keys(series: &[UsageSeries]) -> Vec<String> {
    let mut ranked = series
        .iter()
        .map(|series| {
            let total_virtual_cost_micros = series.buckets.iter().fold(0_u128, |total, bucket| {
                total + u128::from(bucket.virtual_cost_micros)
            });
            (series.key.as_str(), total_virtual_cost_micros)
        })
        .collect::<Vec<_>>();
    ranked.sort_unstable_by(
        |(left_key, left_cost_micros), (right_key, right_cost_micros)| {
            right_cost_micros
                .cmp(left_cost_micros)
                .then_with(|| left_key.cmp(right_key))
        },
    );
    ranked
        .into_iter()
        .take(MAX_PRINCIPAL_COST_SERIES)
        .map(|(key, _)| key.to_owned())
        .collect()
}

fn seed_principal_cost_buckets(
    series: &mut [UsageSeries],
    costs: &[RequestEventPrincipalCostBucket],
) {
    for cost in costs {
        let Some(principal_series) = series
            .iter_mut()
            .find(|principal_series| principal_series.key == cost.principal)
        else {
            continue;
        };
        match principal_series
            .buckets
            .binary_search_by_key(&cost.bucket_start_unix_secs, |bucket| {
                bucket.bucket_start_unix_secs
            }) {
            Ok(_) => {}
            Err(index) => principal_series
                .buckets
                .insert(index, empty_bucket(cost.bucket_start_unix_secs)),
        }
    }
}

fn enrich_principal_costs(series: &mut [UsageSeries], costs: Vec<RequestEventPrincipalCostBucket>) {
    let mut costs_by_principal =
        BTreeMap::<String, BTreeMap<u64, RequestEventPrincipalCostBucket>>::new();
    for cost in costs {
        costs_by_principal
            .entry(cost.principal.clone())
            .or_default()
            .insert(cost.bucket_start_unix_secs, cost);
    }

    for principal_series in series {
        let Some(cost_buckets) = costs_by_principal.get(&principal_series.key) else {
            continue;
        };
        for bucket in &mut principal_series.buckets {
            let Some(costs) = cost_buckets.get(&bucket.bucket_start_unix_secs) else {
                continue;
            };
            let component_total = costs
                .cost_input_micros
                .checked_add(costs.cost_output_micros)
                .and_then(|total| total.checked_add(costs.cost_cache_creation_5m_micros))
                .and_then(|total| total.checked_add(costs.cost_cache_creation_1h_micros))
                .and_then(|total| total.checked_add(costs.cost_cache_read_micros));
            if !costs.component_costs_recorded
                || component_total.is_none_or(|total| total > costs.total_cost_micros)
                || costs.total_cost_micros > bucket.virtual_cost_micros
            {
                continue;
            }

            bucket.cost_input_micros = Some(costs.cost_input_micros);
            bucket.cost_output_micros = Some(costs.cost_output_micros);
            bucket.cost_cache_creation_5m_micros = Some(costs.cost_cache_creation_5m_micros);
            bucket.cost_cache_creation_1h_micros = Some(costs.cost_cache_creation_1h_micros);
            bucket.cost_cache_read_micros = Some(costs.cost_cache_read_micros);
        }
    }
}

fn group_key(group_by: UsageGroupBy, rollup: &UsageRollup) -> (String, Option<String>) {
    match group_by {
        UsageGroupBy::None => ("all".to_owned(), None),
        UsageGroupBy::Model => (rollup.model.clone(), None),
        UsageGroupBy::Principal => (rollup.principal.clone(), None),
        UsageGroupBy::Upstream => (
            rollup.upstream_id.to_string(),
            Some(rollup.upstream_name.clone()),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALIGNED_HOUR_UNIX_SECS: u64 = 8 * 24 * HOUR_SECS;
    const ALIGNED_MINUTE_UNIX_SECS: u64 = ALIGNED_HOUR_UNIX_SECS + MINUTE_SECS;
    const UNALIGNED_NOW_UNIX_SECS: u64 = ALIGNED_HOUR_UNIX_SECS + 1;

    #[test]
    fn unaligned_auto_step_windows_include_the_current_partial_bucket() {
        let cases = [
            (DashboardRange::OneHour, MINUTE_SECS, 60),
            (DashboardRange::SixHours, MINUTE_SECS, 360),
            (DashboardRange::TwentyFourHours, HOUR_SECS, 24),
            (DashboardRange::SevenDays, HOUR_SECS, 168),
        ];

        for (range, width, expected_bucket_count) in cases {
            let step = auto_step(range);
            let (start, end) = build_window(range, UNALIGNED_NOW_UNIX_SECS);
            let current_bucket_start = UNALIGNED_NOW_UNIX_SECS - (UNALIGNED_NOW_UNIX_SECS % width);

            assert_eq!(end, current_bucket_start + width);
            assert_eq!(end - start, range.as_secs());

            let buckets = zero_filled_buckets(start, end, step);
            assert_eq!(buckets.len(), expected_bucket_count);
            assert_eq!(
                buckets.last().map(|bucket| bucket.bucket_start_unix_secs),
                Some(current_bucket_start)
            );
        }
    }

    #[test]
    fn exact_boundaries_do_not_advance_or_add_a_bucket() {
        let cases = [
            (
                DashboardRange::OneHour,
                ALIGNED_MINUTE_UNIX_SECS,
                MINUTE_SECS,
                60,
            ),
            (
                DashboardRange::SixHours,
                ALIGNED_MINUTE_UNIX_SECS,
                MINUTE_SECS,
                360,
            ),
            (
                DashboardRange::TwentyFourHours,
                ALIGNED_HOUR_UNIX_SECS,
                HOUR_SECS,
                24,
            ),
            (
                DashboardRange::SevenDays,
                ALIGNED_HOUR_UNIX_SECS,
                HOUR_SECS,
                168,
            ),
        ];

        for (range, now_unix_secs, width, expected_bucket_count) in cases {
            let step = auto_step(range);
            let (start, end) = build_window(range, now_unix_secs);

            assert_eq!(end, now_unix_secs);
            assert_eq!(end - start, range.as_secs());

            let buckets = zero_filled_buckets(start, end, step);
            assert_eq!(buckets.len(), expected_bucket_count);
            assert_eq!(
                buckets.last().map(|bucket| bucket.bucket_start_unix_secs),
                Some(end - width)
            );
        }
    }

    #[test]
    fn zero_time_stays_at_the_exact_boundary() {
        for range in [
            DashboardRange::OneHour,
            DashboardRange::SixHours,
            DashboardRange::TwentyFourHours,
            DashboardRange::SevenDays,
        ] {
            let (start, end) = build_window(range, 0);
            assert_eq!((start, end), (0, 0));
            assert!(zero_filled_buckets(start, end, auto_step(range)).is_empty());
        }
    }

    #[test]
    fn window_rounding_saturates_near_u64_max_without_extra_buckets() {
        let cases = [
            (DashboardRange::OneHour, UsageRollupResolution::Minute, 60),
            (
                DashboardRange::TwentyFourHours,
                UsageRollupResolution::Hour,
                24,
            ),
        ];

        for (range, step, expected_bucket_count) in cases {
            let (start, end) = build_window_for_step(range, step, u64::MAX);

            assert_eq!(end, u64::MAX);
            assert_eq!(end - start, range.as_secs());
            assert_eq!(
                zero_filled_buckets(start, end, step).len(),
                expected_bucket_count
            );
        }
    }

    #[test]
    fn totals_series_materializes_only_observed_rollup_buckets() {
        let rollup = |bucket_start, request_count| UsageRollup {
            resolution: UsageRollupResolution::Minute,
            bucket_start,
            principal: "principal-a".to_owned(),
            upstream_id: Uuid::from_u128(1),
            upstream_name: "upstream-a".to_owned(),
            model: "model-a".to_owned(),
            request_count,
            input_tokens: request_count,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            error_count: 0,
            latency_count: request_count,
            latency_ms_sum: request_count * 10,
            latency_ms_min: Some(10),
            latency_ms_max: Some(10),
            proxy_setup_ms_count: 0,
            proxy_setup_ms_sum: 0,
            shape_ms_count: 0,
            shape_ms_sum: 0,
            sign_ms_count: 0,
            sign_ms_sum: 0,
            upstream_ttfb_ms_count: 0,
            upstream_ttfb_ms_sum: 0,
            upstream_body_ms_count: 0,
            upstream_body_ms_sum: 0,
            virtual_cost_micros: request_count,
        };
        let rollups = [rollup(MINUTE_SECS, 1), rollup(59 * MINUTE_SECS, 2)];

        let (mut series, truncated_series_count) =
            build_usage_totals_series(UsageGroupBy::Principal, &rollups, 0);

        assert_eq!(truncated_series_count, None);
        assert_eq!(series.len(), 1);
        assert_eq!(
            series[0]
                .buckets
                .iter()
                .map(|bucket| bucket.bucket_start_unix_secs)
                .collect::<Vec<_>>(),
            vec![MINUTE_SECS, 59 * MINUTE_SECS]
        );
        collapse_usage_series(&mut series, 0);
        assert_eq!(series[0].buckets.len(), 1);
        assert_eq!(series[0].buckets[0].request_count, 3);
    }

    #[test]
    fn principal_cost_keys_are_limited_and_ranked_by_summed_virtual_cost() {
        let principal_series = |key: &str, costs: &[u64]| UsageSeries {
            key: key.to_owned(),
            upstream_name: None,
            buckets: costs
                .iter()
                .map(|cost| {
                    let mut bucket = empty_bucket(0);
                    bucket.virtual_cost_micros = *cost;
                    bucket
                })
                .collect(),
        };
        let series = vec![
            principal_series("principal-a", &[5]),
            principal_series("principal-b", &[50, 25]),
            principal_series("principal-c", &[100]),
            principal_series("principal-d", &[90]),
            principal_series("principal-e", &[80]),
            principal_series("principal-f", &[70]),
            principal_series("principal-g", &[70]),
        ];
        assert_eq!(
            selected_principal_cost_keys(&series),
            vec![
                "principal-c",
                "principal-d",
                "principal-e",
                "principal-b",
                "principal-f",
            ]
        );
    }

    struct DefaultPrincipalCostStore;

    #[async_trait::async_trait]
    impl cc_lb_storage_api::RequestEventStore for DefaultPrincipalCostStore {
        async fn append_request_event(
            &self,
            _event: &cc_lb_storage_api::RequestEvent,
        ) -> cc_lb_storage_api::StorageResult<u64> {
            Ok(0)
        }

        async fn query_request_events(
            &self,
            _since: u64,
            _until: u64,
            _limit: usize,
        ) -> cc_lb_storage_api::StorageResult<Vec<cc_lb_storage_api::RequestEvent>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn optional_principal_cost_enrichment_defaults_to_absent_fields() {
        let rows = cc_lb_storage_api::RequestEventStore::request_event_principal_costs(
            &DefaultPrincipalCostStore,
            &RequestEventPrincipalCostQuery {
                since_unix_secs: 0,
                until_unix_secs: MINUTE_SECS,
                bucket_width_secs: MINUTE_SECS,
                upstream_id: None,
                principal_keys: vec!["principal-a".to_owned()],
            },
        )
        .await
        .expect("optional principal cost enrichment");

        assert!(rows.is_empty());

        let mut bucket = empty_bucket(0);
        bucket.virtual_cost_micros = 10;
        let mut series = vec![UsageSeries {
            key: "principal-a".to_owned(),
            upstream_name: None,
            buckets: vec![bucket],
        }];
        enrich_principal_costs(&mut series, rows);

        let bucket = &series[0].buckets[0];
        assert_eq!(bucket.cost_input_micros, None);
        assert_eq!(bucket.cost_output_micros, None);
        assert_eq!(bucket.cost_cache_creation_5m_micros, None);
        assert_eq!(bucket.cost_cache_creation_1h_micros, None);
        assert_eq!(bucket.cost_cache_read_micros, None);
    }

    #[test]
    fn principal_cost_enrichment_omits_inconsistent_component_sum() {
        let bucket_start = 1_700_000_000;
        let mut bucket = empty_bucket(bucket_start);
        bucket.virtual_cost_micros = 10;
        let mut series = vec![UsageSeries {
            key: "principal-a".to_owned(),
            upstream_name: None,
            buckets: vec![bucket],
        }];

        enrich_principal_costs(
            &mut series,
            vec![RequestEventPrincipalCostBucket {
                principal: "principal-a".to_owned(),
                bucket_start_unix_secs: bucket_start,
                total_cost_micros: 10,
                component_costs_recorded: true,
                cost_input_micros: 11,
                ..RequestEventPrincipalCostBucket::default()
            }],
        );

        let bucket = &series[0].buckets[0];
        assert_eq!(bucket.cost_input_micros, None);
        assert_eq!(bucket.cost_output_micros, None);
        assert_eq!(bucket.cost_cache_creation_5m_micros, None);
        assert_eq!(bucket.cost_cache_creation_1h_micros, None);
        assert_eq!(bucket.cost_cache_read_micros, None);
    }
}
