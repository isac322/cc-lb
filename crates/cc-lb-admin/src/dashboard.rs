use std::collections::BTreeMap;

use cc_lb_storage_redb::{Storage, StorageError, UsageRollup, UsageRollupResolution};
use serde::Serialize;

const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const MAX_BUCKETS_PER_SERIES: u64 = 1_440;
const MAX_GROUPED_SERIES: usize = 20;

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
    pub error_count: u64,
    pub error_rate: f64,
    pub virtual_cost_micros: u64,
    pub avg_latency_ms: f64,
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
    pub error_count: u64,
    pub virtual_cost_micros: u64,
    pub latency_ms_sum: u64,
    pub latency_count: u64,
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
    pub buckets: Vec<UsageBucket>,
}

#[derive(Debug)]
struct GroupAccumulator {
    total_request_count: u64,
    buckets: Vec<UsageBucket>,
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

pub fn validate_step_for_range(
    range: DashboardRange,
    step: UsageRollupResolution,
) -> Result<(), DashboardQueryError> {
    let width = step_width_secs(step);
    let range_secs = range.as_secs();
    let bucket_count = range_secs / width;
    if bucket_count == 0 || range_secs % width != 0 || bucket_count > MAX_BUCKETS_PER_SERIES {
        return Err(DashboardQueryError::StepTooFineForRange);
    }
    Ok(())
}

pub fn build_dashboard_summary(
    storage: Option<&Storage>,
    range: DashboardRange,
    now_unix_secs: u64,
) -> Result<DashboardSummaryResponse, StorageError> {
    let step = auto_step(range);
    let (window_start_unix_secs, window_end_unix_secs) = build_window(range, now_unix_secs);
    let mut buckets = zero_filled_buckets(window_start_unix_secs, window_end_unix_secs, step);
    let mut observed = false;

    if let Some(storage) = storage {
        let rollups = storage.query_usage_rollups_in_range(
            step,
            window_start_unix_secs,
            window_end_unix_secs,
        )?;
        observed = !rollups.is_empty();
        for rollup in &rollups {
            add_rollup_to_buckets(&mut buckets, rollup, window_start_unix_secs, step);
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

pub fn build_dashboard_usage(
    storage: Option<&Storage>,
    range: DashboardRange,
    step: UsageRollupResolution,
    group_by: UsageGroupBy,
    now_unix_secs: u64,
) -> Result<DashboardUsageResponse, StorageError> {
    let (window_start_unix_secs, window_end_unix_secs) =
        build_window_for_step(range, step, now_unix_secs);
    let rollups = match storage {
        Some(storage) => storage.query_usage_rollups_in_range(
            step,
            window_start_unix_secs,
            window_end_unix_secs,
        )?,
        None => Vec::new(),
    };
    let observed = storage.is_some() && !rollups.is_empty();
    let (series, truncated_series_count) = build_usage_series(
        group_by,
        &rollups,
        window_start_unix_secs,
        window_end_unix_secs,
        step,
    );

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

pub fn build_dashboard_usage_checked(
    storage: Option<&Storage>,
    range: DashboardRange,
    step: UsageRollupResolution,
    group_by: UsageGroupBy,
    now_unix_secs: u64,
) -> Result<DashboardUsageResponse, DashboardBuildError> {
    validate_step_for_range(range, step)?;
    build_dashboard_usage(storage, range, step, group_by, now_unix_secs).map_err(Into::into)
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
    let window_end_unix_secs = now_unix_secs - (now_unix_secs % width);
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
        buckets.push(UsageBucket {
            bucket_start_unix_secs,
            request_count: 0,
            input_tokens: 0,
            output_tokens: 0,
            error_count: 0,
            virtual_cost_micros: 0,
            latency_ms_sum: 0,
            latency_count: 0,
        });
        bucket_start_unix_secs += width;
    }
    buckets
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

    bucket.request_count += rollup.request_count;
    bucket.input_tokens += rollup.input_tokens;
    bucket.output_tokens += rollup.output_tokens;
    bucket.error_count += rollup.error_count;
    bucket.virtual_cost_micros += rollup.virtual_cost_micros;
    bucket.latency_ms_sum += rollup.latency_ms_sum;
    bucket.latency_count += rollup.latency_count;
}

fn summary_totals(buckets: &[UsageBucket]) -> SummaryTotals {
    let mut request_count = 0;
    let mut input_tokens = 0;
    let mut output_tokens = 0;
    let mut error_count = 0;
    let mut virtual_cost_micros = 0;
    let mut latency_ms_sum = 0;
    let mut latency_count = 0;

    for bucket in buckets {
        request_count += bucket.request_count;
        input_tokens += bucket.input_tokens;
        output_tokens += bucket.output_tokens;
        error_count += bucket.error_count;
        virtual_cost_micros += bucket.virtual_cost_micros;
        latency_ms_sum += bucket.latency_ms_sum;
        latency_count += bucket.latency_count;
    }

    SummaryTotals {
        request_count,
        input_tokens,
        output_tokens,
        error_count,
        error_rate: if request_count == 0 {
            0.0
        } else {
            error_count as f64 / request_count as f64
        },
        virtual_cost_micros,
        avg_latency_ms: if latency_count == 0 {
            0.0
        } else {
            latency_ms_sum as f64 / latency_count as f64
        },
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
                buckets,
            }],
            None,
        );
    }

    let mut groups: BTreeMap<String, GroupAccumulator> = BTreeMap::new();
    for rollup in rollups {
        let key = group_key(group_by, rollup).to_owned();
        let entry = groups.entry(key).or_insert_with(|| GroupAccumulator {
            total_request_count: 0,
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

fn group_key(group_by: UsageGroupBy, rollup: &UsageRollup) -> &str {
    match group_by {
        UsageGroupBy::None => "all",
        UsageGroupBy::Model => &rollup.model,
        UsageGroupBy::Principal => &rollup.principal,
        UsageGroupBy::Upstream => &rollup.upstream,
    }
}
