use std::collections::{BTreeMap, HashMap, HashSet};

use cc_lb_storage_api::{
    PricingCompleteness, PricingCompletenessInput, Provider, Storage, StorageError, UpstreamStore,
    UsageRollup, UsageRollupResolution, derive_pricing_completeness,
};
use serde::Serialize;
use uuid::Uuid;

const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const MAX_BUCKETS_PER_SERIES: u64 = 1_440;
const MAX_GROUPED_SERIES: usize = 20;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ProviderFamily {
    Anthropic,
    Openai,
    Gemini,
}

impl ProviderFamily {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::Openai => "openai",
            Self::Gemini => "gemini",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParseProvidersError;

pub(crate) fn parse_provider_families(
    value: Option<&str>,
) -> Result<Option<Vec<ProviderFamily>>, ParseProvidersError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let mut families = HashSet::new();
    for item in value.split(',').map(str::trim) {
        let family = match item {
            "anthropic" => ProviderFamily::Anthropic,
            "openai" => ProviderFamily::Openai,
            "gemini" => ProviderFamily::Gemini,
            _ => return Err(ParseProvidersError),
        };
        families.insert(family);
    }
    if families.is_empty() {
        return Err(ParseProvidersError);
    }
    let mut families = families.into_iter().collect::<Vec<_>>();
    families.sort_by_key(|family| family.as_str());
    Ok(Some(families))
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ProviderScope {
    pub(crate) upstream_ids: Option<Vec<Uuid>>,
    pub(crate) family_by_upstream: HashMap<Uuid, ProviderFamily>,
}

impl ProviderScope {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    fn matches_upstream(&self, upstream_id: Uuid) -> bool {
        self.upstream_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(&upstream_id))
    }
}

pub(crate) async fn resolve_provider_scope(
    storage: &dyn Storage,
    families: Option<&[ProviderFamily]>,
    include_all: bool,
) -> Result<ProviderScope, StorageError> {
    if families.is_none() && !include_all {
        return Ok(ProviderScope::none());
    }
    let requested = families.map(|families| families.iter().copied().collect::<HashSet<_>>());
    let mut after = None;
    let mut records = Vec::new();
    loop {
        let page = UpstreamStore::list(storage, after, 1_000).await?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|record| record.id);
        let page_len = page.len();
        records.extend(page);
        if page_len < 1_000 {
            break;
        }
    }
    let mut upstream_ids = families.map(|_| Vec::new());
    let mut family_by_upstream = HashMap::new();
    for record in records {
        if record.deleted_at_unix_secs.is_some() {
            continue;
        }
        let family = match record.kind.provider() {
            Provider::Anthropic => ProviderFamily::Anthropic,
            Provider::Openai => ProviderFamily::Openai,
            Provider::Gemini => ProviderFamily::Gemini,
        };
        if requested
            .as_ref()
            .is_some_and(|requested| !requested.contains(&family))
        {
            continue;
        }
        if let Some(ids) = upstream_ids.as_mut() {
            ids.push(record.id);
        }
        if include_all || families.is_some() {
            family_by_upstream.insert(record.id, family);
        }
    }
    Ok(ProviderScope {
        upstream_ids,
        family_by_upstream,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct UsageFilter {
    pub(crate) group_by: UsageGroupBy,
    pub(crate) upstream_id: Option<Uuid>,
    pub(crate) principal_id: Option<String>,
    pub(crate) projection: UsageProjection,
    pub(crate) provider_scope: ProviderScope,
}

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
    Provider,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_completeness: Option<PricingCompleteness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priced_request_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unpriced_request_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unmetered_request_count: Option<u64>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens_30m: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_write_input_tokens_12h: Option<u64>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_cache_write_30m_micros: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost_cache_write_12h_micros: Option<u64>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_completeness: Option<PricingCompleteness>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priced_request_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unpriced_request_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unmetered_request_count: Option<u64>,
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
    bucket: UsageBucket,
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
        "provider" => Ok(UsageGroupBy::Provider),
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
    build_dashboard_summary_filtered(storage, range, now_unix_secs, &ProviderScope::none()).await
}

pub(crate) async fn build_dashboard_summary_filtered(
    storage: &dyn Storage,
    range: DashboardRange,
    now_unix_secs: u64,
    provider_scope: &ProviderScope,
) -> Result<DashboardSummaryResponse, StorageError> {
    let step = auto_step(range);
    let (window_start_unix_secs, window_end_unix_secs) = build_window(range, now_unix_secs);
    let mut buckets = zero_filled_buckets(window_start_unix_secs, window_end_unix_secs, step);

    let rollups = storage
        .query_usage_rollups_in_range(step, window_start_unix_secs, window_end_unix_secs)
        .await?;
    let rollups = rollups
        .into_iter()
        .filter(|rollup| provider_scope.matches_upstream(rollup.upstream_id))
        .collect::<Vec<_>>();
    let observed = !rollups.is_empty();
    for rollup in &rollups {
        add_rollup_to_buckets(&mut buckets, rollup, window_start_unix_secs, step, false);
    }
    if provider_scope.upstream_ids.is_none() {
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
    filter: &UsageFilter,
    now_unix_secs: u64,
) -> Result<DashboardUsageResponse, StorageError> {
    let group_by = filter.group_by;
    let upstream_id = filter.upstream_id;
    let principal_id = filter.principal_id.as_deref();
    let projection = filter.projection;
    let provider_scope = &filter.provider_scope;
    let (window_start_unix_secs, window_end_unix_secs) =
        build_window_for_step(range, step, now_unix_secs);
    let rollups = storage
        .query_usage_rollups_in_range(step, window_start_unix_secs, window_end_unix_secs)
        .await?;
    let rollups = rollups
        .into_iter()
        .filter(|rollup| {
            upstream_id.is_none_or(|id| rollup.upstream_id == id)
                && provider_scope.matches_upstream(rollup.upstream_id)
                && principal_id.is_none_or(|id| rollup.principal == id)
        })
        .collect::<Vec<_>>();
    let observed = !rollups.is_empty();
    let (series, truncated_series_count) = match projection {
        _ if principal_id.is_some() && !observed => (Vec::new(), None),
        UsageProjection::Full => build_usage_series_with_provider(
            group_by,
            &rollups,
            window_start_unix_secs,
            window_end_unix_secs,
            step,
            &provider_scope.family_by_upstream,
        ),
        UsageProjection::Totals => build_usage_totals_series(
            group_by,
            &rollups,
            window_start_unix_secs,
            &provider_scope.family_by_upstream,
        ),
    };

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
    filter: &UsageFilter,
    now_unix_secs: u64,
) -> Result<DashboardUsageResponse, DashboardBuildError> {
    validate_step_for_range(range, step)?;
    build_dashboard_usage_with_projection(storage, range, step, filter, now_unix_secs)
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
            Self::Provider => "provider",
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
        cache_write_input_tokens: None,
        cache_write_input_tokens_30m: None,
        cache_write_input_tokens_12h: None,
        error_count: 0,
        virtual_cost_micros: 0,
        cost_input_micros: None,
        cost_output_micros: None,
        cost_cache_creation_5m_micros: None,
        cost_cache_creation_1h_micros: None,
        cost_cache_read_micros: None,
        cost_cache_write_30m_micros: None,
        cost_cache_write_12h_micros: None,
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
        pricing_completeness: None,
        priced_request_count: None,
        unpriced_request_count: None,
        unmetered_request_count: None,
    }
}

fn add_rollup_to_buckets(
    buckets: &mut [UsageBucket],
    rollup: &UsageRollup,
    window_start_unix_secs: u64,
    step: UsageRollupResolution,
    include_cost_components: bool,
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

    add_rollup_to_bucket(bucket, rollup, include_cost_components);
}

fn bucket_total_tokens(bucket: &UsageBucket) -> u64 {
    [
        bucket.input_tokens,
        bucket.output_tokens,
        bucket.cache_creation_input_tokens,
        bucket.cache_read_input_tokens,
        bucket.cache_write_input_tokens.unwrap_or_else(|| {
            bucket
                .cache_write_input_tokens_30m
                .unwrap_or(0)
                .saturating_add(bucket.cache_write_input_tokens_12h.unwrap_or(0))
        }),
    ]
    .into_iter()
    .fold(0, u64::saturating_add)
}

fn add_rollup_to_bucket(
    bucket: &mut UsageBucket,
    rollup: &UsageRollup,
    include_cost_components: bool,
) {
    bucket.request_count += rollup.request_count;
    bucket.input_tokens += rollup.input_tokens;
    bucket.output_tokens += rollup.output_tokens;
    bucket.cache_creation_input_tokens += rollup.cache_creation_input_tokens;
    bucket.cache_read_input_tokens += rollup.cache_read_input_tokens;
    if let Some(value) = rollup.cache_write_input_tokens {
        bucket.cache_write_input_tokens =
            Some(bucket.cache_write_input_tokens.unwrap_or_default() + value);
    }
    if let Some(value) = rollup.cache_write_input_tokens_30m {
        bucket.cache_write_input_tokens_30m =
            Some(bucket.cache_write_input_tokens_30m.unwrap_or_default() + value);
    }
    if let Some(value) = rollup.cache_write_input_tokens_12h {
        bucket.cache_write_input_tokens_12h =
            Some(bucket.cache_write_input_tokens_12h.unwrap_or_default() + value);
    }
    bucket.error_count += rollup.error_count;
    bucket.virtual_cost_micros += rollup.virtual_cost_micros;
    if include_cost_components && rollup.cost_components_event_count > 0 {
        bucket.cost_input_micros =
            sum_optional(bucket.cost_input_micros, Some(rollup.cost_input_micros));
        bucket.cost_output_micros =
            sum_optional(bucket.cost_output_micros, Some(rollup.cost_output_micros));
        bucket.cost_cache_creation_5m_micros = sum_optional(
            bucket.cost_cache_creation_5m_micros,
            Some(rollup.cost_cache_creation_5m_micros),
        );
        bucket.cost_cache_creation_1h_micros = sum_optional(
            bucket.cost_cache_creation_1h_micros,
            Some(rollup.cost_cache_creation_1h_micros),
        );
        bucket.cost_cache_read_micros = sum_optional(
            bucket.cost_cache_read_micros,
            Some(rollup.cost_cache_read_micros),
        );
        bucket.cost_cache_write_30m_micros = sum_optional(
            bucket.cost_cache_write_30m_micros,
            Some(rollup.cost_cache_write_30m_micros),
        );
        bucket.cost_cache_write_12h_micros = sum_optional(
            bucket.cost_cache_write_12h_micros,
            Some(rollup.cost_cache_write_12h_micros),
        );
    }
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
    if bucket.request_count > 0 {
        let priced = bucket
            .priced_request_count
            .unwrap_or(0)
            .saturating_add(rollup.priced_event_count);
        let unpriced = bucket
            .unpriced_request_count
            .unwrap_or(0)
            .saturating_add(rollup.unpriced_event_count);
        let unmetered = bucket
            .unmetered_request_count
            .unwrap_or(0)
            .saturating_add(rollup.unmetered_event_count);
        bucket.priced_request_count = Some(priced);
        bucket.unpriced_request_count = Some(unpriced);
        bucket.unmetered_request_count = Some(unmetered);
        bucket.pricing_completeness = derive_pricing_completeness(PricingCompletenessInput {
            request_count: bucket.request_count,
            priced_event_count: priced,
            unpriced_event_count: unpriced,
            unmetered_event_count: unmetered,
            total_tokens: bucket_total_tokens(bucket),
            virtual_cost_micros: bucket.virtual_cost_micros,
        });
    }
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
    let mut pricing = PricingCompletenessInput::default();

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
        pricing.priced_event_count = pricing
            .priced_event_count
            .saturating_add(bucket.priced_request_count.unwrap_or(0));
        pricing.unpriced_event_count = pricing
            .unpriced_event_count
            .saturating_add(bucket.unpriced_request_count.unwrap_or(0));
        pricing.unmetered_event_count = pricing
            .unmetered_event_count
            .saturating_add(bucket.unmetered_request_count.unwrap_or(0));
        pricing.total_tokens = pricing
            .total_tokens
            .saturating_add(bucket_total_tokens(bucket));
    }

    pricing.request_count = request_count;
    pricing.virtual_cost_micros = virtual_cost_micros;
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
        pricing_completeness: derive_pricing_completeness(pricing),
        priced_request_count: (request_count > 0).then_some(pricing.priced_event_count),
        unpriced_request_count: (request_count > 0).then_some(pricing.unpriced_event_count),
        unmetered_request_count: (request_count > 0).then_some(pricing.unmetered_event_count),
    }
}

fn safe_avg(sum: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        sum as f64 / count as f64
    }
}

fn build_usage_series_with_provider(
    group_by: UsageGroupBy,
    rollups: &[UsageRollup],
    window_start_unix_secs: u64,
    window_end_unix_secs: u64,
    step: UsageRollupResolution,
    family_by_upstream: &HashMap<Uuid, ProviderFamily>,
) -> (Vec<UsageSeries>, Option<u64>) {
    if group_by == UsageGroupBy::None {
        let mut buckets = zero_filled_buckets(window_start_unix_secs, window_end_unix_secs, step);
        for rollup in rollups {
            add_rollup_to_buckets(&mut buckets, rollup, window_start_unix_secs, step, false);
        }
        for bucket in &mut buckets {
            sanitize_bucket_cost_components(bucket);
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
        let Some((key, upstream_name)) = group_key(group_by, rollup, family_by_upstream) else {
            continue;
        };
        let entry = groups.entry(key).or_insert_with(|| GroupAccumulator {
            total_request_count: 0,
            upstream_name,
            buckets: zero_filled_buckets(window_start_unix_secs, window_end_unix_secs, step),
        });
        entry.total_request_count += rollup.request_count;
        add_rollup_to_buckets(
            &mut entry.buckets,
            rollup,
            window_start_unix_secs,
            step,
            group_by == UsageGroupBy::Principal,
        );
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
        .map(|(key, mut group)| {
            for bucket in &mut group.buckets {
                sanitize_bucket_cost_components(bucket);
            }
            UsageSeries {
                key,
                upstream_name: group.upstream_name,
                buckets: group.buckets,
            }
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
    family_by_upstream: &HashMap<Uuid, ProviderFamily>,
) -> (Vec<UsageSeries>, Option<u64>) {
    if group_by == UsageGroupBy::None {
        let mut bucket = empty_bucket(window_start_unix_secs);
        for rollup in rollups {
            add_rollup_to_bucket(&mut bucket, rollup, false);
        }
        sanitize_bucket_cost_components(&mut bucket);
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
        let Some((key, upstream_name)) = group_key(group_by, rollup, family_by_upstream) else {
            continue;
        };
        let entry = groups.entry(key).or_insert_with(|| TotalGroupAccumulator {
            total_request_count: 0,
            upstream_name,
            bucket: empty_bucket(window_start_unix_secs),
        });
        entry.total_request_count += rollup.request_count;
        add_rollup_to_bucket(
            &mut entry.bucket,
            rollup,
            group_by == UsageGroupBy::Principal,
        );
    }

    let mut ranked = groups.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|(left_key, left), (right_key, right)| {
        right
            .total_request_count
            .cmp(&left.total_request_count)
            .then_with(|| left_key.cmp(right_key))
    });

    let series = ranked
        .into_iter()
        .map(|(key, mut group)| {
            sanitize_bucket_cost_components(&mut group.bucket);
            UsageSeries {
                key,
                upstream_name: group.upstream_name,
                buckets: vec![group.bucket],
            }
        })
        .collect();
    (series, None)
}

fn sum_optional(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.saturating_add(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn clear_bucket_cost_components(bucket: &mut UsageBucket) {
    bucket.cost_input_micros = None;
    bucket.cost_output_micros = None;
    bucket.cost_cache_creation_5m_micros = None;
    bucket.cost_cache_creation_1h_micros = None;
    bucket.cost_cache_read_micros = None;
}

fn sanitize_bucket_cost_components(bucket: &mut UsageBucket) {
    let components = [
        bucket.cost_input_micros,
        bucket.cost_output_micros,
        bucket.cost_cache_creation_5m_micros,
        bucket.cost_cache_creation_1h_micros,
        bucket.cost_cache_read_micros,
    ];
    if components.iter().all(Option::is_none) {
        return;
    }
    let mut component_total = 0_u64;
    for value in components {
        let Some(value) = value else {
            clear_bucket_cost_components(bucket);
            return;
        };
        let Some(sum) = component_total.checked_add(value) else {
            clear_bucket_cost_components(bucket);
            return;
        };
        component_total = sum;
    }
    if component_total > bucket.virtual_cost_micros {
        clear_bucket_cost_components(bucket);
    }
}

fn group_key(
    group_by: UsageGroupBy,
    rollup: &UsageRollup,
    family_by_upstream: &HashMap<Uuid, ProviderFamily>,
) -> Option<(String, Option<String>)> {
    match group_by {
        UsageGroupBy::None => Some(("all".to_owned(), None)),
        UsageGroupBy::Model => Some((rollup.model.clone(), None)),
        UsageGroupBy::Principal => Some((rollup.principal.clone(), None)),
        UsageGroupBy::Upstream => Some((
            rollup.upstream_id.to_string(),
            Some(rollup.upstream_name.clone()),
        )),
        UsageGroupBy::Provider => family_by_upstream.get(&rollup.upstream_id).map(|family| {
            let family = family.as_str().to_owned();
            (family.clone(), Some(family))
        }),
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

    fn test_rollup(
        principal: &str,
        bucket_start: u64,
        request_count: u64,
        virtual_cost_micros: u64,
    ) -> UsageRollup {
        UsageRollup {
            resolution: UsageRollupResolution::Minute,
            bucket_start,
            principal: principal.to_owned(),
            upstream_id: Uuid::from_u128(1),
            upstream_name: "upstream-a".to_owned(),
            model: "model-a".to_owned(),
            request_count,
            input_tokens: request_count,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            cache_write_input_tokens: None,
            cache_write_input_tokens_30m: None,
            cache_write_input_tokens_12h: None,
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
            virtual_cost_micros,
            cost_input_micros: 0,
            cost_output_micros: 0,
            cost_cache_creation_5m_micros: 0,
            cost_cache_creation_1h_micros: 0,
            cost_cache_read_micros: 0,
            cost_cache_write_30m_micros: 0,
            cost_cache_write_12h_micros: 0,
            cost_components_event_count: 0,
            priced_event_count: if virtual_cost_micros > 0 {
                request_count
            } else {
                0
            },
            unpriced_event_count: 0,
            unmetered_event_count: 0,
        }
    }

    #[test]
    fn totals_series_collapses_observed_rollup_buckets() {
        let rollups = [
            test_rollup("principal-a", MINUTE_SECS, 1, 1),
            test_rollup("principal-a", 59 * MINUTE_SECS, 2, 2),
        ];

        let (series, truncated_series_count) =
            build_usage_totals_series(UsageGroupBy::Principal, &rollups, 0, &HashMap::new());

        assert_eq!(truncated_series_count, None);
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].buckets.len(), 1);
        assert_eq!(series[0].buckets[0].bucket_start_unix_secs, 0);
        assert_eq!(series[0].buckets[0].request_count, 3);
    }

    #[test]
    fn rollup_component_aggregation_preserves_recorded_values() {
        let mut first = test_rollup("principal-a", 0, 1, 15);
        first.cost_input_micros = 1;
        first.cost_output_micros = 2;
        first.cost_cache_creation_5m_micros = 3;
        first.cost_cache_creation_1h_micros = 4;
        first.cost_cache_read_micros = 5;
        first.cost_components_event_count = 1;
        let mut second = first.clone();
        second.virtual_cost_micros = 150;
        second.cost_input_micros = 10;
        second.cost_output_micros = 20;
        second.cost_cache_creation_5m_micros = 30;
        second.cost_cache_creation_1h_micros = 40;
        second.cost_cache_read_micros = 50;
        second.cost_components_event_count = 2;

        let (series, _) = build_usage_series_with_provider(
            UsageGroupBy::Principal,
            &[first, second],
            0,
            MINUTE_SECS,
            UsageRollupResolution::Minute,
            &HashMap::new(),
        );
        let bucket = &series[0].buckets[0];
        assert_eq!(bucket.cost_input_micros, Some(11));
        assert_eq!(bucket.cost_output_micros, Some(22));
        assert_eq!(bucket.cost_cache_creation_5m_micros, Some(33));
        assert_eq!(bucket.cost_cache_creation_1h_micros, Some(44));
        assert_eq!(bucket.cost_cache_read_micros, Some(55));
    }

    #[test]
    fn unrecorded_components_are_absent_and_recorded_zeroes_are_present() {
        let unrecorded = test_rollup("unrecorded", 0, 1, 0);
        let mut recorded = test_rollup("recorded", MINUTE_SECS, 1, 0);
        recorded.cost_components_event_count = 1;

        let (series, _) = build_usage_series_with_provider(
            UsageGroupBy::Principal,
            &[unrecorded, recorded],
            0,
            2 * MINUTE_SECS,
            UsageRollupResolution::Minute,
            &HashMap::new(),
        );
        let unrecorded_bucket = &series
            .iter()
            .find(|series| series.key == "unrecorded")
            .unwrap()
            .buckets[0];
        assert_eq!(unrecorded_bucket.cost_input_micros, None);
        let recorded_bucket = &series
            .iter()
            .find(|series| series.key == "recorded")
            .unwrap()
            .buckets[1];
        assert_eq!(recorded_bucket.cost_input_micros, Some(0));
        assert_eq!(recorded_bucket.cost_output_micros, Some(0));
        assert_eq!(recorded_bucket.cost_cache_creation_5m_micros, Some(0));
        assert_eq!(recorded_bucket.cost_cache_creation_1h_micros, Some(0));
        assert_eq!(recorded_bucket.cost_cache_read_micros, Some(0));
    }

    #[test]
    fn component_sum_above_virtual_cost_is_sanitized() {
        let mut rollup = test_rollup("principal-a", 0, 1, 10);
        rollup.cost_input_micros = 11;
        rollup.cost_components_event_count = 1;
        let (series, _) = build_usage_series_with_provider(
            UsageGroupBy::Principal,
            &[rollup],
            0,
            MINUTE_SECS,
            UsageRollupResolution::Minute,
            &HashMap::new(),
        );
        let bucket = &series[0].buckets[0];
        assert_eq!(bucket.cost_input_micros, None);
        assert_eq!(bucket.cost_output_micros, None);
        assert_eq!(bucket.cost_cache_creation_5m_micros, None);
        assert_eq!(bucket.cost_cache_creation_1h_micros, None);
        assert_eq!(bucket.cost_cache_read_micros, None);
    }

    #[test]
    fn component_sum_overflow_is_sanitized() {
        let mut bucket = empty_bucket(0);
        bucket.virtual_cost_micros = u64::MAX;
        bucket.cost_input_micros = Some(u64::MAX);
        bucket.cost_output_micros = Some(1);
        bucket.cost_cache_creation_5m_micros = Some(0);
        bucket.cost_cache_creation_1h_micros = Some(0);
        bucket.cost_cache_read_micros = Some(0);
        sanitize_bucket_cost_components(&mut bucket);
        assert_eq!(bucket.cost_input_micros, None);
        assert_eq!(bucket.cost_output_micros, None);
        assert_eq!(bucket.cost_cache_creation_5m_micros, None);
        assert_eq!(bucket.cost_cache_creation_1h_micros, None);
        assert_eq!(bucket.cost_cache_read_micros, None);
    }

    #[test]
    fn totals_series_is_not_capped_at_twenty_principals() {
        let rollups = (0..25)
            .map(|index| test_rollup(&format!("principal-{index:02}"), 0, 1, 1))
            .collect::<Vec<_>>();
        let (series, truncated_series_count) =
            build_usage_totals_series(UsageGroupBy::Principal, &rollups, 0, &HashMap::new());
        assert_eq!(series.len(), 25);
        assert_eq!(truncated_series_count, None);
    }

    #[test]
    fn non_principal_groupings_omit_cost_components_even_when_recorded() {
        let mut rollup = test_rollup("principal-a", 0, 1, 15);
        rollup.cost_input_micros = 5;
        rollup.cost_output_micros = 10;
        rollup.cost_components_event_count = 1;
        for group_by in [
            UsageGroupBy::None,
            UsageGroupBy::Model,
            UsageGroupBy::Upstream,
        ] {
            let (series, _) =
                build_usage_totals_series(group_by, &[rollup.clone()], 0, &HashMap::new());
            let bucket = &series[0].buckets[0];
            assert_eq!(bucket.cost_input_micros, None);
            assert_eq!(bucket.cost_output_micros, None);
        }
    }

    #[test]
    fn mixed_recorded_and_unrecorded_rollups_preserve_partial_components() {
        let unrecorded = test_rollup("principal-a", 0, 1, 20);
        let mut recorded = test_rollup("principal-a", 0, 1, 15);
        recorded.cost_input_micros = 5;
        recorded.cost_output_micros = 10;
        recorded.cost_components_event_count = 1;
        let (series, _) = build_usage_totals_series(
            UsageGroupBy::Principal,
            &[unrecorded, recorded],
            0,
            &HashMap::new(),
        );
        let bucket = &series[0].buckets[0];
        assert_eq!(bucket.virtual_cost_micros, 35);
        assert_eq!(bucket.cost_input_micros, Some(5));
        assert_eq!(bucket.cost_output_micros, Some(10));
        assert_eq!(bucket.cost_cache_creation_5m_micros, Some(0));
        assert_eq!(bucket.cost_cache_creation_1h_micros, Some(0));
        assert_eq!(bucket.cost_cache_read_micros, Some(0));
    }

    #[test]
    fn pricing_completeness_truth_table_matches_bucket_and_summary_aggregation() {
        use PricingCompleteness::{KnownZero, Partial, Priced, Unknown, Unmetered, Unpriced};

        // requests, priced, unpriced, unmetered, tokens, cost, expected
        let cases = [
            (0, 0, 0, 0, 0, 0, None),
            (1, 1, 0, 0, 10, 5, Some(Priced)),
            (1, 1, 0, 0, 10, 0, Some(KnownZero)),
            (1, 0, 1, 0, 10, 0, Some(Unpriced)),
            (1, 0, 0, 1, 0, 0, Some(Unmetered)),
            (2, 1, 1, 0, 10, 5, Some(Partial)),
            (2, 1, 1, 0, 10, 0, Some(Partial)),
            (2, 1, 0, 1, 10, 5, Some(Priced)),
            (2, 1, 0, 1, 10, 0, Some(KnownZero)),
            (2, 0, 1, 1, 10, 0, Some(Unpriced)),
            (1, 0, 0, 0, 10, 0, Some(Unknown)),
            (1, 0, 0, 0, 0, 0, Some(Unmetered)),
            (1, 0, 0, 0, 10, 5, Some(Partial)),
            (2, 1, 0, 0, 10, 5, Some(Partial)),
            (2, 1, 0, 0, 10, 0, Some(Partial)),
            (2, 0, 1, 0, 10, 0, Some(Partial)),
            (2, 0, 0, 1, 10, 0, Some(Unknown)),
            (2, 0, 0, 1, 0, 0, Some(Unmetered)),
        ];
        for (requests, priced, unpriced, unmetered, tokens, cost, expected) in cases {
            let mut rollup = test_rollup("principal-a", 0, requests, cost);
            rollup.input_tokens = tokens;
            rollup.priced_event_count = priced;
            rollup.unpriced_event_count = unpriced;
            rollup.unmetered_event_count = unmetered;
            let mut bucket = empty_bucket(0);
            add_rollup_to_bucket(&mut bucket, &rollup, false);
            assert_eq!(bucket.pricing_completeness, expected, "{rollup:?}");
            assert_eq!(
                bucket.priced_request_count,
                (requests > 0).then_some(priced)
            );
            assert_eq!(
                bucket.unpriced_request_count,
                (requests > 0).then_some(unpriced)
            );
            assert_eq!(
                bucket.unmetered_request_count,
                (requests > 0).then_some(unmetered)
            );
            let totals = summary_totals(&[empty_bucket(60), bucket]);
            assert_eq!(totals.pricing_completeness, expected, "{rollup:?}");
            assert_eq!(
                totals.priced_request_count,
                (requests > 0).then_some(priced)
            );
            assert_eq!(
                totals.unpriced_request_count,
                (requests > 0).then_some(unpriced)
            );
            assert_eq!(
                totals.unmetered_request_count,
                (requests > 0).then_some(unmetered)
            );
        }
    }

    #[test]
    fn legacy_cache_write_only_usage_is_unknown_and_cache_totals_are_not_double_counted() {
        for (aggregate, short, long, expected_tokens) in [
            (Some(7), None, None, 7),
            (None, Some(3), Some(4), 7),
            (None, Some(7), None, 7),
            (None, None, Some(7), 7),
            (Some(7), Some(3), Some(4), 7),
        ] {
            let mut rollup = test_rollup("principal-a", 0, 1, 0);
            rollup.input_tokens = 0;
            rollup.cache_write_input_tokens = aggregate;
            rollup.cache_write_input_tokens_30m = short;
            rollup.cache_write_input_tokens_12h = long;
            let mut bucket = empty_bucket(0);
            add_rollup_to_bucket(&mut bucket, &rollup, false);
            assert_eq!(bucket_total_tokens(&bucket), expected_tokens);
            assert_eq!(
                bucket.pricing_completeness,
                Some(PricingCompleteness::Unknown)
            );
            assert_eq!(
                summary_totals(&[bucket]).pricing_completeness,
                Some(PricingCompleteness::Unknown)
            );
        }
    }

    #[test]
    fn empty_buckets_and_summary_omit_all_pricing_fields() {
        let bucket = empty_bucket(0);
        let totals = summary_totals(std::slice::from_ref(&bucket));
        for value in [
            serde_json::to_value(&bucket).unwrap(),
            serde_json::to_value(&totals).unwrap(),
        ] {
            for field in [
                "pricing_completeness",
                "priced_request_count",
                "unpriced_request_count",
                "unmetered_request_count",
            ] {
                assert!(value.get(field).is_none(), "{field}: {value}");
            }
        }
    }

    #[test]
    fn mixed_pricing_counts_collapse_identically_for_every_grouping() {
        let mut priced = test_rollup("principal-a", 0, 2, 10);
        priced.priced_event_count = 2;
        let mut unpriced = test_rollup("principal-a", MINUTE_SECS, 3, 0);
        unpriced.unpriced_event_count = 3;
        let mut unmetered = test_rollup("principal-a", 2 * MINUTE_SECS, 1, 0);
        unmetered.input_tokens = 0;
        unmetered.unmetered_event_count = 1;
        let rollups = [priced, unpriced, unmetered];
        let families = HashMap::from([(Uuid::from_u128(1), ProviderFamily::Anthropic)]);
        for group_by in [
            UsageGroupBy::None,
            UsageGroupBy::Model,
            UsageGroupBy::Principal,
            UsageGroupBy::Upstream,
            UsageGroupBy::Provider,
        ] {
            let (full, _) = build_usage_series_with_provider(
                group_by,
                &rollups,
                0,
                3 * MINUTE_SECS,
                UsageRollupResolution::Minute,
                &families,
            );
            let (totals, _) = build_usage_totals_series(group_by, &rollups, 0, &families);
            let collapsed = summary_totals(&full[0].buckets);
            let total = &totals[0].buckets[0];
            assert_eq!(
                total.pricing_completeness,
                Some(PricingCompleteness::Partial)
            );
            assert_eq!(total.pricing_completeness, collapsed.pricing_completeness);
            assert_eq!(total.priced_request_count, Some(2));
            assert_eq!(total.unpriced_request_count, Some(3));
            assert_eq!(total.unmetered_request_count, Some(1));
            assert_eq!(total.priced_request_count, collapsed.priced_request_count);
            assert_eq!(
                total.unpriced_request_count,
                collapsed.unpriced_request_count
            );
            assert_eq!(
                total.unmetered_request_count,
                collapsed.unmetered_request_count
            );
        }
    }
}
