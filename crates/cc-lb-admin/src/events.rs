use std::collections::HashMap;

use crate::dashboard::parse_provider_families;
use cc_lb_request_log::RequestEventKind;
use cc_lb_storage_api::{
    RequestEvent, RequestEventHistogramBucket, RequestEventHistogramQuery, RequestEventListItem,
    RequestEventListQuery, RequestEventStreamFilters, StatusClass, Storage, StorageError,
    model_filter_matches,
};
use serde::Serialize;
use tokio::sync::broadcast;
use uuid::Uuid;
pub const DEFAULT_RECENT_EVENTS_LIMIT: usize = 100;
pub const MAX_RECENT_EVENTS_LIMIT: usize = 500;
pub const MAX_HISTOGRAM_BUCKETS: u64 = 240;
pub fn parse_query_map(raw: Option<&str>) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut event_kinds = Vec::new();
    for (key, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
        if key == "event_kind" {
            event_kinds.push(value.into_owned());
        } else {
            map.insert(key.into_owned(), value.into_owned());
        }
    }
    if !event_kinds.is_empty() {
        map.insert("event_kind".to_owned(), event_kinds.join(","));
    }
    map
}

/// Upper bound on matching durable events replayed by one reconnect.
///
/// Filters apply before this limit, so storage may scan more raw rows. Reaching
/// the limit emits a reset at the last delivered cursor for safe continuation.
pub const BACKFILL_MAX_EVENTS: usize = 500;
pub const DEFAULT_STORAGE_TAIL_CAPACITY: usize = 4096;

const DEFAULT_DELTA_EVENTS_LIMIT: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentEventsParams {
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub until_ts_ms: Option<u64>,
    pub until_event_id: Option<String>,
    pub limit: usize,
    pub principal_id: Option<String>,
    pub thread_id: Option<String>,
    pub model: Option<String>,
    pub upstream_id: Option<Uuid>,
    pub provider_upstream_ids: Option<Vec<Uuid>>,
    pub status_class: Option<StatusClass>,
    pub errors_only: bool,
    pub source_kind: Option<String>,
    pub event_kinds: Option<Vec<RequestEventKind>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamFilters {
    pub principal_id: Option<String>,
    pub thread_id: Option<String>,
    pub model: Option<String>,
    pub upstream_id: Option<Uuid>,
    pub provider_upstream_ids: Option<Vec<Uuid>>,
    pub status_class: Option<StatusClass>,
    pub errors_only: bool,
    pub source_kind: Option<String>,
    pub event_kinds: Option<Vec<RequestEventKind>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventsDeltaQuery {
    pub since_cursor: u64,
    pub limit: usize,
    pub filters: StreamFilters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramParams {
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub bucket_ms: u64,
    pub bucket_count: u64,
    pub filters: StreamFilters,
}

pub use cc_lb_request_log::StorageTailUpdate;

#[derive(Debug, Clone, Serialize)]
pub struct RecentEventsPayload {
    pub events: Vec<RequestEventListItem>,
    pub observed: bool,
    pub count: usize,
    pub limit: usize,
    /// Live-stream watermark captured *before* the list query. Every event at
    /// or below it is visible to the list; every later event is replayed by
    /// `/events/stream` when this value is sent as `Last-Event-ID`, so a client
    /// that seeds its live tail with it cannot miss rows committed between the
    /// history request and the stream connect. Overlap is deduped by event id.
    pub cursor: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistogramPayload {
    pub buckets: Vec<RequestEventHistogramBucket>,
    pub bucket_ms: u64,
    pub bucket_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventsDeltaPayload {
    pub events: Vec<RequestEvent>,
    pub next_cursor: u64,
    pub exhausted: bool,
}

#[derive(Debug)]
pub enum EventsError {
    InvalidSinceUnixSecs,
    InvalidUntilUnixSecs,
    InvalidUntilTsMs,
    InvalidBucketMs,
    InvalidHistogramRange,
    TooManyBuckets,
    InvalidSinceCursor,
    InvalidLimit,
    LimitTooLarge,
    InvalidUpstreamId,
    InvalidProviders,
    InvalidStatusClass,
    InvalidEventKind,
    Storage(StorageError),
}

pub fn parse_recent_params(
    map: &HashMap<String, String>,
) -> Result<RecentEventsParams, EventsError> {
    let since_unix_secs = match map.get("since_unix_secs") {
        Some(value) => value
            .parse::<u64>()
            .map_err(|_| EventsError::InvalidSinceUnixSecs)?,
        None => 0,
    };
    let until_unix_secs = match map.get("until_unix_secs") {
        Some(value) => value
            .parse::<u64>()
            .map_err(|_| EventsError::InvalidUntilUnixSecs)?,
        None => u64::MAX,
    };
    let until_ts_ms = map
        .get("until_ts_ms")
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| EventsError::InvalidUntilTsMs)
        })
        .transpose()?;
    let until_event_id = map
        .get("until_event_id")
        .filter(|value| !value.is_empty())
        .cloned();
    let limit = match map.get("limit") {
        Some(value) => value
            .parse::<usize>()
            .map_err(|_| EventsError::InvalidLimit)?,
        None => DEFAULT_RECENT_EVENTS_LIMIT,
    };
    if limit == 0 {
        return Err(EventsError::InvalidLimit);
    }
    if limit > MAX_RECENT_EVENTS_LIMIT {
        return Err(EventsError::LimitTooLarge);
    }

    let filters = parse_stream_filters(map)?;
    Ok(RecentEventsParams {
        since_unix_secs,
        until_unix_secs,
        until_ts_ms,
        until_event_id,
        limit,
        principal_id: filters.principal_id,
        thread_id: filters.thread_id,
        model: filters.model,
        upstream_id: filters.upstream_id,
        provider_upstream_ids: filters.provider_upstream_ids,
        status_class: filters.status_class,
        errors_only: filters.errors_only,
        source_kind: filters.source_kind,
        event_kinds: filters.event_kinds,
    })
}

pub fn parse_histogram_params(
    map: &HashMap<String, String>,
) -> Result<HistogramParams, EventsError> {
    let since_unix_secs = map
        .get("since_unix_secs")
        .ok_or(EventsError::InvalidSinceUnixSecs)?
        .parse::<u64>()
        .map_err(|_| EventsError::InvalidSinceUnixSecs)?;
    let until_unix_secs = map
        .get("until_unix_secs")
        .ok_or(EventsError::InvalidUntilUnixSecs)?
        .parse::<u64>()
        .map_err(|_| EventsError::InvalidUntilUnixSecs)?;
    let bucket_ms = map
        .get("bucket_ms")
        .ok_or(EventsError::InvalidBucketMs)?
        .parse::<u64>()
        .map_err(|_| EventsError::InvalidBucketMs)?;

    if bucket_ms == 0 || bucket_ms % 1_000 != 0 || bucket_ms > i64::MAX as u64 {
        return Err(EventsError::InvalidBucketMs);
    }
    if since_unix_secs > until_unix_secs {
        return Err(EventsError::InvalidHistogramRange);
    }

    let range_start_ms = since_unix_secs
        .checked_mul(1_000)
        .ok_or(EventsError::InvalidHistogramRange)?;
    let range_end_ms = until_unix_secs
        .checked_mul(1_000)
        .ok_or(EventsError::InvalidHistogramRange)?;
    if range_start_ms > i64::MAX as u64 || range_end_ms > i64::MAX as u64 {
        return Err(EventsError::InvalidHistogramRange);
    }

    let span_ms = until_unix_secs
        .checked_sub(since_unix_secs)
        .and_then(|span| span.checked_add(1))
        .and_then(|span| span.checked_mul(1_000))
        .ok_or(EventsError::InvalidHistogramRange)?;
    let bucket_count = span_ms.div_ceil(bucket_ms);
    if bucket_count > MAX_HISTOGRAM_BUCKETS {
        return Err(EventsError::TooManyBuckets);
    }

    Ok(HistogramParams {
        since_unix_secs,
        until_unix_secs,
        bucket_ms,
        bucket_count,
        filters: parse_stream_filters(map)?,
    })
}

pub fn parse_delta_query(map: &HashMap<String, String>) -> Result<EventsDeltaQuery, EventsError> {
    let since_cursor = map
        .get("since_cursor")
        .ok_or(EventsError::InvalidSinceCursor)?
        .parse::<u64>()
        .map_err(|_| EventsError::InvalidSinceCursor)?;
    let limit = match map.get("limit") {
        Some(value) => value
            .parse::<u16>()
            .map_err(|_| EventsError::InvalidLimit)?
            .into(),
        None => DEFAULT_DELTA_EVENTS_LIMIT,
    };
    if limit == 0 {
        return Err(EventsError::InvalidLimit);
    }

    Ok(EventsDeltaQuery {
        since_cursor,
        limit: limit.min(DEFAULT_DELTA_EVENTS_LIMIT),
        filters: parse_stream_filters(map)?,
    })
}

pub fn parse_last_event_id(header: Option<&str>) -> Option<u64> {
    header.and_then(|value| value.trim().parse::<u64>().ok())
}

pub fn sse_id_from_cursor(cursor: u64) -> String {
    cursor.to_string()
}

pub fn storage_tail_channel() -> broadcast::Sender<StorageTailUpdate> {
    let (tx, _) = broadcast::channel(DEFAULT_STORAGE_TAIL_CAPACITY);
    tx
}

pub async fn build_delta_events_payload(
    storage: &dyn Storage,
    query: &EventsDeltaQuery,
) -> Result<EventsDeltaPayload, EventsError> {
    let cursor_hi = storage.current_request_event_cursor().await?;
    let rows = storage
        .query_request_events_between_cursors(
            query.since_cursor,
            cursor_hi,
            query.limit,
            &query.filters.storage_filters(),
        )
        .await?;
    let exhausted = rows.len() < query.limit;
    let mut next_cursor = query.since_cursor;
    let mut events = Vec::with_capacity(rows.len());
    for (cursor, event) in rows {
        if apply_filters_to_event(&event, &query.filters) {
            next_cursor = cursor;
            events.push(event);
        }
    }
    if exhausted {
        next_cursor = query.since_cursor.max(cursor_hi);
    }
    Ok(EventsDeltaPayload {
        events,
        next_cursor,
        exhausted,
    })
}

pub fn parse_stream_filters(map: &HashMap<String, String>) -> Result<StreamFilters, EventsError> {
    if let Some(value) = map.get("providers") {
        parse_provider_families(Some(value.as_str())).map_err(|_| EventsError::InvalidProviders)?;
    }
    Ok(StreamFilters {
        principal_id: map.get("principal_id").cloned(),
        thread_id: map.get("thread_id").cloned(),
        model: map
            .get("model")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        upstream_id: match map.get("upstream_id") {
            Some(value) => {
                Some(Uuid::parse_str(value).map_err(|_| EventsError::InvalidUpstreamId)?)
            }
            None => None,
        },
        provider_upstream_ids: None,
        status_class: match map.get("status_class") {
            Some(value) if value == "errors" => None,
            Some(value) => Some(parse_status_class(value)?),
            None => None,
        },
        errors_only: map
            .get("status_class")
            .is_some_and(|value| value == "errors"),
        source_kind: map.get("source_kind").cloned(),
        event_kinds: match map.get("event_kind") {
            Some(value) => {
                let kinds = value
                    .split(',')
                    .map(str::trim)
                    .map(|value| value.parse::<RequestEventKind>())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| EventsError::InvalidEventKind)?;
                if kinds.is_empty() {
                    return Err(EventsError::InvalidEventKind);
                }
                Some(kinds)
            }
            None => None,
        },
    })
}

pub fn apply_filters_to_event(event: &RequestEvent, filters: &StreamFilters) -> bool {
    if let Some(principal_id) = filters.principal_id.as_deref()
        && event.principal_id.as_deref() != Some(principal_id)
    {
        return false;
    }
    if let Some(thread_id) = filters.thread_id.as_deref()
        && event.thread_id.as_deref() != Some(thread_id)
    {
        return false;
    }
    if let Some(model) = filters.model.as_deref()
        && !model_filter_matches(model, event.model.as_deref())
    {
        return false;
    }
    if let Some(upstream_id) = filters.upstream_id
        && event.upstream_id != Some(upstream_id)
    {
        return false;
    }
    if let Some(provider_upstream_ids) = filters.provider_upstream_ids.as_ref()
        && !event
            .upstream_id
            .is_some_and(|id| provider_upstream_ids.contains(&id))
    {
        return false;
    }
    if let Some(status_class) = filters.status_class
        && !status_class.matches(event.status)
    {
        return false;
    }
    if filters.errors_only && event.status < 400 && event.error_code.is_none() {
        return false;
    }
    if let Some(event_kinds) = filters.event_kinds.as_ref()
        && !event_kinds.contains(&RequestEventKind::effective(
            event.source_kind.as_deref(),
            event.event_kind,
        ))
    {
        return false;
    }
    if let Some(source_kind) = filters.source_kind.as_deref() {
        if source_kind != "all" && event.source_kind.as_deref() != Some(source_kind) {
            return false;
        }
    } else if filters.event_kinds.is_none() && event.source_kind.as_deref() == Some("renewal") {
        return false;
    }
    true
}

pub async fn build_recent_events_payload(
    storage: &dyn Storage,
    params: &RecentEventsParams,
) -> Result<RecentEventsPayload, EventsError> {
    let cursor = storage.current_request_event_cursor().await?;
    let events = storage.list_request_events(&params.list_query()).await?;
    let count = events.len();
    let observed = !events.is_empty();
    Ok(RecentEventsPayload {
        events,
        observed,
        count,
        limit: params.limit,
        cursor,
    })
}

pub async fn build_histogram_payload(
    storage: &dyn Storage,
    params: &HistogramParams,
) -> Result<HistogramPayload, EventsError> {
    let buckets = storage
        .request_event_histogram(&params.histogram_query())
        .await?;
    Ok(HistogramPayload {
        buckets,
        bucket_ms: params.bucket_ms,
        bucket_count: params.bucket_count,
    })
}

pub async fn fetch_request_event_detail(
    storage: &dyn Storage,
    event_id: &str,
) -> Result<Option<RequestEvent>, EventsError> {
    Ok(storage.get_request_event(event_id).await?)
}

impl RecentEventsParams {
    pub fn stream_filters(&self) -> StreamFilters {
        StreamFilters {
            principal_id: self.principal_id.clone(),
            thread_id: self.thread_id.clone(),
            model: self.model.clone(),
            upstream_id: self.upstream_id,
            provider_upstream_ids: self.provider_upstream_ids.clone(),
            status_class: self.status_class,
            errors_only: self.errors_only,
            source_kind: self.source_kind.clone(),
            event_kinds: self.event_kinds.clone(),
        }
    }

    pub fn list_query(&self) -> RequestEventListQuery {
        RequestEventListQuery {
            since_unix_secs: self.since_unix_secs,
            until_unix_secs: self.until_unix_secs,
            until_ts_ms: self.until_ts_ms,
            until_event_id: self.until_event_id.clone(),
            limit: self.limit,
            filters: self.stream_filters().storage_filters(),
            source_kind: self.source_kind.clone(),
        }
    }
}

impl HistogramParams {
    fn histogram_query(&self) -> RequestEventHistogramQuery {
        RequestEventHistogramQuery {
            since_unix_secs: self.since_unix_secs,
            until_unix_secs: self.until_unix_secs,
            bucket_ms: self.bucket_ms,
            bucket_count: self.bucket_count,
            filters: self.filters.storage_filters(),
            source_kind: self.filters.source_kind.clone(),
        }
    }
}

impl StreamFilters {
    pub fn storage_filters(&self) -> RequestEventStreamFilters {
        RequestEventStreamFilters {
            principal_id: self.principal_id.clone(),
            thread_id: self.thread_id.clone(),
            model: self.model.clone(),
            upstream_id: self.upstream_id,
            provider_upstream_ids: self.provider_upstream_ids.clone(),
            status_class: self.status_class,
            errors_only: self.errors_only,
            event_kinds: self.event_kinds.clone(),
            source_kind: self
                .source_kind
                .as_ref()
                .filter(|source_kind| source_kind.as_str() != "all")
                .cloned(),
            exclude_renewal: self.source_kind.is_none() && self.event_kinds.is_none(),
        }
    }
}

impl EventsError {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidSinceUnixSecs => "invalid_since_unix_secs",
            Self::InvalidUntilUnixSecs => "invalid_until_unix_secs",
            Self::InvalidUntilTsMs => "invalid_until_ts_ms",
            Self::InvalidBucketMs => "invalid_bucket_ms",
            Self::InvalidHistogramRange => "invalid_histogram_range",
            Self::TooManyBuckets => "too_many_buckets",
            Self::InvalidSinceCursor => "invalid_since_cursor",
            Self::InvalidLimit => "invalid_limit",
            Self::LimitTooLarge => "limit_too_large",
            Self::InvalidUpstreamId => "invalid_upstream_id",
            Self::InvalidProviders => "invalid_providers",
            Self::InvalidStatusClass => "invalid_status_class",
            Self::InvalidEventKind => "invalid_event_kind",
            Self::Storage(_) => "storage_error",
        }
    }
}

impl From<StorageError> for EventsError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

fn parse_status_class(value: &str) -> Result<StatusClass, EventsError> {
    match value {
        "2xx" => Ok(StatusClass::TwoXx),
        "3xx" => Ok(StatusClass::ThreeXx),
        "4xx" => Ok(StatusClass::FourXx),
        "5xx" => Ok(StatusClass::FiveXx),
        _ => Err(EventsError::InvalidStatusClass),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_provider_kinds_parse_and_filter_request_events() {
        for kind in [
            RequestEventKind::OpenaiResponses,
            RequestEventKind::GeminiGenerateContent,
            RequestEventKind::GeminiCountTokens,
        ] {
            let params = HashMap::from([("event_kind".to_owned(), kind.as_str().to_owned())]);
            let filters = parse_stream_filters(&params).expect("multi-provider event kind");
            assert_eq!(filters.event_kinds, Some(vec![kind]));
            let mut event = RequestEvent {
                event_kind: Some(kind),
                ..Default::default()
            };
            assert!(apply_filters_to_event(&event, &filters));
            event.event_kind = Some(RequestEventKind::Other);
            assert!(!apply_filters_to_event(&event, &filters));
            event.event_kind = Some(kind);
            event.source_kind = Some("renewal".to_owned());
            assert!(!apply_filters_to_event(&event, &filters));
        }
    }

    #[test]
    fn event_kind_query_forms_combine_into_one_set() {
        for query in [
            "event_kind=messages,openai_responses",
            "event_kind=messages&event_kind=openai_responses",
            "event_kind=messages%2Copenai_responses&event_kind=messages",
        ] {
            let params = parse_query_map(Some(query));
            let recent = parse_recent_params(&params).expect("recent event kinds");
            let filters = recent.stream_filters();
            for kind in [
                RequestEventKind::Messages,
                RequestEventKind::OpenaiResponses,
            ] {
                assert!(
                    filters.event_kinds.as_ref().unwrap().contains(&kind),
                    "{query}"
                );
            }
            assert_eq!(filters.storage_filters().event_kinds, filters.event_kinds);
            assert!(!filters.storage_filters().exclude_renewal);
            assert!(!apply_filters_to_event(
                &RequestEvent {
                    event_kind: Some(RequestEventKind::Other),
                    ..Default::default()
                },
                &filters
            ));
        }
    }

    #[test]
    fn event_kind_query_rejects_each_invalid_or_empty_entry() {
        for query in [
            "event_kind=",
            "event_kind",
            "event_kind=messages,",
            "event_kind=,messages",
            "event_kind=messages,,openai_responses",
            "event_kind=messages&event_kind=",
            "event_kind=&event_kind=messages",
            "event_kind=messages&event_kind=bogus",
            "event_kind=bogus&event_kind=messages",
            "event_kind=messages,bogus",
            "event_kind=MESSAGES",
            "event_kind=all",
            "event_kind=%20",
        ] {
            let params = parse_query_map(Some(query));
            assert!(
                matches!(
                    parse_stream_filters(&params),
                    Err(EventsError::InvalidEventKind)
                ),
                "{query}"
            );
        }
    }

    #[test]
    fn event_kind_set_matching_preserves_renewal_and_source_precedence() {
        let cases = [
            (None, None, vec![true, true, true, false, true]),
            (
                Some(vec![
                    RequestEventKind::Messages,
                    RequestEventKind::OpenaiResponses,
                ]),
                None,
                vec![true, true, false, false, false],
            ),
            (
                Some(vec![RequestEventKind::Messages, RequestEventKind::Renewal]),
                None,
                vec![true, false, false, true, false],
            ),
            (
                Some(vec![RequestEventKind::Renewal, RequestEventKind::Messages]),
                Some("request"),
                vec![true, false, false, false, false],
            ),
            (
                Some(vec![RequestEventKind::Renewal, RequestEventKind::Messages]),
                Some("renewal"),
                vec![false, false, false, true, false],
            ),
            (
                Some(vec![RequestEventKind::Renewal, RequestEventKind::Messages]),
                Some("all"),
                vec![true, false, false, true, false],
            ),
            (Some(vec![]), None, vec![false; 5]),
        ];
        for (event_kinds, source_kind, expected) in cases {
            let filters = StreamFilters {
                event_kinds,
                source_kind: source_kind.map(str::to_owned),
                ..Default::default()
            };
            for ((event_kind, source), expected) in [
                (Some(RequestEventKind::Messages), Some("request")),
                (Some(RequestEventKind::OpenaiResponses), Some("request")),
                (Some(RequestEventKind::Models), Some("request")),
                (Some(RequestEventKind::Messages), Some("renewal")),
                (None, None),
            ]
            .into_iter()
            .zip(expected)
            {
                let event = RequestEvent {
                    event_kind,
                    source_kind: source.map(str::to_owned),
                    ..Default::default()
                };
                assert_eq!(
                    apply_filters_to_event(&event, &filters),
                    expected,
                    "filters={filters:?}, event={event:?}"
                );
            }
        }
    }
}
