use std::collections::HashMap;

use cc_lb_storage_api::{RequestEvent, RequestEventUpstream, Storage, StorageError};
use serde::Serialize;
use uuid::Uuid;

pub const DEFAULT_RECENT_EVENTS_LIMIT: usize = 100;
pub const MAX_RECENT_EVENTS_LIMIT: usize = 500;
pub const RECENT_EVENTS_PULL_INFLATION_FACTOR: usize = 10;
pub const MAX_RECENT_EVENTS_PULL_LIMIT: usize = 5_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentEventsParams {
    pub since_unix_secs: u64,
    pub until_unix_secs: u64,
    pub limit: usize,
    pub principal_id: Option<String>,
    pub model: Option<String>,
    pub upstream_id: Option<Uuid>,
    pub upstream: Option<RequestEventUpstream>,
    pub status_class: Option<StatusClass>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamFilters {
    pub principal_id: Option<String>,
    pub model: Option<String>,
    pub upstream: Option<RequestEventUpstream>,
    pub status_class: Option<StatusClass>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    TwoXx,
    ThreeXx,
    FourXx,
    FiveXx,
}

#[derive(Debug, Clone, Serialize)]
pub struct RecentEventsPayload {
    pub events: Vec<RequestEvent>,
    pub observed: bool,
    pub count: usize,
    pub limit: usize,
}

#[derive(Debug)]
pub enum EventsError {
    InvalidSinceUnixSecs,
    InvalidUntilUnixSecs,
    InvalidLimit,
    LimitTooLarge,
    InvalidUpstreamId,
    InvalidUpstream,
    InvalidStatusClass,
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
        limit,
        principal_id: filters.principal_id,
        model: filters.model,
        upstream_id: match map.get("upstream_id") {
            Some(value) => {
                Some(Uuid::parse_str(value).map_err(|_| EventsError::InvalidUpstreamId)?)
            }
            None => None,
        },
        upstream: filters.upstream,
        status_class: filters.status_class,
    })
}

pub fn parse_stream_filters(map: &HashMap<String, String>) -> Result<StreamFilters, EventsError> {
    Ok(StreamFilters {
        principal_id: map.get("principal_id").cloned(),
        model: map.get("model").cloned(),
        upstream: match map.get("upstream") {
            Some(value) => Some(parse_upstream(value)?),
            None => None,
        },
        status_class: match map.get("status_class") {
            Some(value) => Some(parse_status_class(value)?),
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
    if let Some(model) = filters.model.as_deref()
        && event.model.as_deref() != Some(model)
    {
        return false;
    }
    if let Some(upstream) = filters.upstream
        && event.upstream != Some(upstream)
    {
        return false;
    }
    if let Some(status_class) = filters.status_class
        && !status_class.matches(event.status)
    {
        return false;
    }
    true
}

pub async fn build_recent_events_payload(
    storage: &dyn Storage,
    params: &RecentEventsParams,
) -> Result<RecentEventsPayload, EventsError> {
    let pull_limit =
        (params.limit * RECENT_EVENTS_PULL_INFLATION_FACTOR).min(MAX_RECENT_EVENTS_PULL_LIMIT);
    let filters = params.stream_filters();
    let mut events = storage
        .query_request_events(params.since_unix_secs, params.until_unix_secs, pull_limit)
        .await?;
    events.sort_by(|left, right| {
        right
            .ts
            .cmp(&left.ts)
            .then_with(|| left.request_id.cmp(&right.request_id))
    });
    events.retain(|event| apply_filters_to_event(event, &filters));
    if let Some(upstream_id) = params.upstream_id {
        events.retain(|event| event.upstream_id == Some(upstream_id));
    }
    events.truncate(params.limit);
    let count = events.len();

    let observed = !events.is_empty();
    Ok(RecentEventsPayload {
        events,
        observed,
        count,
        limit: params.limit,
    })
}

impl RecentEventsParams {
    pub fn stream_filters(&self) -> StreamFilters {
        StreamFilters {
            principal_id: self.principal_id.clone(),
            model: self.model.clone(),
            upstream: self.upstream,
            status_class: self.status_class,
        }
    }
}

impl StatusClass {
    fn matches(self, status: u16) -> bool {
        match self {
            Self::TwoXx => (200..=299).contains(&status),
            Self::ThreeXx => (300..=399).contains(&status),
            Self::FourXx => (400..=499).contains(&status),
            Self::FiveXx => (500..=599).contains(&status),
        }
    }
}

impl EventsError {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidSinceUnixSecs => "invalid_since_unix_secs",
            Self::InvalidUntilUnixSecs => "invalid_until_unix_secs",
            Self::InvalidLimit => "invalid_limit",
            Self::LimitTooLarge => "limit_too_large",
            Self::InvalidUpstreamId => "invalid_upstream_id",
            Self::InvalidUpstream => "invalid_upstream",
            Self::InvalidStatusClass => "invalid_status_class",
            Self::Storage(_) => "storage_error",
        }
    }
}

impl From<StorageError> for EventsError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

fn parse_upstream(value: &str) -> Result<RequestEventUpstream, EventsError> {
    match value {
        "anthropic_direct" => Ok(RequestEventUpstream::AnthropicDirect),
        "custom_anthropic_spec" => Ok(RequestEventUpstream::CustomAnthropicSpec),
        _ => Err(EventsError::InvalidUpstream),
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
