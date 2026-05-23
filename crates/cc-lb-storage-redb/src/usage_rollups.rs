use std::collections::BTreeMap;

use cc_lb_pricing::virtual_cost_micros;
use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::{
    REQUEST_EVENTS_V1, RequestEvent, RequestEventUpstream, Storage, StorageError,
    USAGE_ROLLUP_CHECKPOINTS_V1, USAGE_ROLLUPS_V1,
};

const REQUEST_EVENT_CHECKPOINT_KEY: &str = "request_events_v1_high_water";
const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const UNKNOWN_DIMENSION: &str = "unknown";
const MAX_DIMENSION_CHARS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageRollupResolution {
    Minute,
    Hour,
}

impl UsageRollupResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Minute => "minute",
            Self::Hour => "hour",
        }
    }

    fn bucket_start(self, ts: u64) -> u64 {
        let width = match self {
            Self::Minute => MINUTE_SECS,
            Self::Hour => HOUR_SECS,
        };
        ts - (ts % width)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UsageRollupKey {
    pub resolution: UsageRollupResolution,
    pub bucket_start: u64,
    pub principal: String,
    pub upstream: String,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageRollup {
    pub resolution: UsageRollupResolution,
    pub bucket_start: u64,
    pub principal: String,
    pub upstream: String,
    pub model: String,
    pub request_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub error_count: u64,
    pub latency_count: u64,
    pub latency_ms_sum: u64,
    pub latency_ms_min: Option<u64>,
    pub latency_ms_max: Option<u64>,
    pub virtual_cost_micros: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageRollupRun {
    pub processed_events: u64,
    pub updated_rollups: u64,
    pub checkpoint: Option<u64>,
}

#[derive(Debug, Default)]
struct UsageRollupDelta {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    error_count: u64,
    latency_count: u64,
    latency_ms_sum: u64,
    latency_ms_min: Option<u64>,
    latency_ms_max: Option<u64>,
    virtual_cost_micros: u64,
}

impl Storage {
    pub fn rollup_usage_once(&self) -> Result<UsageRollupRun, StorageError> {
        let write_txn = self.db.begin_write()?;
        let previous_checkpoint = {
            let checkpoints = write_txn.open_table(USAGE_ROLLUP_CHECKPOINTS_V1)?;
            checkpoints
                .get(REQUEST_EVENT_CHECKPOINT_KEY)?
                .map(|stored| stored.value())
        };
        let mut checkpoint = previous_checkpoint;
        let mut processed_events = 0;
        let mut deltas = BTreeMap::new();

        {
            let events = write_txn.open_table(REQUEST_EVENTS_V1)?;
            for row in events.iter()? {
                let (key, value) = row?;
                let event_key = decode_request_event_key(key.value())?;
                if previous_checkpoint.is_some_and(|previous| event_key <= previous) {
                    continue;
                }
                let event: RequestEvent = serde_json::from_slice(value.value())?;
                add_event_deltas(&mut deltas, &event);
                processed_events += 1;
                checkpoint = Some(checkpoint.map_or(event_key, |current| current.max(event_key)));
            }
        }

        let mut updated_rollups = 0;
        if !deltas.is_empty() {
            let mut rollups = write_txn.open_table(USAGE_ROLLUPS_V1)?;
            for (key, delta) in deltas {
                let encoded_key = usage_rollup_key(&key);
                let mut rollup = match rollups.get(encoded_key.as_slice())? {
                    Some(stored) => serde_json::from_slice(stored.value())?,
                    None => UsageRollup::empty(&key),
                };
                rollup.apply_delta(delta);
                let payload = serde_json::to_vec(&rollup)?;
                rollups.insert(encoded_key.as_slice(), payload.as_slice())?;
                updated_rollups += 1;
            }
        }

        if checkpoint != previous_checkpoint {
            let mut checkpoints = write_txn.open_table(USAGE_ROLLUP_CHECKPOINTS_V1)?;
            if let Some(checkpoint) = checkpoint {
                checkpoints.insert(REQUEST_EVENT_CHECKPOINT_KEY, &checkpoint)?;
            }
        }

        write_txn.commit()?;
        Ok(UsageRollupRun {
            processed_events,
            updated_rollups,
            checkpoint,
        })
    }

    pub fn query_usage_rollups(&self) -> Result<Vec<UsageRollup>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(USAGE_ROLLUPS_V1)?;
        let mut rollups = Vec::new();
        for row in table.iter()? {
            let (_, value) = row?;
            rollups.push(serde_json::from_slice(value.value())?);
        }
        Ok(rollups)
    }

    pub fn query_usage_rollups_in_range(
        &self,
        resolution: UsageRollupResolution,
        window_start_unix_secs: u64,
        window_end_unix_secs: u64,
    ) -> Result<Vec<UsageRollup>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(USAGE_ROLLUPS_V1)?;
        let mut rollups = Vec::new();
        for row in table.iter()? {
            let (_, value) = row?;
            let rollup: UsageRollup = serde_json::from_slice(value.value())?;
            if rollup.resolution == resolution
                && rollup.bucket_start >= window_start_unix_secs
                && rollup.bucket_start < window_end_unix_secs
            {
                rollups.push(rollup);
            }
        }
        Ok(rollups)
    }

    pub fn usage_rollup_checkpoint(&self) -> Result<Option<u64>, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(USAGE_ROLLUP_CHECKPOINTS_V1)?;
        let checkpoint = table
            .get(REQUEST_EVENT_CHECKPOINT_KEY)?
            .map(|stored| stored.value());
        Ok(checkpoint)
    }
}

pub fn usage_rollup_key(key: &UsageRollupKey) -> Vec<u8> {
    let mut encoded = Vec::new();
    push_segment(&mut encoded, key.resolution.as_str());
    encoded.extend_from_slice(&key.bucket_start.to_be_bytes());
    push_segment(&mut encoded, &key.principal);
    push_segment(&mut encoded, &key.upstream);
    push_segment(&mut encoded, &key.model);
    encoded
}

impl UsageRollup {
    fn empty(key: &UsageRollupKey) -> Self {
        Self {
            resolution: key.resolution,
            bucket_start: key.bucket_start,
            principal: key.principal.clone(),
            upstream: key.upstream.clone(),
            model: key.model.clone(),
            request_count: 0,
            input_tokens: 0,
            output_tokens: 0,
            error_count: 0,
            latency_count: 0,
            latency_ms_sum: 0,
            latency_ms_min: None,
            latency_ms_max: None,
            virtual_cost_micros: 0,
        }
    }

    fn apply_delta(&mut self, delta: UsageRollupDelta) {
        self.request_count += delta.request_count;
        self.input_tokens += delta.input_tokens;
        self.output_tokens += delta.output_tokens;
        self.error_count += delta.error_count;
        self.latency_count += delta.latency_count;
        self.latency_ms_sum += delta.latency_ms_sum;
        self.latency_ms_min = min_option(self.latency_ms_min, delta.latency_ms_min);
        self.latency_ms_max = max_option(self.latency_ms_max, delta.latency_ms_max);
        self.virtual_cost_micros += delta.virtual_cost_micros;
    }
}

impl UsageRollupKey {
    fn from_event(resolution: UsageRollupResolution, event: &RequestEvent) -> Self {
        Self {
            resolution,
            bucket_start: resolution.bucket_start(event.ts),
            principal: normalize_dimension(event.principal_id.as_deref()),
            upstream: event
                .upstream
                .map(upstream_dimension)
                .unwrap_or_else(|| UNKNOWN_DIMENSION.to_owned()),
            model: normalize_dimension(event.model.as_deref()),
        }
    }
}

impl UsageRollupDelta {
    fn add_event(&mut self, event: &RequestEvent) {
        self.request_count += 1;
        self.input_tokens += event.input_tokens.unwrap_or(0);
        self.output_tokens += event.output_tokens.unwrap_or(0);
        if event.status >= 400 {
            self.error_count += 1;
        }
        self.latency_count += 1;
        self.latency_ms_sum += event.duration_ms;
        self.latency_ms_min = min_option(self.latency_ms_min, Some(event.duration_ms));
        self.latency_ms_max = max_option(self.latency_ms_max, Some(event.duration_ms));
        if let Some(model) = event.model.as_deref() {
            let estimate = virtual_cost_micros(
                model,
                event.input_tokens.unwrap_or(0),
                event.output_tokens.unwrap_or(0),
            );
            self.virtual_cost_micros += estimate.micros_usd.unwrap_or(0);
        }
    }
}

fn add_event_deltas(deltas: &mut BTreeMap<UsageRollupKey, UsageRollupDelta>, event: &RequestEvent) {
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        deltas
            .entry(UsageRollupKey::from_event(resolution, event))
            .or_default()
            .add_event(event);
    }
}

fn normalize_dimension(value: Option<&str>) -> String {
    let Some(value) = value else {
        return UNKNOWN_DIMENSION.to_owned();
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return UNKNOWN_DIMENSION.to_owned();
    }

    let mut normalized = String::new();
    for ch in trimmed.chars().take(MAX_DIMENSION_CHARS) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':' | '@') {
            normalized.push(ch);
        } else {
            normalized.push('_');
        }
    }
    if normalized.is_empty() {
        UNKNOWN_DIMENSION.to_owned()
    } else {
        normalized
    }
}

fn upstream_dimension(upstream: RequestEventUpstream) -> String {
    match upstream {
        RequestEventUpstream::AnthropicDirect => "anthropic_direct",
        RequestEventUpstream::BedrockRuntime => "bedrock_runtime",
        RequestEventUpstream::BedrockMantle => "bedrock_mantle",
        RequestEventUpstream::Vertex => "vertex",
        RequestEventUpstream::CustomAnthropicSpec => "custom_anthropic_spec",
    }
    .to_owned()
}

fn push_segment(key: &mut Vec<u8>, value: &str) {
    key.extend_from_slice(&(value.len() as u64).to_be_bytes());
    key.extend_from_slice(value.as_bytes());
}

fn decode_request_event_key(key: &[u8]) -> Result<u64, StorageError> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| StorageError::InvalidRequestEventKey)?;
    Ok(u64::from_be_bytes(bytes))
}

fn min_option(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.min(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}

fn max_option(current: Option<u64>, next: Option<u64>) -> Option<u64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.max(next)),
        (Some(current), None) => Some(current),
        (None, Some(next)) => Some(next),
        (None, None) => None,
    }
}
