use std::collections::{BTreeMap, HashMap};

use bincode::{config, serde as bincode_serde};
use cc_lb_storage_api::{
    UpstreamRecord,
    types::{
        RequestEvent, RequestEventUpstream, UsageRollup, UsageRollupKey, UsageRollupResolution,
        UsageRollupRun,
    },
};
use redb::{ReadableDatabase, ReadableTable};
use uuid::Uuid;

use crate::{
    REQUEST_EVENTS_V1, Storage, StorageError, UPSTREAMS_V2, USAGE_ROLLUP_CHECKPOINTS_V1,
    USAGE_ROLLUPS_V2,
};

const REQUEST_EVENT_CHECKPOINT_KEY: &str = "request_events_v1_high_water";
const MINUTE_SECS: u64 = 60;
const HOUR_SECS: u64 = 60 * 60;
const UNKNOWN_DIMENSION: &str = "unknown";
const MAX_DIMENSION_CHARS: usize = 64;

#[derive(Debug, Default)]
struct UsageRollupDelta {
    request_count: u64,
    input_tokens: u64,
    output_tokens: u64,
    cache_creation_input_tokens: u64,
    cache_read_input_tokens: u64,
    error_count: u64,
    latency_count: u64,
    latency_ms_sum: u64,
    latency_ms_min: Option<u64>,
    latency_ms_max: Option<u64>,
    proxy_setup_ms_count: u64,
    proxy_setup_ms_sum: u64,
    shape_ms_count: u64,
    shape_ms_sum: u64,
    sign_ms_count: u64,
    sign_ms_sum: u64,
    upstream_ttfb_ms_count: u64,
    upstream_ttfb_ms_sum: u64,
    upstream_body_ms_count: u64,
    upstream_body_ms_sum: u64,
    virtual_cost_micros: u64,
}

#[derive(Debug, Clone)]
struct UpstreamIdentity {
    id: Uuid,
    name: String,
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
            let upstreams = load_upstream_identities(&write_txn)?;
            let events = write_txn.open_table(REQUEST_EVENTS_V1)?;
            for row in events.iter()? {
                let (key, value) = row?;
                let event_key = decode_request_event_key(key.value())?;
                if previous_checkpoint.is_some_and(|previous| event_key <= previous) {
                    continue;
                }
                let event: RequestEvent = serde_json::from_slice(value.value())?;
                add_event_deltas(&mut deltas, &event, &upstreams);
                processed_events += 1;
                checkpoint = Some(checkpoint.map_or(event_key, |current| current.max(event_key)));
            }
        }

        let mut updated_rollups = 0;
        if !deltas.is_empty() {
            let mut rollups = write_txn.open_table(USAGE_ROLLUPS_V2)?;
            for (key, delta) in deltas {
                let encoded_key = usage_rollup_key(&key);
                let mut rollup = match rollups.get(encoded_key.as_slice())? {
                    Some(stored) => decode_usage_rollup(stored.value())?,
                    None => empty_usage_rollup(&key),
                };
                rollup.upstream_name = key.upstream_name.clone();
                apply_delta(&mut rollup, delta);
                let payload = encode_usage_rollup(&rollup)?;
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
        let table = match read_txn.open_table(USAGE_ROLLUPS_V2) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut rollups = Vec::new();
        for row in table.iter()? {
            let (_, value) = row?;
            rollups.push(decode_usage_rollup(value.value())?);
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
        let table = match read_txn.open_table(USAGE_ROLLUPS_V2) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut rollups = Vec::new();
        for row in table.iter()? {
            let (_, value) = row?;
            let rollup = decode_usage_rollup(value.value())?;
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
    encoded.push(resolution_code(key.resolution));
    encoded.extend_from_slice(&key.bucket_start.to_be_bytes());
    encoded.extend_from_slice(key.principal.as_bytes());
    encoded.push(0xFF);
    encoded.extend_from_slice(key.upstream_id.as_bytes());
    encoded.push(0xFF);
    encoded.extend_from_slice(key.model.as_bytes());
    encoded
}

fn empty_usage_rollup(key: &UsageRollupKey) -> UsageRollup {
    UsageRollup {
        resolution: key.resolution,
        bucket_start: key.bucket_start,
        principal: key.principal.clone(),
        upstream_id: key.upstream_id,
        upstream_name: key.upstream_name.clone(),
        model: key.model.clone(),
        request_count: 0,
        input_tokens: 0,
        output_tokens: 0,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
        error_count: 0,
        latency_count: 0,
        latency_ms_sum: 0,
        latency_ms_min: None,
        latency_ms_max: None,
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
        virtual_cost_micros: 0,
    }
}

fn apply_delta(rollup: &mut UsageRollup, delta: UsageRollupDelta) {
    rollup.request_count += delta.request_count;
    rollup.input_tokens += delta.input_tokens;
    rollup.output_tokens += delta.output_tokens;
    rollup.cache_creation_input_tokens += delta.cache_creation_input_tokens;
    rollup.cache_read_input_tokens += delta.cache_read_input_tokens;
    rollup.error_count += delta.error_count;
    rollup.latency_count += delta.latency_count;
    rollup.latency_ms_sum += delta.latency_ms_sum;
    rollup.latency_ms_min = min_option(rollup.latency_ms_min, delta.latency_ms_min);
    rollup.latency_ms_max = max_option(rollup.latency_ms_max, delta.latency_ms_max);
    rollup.proxy_setup_ms_count += delta.proxy_setup_ms_count;
    rollup.proxy_setup_ms_sum += delta.proxy_setup_ms_sum;
    rollup.shape_ms_count += delta.shape_ms_count;
    rollup.shape_ms_sum += delta.shape_ms_sum;
    rollup.sign_ms_count += delta.sign_ms_count;
    rollup.sign_ms_sum += delta.sign_ms_sum;
    rollup.upstream_ttfb_ms_count += delta.upstream_ttfb_ms_count;
    rollup.upstream_ttfb_ms_sum += delta.upstream_ttfb_ms_sum;
    rollup.upstream_body_ms_count += delta.upstream_body_ms_count;
    rollup.upstream_body_ms_sum += delta.upstream_body_ms_sum;
    rollup.virtual_cost_micros += delta.virtual_cost_micros;
}

fn usage_rollup_key_from_event(
    resolution: UsageRollupResolution,
    event: &RequestEvent,
    upstreams: &HashMap<String, UpstreamIdentity>,
) -> UsageRollupKey {
    let upstream = resolve_upstream_identity(event, upstreams);
    UsageRollupKey {
        resolution,
        bucket_start: bucket_start(resolution, event_ts_ms(event) / 1000),
        principal: normalize_dimension(event.principal_id.as_deref()),
        upstream_id: upstream.id,
        upstream_name: normalize_dimension(Some(&upstream.name)),
        model: normalize_dimension(event.model.as_deref()),
    }
}

impl UsageRollupDelta {
    fn add_event(&mut self, event: &RequestEvent) {
        self.request_count += 1;
        self.input_tokens += event.input_tokens.unwrap_or(0);
        self.cache_creation_input_tokens += event.cache_creation_input_tokens.unwrap_or(0);
        self.cache_read_input_tokens += event.cache_read_input_tokens.unwrap_or(0);
        self.output_tokens += event.output_tokens.unwrap_or(0);
        if event.status >= 400 {
            self.error_count += 1;
        }
        self.latency_count += 1;
        self.latency_ms_sum += event.duration_ms;
        self.latency_ms_min = min_option(self.latency_ms_min, Some(event.duration_ms));
        self.latency_ms_max = max_option(self.latency_ms_max, Some(event.duration_ms));
        if let Some(value) = event.proxy_setup_ms {
            self.proxy_setup_ms_count += 1;
            self.proxy_setup_ms_sum += value;
        }
        if let Some(value) = event.shape_ms {
            self.shape_ms_count += 1;
            self.shape_ms_sum += value;
        }
        if let Some(value) = event.sign_ms {
            self.sign_ms_count += 1;
            self.sign_ms_sum += value;
        }
        if let Some(value) = event.upstream_ttfb_ms {
            self.upstream_ttfb_ms_count += 1;
            self.upstream_ttfb_ms_sum += value;
        }
        if let Some(value) = event.upstream_body_ms {
            self.upstream_body_ms_count += 1;
            self.upstream_body_ms_sum += value;
        }
        self.virtual_cost_micros += event.cost_usd_micros.unwrap_or(0).max(0) as u64;
    }
}

fn add_event_deltas(
    deltas: &mut BTreeMap<UsageRollupKey, UsageRollupDelta>,
    event: &RequestEvent,
    upstreams: &HashMap<String, UpstreamIdentity>,
) {
    for resolution in [UsageRollupResolution::Minute, UsageRollupResolution::Hour] {
        deltas
            .entry(usage_rollup_key_from_event(resolution, event, upstreams))
            .or_default()
            .add_event(event);
    }
}

fn load_upstream_identities(
    tx: &redb::WriteTransaction,
) -> Result<HashMap<String, UpstreamIdentity>, StorageError> {
    let table = tx.open_table(UPSTREAMS_V2)?;
    let mut upstreams = HashMap::new();
    for row in table.iter()? {
        let (_, value) = row?;
        let record: UpstreamRecord = serde_json::from_slice(value.value())?;
        if record.deleted_at_unix_secs.is_some() {
            continue;
        }
        let identity = UpstreamIdentity {
            id: record.id,
            name: record.name,
        };
        upstreams.insert(identity.id.to_string(), identity.clone());
        upstreams.insert(identity.name.clone(), identity);
    }
    Ok(upstreams)
}

fn resolve_upstream_identity(
    event: &RequestEvent,
    upstreams: &HashMap<String, UpstreamIdentity>,
) -> UpstreamIdentity {
    if let Some(upstream_id) = event.upstream_id {
        if let Some(name) = event.upstream_name.clone() {
            return UpstreamIdentity {
                id: upstream_id,
                name,
            };
        }
        if let Some(identity) = upstreams.get(&upstream_id.to_string()) {
            return identity.clone();
        }
        return UpstreamIdentity {
            id: upstream_id,
            name: event_upstream_name(event),
        };
    }
    let name = event_upstream_name(event);
    upstreams.get(&name).cloned().unwrap_or(UpstreamIdentity {
        id: Uuid::nil(),
        name,
    })
}

fn bucket_start(resolution: UsageRollupResolution, ts: u64) -> u64 {
    let width = match resolution {
        UsageRollupResolution::Minute => MINUTE_SECS,
        UsageRollupResolution::Hour => HOUR_SECS,
    };
    ts - (ts % width)
}

fn event_ts_ms(event: &RequestEvent) -> u64 {
    event.ts_ms.unwrap_or_else(|| event.ts.saturating_mul(1000))
}

fn event_upstream_name(event: &RequestEvent) -> String {
    event
        .upstream_name
        .as_deref()
        .map(ToOwned::to_owned)
        .or_else(|| event.upstream.map(upstream_dimension))
        .unwrap_or_else(|| UNKNOWN_DIMENSION.to_owned())
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
    }
    .to_owned()
}

fn resolution_code(resolution: UsageRollupResolution) -> u8 {
    match resolution {
        UsageRollupResolution::Minute => 0,
        UsageRollupResolution::Hour => 1,
    }
}

fn encode_usage_rollup(rollup: &UsageRollup) -> Result<Vec<u8>, StorageError> {
    Ok(bincode_serde::encode_to_vec(rollup, config::standard())?)
}

fn decode_usage_rollup(value: &[u8]) -> Result<UsageRollup, StorageError> {
    let (rollup, _) = bincode_serde::decode_from_slice(value, config::standard())?;
    Ok(rollup)
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
