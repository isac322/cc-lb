use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::{REQUEST_EVENTS_V1, RedbStorage, StorageError};

const REQUEST_EVENT_SEQUENCE_SCALE: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestEvent {
    pub ts: u64,
    pub request_id: String,
    pub principal_id: Option<String>,
    pub principal_kind: Option<String>,
    pub upstream: Option<RequestEventUpstream>,
    pub model: Option<String>,
    pub status: u16,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub duration_ms: u64,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestEventUpstream {
    AnthropicDirect,
    BedrockRuntime,
    BedrockMantle,
    Vertex,
    CustomAnthropicSpec,
}

impl RedbStorage {
    pub fn append_request_event(&self, event: &RequestEvent) -> Result<(), StorageError> {
        let payload = serde_json::to_vec(event)?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REQUEST_EVENTS_V1)?;
            let key = next_request_event_key(&table, event.ts)?;
            let encoded_key = key.to_be_bytes();
            table.insert(encoded_key.as_slice(), payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn query_request_events(
        &self,
        since: u64,
        until: u64,
        limit: usize,
    ) -> Result<Vec<RequestEvent>, StorageError> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(REQUEST_EVENTS_V1)?;
        let mut events = Vec::new();

        for row in table.iter()? {
            let (_, value) = row?;
            let event: RequestEvent = serde_json::from_slice(value.value())?;
            if event.ts < since || event.ts > until {
                continue;
            }
            events.push(event);
            if events.len() >= limit {
                break;
            }
        }

        Ok(events)
    }
}

fn next_request_event_key(
    table: &redb::Table<'_, &[u8], &[u8]>,
    ts: u64,
) -> Result<u64, StorageError> {
    let base = request_event_base_key(ts)?;
    match table.last()? {
        Some((key, _)) => {
            let last = decode_request_event_key(key.value())?;
            if last >= base {
                last.checked_add(1)
                    .ok_or(StorageError::RequestEventKeyOverflow)
            } else {
                Ok(base)
            }
        }
        None => Ok(base),
    }
}

fn request_event_base_key(ts: u64) -> Result<u64, StorageError> {
    ts.checked_mul(REQUEST_EVENT_SEQUENCE_SCALE)
        .ok_or(StorageError::RequestEventKeyOverflow)
}

fn decode_request_event_key(key: &[u8]) -> Result<u64, StorageError> {
    let bytes: [u8; 8] = key
        .try_into()
        .map_err(|_| StorageError::InvalidRequestEventKey)?;
    Ok(u64::from_be_bytes(bytes))
}
