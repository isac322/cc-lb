use cc_lb_storage_api::types::RequestEvent;
use redb::{ReadableDatabase, ReadableTable};

use crate::{REQUEST_EVENTS_V1, Storage, StorageError};

const REQUEST_EVENT_SEQUENCE_SCALE: u64 = 1_000_000;

impl Storage {
    pub fn append_request_event(&self, event: &RequestEvent) -> Result<(), StorageError> {
        let payload = serde_json::to_vec(event)?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(REQUEST_EVENTS_V1)?;
            let key = next_request_event_key(&table, event_ts_ms(event))?;
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
            let event_ts_ms = event_ts_ms(&event);
            if event_ts_ms < since || event_ts_ms > until {
                continue;
            }
            events.push(event);
            if events.len() >= limit {
                break;
            }
        }

        Ok(events)
    }

    pub fn query_recent_request_events(
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

        for row in table.iter()?.rev() {
            let (_, value) = row?;
            let event: RequestEvent = serde_json::from_slice(value.value())?;
            let event_ts_ms = event_ts_ms(&event);
            if event_ts_ms < since || event_ts_ms > until {
                continue;
            }
            events.push(event);
            if events.len() >= limit {
                break;
            }
        }

        Ok(events)
    }

    pub fn prune_request_events_before(
        &self,
        cutoff_ms_x_1m: u64,
        batch_size: usize,
    ) -> Result<u64, StorageError> {
        if batch_size == 0 {
            return Ok(0);
        }

        let write_txn = self.db.begin_write()?;
        let keys_to_remove = {
            let table = write_txn.open_table(REQUEST_EVENTS_V1)?;
            let mut keys = Vec::new();
            for row in table.iter()? {
                let (key, _) = row?;
                let decoded_key = decode_request_event_key(key.value())?;
                if decoded_key >= cutoff_ms_x_1m {
                    break;
                }
                keys.push(key.value().to_vec());
                if keys.len() >= batch_size {
                    break;
                }
            }
            keys
        };

        let mut deleted = 0;
        {
            let mut table = write_txn.open_table(REQUEST_EVENTS_V1)?;
            for key in keys_to_remove {
                if table.remove(key.as_slice())?.is_some() {
                    deleted += 1;
                }
            }
        }
        write_txn.commit()?;
        Ok(deleted)
    }
}

fn event_ts_ms(event: &RequestEvent) -> u64 {
    event.ts_ms.unwrap_or_else(|| event.ts.saturating_mul(1000))
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
