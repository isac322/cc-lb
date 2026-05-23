use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{AUDIT_LOG_V1, Storage, StorageError};

const AUDIT_SEQUENCE_SCALE: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub ts: u64,
    pub request_id: String,
    pub principal_id: String,
    pub route: String,
    pub upstream: String,
    pub model: Option<String>,
    pub status: u16,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub duration_ms: u64,
    pub agent_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}

impl Storage {
    pub fn append_audit(&self, entry: &AuditEntry) -> Result<(), StorageError> {
        let payload = serde_json::to_vec(entry)?;
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(AUDIT_LOG_V1)?;
            let key = next_audit_key(&table, entry.ts)?;
            let encoded_key = key.to_be_bytes();
            table.insert(encoded_key.as_slice(), payload.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn query_audit(
        &self,
        principal_id: Option<&str>,
        since: u64,
        until: u64,
        limit: usize,
    ) -> Result<Vec<AuditEntry>, StorageError> {
        if limit == 0 || until < since {
            return Ok(Vec::new());
        }

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(AUDIT_LOG_V1)?;
        let mut entries = Vec::new();

        for row in table.iter()? {
            let (_, value) = row?;
            let entry: AuditEntry = serde_json::from_slice(value.value())?;
            if entry.ts < since || entry.ts > until {
                continue;
            }
            if principal_id.is_some_and(|wanted| wanted != entry.principal_id) {
                continue;
            }
            entries.push(entry);
            if entries.len() >= limit {
                break;
            }
        }

        Ok(entries)
    }

    pub fn prune_audit(&self, older_than: u64) -> Result<u64, StorageError> {
        let threshold = audit_base_key(older_than)?;
        let write_txn = self.db.begin_write()?;
        let keys_to_remove = {
            let table = write_txn.open_table(AUDIT_LOG_V1)?;
            let mut keys = Vec::new();
            for row in table.iter()? {
                let (key, _) = row?;
                if decode_audit_key(key.value())? < threshold {
                    keys.push(key.value().to_vec());
                }
            }
            keys
        };

        let mut deleted = 0;
        {
            let mut table = write_txn.open_table(AUDIT_LOG_V1)?;
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

fn next_audit_key(table: &redb::Table<'_, &[u8], &[u8]>, ts: u64) -> Result<u64, StorageError> {
    let base = audit_base_key(ts)?;
    match table.last()? {
        Some((key, _)) => {
            let last = decode_audit_key(key.value())?;
            if last >= base {
                last.checked_add(1).ok_or(StorageError::AuditKeyOverflow)
            } else {
                Ok(base)
            }
        }
        None => Ok(base),
    }
}

fn audit_base_key(ts: u64) -> Result<u64, StorageError> {
    ts.checked_mul(AUDIT_SEQUENCE_SCALE)
        .ok_or(StorageError::AuditKeyOverflow)
}

fn decode_audit_key(key: &[u8]) -> Result<u64, StorageError> {
    let bytes: [u8; 8] = key.try_into().map_err(|_| StorageError::InvalidAuditKey)?;
    Ok(u64::from_be_bytes(bytes))
}
