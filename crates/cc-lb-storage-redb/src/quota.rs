use std::fmt::Write as _;

use redb::ReadableTable;
use serde::{Deserialize, Serialize};

use crate::{Storage, StorageError, QUOTAS_BY_PRINCIPAL_V1};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BucketKind {
    Requests,
    InputTokens,
    OutputTokens,
}

impl BucketKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::InputTokens => "input_tokens",
            Self::OutputTokens => "output_tokens",
        }
    }
}

pub fn quota_key(principal_id: &str, window_start: u64, kind: BucketKind) -> Vec<u8> {
    let kind = kind.as_str();
    let mut key = String::with_capacity(principal_id.len() + 1 + 20 + 1 + kind.len());
    key.push_str(principal_id);
    key.push(':');
    let _ = write!(&mut key, "{window_start}");
    key.push(':');
    key.push_str(kind);
    key.into_bytes()
}

impl Storage {
    pub fn incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        amount: u64,
    ) -> Result<u64, StorageError> {
        let key = quota_key(principal_id, window_start, kind);
        let write_txn = self.db.begin_write()?;
        let new_value = {
            let mut table = write_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
            let current = match table.get(key.as_slice())? {
                Some(stored) => decode_counter(key.as_slice(), stored.value())?,
                None => 0,
            };
            let next = current.checked_add(amount).ok_or_else(|| {
                StorageError::QuotaCounterOverflow(String::from_utf8_lossy(&key).into_owned())
            })?;
            let encoded = next.to_le_bytes();
            table.insert(key.as_slice(), encoded.as_slice())?;
            next
        };
        write_txn.commit()?;
        Ok(new_value)
    }

    pub fn try_incr_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        amount: u64,
        capacity: u64,
    ) -> Result<Option<u64>, StorageError> {
        let key = quota_key(principal_id, window_start, kind);
        let write_txn = self.db.begin_write()?;
        let new_value = {
            let mut table = write_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
            let current = match table.get(key.as_slice())? {
                Some(stored) => decode_counter(key.as_slice(), stored.value())?,
                None => 0,
            };
            let Some(next) = current.checked_add(amount) else {
                return Ok(None);
            };
            if next > capacity {
                return Ok(None);
            }
            let encoded = next.to_le_bytes();
            table.insert(key.as_slice(), encoded.as_slice())?;
            Some(next)
        };
        write_txn.commit()?;
        Ok(new_value)
    }

    pub fn get_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
    ) -> Result<u64, StorageError> {
        let key = quota_key(principal_id, window_start, kind);
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
        match table.get(key.as_slice())? {
            Some(stored) => decode_counter(key.as_slice(), stored.value()),
            None => Ok(0),
        }
    }

    pub fn adjust_quota(
        &self,
        principal_id: &str,
        window_start: u64,
        kind: BucketKind,
        delta: i64,
    ) -> Result<u64, StorageError> {
        let key = quota_key(principal_id, window_start, kind);
        let write_txn = self.db.begin_write()?;
        let new_value = {
            let mut table = write_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
            let current = match table.get(key.as_slice())? {
                Some(stored) => decode_counter(key.as_slice(), stored.value())?,
                None => 0,
            };
            let next = if delta >= 0 {
                current.checked_add(delta as u64).ok_or_else(|| {
                    StorageError::QuotaCounterOverflow(String::from_utf8_lossy(&key).into_owned())
                })?
            } else {
                current.checked_sub(delta.unsigned_abs()).ok_or_else(|| {
                    StorageError::QuotaCounterUnderflow(String::from_utf8_lossy(&key).into_owned())
                })?
            };
            let encoded = next.to_le_bytes();
            table.insert(key.as_slice(), encoded.as_slice())?;
            next
        };
        write_txn.commit()?;
        Ok(new_value)
    }

    pub fn sweep_old_quotas(&self, older_than_window_start: u64) -> Result<u64, StorageError> {
        let write_txn = self.db.begin_write()?;
        let keys_to_remove = {
            let table = write_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
            let mut keys = Vec::new();
            for row in table.iter()? {
                let (key, _) = row?;
                let key_bytes = key.value();
                let window_start = parse_quota_window_start(key_bytes)?;
                if window_start < older_than_window_start {
                    keys.push(key_bytes.to_vec());
                }
            }
            keys
        };

        let mut deleted = 0;
        {
            let mut table = write_txn.open_table(QUOTAS_BY_PRINCIPAL_V1)?;
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

fn decode_counter(key: &[u8], value: &[u8]) -> Result<u64, StorageError> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        StorageError::InvalidQuotaCounter(String::from_utf8_lossy(key).into_owned())
    })?;
    Ok(u64::from_le_bytes(bytes))
}

fn parse_quota_window_start(key: &[u8]) -> Result<u64, StorageError> {
    let text = std::str::from_utf8(key)?;
    let mut parts = text.rsplitn(3, ':');
    let kind = parts.next();
    let window_start = parts.next();

    if kind.is_none() {
        return Err(StorageError::InvalidQuotaKey(text.to_owned()));
    }

    window_start
        .ok_or_else(|| StorageError::InvalidQuotaKey(text.to_owned()))?
        .parse::<u64>()
        .map_err(|_| StorageError::InvalidQuotaKey(text.to_owned()))
}
