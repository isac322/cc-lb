use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use redb::{ReadableDatabase, ReadableTable};
use serde::{Deserialize, Serialize};

use crate::{Storage, StorageError, AUDIT_LOG_V1};

const AUDIT_SEQUENCE_SCALE: u64 = 1_000_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuditEntry {
    pub ts: u64,
    pub request_id: String,
    pub principal_id: String,
    pub route: String,
    pub upstream: String,
    pub model: Option<String>,
    pub status: u16,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub duration_ms: u64,
    pub agent_label: Option<String>,
    pub api_key_id: Option<String>,
    pub cost_usd_micros: Option<u64>,
    pub limit_violation: Option<String>,
    pub admin_action: Option<String>,
    pub actor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AuditEntryV0 {
    ts: u64,
    request_id: String,
    principal_id: String,
    route: String,
    upstream: String,
    model: Option<String>,
    status: u16,
    input_tokens: u64,
    output_tokens: u64,
    duration_ms: u64,
    agent_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AuditEntryV1 {
    ts: u64,
    request_id: String,
    principal_id: String,
    route: String,
    upstream: String,
    model: Option<String>,
    status: u16,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    duration_ms: u64,
    agent_label: Option<String>,
    api_key_id: Option<String>,
    cost_usd_micros: Option<u64>,
    limit_violation: Option<String>,
    admin_action: Option<String>,
    actor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum AuditEntryWire {
    V0(AuditEntryV0),
    V1(AuditEntryV1),
}

impl From<&AuditEntry> for AuditEntryV1 {
    fn from(value: &AuditEntry) -> Self {
        Self {
            ts: value.ts,
            request_id: value.request_id.clone(),
            principal_id: value.principal_id.clone(),
            route: value.route.clone(),
            upstream: value.upstream.clone(),
            model: value.model.clone(),
            status: value.status,
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            duration_ms: value.duration_ms,
            agent_label: value.agent_label.clone(),
            api_key_id: value.api_key_id.clone(),
            cost_usd_micros: value.cost_usd_micros,
            limit_violation: value.limit_violation.clone(),
            admin_action: value.admin_action.clone(),
            actor: value.actor.clone(),
        }
    }
}

impl From<AuditEntryV0> for AuditEntry {
    fn from(value: AuditEntryV0) -> Self {
        Self {
            ts: value.ts,
            request_id: value.request_id,
            principal_id: value.principal_id,
            route: value.route,
            upstream: value.upstream,
            model: value.model,
            status: value.status,
            input_tokens: Some(value.input_tokens),
            output_tokens: Some(value.output_tokens),
            duration_ms: value.duration_ms,
            agent_label: value.agent_label,
            api_key_id: None,
            cost_usd_micros: None,
            limit_violation: None,
            admin_action: None,
            actor: None,
        }
    }
}

impl From<AuditEntryV1> for AuditEntry {
    fn from(value: AuditEntryV1) -> Self {
        Self {
            ts: value.ts,
            request_id: value.request_id,
            principal_id: value.principal_id,
            route: value.route,
            upstream: value.upstream,
            model: value.model,
            status: value.status,
            input_tokens: value.input_tokens,
            output_tokens: value.output_tokens,
            duration_ms: value.duration_ms,
            agent_label: value.agent_label,
            api_key_id: value.api_key_id,
            cost_usd_micros: value.cost_usd_micros,
            limit_violation: value.limit_violation,
            admin_action: value.admin_action,
            actor: value.actor,
        }
    }
}

impl Storage {
    pub fn append_audit(&self, entry: &AuditEntry) -> Result<(), StorageError> {
        self.append_audit_entries(std::slice::from_ref(entry))
    }

    pub fn append_audit_entries(&self, entries: &[AuditEntry]) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }

        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(AUDIT_LOG_V1)?;
            let mut last_key = match table.last()? {
                Some((key, _)) => Some(decode_audit_key(key.value())?),
                None => None,
            };

            for entry in entries {
                let payload = encode_audit_entry(entry)?;
                let key = next_audit_key_after(last_key, entry.ts)?;
                let encoded_key = key.to_be_bytes();
                table.insert(encoded_key.as_slice(), payload.as_slice())?;
                last_key = Some(key);
            }
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
            let entry = decode_audit_entry(value.value())?;
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

    pub fn count_audit_entries(&self) -> Result<usize, StorageError> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(AUDIT_LOG_V1)?;
        let mut count = 0_usize;
        for row in table.iter()? {
            let _ = row?;
            count = count.saturating_add(1);
        }
        Ok(count)
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

    pub fn prune_audit_before(
        &self,
        cutoff_ts_x_1m: u64,
        batch_size: usize,
    ) -> Result<u64, StorageError> {
        if batch_size == 0 {
            return Ok(0);
        }

        let write_txn = self.db.begin_write()?;
        let keys_to_remove = {
            let table = write_txn.open_table(AUDIT_LOG_V1)?;
            let mut keys = Vec::new();
            for row in table.iter()? {
                let (key, _) = row?;
                let decoded_key = decode_audit_key(key.value())?;
                if decoded_key >= cutoff_ts_x_1m {
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

fn next_audit_key_after(last_key: Option<u64>, ts: u64) -> Result<u64, StorageError> {
    let base = audit_base_key(ts)?;
    match last_key {
        Some(last) if last >= base => last.checked_add(1).ok_or(StorageError::AuditKeyOverflow),
        _ => Ok(base),
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

fn encode_audit_entry(entry: &AuditEntry) -> Result<Vec<u8>, StorageError> {
    Ok(encode_to_vec(
        AuditEntryWire::V1(AuditEntryV1::from(entry)),
        standard().with_variable_int_encoding(),
    )?)
}

fn decode_audit_entry(value: &[u8]) -> Result<AuditEntry, StorageError> {
    match decode_from_slice::<AuditEntryWire, _>(value, standard().with_variable_int_encoding()) {
        Ok((AuditEntryWire::V0(entry), _)) => Ok(entry.into()),
        Ok((AuditEntryWire::V1(entry), _)) => Ok(entry.into()),
        Err(_) => Ok(serde_json::from_slice(value)?),
    }
}
